package sftp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/sftp/sftptest"
)

func TestNewValidatesArguments(t *testing.T) {
	if _, err := New(nil, "store", Options{}); err == nil {
		t.Fatal("New accepted a nil client")
	}
	server := sftptest.NewServer(t)
	if _, err := New(server.Client(t), "", Options{}); err == nil {
		t.Fatal("New accepted an empty root")
	}
	store, err := New(server.Client(t), "nested/root/", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if store.Root() != "nested/root" {
		t.Fatalf("Root = %q", store.Root())
	}
	if info, err := os.Stat(filepath.Join(server.Dir, "nested", "root")); err != nil || !info.IsDir() {
		t.Fatalf("root directory not created: %v", err)
	}
}

func TestExtensionDetection(t *testing.T) {
	if store := newTestStore(t, sftptest.Options{}, Options{}); !store.AtomicReplace() || !store.hardlink {
		t.Fatalf("default server: AtomicReplace=%v hardlink=%v, want both", store.AtomicReplace(), store.hardlink)
	}
	if store := newTestStore(t, sftptest.Options{DisableExtensions: true}, Options{}); store.AtomicReplace() || store.hardlink {
		t.Fatalf("server without extensions: AtomicReplace=%v hardlink=%v, want neither", store.AtomicReplace(), store.hardlink)
	}
}

func TestKeyEscaping(t *testing.T) {
	server := sftptest.NewServer(t)
	store, err := New(server.Client(t), "store", Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	keys := []objectstore.Key{"@@twice", "@object.json", "a", "a/@data-x", "a/b", "x/@y/z"} // key order
	for _, key := range keys {
		if _, err := store.Put(ctx, "objects", key, bytes.NewReader([]byte(key)), objectstore.PutOptions{}); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}
	page, err := store.List(ctx, "objects", objectstore.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var listed []objectstore.Key
	for _, object := range page.Objects {
		listed = append(listed, object.Key)
	}
	if !reflect.DeepEqual(listed, keys) {
		t.Fatalf("listed keys = %q, want %q", listed, keys)
	}
	for _, key := range keys {
		body, _, err := store.Get(ctx, "objects", key, objectstore.GetOptions{})
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil || string(data) != string(key) {
			t.Fatalf("Get %q = (%q, %v)", key, data, err)
		}
	}
	objects := filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")), "objects")
	for _, name := range []string{"@@object.json/@object.json", "@@@twice/@object.json", "a/@@data-x/@object.json", "x/@@y/z/@object.json"} {
		if _, err := os.Stat(filepath.Join(objects, filepath.FromSlash(name))); err != nil {
			t.Fatalf("escaped path %s: %v", name, err)
		}
	}
	for _, key := range []objectstore.Key{"a//b", "./a", "a/.", "a/", "../a", "/a", `a\b`} {
		if _, err := store.Put(ctx, "objects", key, bytes.NewReader(nil), objectstore.PutOptions{}); !errors.Is(err, objectstore.ErrInvalidKey) {
			t.Fatalf("Put %q error = %v, want ErrInvalidKey", key, err)
		}
	}
}

// TestFallbackPublicationCrashState simulates a crash between the two
// renames of a fallback publication: only @object.previous.json exists.
func TestFallbackPublicationCrashState(t *testing.T) {
	server := sftptest.NewServerWithOptions(t, sftptest.Options{DisableExtensions: true, StrictRename: true})
	store, err := New(server.Client(t), "store", Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.Put(ctx, "objects", "key", bytes.NewReader([]byte("first")), objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")), "objects", "key")
	if err := os.Rename(filepath.Join(directory, metadataName), filepath.Join(directory, previousName)); err != nil {
		t.Fatal(err)
	}
	stat, err := store.Stat(ctx, "objects", "key")
	if err != nil || !reflect.DeepEqual(stat, first) {
		t.Fatalf("Stat in crash state = (%#v, %v), want %#v", stat, err, first)
	}
	second, err := store.Put(ctx, "objects", "key", bytes.NewReader([]byte("second")), objectstore.PutOptions{IfMatchVersion: first.Version})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, previousName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("previous metadata after publication: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, dataPrefix+first.Version)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replaced data file still present: %v", err)
	}
	if err := os.Rename(filepath.Join(directory, metadataName), filepath.Join(directory, previousName)); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "objects", "key", second.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(ctx, "objects", "key"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Stat after Delete error = %v, want ErrNotFound", err)
	}
}

func TestCopyHardLinksWhenSupported(t *testing.T) {
	for _, test := range []struct {
		name    string
		options sftptest.Options
		linked  bool
	}{
		{name: "hardlink", linked: true},
		{name: "stream", options: sftptest.Options{DisableExtensions: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := sftptest.NewServerWithOptions(t, test.options)
			store, err := New(server.Client(t), "store", Options{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			source, err := store.Put(ctx, "objects", "source", bytes.NewReader([]byte("payload")), objectstore.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			copied, err := store.Copy(ctx, "objects", "source", "objects-copy", "copy", objectstore.CopyOptions{})
			if err != nil {
				t.Fatal(err)
			}
			sourceInfo, err := os.Stat(filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")), "objects", "source", dataPrefix+source.Version))
			if err != nil {
				t.Fatal(err)
			}
			copyInfo, err := os.Stat(filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects-copy")), "objects", "copy", dataPrefix+copied.Version))
			if err != nil {
				t.Fatal(err)
			}
			if linked := os.SameFile(sourceInfo, copyInfo); linked != test.linked {
				t.Fatalf("copy shares the source file = %v, want %v", linked, test.linked)
			}
		})
	}
}

// TestCompareAndSwapAcrossStores increments one counter through two Store
// values on separate SSH connections, which coordinate only through the
// remote lock file, as separate processes would.
func TestCompareAndSwapAcrossStores(t *testing.T) {
	for _, test := range []struct {
		name    string
		options sftptest.Options
	}{
		{name: "posix-rename"},
		{name: "fallback", options: sftptest.Options{DisableExtensions: true, StrictRename: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const (
				workers    = 3
				increments = 5
			)
			server := sftptest.NewServerWithOptions(t, test.options)
			ctx := context.Background()
			stores := make([]*Store, 2)
			for index := range stores {
				store, err := New(server.Client(t), "store", Options{LockPollInterval: 5 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				stores[index] = store
			}
			if _, err := stores[0].Put(ctx, "objects", "counter", bytes.NewReader([]byte("0")), objectstore.PutOptions{IfAbsent: true}); err != nil {
				t.Fatal(err)
			}
			var wait sync.WaitGroup
			failures := make(chan error, 2*workers)
			for _, store := range stores {
				for range workers {
					wait.Add(1)
					go func() {
						defer wait.Done()
						for done := 0; done < increments; {
							body, object, err := store.Get(ctx, "objects", "counter", objectstore.GetOptions{})
							if err != nil {
								failures <- err
								return
							}
							data, err := io.ReadAll(body)
							_ = body.Close()
							if err != nil {
								failures <- err
								return
							}
							value, err := strconv.Atoi(string(data))
							if err != nil {
								failures <- err
								return
							}
							_, err = store.Put(ctx, "objects", "counter", bytes.NewReader([]byte(strconv.Itoa(value+1))), objectstore.PutOptions{IfMatchVersion: object.Version})
							switch {
							case err == nil:
								done++
							case !errors.Is(err, objectstore.ErrPrecondition):
								failures <- err
								return
							}
						}
					}()
				}
			}
			wait.Wait()
			close(failures)
			for err := range failures {
				t.Fatal(err)
			}
			body, _, err := stores[1].Get(ctx, "objects", "counter", objectstore.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(body)
			_ = body.Close()
			if string(data) != strconv.Itoa(2*workers*increments) {
				t.Fatalf("counter = %q, want %d", data, 2*workers*increments)
			}
			if _, err := os.Stat(filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")), lockName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("lock file left behind: %v", err)
			}
		})
	}
}

func TestStaleLockFileIsBroken(t *testing.T) {
	const staleAge = 300 * time.Millisecond
	server := sftptest.NewServer(t)
	store, err := New(server.Client(t), "store", Options{StaleLockAge: staleAge, LockPollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	bucketDirectory := filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")))
	if err := os.MkdirAll(bucketDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bucketDirectory, lockName), lockRecord("deadbeefdeadbeefdeadbeefdeadbeef", 3), 0o644); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := store.Put(context.Background(), "objects", "key", bytes.NewReader([]byte("value")), objectstore.PutOptions{IfAbsent: true}); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited < staleAge {
		t.Fatalf("Put broke a lock after %v, before it was stale for %v", waited, staleAge)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := os.WriteFile(filepath.Join(bucketDirectory, lockName), lockRecord("deadbeefdeadbeefdeadbeefdeadbeef", 4), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "objects", "key", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Delete waiting for a held lock error = %v, want DeadlineExceeded", err)
	}
}

func TestGetBodyIsBoundToContext(t *testing.T) {
	store := newTestStore(t, sftptest.Options{}, Options{})
	if _, err := store.Put(context.Background(), "objects", "key", bytes.NewReader(make([]byte, 1<<20)), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	body, _, err := store.Get(ctx, "objects", "key", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 1, Length: 1 << 19}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	if _, err := body.Read(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := io.ReadAll(body); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancel error = %v, want context.Canceled", err)
	}
}

func TestPutPartReplacesAndAbortCleansUp(t *testing.T) {
	server := sftptest.NewServerWithOptions(t, sftptest.Options{DisableExtensions: true, StrictRename: true})
	store, err := New(server.Client(t), "store", Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "objects", "key", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{[]byte("first"), []byte("replacement")} {
		if _, err := store.PutPart(ctx, upload, 1, bytes.NewReader(data), int64(len(data)), sha256.Sum256(data)); err != nil {
			t.Fatal(err)
		}
	}
	part, err := os.ReadFile(filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")), "uploads", upload.ID, partPrefix+"1"))
	if err != nil || string(part) != "replacement" {
		t.Fatalf("part file = (%q, %v)", part, err)
	}
	if err := store.AbortMultipart(ctx, upload); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(server.Dir, "store", "b-"+hex.EncodeToString([]byte("objects")), "uploads", upload.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upload directory after abort: %v", err)
	}
	if err := store.AbortMultipart(ctx, upload); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("second abort error = %v, want ErrNotFound", err)
	}
}
