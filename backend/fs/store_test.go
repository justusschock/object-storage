package fs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/fs"
	"github.com/justusschock/go-object-storage/internal/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		return newStore(t)
	})
}

func TestListOrdersNestedAndEscapedKeys(t *testing.T) {
	store := newStore(t)
	keys := []objectstore.Key{"a", "a/b", "a-c", "a/b/c", "a0", "@x", "@@y/z", "b/@object.json", "b/@data-1", "a/.hidden", "ü/对象"}
	for _, key := range keys {
		if _, err := store.Put(context.Background(), "objects", key, strings.NewReader(string(key)), objectstore.PutOptions{}); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}
	want := append([]objectstore.Key(nil), keys...)
	sort.Slice(want, func(left, right int) bool { return want[left] < want[right] })

	var got []objectstore.Key
	after := objectstore.Key("")
	for {
		page, err := store.List(context.Background(), "objects", objectstore.ListOptions{After: after, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range page.Objects {
			got = append(got, object.Key)
			body, _, err := store.Get(context.Background(), "objects", object.Key, objectstore.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(body)
			_ = body.Close()
			if string(data) != string(object.Key) {
				t.Fatalf("key %q holds %q", object.Key, data)
			}
		}
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged keys = %q, want %q", got, want)
	}

	for prefix, expected := range map[string][]objectstore.Key{
		"a/":  {"a/.hidden", "a/b", "a/b/c"},
		"a/b": {"a/b", "a/b/c"},
		"@":   {"@@y/z", "@x"},
		"b/@": {"b/@data-1", "b/@object.json"},
		"zz":  nil,
	} {
		page, err := store.List(context.Background(), "objects", objectstore.ListOptions{Prefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		var listed []objectstore.Key
		for _, object := range page.Objects {
			listed = append(listed, object.Key)
		}
		if !reflect.DeepEqual(listed, expected) {
			t.Fatalf("List prefix %q = %q, want %q", prefix, listed, expected)
		}
	}
}

func TestRejectsUnrepresentableKeys(t *testing.T) {
	store := newStore(t)
	for _, key := range []objectstore.Key{"a//b", "a/./b", "a/b/", "."} {
		if _, err := store.Put(context.Background(), "objects", key, strings.NewReader("x"), objectstore.PutOptions{}); !errors.Is(err, objectstore.ErrInvalidKey) {
			t.Fatalf("Put %q error = %v, want ErrInvalidKey", key, err)
		}
	}
}

func TestDeletePrunesDirectoriesAndKeepsSiblings(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	for _, key := range []objectstore.Key{"deep/a/b/c", "deep/a"} {
		if _, err := store.Put(ctx, "objects", key, strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Delete(ctx, "objects", "deep/a/b/c", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(ctx, "objects", "deep/a"); err != nil {
		t.Fatalf("sibling object lost: %v", err)
	}
	if err := store.Delete(ctx, "objects", "deep/a", ""); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(store.Root()))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		objects, err := os.ReadDir(filepath.Join(store.Root(), entry.Name(), "objects"))
		if err == nil && len(objects) != 0 {
			t.Fatalf("delete left directories behind: %v", objects)
		}
	}
	if err := store.Delete(ctx, "objects", "deep/a", ""); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Delete absent error = %v, want ErrNotFound", err)
	}
}

func TestCopyHardLinksImmutableData(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	if _, err := store.Put(ctx, "objects", "source", strings.NewReader("linked"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Copy(ctx, "objects", "source", "objects", "copy", objectstore.CopyOptions{}); err != nil {
		t.Fatal(err)
	}
	// Replacing the source must not change the copy even though they shared
	// one inode.
	if _, err := store.Put(ctx, "objects", "source", strings.NewReader("replaced"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	body, object, err := store.Get(ctx, "objects", "copy", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	data, _ := io.ReadAll(body)
	if string(data) != "linked" || object.SHA256 != sha256.Sum256([]byte("linked")) {
		t.Fatalf("copy = %q, %#v", data, object)
	}
}

func TestReaderSurvivesConcurrentReplacement(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	if _, err := store.Put(ctx, "objects", "key", strings.NewReader("first"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	body, object, err := store.Get(ctx, "objects", "key", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "objects", "key", strings.NewReader("second"), objectstore.PutOptions{IfMatchVersion: object.Version}); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(data) != "first" {
		t.Fatalf("open reader after replacement = (%q, %v), want first", data, err)
	}
}

func TestStagingFilesAreRemoved(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	wrong := sha256.Sum256([]byte("other"))
	_, _ = store.Put(ctx, "objects", "rejected", strings.NewReader("value"), objectstore.PutOptions{SHA256: &wrong})
	if _, err := store.Put(ctx, "objects", "accepted", strings.NewReader("value"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "objects", "accepted", strings.NewReader("value"), objectstore.PutOptions{IfAbsent: true}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("IfAbsent error = %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(store.Root(), "*", "tmp", "*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("staging files left behind: %v (%v)", matches, err)
	}
}

const (
	helperEnvironment = "GO_OBJECT_STORAGE_FS_HELPER"
	helperProcesses   = 4
	helperIncrements  = 15
)

// TestCrossProcessConditionalWrites runs several OS processes against one
// root. Exactly one IfAbsent writer may win, and IfMatchVersion increments
// must never be lost, which requires the file lock to exclude processes.
func TestCrossProcessConditionalWrites(t *testing.T) {
	if os.Getenv(helperEnvironment) != "" {
		t.Skip("helper process")
	}
	root := t.TempDir()
	store, err := fs.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), "objects", "counter", strings.NewReader("0"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	commands := make([]*exec.Cmd, helperProcesses)
	outputs := make([]*bytes.Buffer, helperProcesses)
	for index := range commands {
		command := exec.Command(os.Args[0], "-test.run=^TestCrossProcessHelper$", "-test.count=1")
		command.Env = append(os.Environ(), helperEnvironment+"="+root, "GO_OBJECT_STORAGE_FS_HELPER_ID="+strconv.Itoa(index))
		outputs[index] = &bytes.Buffer{}
		command.Stdout, command.Stderr = outputs[index], outputs[index]
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands[index] = command
	}
	for index, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("helper %d failed: %v\n%s", index, err, outputs[index])
		}
	}
	winners := 0
	for _, output := range outputs {
		winners += strings.Count(output.String(), "IF-ABSENT-WINNER")
	}
	if winners != 1 {
		t.Fatalf("IfAbsent winners across processes = %d, want 1", winners)
	}
	body, _, err := store.Get(context.Background(), "objects", "counter", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if string(data) != strconv.Itoa(helperProcesses*helperIncrements) {
		t.Fatalf("cross-process counter = %s, want %d", data, helperProcesses*helperIncrements)
	}
}

func TestCrossProcessHelper(t *testing.T) {
	root := os.Getenv(helperEnvironment)
	if root == "" {
		t.Skip("run by TestCrossProcessConditionalWrites")
	}
	store, err := fs.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := os.Getenv("GO_OBJECT_STORAGE_FS_HELPER_ID")
	if _, err := store.Put(ctx, "objects", "winner", strings.NewReader(id), objectstore.PutOptions{IfAbsent: true}); err == nil {
		fmt.Println("IF-ABSENT-WINNER")
	} else if !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatal(err)
	}
	for done := 0; done < helperIncrements; {
		body, object, err := store.Get(ctx, "objects", "counter", objectstore.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			t.Fatal(err)
		}
		value, err := strconv.Atoi(string(data))
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.Put(ctx, "objects", "counter", strings.NewReader(strconv.Itoa(value+1)), objectstore.PutOptions{IfMatchVersion: object.Version})
		switch {
		case err == nil:
			done++
		case errors.Is(err, objectstore.ErrPrecondition):
		default:
			t.Fatal(err)
		}
	}
}

func newStore(t *testing.T) *fs.Store {
	t.Helper()
	store, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}
