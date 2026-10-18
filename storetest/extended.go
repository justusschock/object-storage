package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"reflect"
	"strconv"
	"sync"
	"testing"

	objectstore "github.com/justusschock/object-storage"
)

// StreamingSize is the body length used by the streaming-put case. It spans
// more than two minimum-size (5 MiB) parts so that backends with small
// configured part sizes exercise their multipart paths.
const StreamingSize = 11<<20 + 3

func testIfMatchVersion(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.Put(ctx, "objects", "cas", bytes.NewReader([]byte("absent")), objectstore.PutOptions{IfMatchVersion: "missing"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("IfMatchVersion on absent key error = %v, want ErrPrecondition", err)
	}
	first := putObject(t, store, "objects", "cas", []byte("first"), objectstore.PutOptions{IfAbsent: true})
	if _, err := store.Put(ctx, "objects", "cas", bytes.NewReader([]byte("wrong")), objectstore.PutOptions{IfMatchVersion: first.Version + "-stale"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("stale IfMatchVersion error = %v, want ErrPrecondition", err)
	}
	second := putObject(t, store, "objects", "cas", []byte("second"), objectstore.PutOptions{IfMatchVersion: first.Version})
	if second.Version == first.Version {
		t.Fatalf("replacement kept version %q", second.Version)
	}
	if _, err := store.Put(ctx, "objects", "cas", bytes.NewReader([]byte("third")), objectstore.PutOptions{IfMatchVersion: first.Version}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("replayed IfMatchVersion error = %v, want ErrPrecondition", err)
	}
	if data := readObject(t, store, "objects", "cas"); string(data) != "second" {
		t.Fatalf("object after CAS = %q, want second", data)
	}
}

// testConcurrentCompareAndSwap increments one counter object from many
// goroutines using only Get and Put(IfMatchVersion). Lost updates show up as
// a final count below the number of successful increments.
func testConcurrentCompareAndSwap(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	const (
		workers    = 6
		increments = 5
	)
	ctx := context.Background()
	putObject(t, store, "objects", "counter", []byte("0"), objectstore.PutOptions{IfAbsent: true})
	start := make(chan struct{})
	failures := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for done := 0; done < increments; {
				body, object, err := store.Get(ctx, "objects", "counter", objectstore.GetOptions{})
				if err != nil {
					failures <- err
					return
				}
				data, err := io.ReadAll(body)
				if closeErr := body.Close(); err == nil {
					err = closeErr
				}
				if err != nil {
					failures <- err
					return
				}
				value, err := strconv.Atoi(string(data))
				if err != nil {
					failures <- err
					return
				}
				next := []byte(strconv.Itoa(value + 1))
				_, err = store.Put(ctx, "objects", "counter", bytes.NewReader(next), objectstore.PutOptions{IfMatchVersion: object.Version})
				switch {
				case err == nil:
					done++
				case errors.Is(err, objectstore.ErrPrecondition):
				default:
					failures <- err
					return
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("CAS worker error = %v", err)
	}
	if data := readObject(t, store, "objects", "counter"); string(data) != strconv.Itoa(workers*increments) {
		t.Fatalf("counter = %q, want %d (lost compare-and-swap updates)", data, workers*increments)
	}
}

func testSizeOption(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	ctx := context.Background()
	for _, size := range []int64{3, 6} {
		if _, err := store.Put(ctx, "objects", "sized", onlyReader{bytes.NewReader([]byte("value"))}, objectstore.PutOptions{Size: size}); !errors.Is(err, objectstore.ErrPrecondition) {
			t.Fatalf("Put with Size %d for 5 bytes error = %v, want ErrPrecondition", size, err)
		}
	}
	if _, err := store.Stat(ctx, "objects", "sized"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("size mismatch stored object: %v", err)
	}
	if _, err := store.Put(ctx, "objects", "sized", bytes.NewReader([]byte("value")), objectstore.PutOptions{Size: -1}); err == nil {
		t.Fatal("Put accepted a negative Size")
	}
	object := putObject(t, store, "objects", "sized", []byte("value"), objectstore.PutOptions{Size: 5})
	if object.Size != 5 {
		t.Fatalf("sized object = %#v", object)
	}
}

func testStreamingPut(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	ctx := context.Background()
	data := patterned(StreamingSize)
	digest := sha256.Sum256(data)
	wrong := sha256.Sum256([]byte("wrong"))
	for _, test := range []struct {
		key     objectstore.Key
		options objectstore.PutOptions
	}{
		{key: "stream/unknown-digest-unknown-size"},
		{key: "stream/known-digest-unknown-size", options: objectstore.PutOptions{SHA256: &digest}},
		{key: "stream/known-digest-known-size", options: objectstore.PutOptions{SHA256: &digest, Size: int64(len(data)), IfAbsent: true}},
		{key: "stream/unknown-digest-known-size", options: objectstore.PutOptions{Size: int64(len(data))}},
	} {
		object, err := store.Put(ctx, "objects", test.key, onlyReader{bytes.NewReader(data)}, test.options)
		if err != nil {
			t.Fatalf("streaming Put %s: %v", test.key, err)
		}
		if object.Size != int64(len(data)) || !object.SHA256Known || object.SHA256 != digest {
			t.Fatalf("streaming Put %s object = %#v", test.key, object)
		}
		if got := sha256.Sum256(readObject(t, store, "objects", test.key)); got != digest {
			t.Fatalf("streaming Put %s stored different bytes", test.key)
		}
	}
	if _, err := store.Put(ctx, "objects", "stream/wrong", onlyReader{bytes.NewReader(data)}, objectstore.PutOptions{SHA256: &wrong}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("streaming Put with wrong digest error = %v, want ErrPrecondition", err)
	}
	if _, err := store.Stat(ctx, "objects", "stream/wrong"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("streaming digest mismatch published object: %v", err)
	}
}

func testListMetadata(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	ctx := context.Background()
	putObject(t, store, "objects", "listed/a", []byte("alpha"), objectstore.PutOptions{Metadata: map[string]string{"name": "a"}})
	putObject(t, store, "objects", "listed/b", []byte("beta"), objectstore.PutOptions{Metadata: map[string]string{"name": "b"}})
	stats := map[objectstore.Key]objectstore.Object{}
	for _, key := range []objectstore.Key{"listed/a", "listed/b"} {
		object, err := store.Stat(ctx, "objects", key)
		if err != nil {
			t.Fatal(err)
		}
		stats[key] = object
	}
	cheap, err := store.List(ctx, "objects", objectstore.ListOptions{Prefix: "listed/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cheap.Objects) != 2 {
		t.Fatalf("listed %d objects, want 2", len(cheap.Objects))
	}
	for _, object := range cheap.Objects {
		want := stats[object.Key]
		if object.Bucket != "objects" || object.Version != want.Version || object.ETag != want.ETag || object.Size != want.Size || object.ModifiedAt.IsZero() {
			t.Fatalf("listed object = %#v, want summary of %#v", object, want)
		}
		if object.SHA256Known && object.SHA256 != want.SHA256 {
			t.Fatalf("listed object claims digest %x, want %x", object.SHA256, want.SHA256)
		}
	}
	full, err := store.List(ctx, "objects", objectstore.ListOptions{Prefix: "listed/", IncludeMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range full.Objects {
		want := stats[object.Key]
		if !reflect.DeepEqual(object.Metadata, want.Metadata) || object.SHA256Known != want.SHA256Known || object.SHA256 != want.SHA256 || object.Size != want.Size {
			t.Fatalf("listed object with metadata = %#v, want %#v", object, want)
		}
	}
}

// onlyReader hides every optional interface (Len, Seek, WriterTo) of the
// wrapped reader so that backends see an unsized stream.
type onlyReader struct{ reader io.Reader }

func (reader onlyReader) Read(buffer []byte) (int, error) { return reader.reader.Read(buffer) }

func patterned(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index*7 + index/4096)
	}
	return data
}

func readObject(t *testing.T, store objectstore.Store, bucket objectstore.Bucket, key objectstore.Key) []byte {
	t.Helper()
	body, _, err := store.Get(context.Background(), bucket, key, objectstore.GetOptions{})
	if err != nil {
		t.Fatalf("Get %s/%s: %v", bucket, key, err)
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("read %s/%s: %v", bucket, key, err)
	}
	return data
}
