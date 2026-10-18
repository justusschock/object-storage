// Package storetest is the conformance suite for implementations of
// objectstore.Store and objectstore.MultipartStore.
//
// Run exercises the complete Store and MultipartStore contract (immutable
// versions, exact ranges, pagination, conditional writes and their
// concurrency, digest and size verification, multipart ordering and abort,
// cancellation, defensive copies, streaming bodies) and then every optional
// capability (BatchStater, BatchDeleter, Copier, Presigner) the store
// implements. Every backend in this repository passes it; third-party
// backends should run it from their own tests:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
//			return newEmptyStore(t)
//		})
//	}
//
// The suite calls t.Run for each case and t.Fatal on violations; it never
// skips. Cases are not run in parallel with each other.
package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	objectstore "github.com/justusschock/object-storage"
)

// Factory returns a fresh empty store for one conformance case.
type Factory func(*testing.T) objectstore.MultipartStore

// Run verifies the complete Store and MultipartStore contract, then every
// optional capability (BatchStater, BatchDeleter, Copier, Presigner) that the
// factory's stores implement. Stores must accept the buckets "objects" and
// "objects-copy".
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("put get and stat", func(t *testing.T) { testPutGetStat(t, factory(t)) })
	t.Run("exact range", func(t *testing.T) { testExactRange(t, factory(t)) })
	t.Run("list pagination", func(t *testing.T) { testListPagination(t, factory(t)) })
	t.Run("delete version precondition", func(t *testing.T) { testDeleteVersion(t, factory(t)) })
	t.Run("if absent race", func(t *testing.T) { testIfAbsentRace(t, factory(t)) })
	t.Run("checksum rejection", func(t *testing.T) { testChecksumRejection(t, factory(t)) })
	t.Run("multipart ordering and completion", func(t *testing.T) { testMultipartCompletion(t, factory(t)) })
	t.Run("multipart abort", func(t *testing.T) { testMultipartAbort(t, factory(t)) })
	t.Run("context cancellation", func(t *testing.T) { testContextCancellation(t, factory(t)) })
	t.Run("defensive metadata copies", func(t *testing.T) { testDefensiveCopies(t, factory(t)) })
	t.Run("if match version", func(t *testing.T) { testIfMatchVersion(t, factory(t)) })
	t.Run("concurrent compare and swap", func(t *testing.T) { testConcurrentCompareAndSwap(t, factory(t)) })
	t.Run("size option", func(t *testing.T) { testSizeOption(t, factory(t)) })
	t.Run("streaming put", func(t *testing.T) { testStreamingPut(t, factory(t)) })
	t.Run("list metadata", func(t *testing.T) { testListMetadata(t, factory(t)) })
	runCapabilities(t, factory)
}

func testPutGetStat(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	input := []byte("immutable object")
	metadata := map[string]string{"owner": "caller"}
	object, err := store.Put(context.Background(), "objects", "path/value", bytes.NewReader(input), objectstore.PutOptions{
		ContentType: "text/plain",
		Metadata:    metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata["owner"] = "mutated"
	wantDigest := sha256.Sum256(input)
	if object.Bucket != "objects" || object.Key != "path/value" || object.Version == "" || object.ETag == "" ||
		object.Size != int64(len(input)) || object.SHA256 != wantDigest || !object.SHA256Known || object.ModifiedAt.IsZero() || object.Metadata["owner"] != "caller" {
		t.Fatalf("Put object = %#v", object)
	}

	body, got, err := store.Get(context.Background(), "objects", "path/value", objectstore.GetOptions{IfMatchVersion: object.Version})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(data, input) || !reflect.DeepEqual(got, object) {
		t.Fatalf("Get = (%q, %#v, %v, %v), want (%q, %#v)", data, got, readErr, closeErr, input, object)
	}
	stat, err := store.Stat(context.Background(), "objects", "path/value")
	if err != nil || !reflect.DeepEqual(stat, object) {
		t.Fatalf("Stat = (%#v, %v), want %#v", stat, err, object)
	}
	if _, _, err := store.Get(context.Background(), "objects", "path/value", objectstore.GetOptions{IfMatchVersion: "wrong"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Get wrong version error = %v, want ErrPrecondition", err)
	}
}

func testExactRange(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	putObject(t, store, "objects", "range", []byte("abcdefghij"), objectstore.PutOptions{})
	body, object, err := store.Get(context.Background(), "objects", "range", objectstore.GetOptions{
		Range: &objectstore.ByteRange{Offset: 2, Length: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(data) != "cdef" || object.Size != 10 {
		t.Fatalf("ranged Get = (%q, %#v, %v)", data, object, err)
	}
	for _, byteRange := range []objectstore.ByteRange{{Offset: 10, Length: 1}, {Offset: 8, Length: 3}} {
		body, _, err := store.Get(context.Background(), "objects", "range", objectstore.GetOptions{Range: &byteRange})
		if body != nil || !errors.Is(err, objectstore.ErrInvalidRange) {
			t.Fatalf("Get range %+v = (%#v, %v), want nil ErrInvalidRange", byteRange, body, err)
		}
	}
}

func testListPagination(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	for _, key := range []objectstore.Key{"a/3", "b/1", "a/1", "a/2"} {
		putObject(t, store, "objects", key, []byte(key), objectstore.PutOptions{})
	}
	first, err := store.List(context.Background(), "objects", objectstore.ListOptions{Prefix: "a/", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageKeys(first); !reflect.DeepEqual(got, []objectstore.Key{"a/1", "a/2"}) || first.NextAfter != "a/2" {
		t.Fatalf("first page = %#v, keys %v", first, got)
	}
	second, err := store.List(context.Background(), "objects", objectstore.ListOptions{Prefix: "a/", After: first.NextAfter, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := pageKeys(second); !reflect.DeepEqual(got, []objectstore.Key{"a/3"}) || second.NextAfter != "" {
		t.Fatalf("second page = %#v, keys %v", second, got)
	}
}

func testDeleteVersion(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	object := putObject(t, store, "objects", "delete", []byte("value"), objectstore.PutOptions{})
	if err := store.Delete(context.Background(), "objects", "delete", "wrong"); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Delete wrong version error = %v, want ErrPrecondition", err)
	}
	if _, err := store.Stat(context.Background(), "objects", "delete"); err != nil {
		t.Fatalf("wrong-version Delete mutated object: %v", err)
	}
	if err := store.Delete(context.Background(), "objects", "delete", object.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(context.Background(), "objects", "delete"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Stat after Delete error = %v, want ErrNotFound", err)
	}
}

func testIfAbsentRace(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	const contenders = 16
	start := make(chan struct{})
	errorsByCall := make(chan error, contenders)
	var wait sync.WaitGroup
	for index := range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.Put(context.Background(), "objects", "race", bytes.NewReader([]byte(fmt.Sprintf("value-%d", index))), objectstore.PutOptions{IfAbsent: true})
			errorsByCall <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByCall)
	succeeded := 0
	preconditions := 0
	for err := range errorsByCall {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, objectstore.ErrPrecondition):
			preconditions++
		default:
			t.Fatalf("Put race error = %v", err)
		}
	}
	if succeeded != 1 || preconditions != contenders-1 {
		t.Fatalf("race results = %d success, %d precondition", succeeded, preconditions)
	}
}

func testChecksumRejection(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	wrong := sha256.Sum256([]byte("different"))
	if _, err := store.Put(context.Background(), "objects", "checksum", bytes.NewReader([]byte("value")), objectstore.PutOptions{SHA256: &wrong}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Put checksum error = %v, want ErrPrecondition", err)
	}
	if _, err := store.Stat(context.Background(), "objects", "checksum"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("checksum failure stored object: %v", err)
	}
}

func testMultipartCompletion(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	firstData := bytes.Repeat([]byte("a"), 5<<20)
	secondData := []byte("end")
	fullData := append(bytes.Clone(firstData), secondData...)
	fullDigest := sha256.Sum256(fullData)
	upload, err := store.BeginMultipart(context.Background(), "objects", "multipart", objectstore.PutOptions{
		Metadata: map[string]string{"source": "multipart"},
		SHA256:   &fullDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	second := putPart(t, store, upload, 2, secondData)
	first := putPart(t, store, upload, 1, firstData)
	if _, err := store.CompleteMultipart(context.Background(), upload, []objectstore.CompletedPart{second, first}); !errors.Is(err, objectstore.ErrInvalidPart) {
		t.Fatalf("unsorted CompleteMultipart error = %v, want ErrInvalidPart", err)
	}
	object, err := store.CompleteMultipart(context.Background(), upload, []objectstore.CompletedPart{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if object.Size != int64(len(fullData)) || object.SHA256 != fullDigest || !object.SHA256Known || object.Metadata["source"] != "multipart" {
		t.Fatalf("completed object = %#v", object)
	}
	body, _, err := store.Get(context.Background(), "objects", "multipart", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil || !bytes.Equal(data, fullData) {
		t.Fatalf("completed data = %d bytes, error %v", len(data), err)
	}
}

func testMultipartAbort(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	upload, err := store.BeginMultipart(context.Background(), "objects", "abort", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	putPart(t, store, upload, 1, []byte("part"))
	if err := store.AbortMultipart(context.Background(), upload); err != nil {
		t.Fatal(err)
	}
	partDigest := sha256.Sum256([]byte("part"))
	if _, err := store.PutPart(context.Background(), upload, 1, bytes.NewReader([]byte("part")), 4, partDigest); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("PutPart after abort error = %v, want ErrNotFound", err)
	}
	if _, err := store.CompleteMultipart(context.Background(), upload, nil); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Complete after abort error = %v, want ErrNotFound", err)
	}
}

func testContextCancellation(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Put(ctx, "objects", "canceled", bytes.NewReader([]byte("value")), objectstore.PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Put error = %v", err)
	}
	if _, err := store.Stat(context.Background(), "objects", "canceled"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("canceled Put mutated store: %v", err)
	}

	object := putObject(t, store, "objects", "existing", []byte("value"), objectstore.PutOptions{})
	if body, _, err := store.Get(ctx, "objects", "existing", objectstore.GetOptions{}); body != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get = (%#v, %v)", body, err)
	}
	if _, err := store.Stat(ctx, "objects", "existing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Stat error = %v", err)
	}
	if _, err := store.List(ctx, "objects", objectstore.ListOptions{Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled List error = %v", err)
	}
	if err := store.Delete(ctx, "objects", "existing", object.Version); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Delete error = %v", err)
	}
	if _, err := store.BeginMultipart(ctx, "objects", "multipart-canceled", objectstore.PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled BeginMultipart error = %v", err)
	}

	upload, err := store.BeginMultipart(context.Background(), "objects", "active-upload", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("part"))
	if _, err := store.PutPart(ctx, upload, 1, bytes.NewReader([]byte("part")), 4, digest); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PutPart error = %v", err)
	}
	if _, err := store.CompleteMultipart(ctx, upload, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CompleteMultipart error = %v", err)
	}
	if err := store.AbortMultipart(ctx, upload); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled AbortMultipart error = %v", err)
	}
	if err := store.AbortMultipart(context.Background(), upload); err != nil {
		t.Fatal(err)
	}
}

func testDefensiveCopies(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	metadata := map[string]string{"key": "original"}
	options := objectstore.PutOptions{Metadata: metadata}
	object := putObject(t, store, "objects", "copies", []byte("value"), options)
	metadata["key"] = "mutated input"
	object.Metadata["key"] = "mutated result"
	stat, err := store.Stat(context.Background(), "objects", "copies")
	if err != nil || stat.Metadata["key"] != "original" {
		t.Fatalf("stored metadata = %#v, error %v", stat.Metadata, err)
	}
	stat.Metadata["key"] = "mutated stat"
	page, err := store.List(context.Background(), "objects", objectstore.ListOptions{Limit: 10, IncludeMetadata: true})
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Metadata["key"] != "original" {
		t.Fatalf("listed metadata = %#v, error %v", page, err)
	}
	page.Objects[0].Metadata["key"] = "mutated page"
	again, err := store.Stat(context.Background(), "objects", "copies")
	if err != nil || again.Metadata["key"] != "original" {
		t.Fatalf("metadata aliases escaped store: %#v, error %v", again, err)
	}

	uploadMetadata := map[string]string{"upload": "original"}
	upload, err := store.BeginMultipart(context.Background(), "objects", "upload-copies", objectstore.PutOptions{Metadata: uploadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	uploadMetadata["upload"] = "mutated input"
	upload.Options.Metadata["upload"] = "mutated result"
	part := putPart(t, store, uploadWithMetadata(upload, "original"), 1, []byte("part"))
	completed, err := store.CompleteMultipart(context.Background(), uploadWithMetadata(upload, "original"), []objectstore.CompletedPart{part})
	if err != nil || completed.Metadata["upload"] != "original" {
		t.Fatalf("multipart metadata = %#v, error %v", completed.Metadata, err)
	}
}

func putObject(t *testing.T, store objectstore.Store, bucket objectstore.Bucket, key objectstore.Key, data []byte, options objectstore.PutOptions) objectstore.Object {
	t.Helper()
	object, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), options)
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func putPart(t *testing.T, store objectstore.MultipartStore, upload objectstore.Upload, number int, data []byte) objectstore.CompletedPart {
	t.Helper()
	digest := sha256.Sum256(data)
	part, err := store.PutPart(context.Background(), upload, number, bytes.NewReader(data), int64(len(data)), digest)
	if err != nil {
		t.Fatal(err)
	}
	return part
}

func uploadWithMetadata(upload objectstore.Upload, value string) objectstore.Upload {
	upload = upload.Clone()
	upload.Options.Metadata["upload"] = value
	return upload
}

func pageKeys(page objectstore.Page) []objectstore.Key {
	keys := make([]objectstore.Key, len(page.Objects))
	for index, object := range page.Objects {
		keys[index] = object.Key
	}
	return keys
}
