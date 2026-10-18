package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
)

func runCapabilities(t *testing.T, factory Factory) {
	t.Helper()
	probe := factory(t)
	if _, ok := probe.(objectstore.BatchStater); ok {
		t.Run("batch stat", func(t *testing.T) { testStatMany(t, factory(t)) })
	}
	if _, ok := probe.(objectstore.BatchDeleter); ok {
		t.Run("batch delete", func(t *testing.T) { testDeleteMany(t, factory(t)) })
	}
	if _, ok := probe.(objectstore.Copier); ok {
		t.Run("copy", func(t *testing.T) { testCopy(t, factory(t)) })
	}
	if _, ok := probe.(objectstore.Presigner); ok {
		t.Run("presign", func(t *testing.T) { testPresign(t, factory(t)) })
	}
}

func testStatMany(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	stater := store.(objectstore.BatchStater)
	ctx := context.Background()
	putObject(t, store, "objects", "batch/a", []byte("alpha"), objectstore.PutOptions{Metadata: map[string]string{"name": "a"}})
	putObject(t, store, "objects", "batch/b", []byte("beta"), objectstore.PutOptions{})
	keys := []objectstore.Key{"batch/b", "batch/missing", "batch/a"}
	results, err := stater.StatMany(ctx, "objects", keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(keys) {
		t.Fatalf("StatMany returned %d results for %d keys", len(results), len(keys))
	}
	for index, key := range keys {
		if key == "batch/missing" {
			if !errors.Is(results[index].Err, objectstore.ErrNotFound) {
				t.Fatalf("StatMany missing key error = %v, want ErrNotFound", results[index].Err)
			}
			continue
		}
		want, err := store.Stat(ctx, "objects", key)
		if err != nil {
			t.Fatal(err)
		}
		if results[index].Err != nil || !reflect.DeepEqual(results[index].Object, want) {
			t.Fatalf("StatMany[%d] = %#v, want %#v", index, results[index], want)
		}
	}
	if results, err := stater.StatMany(ctx, "objects", nil); err != nil || len(results) != 0 {
		t.Fatalf("StatMany(nil) = (%#v, %v)", results, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := stater.StatMany(canceled, "objects", keys); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled StatMany error = %v", err)
	}
}

func testDeleteMany(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	deleter := store.(objectstore.BatchDeleter)
	ctx := context.Background()
	for _, key := range []objectstore.Key{"bulk/a", "bulk/b", "bulk/c"} {
		putObject(t, store, "objects", key, []byte(key), objectstore.PutOptions{})
	}
	// More than one S3 DeleteObjects request worth of keys; absent keys succeed.
	keys := []objectstore.Key{"bulk/a", "bulk/c"}
	for index := range 1005 {
		keys = append(keys, objectstore.Key(fmt.Sprintf("bulk/absent/%04d", index)))
	}
	errs, err := deleter.DeleteMany(ctx, "objects", keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(errs) != len(keys) {
		t.Fatalf("DeleteMany returned %d errors for %d keys", len(errs), len(keys))
	}
	for index, err := range errs {
		if err != nil {
			t.Fatalf("DeleteMany %s error = %v", keys[index], err)
		}
	}
	for key, want := range map[objectstore.Key]error{"bulk/a": objectstore.ErrNotFound, "bulk/b": nil, "bulk/c": objectstore.ErrNotFound} {
		if _, err := store.Stat(ctx, "objects", key); (want == nil) != (err == nil) || (want != nil && !errors.Is(err, want)) {
			t.Fatalf("Stat %s after DeleteMany error = %v, want %v", key, err, want)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := deleter.DeleteMany(canceled, "objects", []objectstore.Key{"bulk/b"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled DeleteMany error = %v", err)
	}
	if _, err := store.Stat(ctx, "objects", "bulk/b"); err != nil {
		t.Fatalf("canceled DeleteMany removed object: %v", err)
	}
}

func testCopy(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	copier := store.(objectstore.Copier)
	ctx := context.Background()
	data := []byte("copied bytes")
	digest := sha256.Sum256(data)
	source := putObject(t, store, "objects", "copy/source", data, objectstore.PutOptions{SHA256: &digest, Metadata: map[string]string{"origin": "source"}})

	copied, err := copier.Copy(ctx, "objects", "copy/source", "objects-copy", "copy/destination", objectstore.CopyOptions{SourceVersion: source.Version, IfAbsent: true})
	if err != nil {
		t.Fatal(err)
	}
	if copied.Bucket != "objects-copy" || copied.Key != "copy/destination" || copied.Size != int64(len(data)) || !copied.SHA256Known || copied.SHA256 != digest || copied.Metadata["origin"] != "source" {
		t.Fatalf("copied object = %#v", copied)
	}
	stat, err := store.Stat(ctx, "objects-copy", "copy/destination")
	if err != nil || !reflect.DeepEqual(stat, copied) {
		t.Fatalf("Stat copy = (%#v, %v), want %#v", stat, err, copied)
	}
	if got := readObject(t, store, "objects-copy", "copy/destination"); !bytes.Equal(got, data) {
		t.Fatalf("copied bytes = %q", got)
	}

	if _, err := copier.Copy(ctx, "objects", "copy/source", "objects-copy", "copy/destination", objectstore.CopyOptions{IfAbsent: true}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Copy IfAbsent onto existing error = %v, want ErrPrecondition", err)
	}
	if _, err := copier.Copy(ctx, "objects", "copy/source", "objects", "copy/other", objectstore.CopyOptions{SourceVersion: source.Version + "-stale"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Copy stale SourceVersion error = %v, want ErrPrecondition", err)
	}
	if _, err := copier.Copy(ctx, "objects", "copy/missing", "objects", "copy/other", objectstore.CopyOptions{}); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Copy missing source error = %v, want ErrNotFound", err)
	}
	if _, err := store.Stat(ctx, "objects", "copy/other"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("failed Copy published destination: %v", err)
	}
	if _, err := copier.Copy(ctx, "objects", "copy/source", "objects-copy", "copy/destination", objectstore.CopyOptions{IfMatchVersion: copied.Version + "-stale"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Copy stale IfMatchVersion error = %v, want ErrPrecondition", err)
	}

	putObject(t, store, "objects", "copy/second", []byte("second source"), objectstore.PutOptions{})
	replaced, err := copier.Copy(ctx, "objects", "copy/second", "objects-copy", "copy/destination", objectstore.CopyOptions{
		IfMatchVersion: copied.Version,
		Metadata:       map[string]string{"origin": "replaced"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondDigest := sha256.Sum256([]byte("second source"))
	if replaced.Metadata["origin"] != "replaced" || replaced.SHA256 != secondDigest || !replaced.SHA256Known {
		t.Fatalf("replacing copy = %#v", replaced)
	}
	if got := readObject(t, store, "objects-copy", "copy/destination"); string(got) != "second source" {
		t.Fatalf("replaced copy bytes = %q", got)
	}
}

func testPresign(t *testing.T, store objectstore.MultipartStore) {
	t.Helper()
	presigner := store.(objectstore.Presigner)
	ctx := context.Background()
	data := []byte("presigned payload")
	digest := sha256.Sum256(data)

	put, err := presigner.PresignPut(ctx, "objects", "presigned/object", time.Minute, objectstore.PutOptions{
		IfAbsent: true, SHA256: &digest, Size: int64(len(data)), Metadata: map[string]string{"via": "presign"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if put.Method != http.MethodPut || put.URL == "" || !put.ExpiresAt.After(time.Now()) {
		t.Fatalf("PresignPut = %#v", put)
	}
	response := execute(t, put, data)
	if response.StatusCode/100 != 2 {
		t.Fatalf("presigned PUT status = %d", response.StatusCode)
	}
	object, err := store.Stat(ctx, "objects", "presigned/object")
	if err != nil {
		t.Fatal(err)
	}
	if object.Size != int64(len(data)) || !object.SHA256Known || object.SHA256 != digest || object.Metadata["via"] != "presign" {
		t.Fatalf("presigned object = %#v", object)
	}
	if response := execute(t, put, data); response.StatusCode/100 == 2 {
		t.Fatalf("presigned IfAbsent PUT replaced an existing object: status %d", response.StatusCode)
	}

	get, err := presigner.PresignGet(ctx, "objects", "presigned/object", time.Minute, &objectstore.ByteRange{Offset: 4, Length: 6})
	if err != nil {
		t.Fatal(err)
	}
	response = execute(t, get, nil)
	if response.StatusCode/100 != 2 || string(response.body) != string(data[4:10]) {
		t.Fatalf("presigned ranged GET = (%d, %q)", response.StatusCode, response.body)
	}

	upload, err := store.BeginMultipart(ctx, "objects", "presigned/multipart", objectstore.PutOptions{SHA256: &digest})
	if err != nil {
		t.Fatal(err)
	}
	partRequest, err := presigner.PresignUploadPart(ctx, upload, 1, int64(len(data)), digest, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	response = execute(t, partRequest, data)
	etag := response.header.Get("ETag")
	if response.StatusCode/100 != 2 || etag == "" {
		t.Fatalf("presigned part upload = (%d, ETag %q)", response.StatusCode, etag)
	}
	completed, err := store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{{Number: 1, ETag: etag, Size: int64(len(data)), SHA256: digest}})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Size != int64(len(data)) || completed.SHA256 != digest || !completed.SHA256Known {
		t.Fatalf("presigned multipart object = %#v", completed)
	}
	if got := readObject(t, store, "objects", "presigned/multipart"); !bytes.Equal(got, data) {
		t.Fatalf("presigned multipart bytes = %q", got)
	}
}

type executed struct {
	StatusCode int
	header     http.Header
	body       []byte
}

// execute performs a presigned request with http.DefaultClient.
func execute(t *testing.T, presigned objectstore.PresignedRequest, body []byte) executed {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(context.Background(), presigned.Method, presigned.URL, reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range presigned.Header {
		if strings.EqualFold(name, "Host") || strings.EqualFold(name, "Content-Length") {
			continue
		}
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return executed{StatusCode: response.StatusCode, header: response.Header, body: data}
}
