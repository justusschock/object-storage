package gcstest_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/justusschock/go-object-storage/backend/gcs/gcstest"
	"google.golang.org/api/googleapi"
)

func TestNewServerBucketsAndCleanup(t *testing.T) {
	ctx := context.Background()
	client, cleanup := gcstest.NewServer(t, "alpha", "beta")
	for _, bucket := range []string{"alpha", "beta"} {
		if _, err := client.Bucket(bucket).Attrs(ctx); err != nil {
			t.Fatalf("bucket %s: %v", bucket, err)
		}
	}
	if _, err := client.Bucket("gamma").Attrs(ctx); !errors.Is(err, storage.ErrBucketNotExist) {
		t.Fatalf("unknown bucket error = %v", err)
	}
	write(t, client, "alpha", "object", []byte("value"), nil)
	if got := read(t, client.Bucket("alpha").Object("object")); string(got) != "value" {
		t.Fatalf("read %q", got)
	}
	cleanup()
	cleanup()
	// The client retries connection failures until its context ends.
	stopped, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := client.Bucket("alpha").Object("object").Attrs(stopped); err == nil {
		t.Fatal("client still works after cleanup")
	}
	// Servers are independent.
	other, _ := gcstest.NewServer(t, "alpha")
	if _, err := other.Bucket("alpha").Object("object").Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("new server shares state: %v", err)
	}
}

func TestUploadPreconditions(t *testing.T) {
	client, _ := gcstest.NewServer(t, "bucket")
	large := bytes.Repeat([]byte("x"), 1<<20)
	for _, test := range []struct {
		name      string
		data      []byte
		chunkSize int
	}{
		{"multipart", []byte("small"), 16 << 20},
		{"single request", []byte("small"), 0},
		{"resumable", large, 256 << 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := client.Bucket("bucket").Object(test.name)
			first := writeWith(t, object.If(storage.Conditions{DoesNotExist: true}), test.data, test.chunkSize)
			if err := tryWrite(object.If(storage.Conditions{DoesNotExist: true}), test.data, test.chunkSize); !isStatus(err, http.StatusPreconditionFailed) {
				t.Fatalf("second DoesNotExist upload error = %v, want 412", err)
			}
			if err := tryWrite(object.If(storage.Conditions{GenerationMatch: first.Generation + 1}), test.data, test.chunkSize); !isStatus(err, http.StatusPreconditionFailed) {
				t.Fatalf("stale GenerationMatch upload error = %v, want 412", err)
			}
			second := writeWith(t, object.If(storage.Conditions{GenerationMatch: first.Generation}), test.data, test.chunkSize)
			if second.Generation <= first.Generation {
				t.Fatalf("generations %d then %d", first.Generation, second.Generation)
			}
		})
	}
}

func TestGenerationsStrictlyIncrease(t *testing.T) {
	client, _ := gcstest.NewServer(t, "bucket")
	previous := int64(0)
	for range 100 {
		attrs := write(t, client, "bucket", "hot", []byte("same"), nil)
		if attrs.Generation <= previous {
			t.Fatalf("generation %d after %d", attrs.Generation, previous)
		}
		previous = attrs.Generation
	}
}

func TestReadAndDeletePreconditions(t *testing.T) {
	ctx := context.Background()
	client, _ := gcstest.NewServer(t, "bucket")
	object := client.Bucket("bucket").Object("object")
	old := write(t, client, "bucket", "object", []byte("old"), nil)
	current := write(t, client, "bucket", "object", []byte("new"), nil)
	if _, err := object.If(storage.Conditions{GenerationMatch: old.Generation}).NewReader(ctx); !isStatus(err, http.StatusPreconditionFailed) {
		t.Fatalf("read with stale GenerationMatch error = %v", err)
	}
	if _, err := object.Generation(old.Generation).NewReader(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("read of replaced generation error = %v", err)
	}
	if got := read(t, object.Generation(current.Generation)); string(got) != "new" {
		t.Fatalf("pinned read = %q", got)
	}
	if err := object.If(storage.Conditions{GenerationMatch: old.Generation}).Delete(ctx); !isStatus(err, http.StatusPreconditionFailed) {
		t.Fatalf("delete with stale GenerationMatch error = %v", err)
	}
	if err := object.Generation(old.Generation).Delete(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("delete of replaced generation error = %v", err)
	}
	if _, err := object.Attrs(ctx); err != nil {
		t.Fatalf("failed deletes removed the object: %v", err)
	}
	if err := object.If(storage.Conditions{GenerationMatch: current.Generation}).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := object.If(storage.Conditions{GenerationMatch: current.Generation}).Delete(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("conditional delete of absent object error = %v", err)
	}
}

func TestCompose(t *testing.T) {
	ctx := context.Background()
	client, _ := gcstest.NewServer(t, "bucket")
	bucket := client.Bucket("bucket")
	a := write(t, client, "bucket", "a", []byte("aa"), nil)
	b := write(t, client, "bucket", "b", []byte("bb"), nil)
	composer := bucket.Object("ab").If(storage.Conditions{DoesNotExist: true}).ComposerFrom(bucket.Object("a").Generation(a.Generation), bucket.Object("b"))
	composer.Metadata = map[string]string{"k": "v"}
	composer.ContentType = "text/plain"
	first, err := composer.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Size != 4 || first.Metadata["k"] != "v" || first.ContentType != "text/plain" {
		t.Fatalf("composed attrs = %+v", first)
	}
	if got := read(t, bucket.Object("ab")); string(got) != "aabb" {
		t.Fatalf("composed bytes = %q", got)
	}
	if _, err := bucket.Object("ab").If(storage.Conditions{DoesNotExist: true}).ComposerFrom(bucket.Object("a")).Run(ctx); !isStatus(err, http.StatusPreconditionFailed) {
		t.Fatalf("compose onto existing with DoesNotExist error = %v", err)
	}
	// Composing onto an existing object (including itself) publishes a new
	// generation.
	second, err := bucket.Object("ab").If(storage.Conditions{GenerationMatch: first.Generation}).ComposerFrom(bucket.Object("ab"), bucket.Object("b")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation <= first.Generation || second.Size != 6 {
		t.Fatalf("recomposed attrs = %+v after generation %d", second, first.Generation)
	}
	if _, err := bucket.Object("x").ComposerFrom(bucket.Object("a").Generation(b.Generation)).Run(ctx); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("compose of missing source generation error = %v", err)
	}
	if _, err := bucket.Object("x").ComposerFrom(bucket.Object("missing")).Run(ctx); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("compose of missing source error = %v", err)
	}
	sources := make([]*storage.ObjectHandle, 33)
	for index := range sources {
		sources[index] = bucket.Object("a")
	}
	if _, err := bucket.Object("x").ComposerFrom(sources...).Run(ctx); err == nil {
		t.Fatal("compose accepted 33 sources")
	}
	if _, err := bucket.Object("x").Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("failed composes published an object: %v", err)
	}
}

func TestRewritePreconditions(t *testing.T) {
	ctx := context.Background()
	client, _ := gcstest.NewServer(t, "bucket", "other")
	source := write(t, client, "bucket", "source", []byte("bytes"), map[string]string{"k": "v"})
	src := client.Bucket("bucket").Object("source")
	dst := client.Bucket("other").Object("destination")
	if _, err := dst.CopierFrom(src.If(storage.Conditions{GenerationMatch: source.Generation + 1})).Run(ctx); !isStatus(err, http.StatusPreconditionFailed) {
		t.Fatalf("copy with stale source generation error = %v", err)
	}
	copied, err := dst.If(storage.Conditions{DoesNotExist: true}).CopierFrom(src.If(storage.Conditions{GenerationMatch: source.Generation})).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if copied.Metadata["k"] != "v" || copied.Size != 5 {
		t.Fatalf("copied attrs = %+v", copied)
	}
	if _, err := dst.If(storage.Conditions{DoesNotExist: true}).CopierFrom(src).Run(ctx); !isStatus(err, http.StatusPreconditionFailed) {
		t.Fatalf("copy onto existing with DoesNotExist error = %v", err)
	}
	if _, err := dst.If(storage.Conditions{GenerationMatch: copied.Generation + 1}).CopierFrom(src).Run(ctx); !isStatus(err, http.StatusPreconditionFailed) {
		t.Fatalf("copy with stale destination generation error = %v", err)
	}
	if _, err := dst.CopierFrom(client.Bucket("bucket").Object("missing").If(storage.Conditions{GenerationMatch: 1})).Run(ctx); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("copy of missing source error = %v", err)
	}
}

func TestSignedPut(t *testing.T) {
	ctx := context.Background()
	client, _ := gcstest.NewServer(t, "bucket")
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := client.Bucket("bucket").SignedURL("dir/signed object", &storage.SignedURLOptions{
		GoogleAccessID: "signer@example.iam.gserviceaccount.com",
		PrivateKey:     pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
		Method:         http.MethodPut, Expires: time.Now().Add(time.Minute), Scheme: storage.SigningSchemeV4, Insecure: true,
		ContentType: "text/plain",
		Headers:     []string{"x-goog-if-generation-match:0", "x-goog-content-length-range:7,7", "x-goog-meta-origin:signed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	put := func(body string) *http.Response {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, signed, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "text/plain")
		request.Header.Set("X-Goog-If-Generation-Match", "0")
		request.Header.Set("X-Goog-Content-Length-Range", "7,7")
		request.Header.Set("X-Goog-Meta-Origin", "signed")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response
	}
	if response := put("too long body"); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("out-of-range PUT status = %d", response.StatusCode)
	}
	response := put("payload")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("signed PUT status = %d", response.StatusCode)
	}
	sum := md5.Sum([]byte("payload"))
	attrs, err := client.Bucket("bucket").Object("dir/signed object").Attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Get("ETag") != strconv.Quote(hex.EncodeToString(sum[:])) || response.Header.Get("X-Goog-Generation") != strconv.FormatInt(attrs.Generation, 10) {
		t.Fatalf("signed PUT headers = %v, generation %d", response.Header, attrs.Generation)
	}
	if attrs.Metadata["origin"] != "signed" || attrs.ContentType != "text/plain" || attrs.Size != 7 {
		t.Fatalf("signed object attrs = %+v", attrs)
	}
	if response := put("payload"); response.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("second if-generation-match:0 PUT status = %d", response.StatusCode)
	}
}

func write(t *testing.T, client *storage.Client, bucket, name string, data []byte, metadata map[string]string) *storage.ObjectAttrs {
	t.Helper()
	writer := client.Bucket(bucket).Object(name).NewWriter(context.Background())
	writer.Metadata = metadata
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return writer.Attrs()
}

func writeWith(t *testing.T, object *storage.ObjectHandle, data []byte, chunkSize int) *storage.ObjectAttrs {
	t.Helper()
	writer := object.NewWriter(context.Background())
	writer.ChunkSize = chunkSize
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return writer.Attrs()
}

func tryWrite(object *storage.ObjectHandle, data []byte, chunkSize int) error {
	writer := object.NewWriter(context.Background())
	writer.ChunkSize = chunkSize
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func read(t *testing.T, object *storage.ObjectHandle) []byte {
	t.Helper()
	reader, err := object.NewReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func isStatus(err error, code int) bool {
	var apiError *googleapi.Error
	return errors.As(err, &apiError) && apiError.Code == code
}
