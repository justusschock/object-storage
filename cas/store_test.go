package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	coordinationmemory "github.com/justusschock/go-distributed-coordination/memory"
	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/memory"
)

func TestDigestRoundTrip(t *testing.T) {
	digest := Digest(sha256.Sum256([]byte("hello")))
	parsed, err := ParseDigest(digest.String())
	if err != nil || parsed != digest {
		t.Fatalf("ParseDigest(%q) = (%x, %v)", digest.String(), parsed, err)
	}
	for _, value := range []string{"", "md5:00", "sha256:00", "sha256:zzzz"} {
		if _, err := ParseDigest(value); err == nil {
			t.Fatalf("ParseDigest(%q) returned nil error", value)
		}
	}
}

func TestPutDeduplicatesConcurrentWriters(t *testing.T) {
	objects := &countingStore{Store: memory.New()}
	coordination := coordinationmemory.NewBackend(nil)
	store, err := New(objects, "blobs", "sha256", nil, coordination, "writer")
	if err != nil {
		t.Fatal(err)
	}

	const writers = 8
	digests := make(chan Digest, writers)
	errorsByWriter := make(chan error, writers)
	var wait sync.WaitGroup
	for range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			digest, _, err := store.Put(context.Background(), strings.NewReader("hello"))
			digests <- digest
			errorsByWriter <- err
		}()
	}
	wait.Wait()
	close(digests)
	close(errorsByWriter)
	want := Digest(sha256.Sum256([]byte("hello")))
	for digest := range digests {
		if digest != want {
			t.Fatalf("digest = %x, want %x", digest, want)
		}
	}
	for err := range errorsByWriter {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls := objects.putCalls.Load(); calls != 1 {
		t.Fatalf("object Put calls = %d, want 1", calls)
	}
}

func TestOpenFullReadDetectsCorruptionAtEOF(t *testing.T) {
	objects := memory.New()
	store, err := New(objects, "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer")
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest(sha256.Sum256([]byte("expected")))
	if _, err := objects.Put(context.Background(), "blobs", objectstore.Key("sha256/"+digest.hex()), strings.NewReader("corrupt"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	body, _, err := store.Open(context.Background(), digest, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if string(data) != "corrupt" || !errors.Is(errors.Join(readErr, closeErr), ErrDigestMismatch) {
		t.Fatalf("read = (%q, %v), close = %v", data, readErr, closeErr)
	}
}

func TestOpenRangeUsesExactBytes(t *testing.T) {
	objects := memory.New()
	store, err := New(objects, "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer")
	if err != nil {
		t.Fatal(err)
	}
	digest, _, err := store.Put(context.Background(), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	body, object, err := store.Open(context.Background(), digest, &objectstore.ByteRange{Offset: 1, Length: 3})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if string(data) != "ell" || readErr != nil || closeErr != nil || object.SHA256 != [32]byte(digest) {
		t.Fatalf("Open range = (%q, %#v, %v, %v)", data, object, readErr, closeErr)
	}
}

type countingStore struct {
	*memory.Store
	putCalls atomic.Int64
}

func (store *countingStore) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	store.putCalls.Add(1)
	return store.Store.Put(ctx, bucket, key, body, options)
}
