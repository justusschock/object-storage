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
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
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
	if _, err := ParseDigest("sha256:" + strings.ToUpper(digest.hex())); err == nil {
		t.Fatal("ParseDigest accepted non-canonical uppercase hex")
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
	underlying := memory.New()
	objects := &digestMetadataStore{Store: underlying}
	store, err := New(objects, "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer")
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest(sha256.Sum256([]byte("expected")))
	objects.digest = [32]byte(digest)
	if _, err := underlying.Put(context.Background(), "blobs", objectstore.Key("sha256/"+digest.hex()), strings.NewReader("corrupt"), objectstore.PutOptions{}); err != nil {
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

func TestOpenRejectsCorruptFullReadMetadata(t *testing.T) {
	underlying := memory.New()
	digest := Digest(sha256.Sum256([]byte("expected")))
	if _, err := underlying.Put(context.Background(), "blobs", objectstore.Key("sha256/"+digest.hex()), strings.NewReader("expected"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	objects := &digestMetadataStore{Store: underlying, digest: sha256.Sum256([]byte("wrong"))}
	store, err := New(objects, "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer")
	if err != nil {
		t.Fatal(err)
	}
	if body, _, err := store.Open(context.Background(), digest, nil); body != nil || !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Open = (%#v, %v), want nil ErrDigestMismatch", body, err)
	}
}

func TestPutReportsLeaseLoss(t *testing.T) {
	objects := &countingStore{Store: memory.New()}
	store, err := New(objects, "blobs", "sha256", nil, losingBackend{}, "writer")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Put(context.Background(), strings.NewReader("hello")); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("Put error = %v, want ErrLeaseLost", err)
	}
	if calls := objects.putCalls.Load(); calls != 0 {
		t.Fatalf("object Put calls = %d, want 0 after lease loss", calls)
	}
}

func TestNewAndPutRejectTypedNilInputs(t *testing.T) {
	var objects *nilObjectStore
	if _, err := New(objects, "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer"); err == nil {
		t.Fatal("New accepted typed-nil object store")
	}
	var backend *nilCoordinationBackend
	if _, err := New(memory.New(), "blobs", "sha256", nil, backend, "writer"); err == nil {
		t.Fatal("New accepted typed-nil coordination backend")
	}
	store, err := New(memory.New(), "blobs", "sha256", nil, coordinationmemory.NewBackend(nil), "writer")
	if err != nil {
		t.Fatal(err)
	}
	var reader *bytes.Reader
	if _, _, err := store.Put(context.Background(), reader); err == nil {
		t.Fatal("Put accepted typed-nil reader")
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

type digestMetadataStore struct {
	*memory.Store
	digest [32]byte
}

func (store *digestMetadataStore) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	body, object, err := store.Store.Get(ctx, bucket, key, options)
	if err == nil {
		object.SHA256 = store.digest
	}
	return body, object, err
}

type nilObjectStore struct{ objectstore.Store }

type nilCoordinationBackend struct{ coordination.Backend }

type losingBackend struct{}

func (losingBackend) Acquire(context.Context, coordination.AcquireRequest) (coordination.Lease, error) {
	return losingLease{done: make(chan struct{})}, nil
}

type losingLease struct{ done chan struct{} }

func (lease losingLease) Info() coordination.LeaseInfo {
	return coordination.LeaseInfo{Key: "key", Owner: "writer", Token: 1, AcquiredAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}
}

func (lease losingLease) Done() <-chan struct{} { return lease.done }

func (lease losingLease) Renew(context.Context, time.Duration) (coordination.LeaseInfo, error) {
	return lease.Info(), coordination.ErrLeaseLost
}

func (losingLease) Release(context.Context) error { return coordination.ErrLeaseLost }
