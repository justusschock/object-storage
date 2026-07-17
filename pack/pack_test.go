package pack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/memory"
	"github.com/justusschock/go-object-storage/cas"
)

func TestPackCommitIsDeterministicAndReadsOneRange(t *testing.T) {
	digestA := cas.Digest(sha256.Sum256([]byte("a")))
	digestB := cas.Digest(sha256.Sum256([]byte("bb")))
	objects := &recordingStore{Store: memory.New()}

	first, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Add(digestB, strings.NewReader("bb"), 2); err != nil {
		t.Fatal(err)
	}
	if err := first.Add(digestA, strings.NewReader("a"), 1); err != nil {
		t.Fatal(err)
	}
	firstMetadata, err := first.Commit(context.Background(), objects, "packs", "first")
	if err != nil {
		t.Fatal(err)
	}

	second, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Add(digestA, strings.NewReader("a"), 1); err != nil {
		t.Fatal(err)
	}
	if err := second.Add(digestB, strings.NewReader("bb"), 2); err != nil {
		t.Fatal(err)
	}
	secondMetadata, err := second.Commit(context.Background(), objects, "packs", "second")
	if err != nil {
		t.Fatal(err)
	}
	if firstMetadata.IndexSHA256 != secondMetadata.IndexSHA256 || firstMetadata.DataSize != 3 || secondMetadata.DataSize != 3 {
		t.Fatalf("metadata = (%#v, %#v)", firstMetadata, secondMetadata)
	}

	reader, err := OpenReader(context.Background(), objects, firstMetadata)
	if err != nil {
		t.Fatal(err)
	}
	objects.resetGets()
	body, entry, err := reader.Open(context.Background(), digestB)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	getCalls, byteRange := objects.getState()
	if string(data) != "bb" || readErr != nil || closeErr != nil || entry.Length != 2 || getCalls != 1 || byteRange == nil || byteRange.Length != 2 {
		t.Fatalf("Open = (%q, %#v, %v, %v), gets=%d range=%#v", data, entry, readErr, closeErr, getCalls, byteRange)
	}
}

func TestWriterRejectsDuplicateAndMismatchedDigests(t *testing.T) {
	digest := cas.Digest(sha256.Sum256([]byte("value")))
	writer, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(digest, strings.NewReader("value"), 5); err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(digest, strings.NewReader("value"), 5); err == nil {
		t.Fatal("duplicate Add returned nil error")
	}
	wrong := cas.Digest(sha256.Sum256([]byte("different")))
	if err := writer.Add(wrong, strings.NewReader("value"), 5); err == nil {
		t.Fatal("digest-mismatched Add returned nil error")
	}
}

func TestOpenReaderRejectsTruncatedAndOverflowingIndexes(t *testing.T) {
	digest := cas.Digest(sha256.Sum256([]byte("a")))
	for _, test := range []struct {
		name  string
		index []byte
	}{
		{name: "truncated", index: []byte("GOPACK01")},
		{name: "offset overflow", index: maliciousIndex(digest, math.MaxUint64, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := memory.New()
			indexDigest := sha256.Sum256(test.index)
			indexKey := objectstore.Key("bad/" + test.name + ".idx")
			dataKey := objectstore.Key("bad/" + test.name + ".pack")
			if _, err := objects.Put(context.Background(), "packs", indexKey, bytes.NewReader(test.index), objectstore.PutOptions{SHA256: &indexDigest}); err != nil {
				t.Fatal(err)
			}
			if _, err := objects.Put(context.Background(), "packs", dataKey, strings.NewReader("a"), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			metadata := Metadata{Bucket: "packs", IndexKey: indexKey, DataKey: dataKey, DataSize: 1, IndexSHA256: indexDigest}
			if _, err := OpenReader(context.Background(), objects, metadata); err == nil {
				t.Fatal("OpenReader returned nil error")
			}
		})
	}
}

func TestReaderDetectsSameSizeDataTampering(t *testing.T) {
	digest := cas.Digest(sha256.Sum256([]byte("value")))
	objects := memory.New()
	writer, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(digest, strings.NewReader("value"), 5); err != nil {
		t.Fatal(err)
	}
	metadata, err := writer.Commit(context.Background(), objects, "packs", "tamper")
	if err != nil {
		t.Fatal(err)
	}
	dataObject, err := objects.Stat(context.Background(), metadata.Bucket, metadata.DataKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(context.Background(), metadata.Bucket, metadata.DataKey, dataObject.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Put(context.Background(), metadata.Bucket, metadata.DataKey, strings.NewReader("other"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(context.Background(), objects, metadata)
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := reader.Open(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if !errors.Is(errors.Join(readErr, closeErr), ErrInvalidPack) {
		t.Fatalf("read error = %v, close error = %v, want ErrInvalidPack", readErr, closeErr)
	}
}

func TestCommitCanRetryAfterIndexFailure(t *testing.T) {
	digest := cas.Digest(sha256.Sum256([]byte("value")))
	underlying := memory.New()
	objects := &failIndexOnceStore{Store: underlying}
	writer, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(digest, strings.NewReader("value"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Commit(context.Background(), objects, "packs", "retry"); !errors.Is(err, errIndexPut) {
		t.Fatalf("first Commit error = %v, want injected error", err)
	}
	if _, err := writer.Commit(context.Background(), objects, "packs", "retry"); err != nil {
		t.Fatalf("retry Commit error = %v", err)
	}
}

func TestCommitUsesMultipartStoreWhenAvailable(t *testing.T) {
	digest := cas.Digest(sha256.Sum256([]byte("value")))
	objects := &recordingMultipartStore{Store: memory.New()}
	writer, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(digest, strings.NewReader("value"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Commit(context.Background(), objects, "packs", "multipart"); err != nil {
		t.Fatal(err)
	}
	if objects.beginCalls != 1 {
		t.Fatalf("BeginMultipart calls = %d, want 1", objects.beginCalls)
	}
}

func TestWriterRejectsTypedNilInputs(t *testing.T) {
	writer, err := NewWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var reader *bytes.Reader
	if err := writer.Add(cas.Digest{}, reader, 1); err == nil {
		t.Fatal("Add accepted typed-nil reader")
	}
	valid := cas.Digest(sha256.Sum256([]byte("v")))
	if err := writer.Add(valid, strings.NewReader("v"), 1); err != nil {
		t.Fatal(err)
	}
	var store *nilStore
	if _, err := writer.Commit(context.Background(), store, "packs", "nil"); err == nil {
		t.Fatal("Commit accepted typed-nil store")
	}
}

func maliciousIndex(digest cas.Digest, offset, length uint64) []byte {
	buffer := bytes.NewBufferString("GOPACK01")
	_ = binary.Write(buffer, binary.BigEndian, uint64(1))
	_, _ = buffer.Write(digest[:])
	_ = binary.Write(buffer, binary.BigEndian, offset)
	_ = binary.Write(buffer, binary.BigEndian, length)
	return buffer.Bytes()
}

type recordingStore struct {
	*memory.Store
	mu        sync.Mutex
	getCalls  int
	lastRange *objectstore.ByteRange
}

var errIndexPut = errors.New("injected index put failure")

type failIndexOnceStore struct {
	objectstore.Store
	failed bool
}

func (store *failIndexOnceStore) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if strings.HasSuffix(string(key), ".idx") && !store.failed {
		store.failed = true
		return objectstore.Object{}, errIndexPut
	}
	return store.Store.Put(ctx, bucket, key, body, options)
}

type recordingMultipartStore struct {
	*memory.Store
	beginCalls int
}

func (store *recordingMultipartStore) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	store.beginCalls++
	return store.Store.BeginMultipart(ctx, bucket, key, options)
}

type nilStore struct{ objectstore.Store }

func (store *recordingStore) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	store.mu.Lock()
	store.getCalls++
	store.lastRange = options.Clone().Range
	store.mu.Unlock()
	return store.Store.Get(ctx, bucket, key, options)
}

func (store *recordingStore) resetGets() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.getCalls = 0
	store.lastRange = nil
}

func (store *recordingStore) getState() (int, *objectstore.ByteRange) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var byteRange *objectstore.ByteRange
	if store.lastRange != nil {
		copy := *store.lastRange
		byteRange = &copy
	}
	return store.getCalls, byteRange
}
