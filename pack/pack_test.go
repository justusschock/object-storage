package pack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
