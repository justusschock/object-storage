package pack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sort"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/cas"
)

// Reader provides digest lookup over one validated immutable pack.
type Reader struct {
	store    objectstore.Store
	metadata Metadata
	entries  []Entry
}

// OpenReader loads and validates a pack index and its data bounds.
func OpenReader(ctx context.Context, store objectstore.Store, metadata Metadata) (*Reader, error) {
	if store == nil || metadata.DataSize < 0 {
		return nil, ErrInvalidPack
	}
	body, object, err := store.Get(ctx, metadata.Bucket, metadata.IndexKey, objectstore.GetOptions{})
	if err != nil {
		return nil, err
	}
	encoded, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	digest := sha256.Sum256(encoded)
	if digest != metadata.IndexSHA256 || object.SHA256 != metadata.IndexSHA256 {
		return nil, ErrInvalidPack
	}
	dataObject, err := store.Stat(ctx, metadata.Bucket, metadata.DataKey)
	if err != nil {
		return nil, err
	}
	if dataObject.Size != metadata.DataSize {
		return nil, ErrInvalidPack
	}
	entries, err := decodeIndex(encoded, metadata.DataSize)
	if err != nil {
		return nil, err
	}
	return &Reader{store: store, metadata: metadata, entries: entries}, nil
}

// Open opens exactly one ranged read for digest.
func (reader *Reader) Open(ctx context.Context, digest cas.Digest) (io.ReadCloser, Entry, error) {
	index, found := sort.Find(len(reader.entries), func(index int) int {
		return bytes.Compare(digest[:], reader.entries[index].Digest[:])
	})
	if !found {
		return nil, Entry{}, objectstore.ErrNotFound
	}
	entry := reader.entries[index]
	body, _, err := reader.store.Get(ctx, reader.metadata.Bucket, reader.metadata.DataKey, objectstore.GetOptions{
		Range: &objectstore.ByteRange{Offset: entry.Offset, Length: entry.Length},
	})
	if err != nil {
		return nil, Entry{}, err
	}
	return body, entry, nil
}
