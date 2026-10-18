// Package pack bundles many content-addressed blobs into one immutable data
// object plus a binary index, so a reader fetches any blob with one exact
// ranged read.
//
// Writer stages blobs (verifying each declared size and digest) and Commit
// publishes the data object and its index with IfAbsent and declared
// digests at prefix/<index SHA-256>.pack and prefix/<index SHA-256>.idx; the
// output is deterministic for a given set of blobs. The index
// starts with the magic "GOPACK01" and a big-endian entry count, followed by
// one 48-byte record (digest, offset, length) per blob in ascending digest
// order. OpenReader validates the index against Metadata before serving
// reads, and Reader.Open verifies the digest of every blob it streams.
package pack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/justusschock/object-storage/cas"
)

const (
	indexMagic      = "GOPACK01"
	indexHeaderSize = 16
	indexRecordSize = 48
)

var (
	// ErrInvalidPack identifies a malformed, truncated or tampered pack.
	ErrInvalidPack = errors.New("pack: invalid pack")
	// ErrDuplicateDigest identifies a blob added twice to one Writer.
	ErrDuplicateDigest = errors.New("pack: duplicate digest")
)

// Entry locates one digest in packed data.
type Entry struct {
	Digest cas.Digest
	Offset int64
	Length int64
}

func encodeIndex(entries []Entry) []byte {
	encoded := make([]byte, indexHeaderSize+len(entries)*indexRecordSize)
	copy(encoded, indexMagic)
	binary.BigEndian.PutUint64(encoded[8:16], uint64(len(entries)))
	position := indexHeaderSize
	for _, entry := range entries {
		copy(encoded[position:position+32], entry.Digest[:])
		binary.BigEndian.PutUint64(encoded[position+32:position+40], uint64(entry.Offset))
		binary.BigEndian.PutUint64(encoded[position+40:position+48], uint64(entry.Length))
		position += indexRecordSize
	}
	return encoded
}

func decodeIndex(encoded []byte, dataSize int64) ([]Entry, error) {
	if len(encoded) < indexHeaderSize || string(encoded[:8]) != indexMagic || dataSize < 0 {
		return nil, ErrInvalidPack
	}
	count := binary.BigEndian.Uint64(encoded[8:16])
	if count > uint64((len(encoded)-indexHeaderSize)/indexRecordSize) || uint64(len(encoded)) != uint64(indexHeaderSize)+count*indexRecordSize {
		return nil, ErrInvalidPack
	}
	entries := make([]Entry, int(count))
	position := indexHeaderSize
	for index := range entries {
		copy(entries[index].Digest[:], encoded[position:position+32])
		offset := binary.BigEndian.Uint64(encoded[position+32 : position+40])
		length := binary.BigEndian.Uint64(encoded[position+40 : position+48])
		if offset > math.MaxInt64 || length == 0 || length > math.MaxInt64 || offset > uint64(dataSize) || length > uint64(dataSize)-offset {
			return nil, fmt.Errorf("%w: entry %d bounds", ErrInvalidPack, index)
		}
		entries[index].Offset = int64(offset)
		entries[index].Length = int64(length)
		if index > 0 && bytes.Compare(entries[index-1].Digest[:], entries[index].Digest[:]) >= 0 {
			return nil, fmt.Errorf("%w: unsorted or duplicate digest", ErrInvalidPack)
		}
		position += indexRecordSize
	}
	return entries, nil
}
