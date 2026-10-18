package pack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/cas"
	multipartupload "github.com/justusschock/go-object-storage/multipart"
)

// Metadata is sufficient to reopen one immutable pack.
type Metadata struct {
	Bucket      objectstore.Bucket
	DataKey     objectstore.Key
	IndexKey    objectstore.Key
	DataSize    int64
	IndexSHA256 [32]byte
}

type stagedBlob struct {
	path string
	size int64
}

// Writer stages verified blobs for deterministic commit. It is not safe for
// concurrent use, and it is consumed by a successful Commit.
type Writer struct {
	directory string
	blobs     map[cas.Digest]stagedBlob
	consumed  bool
}

// NewWriter creates an owner-only staging directory beneath tempDir.
func NewWriter(tempDir string) (*Writer, error) {
	if tempDir == "" {
		return nil, errors.New("pack: empty temporary directory")
	}
	directory, err := os.MkdirTemp(tempDir, "go-object-storage-pack-*")
	if err != nil {
		return nil, fmt.Errorf("pack: create staging directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return nil, fmt.Errorf("pack: protect staging directory: %w", err)
	}
	return &Writer{directory: directory, blobs: make(map[cas.Digest]stagedBlob)}, nil
}

// Add stages one blob after validating its declared length and digest.
func (writer *Writer) Add(digest cas.Digest, reader io.Reader, size int64) (returnErr error) {
	if writer == nil || writer.consumed {
		return errors.New("pack: writer is closed")
	}
	if isNil(reader) || size <= 0 {
		return ErrInvalidPack
	}
	if _, exists := writer.blobs[digest]; exists {
		return ErrDuplicateDigest
	}
	file, err := os.CreateTemp(writer.directory, "blob-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
		if returnErr != nil {
			_ = os.Remove(path)
		}
	}()
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hasher), io.LimitReader(reader, size+1))
	if err != nil {
		return err
	}
	var computed cas.Digest
	copy(computed[:], hasher.Sum(nil))
	if written != size || computed != digest {
		return ErrInvalidPack
	}
	writer.blobs[digest] = stagedBlob{path: path, size: size}
	return nil
}

// Commit writes deterministically ordered data and index objects.
func (writer *Writer) Commit(ctx context.Context, store objectstore.Store, bucket objectstore.Bucket, prefix string) (Metadata, error) {
	if writer == nil || writer.consumed || len(writer.blobs) == 0 {
		return Metadata{}, ErrInvalidPack
	}
	if isNil(store) {
		return Metadata{}, errors.New("pack: nil store")
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return Metadata{}, err
	}
	if err := objectstore.ValidateKey(objectstore.Key(prefix + "/pack")); err != nil || strings.HasSuffix(prefix, "/") {
		return Metadata{}, fmt.Errorf("pack: invalid prefix %q", prefix)
	}
	digests := make([]cas.Digest, 0, len(writer.blobs))
	for digest := range writer.blobs {
		digests = append(digests, digest)
	}
	sort.Slice(digests, func(left, right int) bool { return bytes.Compare(digests[left][:], digests[right][:]) < 0 })
	dataFile, err := os.CreateTemp(writer.directory, "data-*")
	if err != nil {
		return Metadata{}, err
	}
	defer func() { _ = dataFile.Close() }()
	entries := make([]Entry, 0, len(digests))
	var offset int64
	for _, digest := range digests {
		blob := writer.blobs[digest]
		if offset > math.MaxInt64-blob.size {
			return Metadata{}, ErrInvalidPack
		}
		source, err := os.Open(blob.path)
		if err != nil {
			return Metadata{}, err
		}
		written, copyErr := copyContext(ctx, dataFile, source)
		closeErr := source.Close()
		if copyErr != nil || closeErr != nil || written != blob.size {
			return Metadata{}, errors.Join(copyErr, closeErr, ErrInvalidPack)
		}
		entries = append(entries, Entry{Digest: digest, Offset: offset, Length: blob.size})
		offset += blob.size
	}
	if _, err := dataFile.Seek(0, io.SeekStart); err != nil {
		return Metadata{}, err
	}
	indexBytes := encodeIndex(entries)
	indexDigest := sha256.Sum256(indexBytes)
	name := hex.EncodeToString(indexDigest[:])
	metadata := Metadata{
		Bucket:      bucket,
		DataKey:     objectstore.Key(prefix + "/" + name + ".pack"),
		IndexKey:    objectstore.Key(prefix + "/" + name + ".idx"),
		DataSize:    offset,
		IndexSHA256: indexDigest,
	}
	dataDigest, err := hashFile(dataFile)
	if err != nil {
		return Metadata{}, err
	}
	if _, err := dataFile.Seek(0, io.SeekStart); err != nil {
		return Metadata{}, err
	}
	if err := putDataImmutable(ctx, store, bucket, metadata.DataKey, dataFile, offset, dataDigest); err != nil {
		return Metadata{}, err
	}
	if err := putImmutable(ctx, store, bucket, metadata.IndexKey, bytes.NewReader(indexBytes), indexDigest); err != nil {
		return Metadata{}, err
	}
	writer.consumed = true
	if err := os.RemoveAll(writer.directory); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

func putDataImmutable(ctx context.Context, store objectstore.Store, bucket objectstore.Bucket, key objectstore.Key, file *os.File, size int64, digest [32]byte) error {
	multipartStore, ok := store.(objectstore.MultipartStore)
	if !ok || isNil(multipartStore) {
		return putImmutable(ctx, store, bucket, key, file, digest)
	}
	uploader, err := multipartupload.NewUploader(multipartStore, multipartupload.Options{PartSize: 5 << 20, MaxParallel: 4})
	if err != nil {
		return err
	}
	object, err := uploader.UploadReaderAt(ctx, bucket, key, file, size, objectstore.PutOptions{IfAbsent: true, SHA256: &digest})
	if !errors.Is(err, objectstore.ErrPrecondition) {
		if err != nil {
			return err
		}
		if object.SHA256 != digest {
			return ErrInvalidPack
		}
		return nil
	}
	object, statErr := store.Stat(ctx, bucket, key)
	if statErr != nil {
		return errors.Join(err, statErr)
	}
	if object.SHA256 != digest {
		return ErrInvalidPack
	}
	return nil
}

func putImmutable(ctx context.Context, store objectstore.Store, bucket objectstore.Bucket, key objectstore.Key, reader io.Reader, digest [32]byte) error {
	_, err := store.Put(ctx, bucket, key, reader, objectstore.PutOptions{IfAbsent: true, SHA256: &digest})
	if !errors.Is(err, objectstore.ErrPrecondition) {
		return err
	}
	object, statErr := store.Stat(ctx, bucket, key)
	if statErr != nil {
		return errors.Join(err, statErr)
	}
	if object.SHA256 != digest {
		return ErrInvalidPack
	}
	return nil
}

func hashFile(file *os.File) ([32]byte, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return [32]byte{}, err
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		count, readErr := source.Read(buffer)
		if count > 0 {
			written, writeErr := destination.Write(buffer[:count])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != count {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

func isNil(value any) bool {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return true
	}
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
