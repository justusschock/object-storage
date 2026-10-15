package sftp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	objectstore "github.com/justusschock/go-object-storage"
	pkgsftp "github.com/pkg/sftp"
)

type uploadRecord struct {
	ID      string                 `json:"id"`
	Bucket  objectstore.Bucket     `json:"bucket"`
	Key     objectstore.Key        `json:"key"`
	Options objectstore.PutOptions `json:"options"`
}

// BeginMultipart creates isolated multipart state without mutating objects.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Upload{}, err
	}
	id, err := randomHex()
	if err != nil {
		return objectstore.Upload{}, err
	}
	record := uploadRecord{ID: id, Bucket: bucket, Key: key, Options: options.Clone()}
	data, err := json.Marshal(record)
	if err != nil {
		return objectstore.Upload{}, fmt.Errorf("sftp store: encode upload: %w", err)
	}
	directory := store.uploadPath(bucket, id)
	if err := store.client.MkdirAll(directory); err != nil {
		return objectstore.Upload{}, err
	}
	staged, err := store.writeStaging(ctx, bucket, data)
	if err == nil {
		err = store.client.Rename(staged.path, path.Join(directory, uploadName))
		if err != nil {
			store.discard(staged)
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		_ = store.removeAll(directory)
		return objectstore.Upload{}, err
	}
	return objectstore.Upload{ID: id, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart verifies and stores one numbered part. Uploading a part number
// again replaces the earlier part.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.CompletedPart{}, err
	}
	if number <= 0 || size < 0 {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	if _, err := store.loadUpload(upload); err != nil {
		return objectstore.CompletedPart{}, err
	}
	staged, err := store.spool(ctx, upload.Bucket, body, size)
	if err != nil {
		return objectstore.CompletedPart{}, err
	}
	defer store.discard(staged)
	if staged.size != size || staged.digest != digest {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	if err := ctx.Err(); err != nil {
		return objectstore.CompletedPart{}, err
	}
	directory := store.uploadPath(upload.Bucket, upload.ID)
	partPath := path.Join(directory, partPrefix+strconv.Itoa(number))
	if err := store.replace(staged.path, partPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return objectstore.CompletedPart{}, objectstore.ErrNotFound
		}
		return objectstore.CompletedPart{}, err
	}
	staged.path = ""
	// An abort that removed the upload record while this part was in flight
	// may have missed the part file: remove it and report the abort.
	if _, err := store.client.Lstat(path.Join(directory, uploadName)); errors.Is(err, os.ErrNotExist) {
		_ = store.removeAll(directory)
		return objectstore.CompletedPart{}, objectstore.ErrNotFound
	}
	return objectstore.CompletedPart{Number: number, ETag: hex.EncodeToString(digest[:]), Size: size, SHA256: digest}, nil
}

// CompleteMultipart streams the ordered parts from the server through the
// client into a staging file while re-verifying their digests, then publishes
// atomically with the upload's put conditions.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, parts []objectstore.CompletedPart) (objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	record, err := store.loadUpload(upload)
	if err != nil {
		return objectstore.Object{}, err
	}
	if len(parts) == 0 {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	for index, part := range parts {
		if part.Number != index+1 || part.Size < 0 || part.ETag != hex.EncodeToString(part.SHA256[:]) {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
	}
	staged, err := store.assemble(ctx, record, parts)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer store.discard(staged)
	options := record.Options
	if options.SHA256 != nil && *options.SHA256 != staged.digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != staged.size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	directory := store.uploadPath(record.Bucket, record.ID)
	metadata := metadataRecord{SHA256: hex.EncodeToString(staged.digest[:]), Size: staged.size, ContentType: options.ContentType, Metadata: options.Metadata}
	return store.commitWith(ctx, record.Bucket, record.Key, staged, metadata, options.IfAbsent, options.IfMatchVersion, func() error {
		if _, err := store.client.Lstat(path.Join(directory, uploadName)); err != nil {
			return objectstore.ErrNotFound // aborted while assembling
		}
		return nil
	}, func() {
		_ = store.removeAll(directory)
	})
}

// AbortMultipart discards an active upload without changing complete objects.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := store.loadUpload(upload); err != nil {
		return err
	}
	unlock, err := store.lock(ctx, upload.Bucket)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	directory := store.uploadPath(upload.Bucket, upload.ID)
	// Removing the record first makes the abort visible to PutPart and
	// CompleteMultipart before the parts are deleted.
	if err := store.client.Remove(path.Join(directory, uploadName)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return objectstore.ErrNotFound
		}
		return err
	}
	return store.removeAll(directory)
}

func (store *Store) assemble(ctx context.Context, record uploadRecord, parts []objectstore.CompletedPart) (*stagedFile, error) {
	file, staged, err := store.createStaging(ctx, record.Bucket, "stage-")
	if err != nil {
		return nil, err
	}
	whole := sha256.New()
	for _, part := range parts {
		err = store.appendPart(ctx, file, record, part, whole)
		if err != nil {
			break
		}
		staged.size += part.Size
	}
	if err := store.finish(file, err); err != nil {
		store.discard(staged)
		return nil, err
	}
	copy(staged.digest[:], whole.Sum(nil))
	return staged, nil
}

// appendPart streams one part to the end of output while verifying its size
// and digest.
func (store *Store) appendPart(ctx context.Context, output *pkgsftp.File, record uploadRecord, part objectstore.CompletedPart, whole io.Writer) error {
	source, err := store.client.Open(path.Join(store.uploadPath(record.Bucket, record.ID), partPrefix+strconv.Itoa(part.Number)))
	if errors.Is(err, os.ErrNotExist) {
		return objectstore.ErrInvalidPart
	}
	if err != nil {
		return err
	}
	defer func() { _ = source.Close() }()
	partHash := sha256.New()
	reader := contextReader{ctx: ctx, reader: io.TeeReader(io.LimitReader(source, part.Size+1), io.MultiWriter(whole, partHash))}
	written, err := output.ReadFromWithConcurrency(reader, 0)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if written != part.Size || string(partHash.Sum(nil)) != string(part.SHA256[:]) {
		return objectstore.ErrInvalidPart
	}
	return nil
}

func (store *Store) loadUpload(upload objectstore.Upload) (uploadRecord, error) {
	if err := errors.Join(objectstore.ValidateBucket(upload.Bucket), validateKey(upload.Key)); err != nil {
		return uploadRecord{}, err
	}
	if !isHex(upload.ID) {
		return uploadRecord{}, objectstore.ErrInvalidPart
	}
	data, err := store.readFile(path.Join(store.uploadPath(upload.Bucket, upload.ID), uploadName))
	if errors.Is(err, os.ErrNotExist) {
		return uploadRecord{}, objectstore.ErrNotFound
	}
	if err != nil {
		return uploadRecord{}, err
	}
	var record uploadRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return uploadRecord{}, fmt.Errorf("sftp store: corrupt upload %s: %w", upload.ID, err)
	}
	if record.ID != upload.ID || record.Bucket != upload.Bucket || record.Key != upload.Key {
		return uploadRecord{}, objectstore.ErrInvalidPart
	}
	return record, nil
}

// removeAll deletes a directory tree, tolerating entries that disappear
// concurrently and retrying when entries appear concurrently.
func (store *Store) removeAll(directory string) error {
	var err error
	for range 3 {
		err = store.removeTree(directory)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}
	return err
}

func (store *Store) removeTree(directory string) error {
	entries, err := store.client.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := path.Join(directory, entry.Name())
		if entry.IsDir() {
			err = store.removeTree(name)
		} else {
			err = store.client.Remove(name)
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return store.client.RemoveDirectory(directory)
}
