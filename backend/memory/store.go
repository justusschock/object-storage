// Package memory provides an in-memory object store for tests and local use.
package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
)

type record struct {
	object objectstore.Object
	data   []byte
}

type part struct {
	completed objectstore.CompletedPart
	data      []byte
}

type multipart struct {
	upload objectstore.Upload
	parts  map[int]part
}

// Store keeps immutable object versions and multipart uploads in memory.
type Store struct {
	mu           sync.RWMutex
	objects      map[objectstore.Bucket]map[objectstore.Key]record
	uploads      map[string]multipart
	nextVersion  uint64
	nextUploadID uint64
}

// New returns an empty store.
func New() *Store {
	return &Store{
		objects: make(map[objectstore.Bucket]map[objectstore.Key]record),
		uploads: make(map[string]multipart),
	}
}

// Put stores a complete object after atomically checking its preconditions.
func (store *Store) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	data, err := readAll(ctx, body)
	if err != nil {
		return objectstore.Object{}, err
	}
	digest := sha256.Sum256(data)
	if options.SHA256 != nil && *options.SHA256 != digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	return store.putLocked(bucket, key, data, digest, options)
}

// Get opens an independently owned reader for a complete object or exact range.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return nil, objectstore.Object{}, err
	}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
	}

	store.mu.RLock()
	stored, ok := store.objectLocked(bucket, key)
	if ok {
		stored.data = slices.Clone(stored.data)
		stored.object = stored.object.Clone()
	}
	store.mu.RUnlock()
	if !ok {
		return nil, objectstore.Object{}, objectstore.ErrNotFound
	}
	if options.IfMatchVersion != "" && options.IfMatchVersion != stored.object.Version {
		return nil, objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Range != nil {
		end := options.Range.Offset + options.Range.Length
		if end > int64(len(stored.data)) {
			return nil, objectstore.Object{}, objectstore.ErrInvalidRange
		}
		stored.data = stored.data[options.Range.Offset:end]
	}
	if err := ctx.Err(); err != nil {
		return nil, objectstore.Object{}, err
	}
	return io.NopCloser(bytes.NewReader(stored.data)), stored.object, nil
}

// Stat returns independently owned metadata for a complete object.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	store.mu.RLock()
	stored, ok := store.objectLocked(bucket, key)
	store.mu.RUnlock()
	if !ok {
		return objectstore.Object{}, objectstore.ErrNotFound
	}
	return stored.object.Clone(), nil
}

// Delete atomically removes the matching object version.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	stored, ok := store.objectLocked(bucket, key)
	if !ok {
		return objectstore.ErrNotFound
	}
	if version != "" && version != stored.object.Version {
		return objectstore.ErrPrecondition
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(store.objects[bucket], key)
	if len(store.objects[bucket]) == 0 {
		delete(store.objects, bucket)
	}
	return nil
}

// List returns a stable key-ordered page.
func (store *Store) List(ctx context.Context, bucket objectstore.Bucket, options objectstore.ListOptions) (objectstore.Page, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Page{}, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return objectstore.Page{}, err
	}
	if options.Limit < 0 {
		return objectstore.Page{}, objectstore.ErrInvalidRange
	}

	store.mu.RLock()
	keys := make([]objectstore.Key, 0, len(store.objects[bucket]))
	for key := range store.objects[bucket] {
		if strings.HasPrefix(string(key), options.Prefix) && key > options.After {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(left, right int) bool { return keys[left] < keys[right] })
	count := len(keys)
	if options.Limit > 0 && count > options.Limit {
		count = options.Limit
	}
	page := objectstore.Page{Objects: make([]objectstore.Object, count)}
	for index, key := range keys[:count] {
		page.Objects[index] = store.objects[bucket][key].object.Clone()
	}
	if count < len(keys) {
		page.NextAfter = keys[count-1]
	}
	store.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return objectstore.Page{}, err
	}
	return page, nil
}

// BeginMultipart creates isolated multipart state without mutating objects.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return objectstore.Upload{}, err
	}
	store.nextUploadID++
	upload := objectstore.Upload{
		ID:      strconv.FormatUint(store.nextUploadID, 10),
		Bucket:  bucket,
		Key:     key,
		Options: options.Clone(),
	}
	store.uploads[upload.ID] = multipart{upload: upload.Clone(), parts: make(map[int]part)}
	return upload.Clone(), nil
}

// PutPart stores a validated immutable part under an active upload.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.CompletedPart{}, err
	}
	if upload.ID == "" || number <= 0 || size < 0 {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	data, err := readAll(ctx, body)
	if err != nil {
		return objectstore.CompletedPart{}, err
	}
	computed := sha256.Sum256(data)
	if int64(len(data)) != size || computed != digest {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	completed := objectstore.CompletedPart{
		Number: number,
		ETag:   hex.EncodeToString(computed[:]),
		Size:   size,
		SHA256: computed,
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	active, ok := store.uploads[upload.ID]
	if !ok {
		return objectstore.CompletedPart{}, objectstore.ErrNotFound
	}
	if !sameUpload(active.upload, upload) {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	if err := ctx.Err(); err != nil {
		return objectstore.CompletedPart{}, err
	}
	active.parts[number] = part{completed: completed, data: slices.Clone(data)}
	store.uploads[upload.ID] = active
	return completed, nil
}

// CompleteMultipart atomically assembles ordered parts and applies put preconditions.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	active, ok := store.uploads[upload.ID]
	if !ok {
		return objectstore.Object{}, objectstore.ErrNotFound
	}
	if !sameUpload(active.upload, upload) || len(completed) == 0 {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}

	var total int64
	for index, expected := range completed {
		if expected.Number != index+1 {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
		stored, exists := active.parts[expected.Number]
		if !exists || stored.completed != expected || total > int64(^uint64(0)>>1)-stored.completed.Size {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
		total += stored.completed.Size
	}
	if total > int64(int(^uint(0)>>1)) {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	data := make([]byte, 0, int(total))
	for _, expected := range completed {
		data = append(data, active.parts[expected.Number].data...)
	}
	digest := sha256.Sum256(data)
	if active.upload.Options.SHA256 != nil && *active.upload.Options.SHA256 != digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	object, err := store.putLocked(active.upload.Bucket, active.upload.Key, data, digest, active.upload.Options)
	if err != nil {
		return objectstore.Object{}, err
	}
	delete(store.uploads, upload.ID)
	return object, nil
}

// AbortMultipart discards an active upload without changing complete objects.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	active, ok := store.uploads[upload.ID]
	if !ok {
		return objectstore.ErrNotFound
	}
	if !sameUpload(active.upload, upload) {
		return objectstore.ErrInvalidPart
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(store.uploads, upload.ID)
	return nil
}

func (store *Store) putLocked(bucket objectstore.Bucket, key objectstore.Key, data []byte, digest [32]byte, options objectstore.PutOptions) (objectstore.Object, error) {
	current, exists := store.objectLocked(bucket, key)
	if options.IfAbsent && exists {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.IfMatchVersion != "" && (!exists || current.object.Version != options.IfMatchVersion) {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	store.nextVersion++
	version := strconv.FormatUint(store.nextVersion, 10)
	object := objectstore.Object{
		Bucket:     bucket,
		Key:        key,
		Version:    version,
		ETag:       hex.EncodeToString(digest[:]),
		Size:       int64(len(data)),
		ModifiedAt: time.Now().UTC(),
		SHA256:     digest,
		Metadata:   options.Clone().Metadata,
	}
	if store.objects[bucket] == nil {
		store.objects[bucket] = make(map[objectstore.Key]record)
	}
	store.objects[bucket][key] = record{object: object.Clone(), data: slices.Clone(data)}
	return object.Clone(), nil
}

func (store *Store) objectLocked(bucket objectstore.Bucket, key objectstore.Key) (record, bool) {
	objects, ok := store.objects[bucket]
	if !ok {
		return record{}, false
	}
	stored, ok := objects[key]
	return stored, ok
}

func sameUpload(want, got objectstore.Upload) bool {
	return want.ID == got.ID && want.Bucket == got.Bucket && want.Key == got.Key
}

func validateObjectInput(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(objectstore.ValidateBucket(bucket), objectstore.ValidateKey(key))
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func readAll(ctx context.Context, reader io.Reader) ([]byte, error) {
	if reader == nil {
		return nil, fmt.Errorf("memory store: nil reader")
	}
	data, err := io.ReadAll(contextReader{ctx: ctx, reader: reader})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

var _ objectstore.MultipartStore = (*Store)(nil)
