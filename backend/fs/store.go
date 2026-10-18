// Package fs provides a filesystem object store that is safe for concurrent
// use by many goroutines and many processes sharing one root directory.
//
// Layout under the root directory, per bucket:
//
//	b-<hex bucket>/lock                                      write lock file
//	b-<hex bucket>/tmp/                                      staging files
//	b-<hex bucket>/objects/<key segments>/@object.json       metadata (commit point)
//	b-<hex bucket>/objects/<key segments>/@data-<version>    immutable bytes
//	b-<hex bucket>/uploads/<upload id>/                      multipart parts
//
// Every key segment is a directory, so keys such as "a" and "a/b" coexist.
// Segments beginning with "@" are stored with one extra leading "@", which
// keeps reserved "@object.json" and "@data-*" names out of the key space.
//
// Writes stream into a staging file while hashing, then publish with renames
// while holding an exclusive OS file lock (flock on Unix, LockFileEx on
// Windows) on the bucket lock file. The rename of @object.json is the single
// commit point: it names the immutable data file of the new version, so
// readers never observe partial objects or mismatched data and metadata, and
// a crash leaves either the old or the new version. IfAbsent and
// IfMatchVersion are evaluated under the same lock and are therefore atomic
// across processes. Readers take no lock. Copies hard-link the immutable data
// file when the filesystem allows it.
//
// Limitations: keys map to path segments, so empty or "." segments are
// rejected, case-insensitive filesystems fold keys that differ only in case,
// and segment lengths are bounded by the filesystem. Cross-process safety
// requires a filesystem with working advisory locks; on platforms without
// flock or LockFileEx the store is safe only within one process.
package fs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
)

const (
	metadataName   = "@object.json"
	dataPrefix     = "@data-"
	uploadName     = "upload.json"
	partPrefix     = "part-"
	readRetryLimit = 16
)

// Store is a filesystem object store rooted at one directory.
type Store struct {
	root string
}

// New opens a store rooted at directory, creating the directory if needed.
func New(directory string) (*Store, error) {
	if directory == "" {
		return nil, errors.New("fs store: empty root")
	}
	root, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("fs store: resolve root: %w", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("fs store: create root: %w", err)
	}
	return &Store{root: root}, nil
}

// Root returns the absolute root directory.
func (store *Store) Root() string { return store.root }

type metadataRecord struct {
	Version     string            `json:"version"`
	SHA256      string            `json:"sha256"`
	Size        int64             `json:"size"`
	ModifiedAt  time.Time         `json:"modified_at"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Put streams body into a staging file while hashing it, then publishes it
// atomically under the bucket lock.
func (store *Store) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Object{}, err
	}
	staged, err := store.spool(ctx, bucket, body)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer staged.discard()
	if options.SHA256 != nil && *options.SHA256 != staged.digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != staged.size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	record := metadataRecord{SHA256: hex.EncodeToString(staged.digest[:]), Size: staged.size, ContentType: options.ContentType, Metadata: options.Metadata}
	return store.commit(ctx, bucket, key, staged, record, options.IfAbsent, options.IfMatchVersion)
}

// Get opens an object or exact byte range. The open file stays consistent
// with the returned metadata even if the object is replaced concurrently.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return nil, objectstore.Object{}, err
	}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
	}
	file, object, err := store.open(ctx, bucket, key)
	if err != nil {
		return nil, objectstore.Object{}, err
	}
	fail := func(err error) (io.ReadCloser, objectstore.Object, error) {
		_ = file.Close()
		return nil, objectstore.Object{}, err
	}
	if options.IfMatchVersion != "" && options.IfMatchVersion != object.Version {
		return fail(objectstore.ErrPrecondition)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if options.Range == nil {
		return file, object, nil
	}
	if options.Range.Offset+options.Range.Length > object.Size {
		return fail(objectstore.ErrInvalidRange)
	}
	return &sectionReadCloser{SectionReader: io.NewSectionReader(file, options.Range.Offset, options.Range.Length), file: file}, object, nil
}

// Stat returns object metadata without opening object bytes.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	object, _, err := store.readMetadata(bucket, key)
	if err != nil {
		return objectstore.Object{}, err
	}
	return object, ctx.Err()
}

// Delete removes the matching object version; an empty version is
// unconditional. Absent objects return ErrNotFound.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	unlock, err := store.lock(bucket)
	if err != nil {
		return err
	}
	defer unlock()
	current, _, err := store.readMetadata(bucket, key)
	if err != nil {
		return err
	}
	if version != "" && version != current.Version {
		return objectstore.ErrPrecondition
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.deleteLocked(bucket, key, current.Version)
}

// StatMany returns metadata for every key in input order.
func (store *Store) StatMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]objectstore.StatResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]objectstore.StatResult, len(keys))
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateKey(key); err != nil {
			results[index].Err = err
			continue
		}
		results[index].Object, _, results[index].Err = store.readMetadata(bucket, key)
	}
	return results, nil
}

// DeleteMany removes every listed key under one bucket lock; absent keys
// succeed.
func (store *Store) DeleteMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	unlock, err := store.lock(bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results := make([]error, len(keys))
	for index, key := range keys {
		if err := validateKey(key); err != nil {
			results[index] = err
			continue
		}
		current, _, err := store.readMetadata(bucket, key)
		if errors.Is(err, objectstore.ErrNotFound) {
			continue
		}
		if err == nil {
			err = store.deleteLocked(bucket, key, current.Version)
		}
		results[index] = err
	}
	return results, nil
}

// Copy publishes the source bytes under the destination key, hard-linking the
// immutable source data file when possible.
func (store *Store) Copy(ctx context.Context, srcBucket objectstore.Bucket, srcKey objectstore.Key, dstBucket objectstore.Bucket, dstKey objectstore.Key, options objectstore.CopyOptions) (objectstore.Object, error) {
	if err := errors.Join(validateObjectInput(ctx, srcBucket, srcKey), validateObjectInput(ctx, dstBucket, dstKey)); err != nil {
		return objectstore.Object{}, err
	}
	for attempt := 0; ; attempt++ {
		source, record, err := store.readMetadata(srcBucket, srcKey)
		if err != nil {
			return objectstore.Object{}, err
		}
		if options.SourceVersion != "" && options.SourceVersion != source.Version {
			return objectstore.Object{}, objectstore.ErrPrecondition
		}
		staged, err := store.stageCopy(ctx, store.dataPath(srcBucket, srcKey, source.Version), dstBucket)
		if errors.Is(err, iofs.ErrNotExist) && attempt < readRetryLimit {
			continue // the source version was replaced after its metadata was read
		}
		if err != nil {
			return objectstore.Object{}, err
		}
		defer staged.discard()
		staged.size, staged.digest = source.Size, source.SHA256
		record.Version = ""
		if options.Metadata != nil {
			record.Metadata = options.Metadata
		}
		if options.ContentType != "" {
			record.ContentType = options.ContentType
		}
		return store.commit(ctx, dstBucket, dstKey, staged, record, options.IfAbsent, options.IfMatchVersion)
	}
}

// List returns a stable key-ordered page with full metadata. It walks only
// the directories that can contain keys after options.After with
// options.Prefix.
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
	walker := &listWalker{store: store, ctx: ctx, bucket: bucket, options: options}
	startDirectory, startPrefix := store.objectsPath(bucket), ""
	if index := strings.LastIndexByte(options.Prefix, '/'); index >= 0 {
		head := options.Prefix[:index]
		if validateKey(objectstore.Key(head)) == nil {
			startDirectory, startPrefix = store.keyDirectory(bucket, objectstore.Key(head)), head+"/"
		}
	}
	if _, err := walker.walk(startDirectory, startPrefix); err != nil {
		return objectstore.Page{}, err
	}
	page := objectstore.Page{Objects: walker.objects}
	if options.Limit > 0 && len(page.Objects) > options.Limit {
		page.Objects = page.Objects[:options.Limit]
		page.NextAfter = page.Objects[len(page.Objects)-1].Key
	}
	if err := ctx.Err(); err != nil {
		return objectstore.Page{}, err
	}
	return page, nil
}

type listWalker struct {
	store   *Store
	ctx     context.Context
	bucket  objectstore.Bucket
	options objectstore.ListOptions
	objects []objectstore.Object
}

type listItem struct {
	sortKey string
	name    string
	object  bool
}

// walk visits keys in byte order. A segment name contributes the key
// prefix+name and the subtree prefix+name+"/"; sorting those strings yields
// global key order because no other segment can fall between a subtree's
// prefix and its keys.
func (walker *listWalker) walk(directory, prefix string) (bool, error) {
	if err := walker.ctx.Err(); err != nil {
		return true, err
	}
	entries, err := os.ReadDir(directory)
	if errors.Is(err, iofs.ErrNotExist) || isNotDirectory(err) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	items := make([]listItem, 0, 2*len(entries))
	for _, entry := range entries {
		name, ok := unescapeSegment(entry.Name())
		if !ok || !entry.IsDir() {
			continue
		}
		items = append(items, listItem{sortKey: prefix + name, name: entry.Name(), object: true}, listItem{sortKey: prefix + name + "/", name: entry.Name()})
	}
	sort.Slice(items, func(left, right int) bool { return items[left].sortKey < items[right].sortKey })
	want, after := walker.options.Prefix, string(walker.options.After)
	for _, item := range items {
		if want != "" && item.sortKey > want && !strings.HasPrefix(item.sortKey, want) {
			return true, nil // every later key sorts after all keys with the prefix
		}
		if item.object {
			if !strings.HasPrefix(item.sortKey, want) || item.sortKey <= after {
				continue
			}
			object, _, err := walker.store.readMetadata(walker.bucket, objectstore.Key(item.sortKey))
			if errors.Is(err, objectstore.ErrNotFound) {
				continue
			}
			if err != nil {
				return true, err
			}
			walker.objects = append(walker.objects, object)
			if walker.options.Limit > 0 && len(walker.objects) > walker.options.Limit {
				return true, nil
			}
			continue
		}
		if !strings.HasPrefix(item.sortKey, want) && !strings.HasPrefix(want, item.sortKey) {
			continue
		}
		if after >= item.sortKey && !strings.HasPrefix(after, item.sortKey) {
			continue // every key in the subtree sorts before After
		}
		stop, err := walker.walk(filepath.Join(directory, item.name), item.sortKey)
		if stop || err != nil {
			return stop, err
		}
	}
	return false, nil
}

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
		return objectstore.Upload{}, fmt.Errorf("fs store: encode upload: %w", err)
	}
	directory := store.uploadPath(bucket, id)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return objectstore.Upload{}, err
	}
	if err := store.writeFileAtomic(bucket, filepath.Join(directory, uploadName), data); err != nil {
		_ = os.RemoveAll(directory)
		return objectstore.Upload{}, err
	}
	if err := ctx.Err(); err != nil {
		_ = os.RemoveAll(directory)
		return objectstore.Upload{}, err
	}
	return objectstore.Upload{ID: id, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart verifies and stores one numbered part.
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
	staged, err := store.spool(ctx, upload.Bucket, body)
	if err != nil {
		return objectstore.CompletedPart{}, err
	}
	defer staged.discard()
	if staged.size != size || staged.digest != digest {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	if err := ctx.Err(); err != nil {
		return objectstore.CompletedPart{}, err
	}
	if err := os.Rename(staged.path, filepath.Join(store.uploadPath(upload.Bucket, upload.ID), partPrefix+strconv.Itoa(number))); err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return objectstore.CompletedPart{}, objectstore.ErrNotFound
		}
		return objectstore.CompletedPart{}, err
	}
	return objectstore.CompletedPart{Number: number, ETag: hex.EncodeToString(digest[:]), Size: size, SHA256: digest}, nil
}

// CompleteMultipart assembles ordered parts while re-verifying their digests,
// then publishes atomically with the upload's put conditions.
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
	defer staged.discard()
	options := record.Options
	if options.SHA256 != nil && *options.SHA256 != staged.digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != staged.size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	metadata := metadataRecord{SHA256: hex.EncodeToString(staged.digest[:]), Size: staged.size, ContentType: options.ContentType, Metadata: options.Metadata}
	return store.commitWith(ctx, record.Bucket, record.Key, staged, metadata, options.IfAbsent, options.IfMatchVersion, func() error {
		if _, err := os.Stat(filepath.Join(store.uploadPath(record.Bucket, record.ID), uploadName)); err != nil {
			return objectstore.ErrNotFound // aborted while assembling
		}
		return nil
	}, func() {
		_ = os.RemoveAll(store.uploadPath(record.Bucket, record.ID))
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
	unlock, err := store.lock(upload.Bucket)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(store.uploadPath(upload.Bucket, upload.ID))
}

func (store *Store) assemble(ctx context.Context, record uploadRecord, parts []objectstore.CompletedPart) (*stagedFile, error) {
	file, err := store.createStaging(record.Bucket)
	if err != nil {
		return nil, err
	}
	staged := &stagedFile{path: file.Name()}
	fail := func(err error) (*stagedFile, error) {
		_ = file.Close()
		staged.discard()
		return nil, err
	}
	whole := sha256.New()
	for _, part := range parts {
		source, err := os.Open(filepath.Join(store.uploadPath(record.Bucket, record.ID), partPrefix+strconv.Itoa(part.Number)))
		if errors.Is(err, iofs.ErrNotExist) {
			return fail(objectstore.ErrInvalidPart)
		}
		if err != nil {
			return fail(err)
		}
		partHash := sha256.New()
		written, err := io.Copy(io.MultiWriter(file, whole, partHash), contextReader{ctx: ctx, reader: source})
		closeErr := source.Close()
		if err != nil || closeErr != nil {
			return fail(errors.Join(err, closeErr))
		}
		if written != part.Size || !bytesEqual(partHash.Sum(nil), part.SHA256[:]) {
			return fail(objectstore.ErrInvalidPart)
		}
		staged.size += written
	}
	if err := finishFile(file); err != nil {
		staged.discard()
		return nil, err
	}
	copy(staged.digest[:], whole.Sum(nil))
	return staged, nil
}

func (store *Store) loadUpload(upload objectstore.Upload) (uploadRecord, error) {
	if err := errors.Join(objectstore.ValidateBucket(upload.Bucket), validateKey(upload.Key)); err != nil {
		return uploadRecord{}, err
	}
	if !isHex(upload.ID) {
		return uploadRecord{}, objectstore.ErrInvalidPart
	}
	data, err := os.ReadFile(filepath.Join(store.uploadPath(upload.Bucket, upload.ID), uploadName))
	if errors.Is(err, iofs.ErrNotExist) {
		return uploadRecord{}, objectstore.ErrNotFound
	}
	if err != nil {
		return uploadRecord{}, err
	}
	var record uploadRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return uploadRecord{}, fmt.Errorf("fs store: corrupt upload %s: %w", upload.ID, err)
	}
	if record.ID != upload.ID || record.Bucket != upload.Bucket || record.Key != upload.Key {
		return uploadRecord{}, objectstore.ErrInvalidPart
	}
	return record, nil
}

func (store *Store) commit(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, staged *stagedFile, record metadataRecord, ifAbsent bool, ifMatch string) (objectstore.Object, error) {
	return store.commitWith(ctx, bucket, key, staged, record, ifAbsent, ifMatch, nil, nil)
}

// commitWith publishes staged bytes under the bucket lock. check runs under
// the lock before any condition; after runs under the lock after success.
func (store *Store) commitWith(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, staged *stagedFile, record metadataRecord, ifAbsent bool, ifMatch string, check func() error, after func()) (objectstore.Object, error) {
	version, err := randomHex()
	if err != nil {
		return objectstore.Object{}, err
	}
	record.Version = version
	record.ModifiedAt = time.Now().UTC()
	record.Metadata = cloneMetadata(record.Metadata)
	data, err := json.Marshal(record)
	if err != nil {
		return objectstore.Object{}, fmt.Errorf("fs store: encode metadata: %w", err)
	}
	unlock, err := store.lock(bucket)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer unlock()
	if check != nil {
		if err := check(); err != nil {
			return objectstore.Object{}, err
		}
	}
	current, _, err := store.readMetadata(bucket, key)
	exists := err == nil
	if err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return objectstore.Object{}, err
	}
	if (ifAbsent && exists) || (ifMatch != "" && (!exists || current.Version != ifMatch)) {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	directory := store.keyDirectory(bucket, key)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return objectstore.Object{}, err
	}
	if err := os.Rename(staged.path, store.dataPath(bucket, key, version)); err != nil {
		return objectstore.Object{}, err
	}
	staged.path = ""
	if err := store.writeFileAtomic(bucket, filepath.Join(directory, metadataName), data); err != nil {
		_ = os.Remove(store.dataPath(bucket, key, version))
		return objectstore.Object{}, err
	}
	if exists {
		_ = os.Remove(store.dataPath(bucket, key, current.Version))
	}
	if after != nil {
		after()
	}
	return decodeMetadata(bucket, key, data)
}

// deleteLocked removes the commit point first, then the data file, then the
// now-empty key directories. The bucket lock must be held.
func (store *Store) deleteLocked(bucket objectstore.Bucket, key objectstore.Key, version string) error {
	directory := store.keyDirectory(bucket, key)
	if err := os.Remove(filepath.Join(directory, metadataName)); err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return objectstore.ErrNotFound
		}
		return err
	}
	_ = os.Remove(store.dataPath(bucket, key, version))
	base := store.objectsPath(bucket)
	for directory != base && strings.HasPrefix(directory, base) {
		if os.Remove(directory) != nil {
			break
		}
		directory = filepath.Dir(directory)
	}
	return nil
}

// open reads metadata and opens the matching immutable data file, retrying
// when a concurrent writer replaces the object between the two steps.
func (store *Store) open(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (*os.File, objectstore.Object, error) {
	for attempt := 0; ; attempt++ {
		object, _, err := store.readMetadata(bucket, key)
		if err != nil {
			return nil, objectstore.Object{}, err
		}
		file, err := os.Open(store.dataPath(bucket, key, object.Version))
		if err == nil {
			return file, object, nil
		}
		if !errors.Is(err, iofs.ErrNotExist) || attempt >= readRetryLimit {
			return nil, objectstore.Object{}, err
		}
		if err := ctx.Err(); err != nil {
			return nil, objectstore.Object{}, err
		}
	}
}

func (store *Store) readMetadata(bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, metadataRecord, error) {
	data, err := os.ReadFile(filepath.Join(store.keyDirectory(bucket, key), metadataName))
	if errors.Is(err, iofs.ErrNotExist) || isNotDirectory(err) {
		return objectstore.Object{}, metadataRecord{}, objectstore.ErrNotFound
	}
	if err != nil {
		return objectstore.Object{}, metadataRecord{}, err
	}
	var record metadataRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return objectstore.Object{}, metadataRecord{}, fmt.Errorf("fs store: corrupt metadata for %s/%s: %w", bucket, key, err)
	}
	object, err := decodeMetadata(bucket, key, data)
	return object, record, err
}

func decodeMetadata(bucket objectstore.Bucket, key objectstore.Key, data []byte) (objectstore.Object, error) {
	var record metadataRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return objectstore.Object{}, fmt.Errorf("fs store: corrupt metadata for %s/%s: %w", bucket, key, err)
	}
	digest, err := hex.DecodeString(record.SHA256)
	if err != nil || len(digest) != sha256.Size || !isHex(record.Version) || record.Size < 0 {
		return objectstore.Object{}, fmt.Errorf("fs store: corrupt metadata for %s/%s", bucket, key)
	}
	object := objectstore.Object{
		Bucket: bucket, Key: key, Version: record.Version, ETag: record.SHA256, Size: record.Size,
		ModifiedAt: record.ModifiedAt, SHA256Known: true, Metadata: record.Metadata,
	}
	copy(object.SHA256[:], digest)
	return object, nil
}

type stagedFile struct {
	path   string
	size   int64
	digest [32]byte
}

// discard removes a staging file that was not published.
func (staged *stagedFile) discard() {
	if staged != nil && staged.path != "" {
		_ = os.Remove(staged.path)
		staged.path = ""
	}
}

func (store *Store) spool(ctx context.Context, bucket objectstore.Bucket, body io.Reader) (*stagedFile, error) {
	if body == nil {
		return nil, errors.New("fs store: nil reader")
	}
	file, err := store.createStaging(bucket)
	if err != nil {
		return nil, err
	}
	staged := &stagedFile{path: file.Name()}
	hash := sha256.New()
	staged.size, err = io.Copy(io.MultiWriter(file, hash), contextReader{ctx: ctx, reader: body})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		_ = file.Close()
		staged.discard()
		return nil, err
	}
	if err := finishFile(file); err != nil {
		staged.discard()
		return nil, err
	}
	copy(staged.digest[:], hash.Sum(nil))
	return staged, nil
}

// stageCopy hard-links an immutable data file into the destination staging
// directory, falling back to a byte copy across devices or on filesystems
// without hard links.
func (store *Store) stageCopy(ctx context.Context, source string, bucket objectstore.Bucket) (*stagedFile, error) {
	if err := os.MkdirAll(store.stagingPath(bucket), 0o755); err != nil {
		return nil, err
	}
	name, err := randomHex()
	if err != nil {
		return nil, err
	}
	staged := &stagedFile{path: filepath.Join(store.stagingPath(bucket), "copy-"+name)}
	if err := os.Link(source, staged.path); err == nil {
		return staged, nil
	} else if errors.Is(err, iofs.ErrNotExist) {
		return nil, err
	}
	input, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	defer func() { _ = input.Close() }()
	output, err := os.OpenFile(staged.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(output, contextReader{ctx: ctx, reader: input}); err != nil {
		_ = output.Close()
		staged.discard()
		return nil, err
	}
	if err := finishFile(output); err != nil {
		staged.discard()
		return nil, err
	}
	return staged, nil
}

func (store *Store) createStaging(bucket objectstore.Bucket) (*os.File, error) {
	directory := store.stagingPath(bucket)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(directory, "stage-*")
}

func (store *Store) writeFileAtomic(bucket objectstore.Bucket, path string, data []byte) error {
	file, err := store.createStaging(bucket)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return err
	}
	if err := finishFile(file); err != nil {
		_ = os.Remove(file.Name())
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		_ = os.Remove(file.Name())
		return err
	}
	return nil
}

// finishFile flushes file contents to stable storage and closes the file.
func finishFile(file *os.File) error {
	return errors.Join(file.Sync(), file.Close())
}

func (store *Store) bucketPath(bucket objectstore.Bucket) string {
	return filepath.Join(store.root, "b-"+hex.EncodeToString([]byte(bucket)))
}

func (store *Store) stagingPath(bucket objectstore.Bucket) string {
	return filepath.Join(store.bucketPath(bucket), "tmp")
}

func (store *Store) objectsPath(bucket objectstore.Bucket) string {
	return filepath.Join(store.bucketPath(bucket), "objects")
}

func (store *Store) uploadPath(bucket objectstore.Bucket, id string) string {
	return filepath.Join(store.bucketPath(bucket), "uploads", id)
}

func (store *Store) keyDirectory(bucket objectstore.Bucket, key objectstore.Key) string {
	segments := strings.Split(string(key), "/")
	for index, segment := range segments {
		segments[index] = escapeSegment(segment)
	}
	return filepath.Join(append([]string{store.objectsPath(bucket)}, segments...)...)
}

func (store *Store) dataPath(bucket objectstore.Bucket, key objectstore.Key, version string) string {
	return filepath.Join(store.keyDirectory(bucket, key), dataPrefix+version)
}

func (store *Store) lock(bucket objectstore.Bucket) (func(), error) {
	directory := store.bucketPath(bucket)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	return lockFile(filepath.Join(directory, "lock"))
}

func escapeSegment(segment string) string {
	if strings.HasPrefix(segment, "@") {
		return "@" + segment
	}
	return segment
}

func unescapeSegment(name string) (string, bool) {
	if strings.HasPrefix(name, "@@") {
		return name[1:], true
	}
	if strings.HasPrefix(name, "@") {
		return "", false
	}
	return name, true
}

func validateObjectInput(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(objectstore.ValidateBucket(bucket), validateKey(key))
}

// validateKey applies the portable key rules plus the filesystem mapping
// rules: no empty or "." segments.
func validateKey(key objectstore.Key) error {
	if err := objectstore.ValidateKey(key); err != nil {
		return err
	}
	for _, segment := range strings.Split(string(key), "/") {
		if segment == "" || segment == "." {
			return fmt.Errorf("%w: empty or '.' segment is not representable on a filesystem", objectstore.ErrInvalidKey)
		}
	}
	return nil
}

func randomHex() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("fs store: random identifier: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func isHex(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func isNotDirectory(err error) bool { return errors.Is(err, syscall.ENOTDIR) }

func bytesEqual(left, right []byte) bool { return string(left) == string(right) }

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	clone := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
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

type sectionReadCloser struct {
	*io.SectionReader
	file *os.File
}

func (reader *sectionReadCloser) Close() error { return reader.file.Close() }

var (
	_ objectstore.MultipartStore = (*Store)(nil)
	_ objectstore.BatchStater    = (*Store)(nil)
	_ objectstore.BatchDeleter   = (*Store)(nil)
	_ objectstore.Copier         = (*Store)(nil)
)
