// Package sftp provides an object store on a remote directory reached through
// an SFTP client (github.com/pkg/sftp over golang.org/x/crypto/ssh). It ports
// the layout and commit protocol of the filesystem backend (backend/fs) to
// SFTP requests.
//
// # Layout
//
// Under the root directory, per bucket:
//
//	b-<hex bucket>/sftp.lock                                 write lock file
//	b-<hex bucket>/tmp/                                      staging files
//	b-<hex bucket>/objects/<key segments>/@object.json       metadata (commit point)
//	b-<hex bucket>/objects/<key segments>/@data-<version>    immutable bytes
//	b-<hex bucket>/uploads/<upload id>/                      multipart parts
//
// Every key segment is a directory, so keys such as "a" and "a/b" coexist.
// Segments beginning with "@" are stored with one extra leading "@", which
// keeps reserved "@object.json" and "@data-*" names out of the key space. The
// object layout matches backend/fs, so objects written by one backend can be
// read by the other; the two backends lock differently (backend/fs uses OS
// file locks on b-<hex bucket>/lock), so they must not write to the same
// directory concurrently.
//
// # Writes and reads
//
// Put streams the body into a remote staging file while hashing it (with
// pipelined SFTP writes, see pkg/sftp File.ReadFromWithConcurrency), verifies
// PutOptions.Size and PutOptions.SHA256, and only then publishes: under the
// bucket lock it renames the staging file to a fresh @data-<version> file and
// replaces @object.json, which names that data file. Replacing @object.json is
// the single commit point. When the server advertises
// posix-rename@openssh.com (OpenSSH and pkg/sftp servers do), the replacement
// is an atomic rename(2): readers never observe partial objects or mismatched
// data and metadata, and a crash leaves either the old or the new version.
// Without the extension the store first tries SSH_FXP_RENAME, which many
// servers implement with rename(2). Where the server refuses to rename over
// an existing file (SFTP v3 semantics, as in OpenSSH), it renames the current
// @object.json to @object.previous.json, renames the new file into place, and
// removes the previous file. That fallback is not one atomic step: lock-free
// readers that miss @object.json read @object.previous.json instead
// (alternating between the two names for a few rounds), so they observe the
// old or the new version, and a crash between the renames leaves the old
// version readable; but a reader that loses every round to back-to-back
// publications reports ErrNotFound, absent keys cost up to six metadata
// requests, and a crash after the second rename can leave an orphaned data
// file. Files are fsync'ed before publication
// only when the server advertises fsync@openssh.com.
//
// Readers take no lock. Get reads @object.json and opens the immutable data
// file it names, retrying when a concurrent writer replaced (and removed) that
// version in between. Ranged reads use ReadAt on the open data file. An open
// file stays readable after a concurrent replacement on POSIX servers; servers
// that cannot remove open files (Windows) leave the old data file behind.
//
// # Conditional writes and locking
//
// IfAbsent, IfMatchVersion, versioned Delete, multipart completion, Copy
// destination conditions, and DeleteMany are evaluated and published under a
// per-bucket lock. Within one Store the lock is an in-process semaphore, so
// goroutines queue without polling. Across processes (or Store values) the
// lock is the file b-<hex bucket>/sftp.lock, created with SSH_FXF_CREAT|
// SSH_FXF_EXCL and acquired with context-aware, jittered polling backoff
// (Options.LockPollInterval). The holder rewrites a heartbeat counter in the
// lock file every StaleLockAge/4; a waiter that sees unchanged lock contents
// for Options.StaleLockAge on its own monotonic clock treats the holder as
// dead and removes the lock (best-effort, the check and removal are not
// atomic).
//
// Guarantees: conditional writes are exact (linearizable compare-and-swap)
// among goroutines sharing one Store, and among processes as long as the
// server implements exclusive create atomically (open(O_EXCL) on a local
// filesystem does; some network filesystems behind the server may not) and no
// live lock is broken as stale. A holder whose heartbeats cannot reach the
// server for StaleLockAge (for example a stalled connection) can lose its
// lock, after which concurrent conditional writes are best-effort. A crashed
// holder delays other writers for up to StaleLockAge.
//
// # Multipart, copies, and batches
//
// Multipart parts are verified while they are written and stored as files in
// the upload directory. CompleteMultipart assembles them by streaming every
// part from the server through the client into a staging file while
// re-hashing it (SFTP has no portable server-side concatenation), so
// completion transfers the object twice more over the connection. Copy
// hard-links the immutable source data file with hardlink@openssh.com when
// the server advertises it (no data transfer); otherwise, or when linking
// fails, it streams the bytes through the client and verifies them against
// the source digest. StatMany reads metadata key by key; DeleteMany deletes
// under one bucket lock. SFTP has no presigned URLs, so Store does not
// implement objectstore.Presigner.
//
// # Contexts
//
// SFTP requests are not context-aware. The store checks the context before
// and between requests, and wraps every streamed body (uploads, part
// assembly, copies, and the reader returned by Get) in a reader that fails
// with the context error once the context ends. A request already sent to
// the server runs to completion. Bodies returned by Get are bound to the Get
// context.
//
// # Limitations
//
// Keys map to path segments, so empty or "." segments are rejected,
// case-insensitive server filesystems fold keys that differ only in case, and
// segment lengths and characters are bounded by the server filesystem.
// List reads one metadata file per listed key. Crashed writers leave staging
// files in tmp/ that are never published; they can be deleted when no write is
// in flight.
package sftp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
	pkgsftp "github.com/pkg/sftp"
)

const (
	metadataName   = "@object.json"
	previousName   = "@object.previous.json"
	dataPrefix     = "@data-"
	uploadName     = "upload.json"
	partPrefix     = "part-"
	lockName       = "sftp.lock"
	readRetryLimit = 64
	// metadataReadRounds bounds the lock-free metadata search when the
	// server lacks posix-rename@openssh.com.
	metadataReadRounds = 3

	extensionPosixRename = "posix-rename@openssh.com"
	extensionHardlink    = "hardlink@openssh.com"
	extensionFsync       = "fsync@openssh.com"

	// statusOpUnsupported is SSH_FX_OP_UNSUPPORTED.
	statusOpUnsupported = 8
)

// Default option values.
const (
	DefaultStaleLockAge     = time.Minute
	DefaultLockPollInterval = 100 * time.Millisecond
)

// Options configures a Store. The zero value selects the defaults.
type Options struct {
	// StaleLockAge is how long a waiter must observe unchanged bucket lock
	// contents before it removes the lock as abandoned. Lock holders
	// heartbeat every StaleLockAge/4. Zero selects DefaultStaleLockAge; a
	// negative value disables stale-lock breaking (and heartbeats), so a
	// crashed holder blocks conditional writes until its lock file is
	// removed by hand.
	StaleLockAge time.Duration
	// LockPollInterval caps the backoff between attempts to create a bucket
	// lock file held by another process. Attempts start after about 1ms and
	// double up to this cap. Zero selects DefaultLockPollInterval.
	LockPollInterval time.Duration
}

// Store is an object store in one directory of an SFTP server. It is safe for
// concurrent use by many goroutines, and by many processes sharing the remote
// directory within the guarantees described in the package documentation.
type Store struct {
	client      *pkgsftp.Client
	root        string
	posixRename bool
	hardlink    bool
	fsync       bool
	locker      *bucketLocker
}

// New opens a store rooted at the remote directory root, creating it if
// needed. A relative root is resolved by the server, normally against the
// login directory. The client stays owned by the caller, who must keep it
// open while the store is used and close it afterwards.
func New(client *pkgsftp.Client, root string, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("sftp store: nil client")
	}
	if root == "" {
		return nil, errors.New("sftp store: empty root")
	}
	if options.StaleLockAge == 0 {
		options.StaleLockAge = DefaultStaleLockAge
	}
	if options.LockPollInterval <= 0 {
		options.LockPollInterval = DefaultLockPollInterval
	}
	root = path.Clean(root)
	if err := client.MkdirAll(root); err != nil {
		return nil, fmt.Errorf("sftp store: create root: %w", err)
	}
	store := &Store{client: client, root: root}
	_, store.posixRename = client.HasExtension(extensionPosixRename)
	_, store.hardlink = client.HasExtension(extensionHardlink)
	version, ok := client.HasExtension(extensionFsync)
	store.fsync = ok && version == "1"
	store.locker = newBucketLocker(sftpLockFS{client: client}, options.StaleLockAge, options.LockPollInterval)
	return store, nil
}

// Root returns the remote root directory.
func (store *Store) Root() string { return store.root }

// AtomicReplace reports whether the server advertised
// posix-rename@openssh.com, which makes publication an atomic replacement.
// Without it the store uses SSH_FXP_RENAME and, on servers that refuse to
// rename over an existing file, the non-atomic two-rename fallback described
// in the package documentation.
func (store *Store) AtomicReplace() bool { return store.posixRename }

type metadataRecord struct {
	Version     string            `json:"version"`
	SHA256      string            `json:"sha256"`
	Size        int64             `json:"size"`
	ModifiedAt  time.Time         `json:"modified_at"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Put streams body into a remote staging file while hashing it, then
// publishes it under the bucket lock.
func (store *Store) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Object{}, err
	}
	staged, err := store.spool(ctx, bucket, body, options.Size)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer store.discard(staged)
	if options.SHA256 != nil && *options.SHA256 != staged.digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != staged.size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	record := metadataRecord{SHA256: hex.EncodeToString(staged.digest[:]), Size: staged.size, ContentType: options.ContentType, Metadata: options.Metadata}
	return store.commit(ctx, bucket, key, staged, record, options.IfAbsent, options.IfMatchVersion)
}

// Get opens an object or exact byte range. The returned body stays
// consistent with the returned metadata even if the object is replaced
// concurrently, and fails with the context error once ctx ends.
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
	var reader io.Reader = file
	if options.Range != nil {
		if options.Range.Offset+options.Range.Length > object.Size {
			return fail(objectstore.ErrInvalidRange)
		}
		reader = io.NewSectionReader(file, options.Range.Offset, options.Range.Length)
	}
	return &readCloser{Reader: contextReader{ctx: ctx, reader: reader}, closer: file}, object, nil
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

// Delete removes the matching object version under the bucket lock; an empty
// version is unconditional. Absent objects return ErrNotFound.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	unlock, err := store.lock(ctx, bucket)
	if err != nil {
		return err
	}
	defer unlock()
	current, _, err := store.readMetadataLocked(bucket, key)
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

// StatMany returns metadata for every key in input order, reading one
// metadata file per key.
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
// succeed. Cancellation stops before the next key; keys already deleted stay
// deleted.
func (store *Store) DeleteMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	unlock, err := store.lock(ctx, bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()
	results := make([]error, len(keys))
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateKey(key); err != nil {
			results[index] = err
			continue
		}
		current, _, err := store.readMetadataLocked(bucket, key)
		if errors.Is(err, objectstore.ErrNotFound) {
			continue
		}
		if err == nil {
			err = store.deleteLocked(bucket, key, current.Version)
			if errors.Is(err, objectstore.ErrNotFound) {
				err = nil
			}
		}
		results[index] = err
	}
	return results, nil
}

// Copy publishes the source bytes under the destination key. It hard-links
// the immutable source data file when the server supports
// hardlink@openssh.com and otherwise streams the bytes through the client,
// verifying them against the source digest.
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
		staged, err := store.stageCopy(ctx, store.dataPath(srcBucket, srcKey, source.Version), dstBucket, source)
		if errors.Is(err, os.ErrNotExist) && attempt < readRetryLimit {
			continue // the source version was replaced after its metadata was read
		}
		if err != nil {
			return objectstore.Object{}, err
		}
		defer store.discard(staged)
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

// List returns a stable key-ordered page. Every listed object carries full
// metadata, because the metadata file is read anyway to report the version.
// It walks only the directories that can contain keys after options.After
// with options.Prefix.
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
	entries, err := walker.store.client.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
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
			if err := walker.ctx.Err(); err != nil {
				return true, err
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
		stop, err := walker.walk(path.Join(directory, item.name), item.sortKey)
		if stop || err != nil {
			return stop, err
		}
	}
	return false, nil
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
		return objectstore.Object{}, fmt.Errorf("sftp store: encode metadata: %w", err)
	}
	// Stage the metadata file before taking the lock, so the lock covers
	// only the condition check and three renames or removals.
	metadataStage, err := store.writeStaging(ctx, bucket, data)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer store.discard(metadataStage)
	unlock, err := store.lock(ctx, bucket)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer unlock()
	if check != nil {
		if err := check(); err != nil {
			return objectstore.Object{}, err
		}
	}
	current, _, err := store.readMetadataLocked(bucket, key)
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
	if err := store.client.MkdirAll(directory); err != nil {
		return objectstore.Object{}, err
	}
	dataPath := store.dataPath(bucket, key, version)
	if err := store.client.Rename(staged.path, dataPath); err != nil {
		return objectstore.Object{}, err
	}
	staged.path = ""
	if err := store.publishMetadata(metadataStage.path, directory); err != nil {
		_ = store.client.Remove(dataPath)
		return objectstore.Object{}, err
	}
	metadataStage.path = ""
	if exists {
		_ = store.client.Remove(store.dataPath(bucket, key, current.Version))
	}
	if after != nil {
		after()
	}
	return decodeMetadata(bucket, key, data)
}

// publishMetadata makes the staged metadata file source the @object.json of
// directory. The bucket lock must be held.
//
// With posix-rename@openssh.com this is one atomic rename. Otherwise
// SSH_FXP_RENAME is tried first (many servers replace atomically); when the
// server refuses to overwrite, the current @object.json is renamed to
// @object.previous.json, the new file is renamed into place, and the previous
// file is removed. Readers that miss @object.json fall back to
// @object.previous.json, so they observe the old or the new version, and a
// crash between the renames leaves the old version readable.
func (store *Store) publishMetadata(source, directory string) error {
	target := path.Join(directory, metadataName)
	previous := path.Join(directory, previousName)
	if store.posixRename {
		err := store.client.PosixRename(source, target)
		if !isUnsupported(err) {
			return err
		}
	}
	err := store.client.Rename(source, target)
	if err != nil {
		if _, statErr := store.client.Lstat(target); statErr != nil {
			return err // the target does not block the rename
		}
		_ = store.client.Remove(previous)
		if err := store.client.Rename(target, previous); err != nil {
			return err
		}
		if err := store.client.Rename(source, target); err != nil {
			_ = store.client.Rename(previous, target)
			return err
		}
	}
	if !store.posixRename {
		_ = store.client.Remove(previous)
	}
	return nil
}

// replace renames source over target for files that readers do not use
// without the bucket lock (multipart parts). posix-rename@openssh.com
// replaces atomically; otherwise SSH_FXP_RENAME is tried first, and when the
// server refuses to overwrite an existing target, the target is removed and
// the rename retried.
func (store *Store) replace(source, target string) error {
	if store.posixRename {
		err := store.client.PosixRename(source, target)
		if !isUnsupported(err) {
			return err
		}
	}
	err := store.client.Rename(source, target)
	if err == nil {
		return nil
	}
	if _, statErr := store.client.Lstat(target); statErr != nil {
		return err // the target does not block the rename
	}
	if err := store.client.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return store.client.Rename(source, target)
}

// deleteLocked removes the commit point first, then the data file, then the
// now-empty key directories. The bucket lock must be held.
func (store *Store) deleteLocked(bucket objectstore.Bucket, key objectstore.Key, version string) error {
	directory := store.keyDirectory(bucket, key)
	err := store.client.Remove(path.Join(directory, metadataName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	removed := err == nil
	if !store.posixRename {
		// A crash during a fallback publication can leave the previous
		// metadata file, which readers would otherwise resurrect.
		err := store.client.Remove(path.Join(directory, previousName))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = removed || err == nil
	}
	if !removed {
		return objectstore.ErrNotFound
	}
	_ = store.client.Remove(store.dataPath(bucket, key, version))
	base := store.objectsPath(bucket)
	for directory != base && strings.HasPrefix(directory, base+"/") {
		if store.client.RemoveDirectory(directory) != nil {
			break
		}
		directory = path.Dir(directory)
	}
	return nil
}

// open reads metadata and opens the matching immutable data file, retrying
// when a concurrent writer replaces the object between the two steps.
func (store *Store) open(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (*pkgsftp.File, objectstore.Object, error) {
	for attempt := 0; ; attempt++ {
		object, _, err := store.readMetadata(bucket, key)
		if err != nil {
			return nil, objectstore.Object{}, err
		}
		file, err := store.client.Open(store.dataPath(bucket, key, object.Version))
		if err == nil {
			return file, object, nil
		}
		if !errors.Is(err, os.ErrNotExist) || attempt >= readRetryLimit {
			return nil, objectstore.Object{}, err
		}
		if err := ctx.Err(); err != nil {
			return nil, objectstore.Object{}, err
		}
	}
}

// readMetadata reads the current metadata without the bucket lock.
func (store *Store) readMetadata(bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, metadataRecord, error) {
	return store.readMetadataRounds(bucket, key, metadataReadRounds)
}

// readMetadataLocked reads the current metadata while the bucket lock is
// held, so no fallback publication can be in progress.
func (store *Store) readMetadataLocked(bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, metadataRecord, error) {
	return store.readMetadataRounds(bucket, key, 1)
}

// readMetadataRounds reads @object.json. Without posix-rename it falls back
// to @object.previous.json, which a fallback publication holds while
// @object.json is briefly absent, and alternates between the two names for
// up to rounds rounds before reporting ErrNotFound, because back-to-back
// publications can each open such a window.
func (store *Store) readMetadataRounds(bucket objectstore.Bucket, key objectstore.Key, rounds int) (objectstore.Object, metadataRecord, error) {
	directory := store.keyDirectory(bucket, key)
	names := []string{metadataName}
	if store.posixRename {
		rounds = 1
	} else {
		names = append(names, previousName)
	}
	var (
		data []byte
		err  error
	)
search:
	for range rounds {
		for _, name := range names {
			data, err = store.readFile(path.Join(directory, name))
			if !errors.Is(err, os.ErrNotExist) {
				break search
			}
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return objectstore.Object{}, metadataRecord{}, objectstore.ErrNotFound
	}
	if err != nil {
		return objectstore.Object{}, metadataRecord{}, err
	}
	var record metadataRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return objectstore.Object{}, metadataRecord{}, fmt.Errorf("sftp store: corrupt metadata for %s/%s: %w", bucket, key, err)
	}
	object, err := decodeMetadata(bucket, key, data)
	return object, record, err
}

func (store *Store) readFile(name string) ([]byte, error) {
	file, err := store.client.Open(name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(file)
	return data, errors.Join(err, file.Close())
}

func decodeMetadata(bucket objectstore.Bucket, key objectstore.Key, data []byte) (objectstore.Object, error) {
	var record metadataRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return objectstore.Object{}, fmt.Errorf("sftp store: corrupt metadata for %s/%s: %w", bucket, key, err)
	}
	digest, err := hex.DecodeString(record.SHA256)
	if err != nil || len(digest) != sha256.Size || !isHex(record.Version) || record.Size < 0 {
		return objectstore.Object{}, fmt.Errorf("sftp store: corrupt metadata for %s/%s", bucket, key)
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
func (store *Store) discard(staged *stagedFile) {
	if staged != nil && staged.path != "" {
		_ = store.client.Remove(staged.path)
		staged.path = ""
	}
}

// spool streams body into a new staging file while hashing it. A positive
// size bounds the read to size+1 bytes, which is enough to detect a size
// mismatch without consuming an oversized body.
func (store *Store) spool(ctx context.Context, bucket objectstore.Bucket, body io.Reader, size int64) (*stagedFile, error) {
	if body == nil {
		return nil, errors.New("sftp store: nil reader")
	}
	if size > 0 && size < 1<<62 {
		body = io.LimitReader(body, size+1)
	}
	file, staged, err := store.createStaging(ctx, bucket, "stage-")
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	staged.size, err = file.ReadFromWithConcurrency(contextReader{ctx: ctx, reader: io.TeeReader(body, hash)}, 0)
	if err == nil {
		err = ctx.Err()
	}
	if err := store.finish(file, err); err != nil {
		store.discard(staged)
		return nil, err
	}
	copy(staged.digest[:], hash.Sum(nil))
	return staged, nil
}

// stageCopy places a copy of the immutable data file source in the
// destination bucket's staging directory: a hard link when the server
// supports it, otherwise a verified streamed copy.
func (store *Store) stageCopy(ctx context.Context, source string, bucket objectstore.Bucket, object objectstore.Object) (*stagedFile, error) {
	if store.hardlink {
		name, err := randomHex()
		if err != nil {
			return nil, err
		}
		if err := store.client.MkdirAll(store.stagingPath(bucket)); err != nil {
			return nil, err
		}
		staged := &stagedFile{path: path.Join(store.stagingPath(bucket), "copy-"+name), size: object.Size, digest: object.SHA256}
		err = store.client.Link(source, staged.path)
		if err == nil {
			return staged, nil
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		// Linking can fail across filesystems or on servers that advertise
		// the extension without supporting it everywhere: stream instead.
	}
	input, err := store.client.Open(source)
	if err != nil {
		return nil, err
	}
	defer func() { _ = input.Close() }()
	staged, err := store.spool(ctx, bucket, input, object.Size)
	if err != nil {
		return nil, err
	}
	if staged.size != object.Size || staged.digest != object.SHA256 {
		store.discard(staged)
		return nil, fmt.Errorf("sftp store: copy of %s does not match its recorded size and digest", source)
	}
	return staged, nil
}

func (store *Store) createStaging(ctx context.Context, bucket objectstore.Bucket, prefix string) (*pkgsftp.File, *stagedFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	name, err := randomHex()
	if err != nil {
		return nil, nil, err
	}
	staged := &stagedFile{path: path.Join(store.stagingPath(bucket), prefix+name)}
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	file, err := store.client.OpenFile(staged.path, flags)
	if errors.Is(err, os.ErrNotExist) {
		if err := store.client.MkdirAll(store.stagingPath(bucket)); err != nil {
			return nil, nil, err
		}
		file, err = store.client.OpenFile(staged.path, flags)
	}
	if err != nil {
		return nil, nil, err
	}
	return file, staged, nil
}

// writeStaging writes data to a new staging file.
func (store *Store) writeStaging(ctx context.Context, bucket objectstore.Bucket, data []byte) (*stagedFile, error) {
	file, staged, err := store.createStaging(ctx, bucket, "meta-")
	if err != nil {
		return nil, err
	}
	_, err = file.Write(data)
	if err := store.finish(file, err); err != nil {
		store.discard(staged)
		return nil, err
	}
	return staged, nil
}

// finish flushes (when the server supports fsync@openssh.com) and closes a
// written file, returning the first error including writeErr.
func (store *Store) finish(file *pkgsftp.File, writeErr error) error {
	if writeErr == nil && store.fsync {
		writeErr = file.Sync()
	}
	return errors.Join(writeErr, file.Close())
}

func (store *Store) lock(ctx context.Context, bucket objectstore.Bucket) (func(), error) {
	return store.locker.lock(ctx, path.Join(store.bucketPath(bucket), lockName))
}

func (store *Store) bucketPath(bucket objectstore.Bucket) string {
	return path.Join(store.root, "b-"+hex.EncodeToString([]byte(bucket)))
}

func (store *Store) stagingPath(bucket objectstore.Bucket) string {
	return path.Join(store.bucketPath(bucket), "tmp")
}

func (store *Store) objectsPath(bucket objectstore.Bucket) string {
	return path.Join(store.bucketPath(bucket), "objects")
}

func (store *Store) uploadPath(bucket objectstore.Bucket, id string) string {
	return path.Join(store.bucketPath(bucket), "uploads", id)
}

func (store *Store) keyDirectory(bucket objectstore.Bucket, key objectstore.Key) string {
	return path.Join(store.objectsPath(bucket), escapeKey(key))
}

func (store *Store) dataPath(bucket objectstore.Bucket, key objectstore.Key, version string) string {
	return path.Join(store.keyDirectory(bucket, key), dataPrefix+version)
}

// escapeKey maps a valid key to its relative directory path.
func escapeKey(key objectstore.Key) string {
	segments := strings.Split(string(key), "/")
	for index, segment := range segments {
		segments[index] = escapeSegment(segment)
	}
	return strings.Join(segments, "/")
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

// validateKey applies the portable key rules plus the path mapping rules: no
// empty or "." segments.
func validateKey(key objectstore.Key) error {
	if err := objectstore.ValidateKey(key); err != nil {
		return err
	}
	for _, segment := range strings.Split(string(key), "/") {
		if segment == "" || segment == "." {
			return fmt.Errorf("%w: empty or '.' segment is not representable as a remote path", objectstore.ErrInvalidKey)
		}
	}
	return nil
}

func isUnsupported(err error) bool {
	var status *pkgsftp.StatusError
	return errors.As(err, &status) && status.Code == statusOpUnsupported
}

func randomHex() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("sftp store: random identifier: %w", err)
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

// contextReader fails with the context error once ctx ends.
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

type readCloser struct {
	io.Reader
	closer io.Closer
}

func (reader *readCloser) Close() error { return reader.closer.Close() }

var (
	_ objectstore.MultipartStore = (*Store)(nil)
	_ objectstore.BatchStater    = (*Store)(nil)
	_ objectstore.BatchDeleter   = (*Store)(nil)
	_ objectstore.Copier         = (*Store)(nil)
)
