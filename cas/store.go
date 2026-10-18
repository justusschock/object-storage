package cas

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	cache "github.com/justusschock/go-cache"
	coordination "github.com/justusschock/go-distributed-coordination"
	objectstore "github.com/justusschock/go-object-storage"
)

const leaseTTL = time.Minute

var errFillComplete = errors.New("cas: fill complete")

// Store keeps immutable SHA-256-addressed objects. It is safe for concurrent
// use, also by many processes that share the object store and the
// coordination backend.
type Store struct {
	objects      objectstore.Store
	bucket       objectstore.Bucket
	prefix       string
	metadata     cache.Cache[string, objectstore.Object]
	coordination coordination.Backend
	owner        string
}

// New constructs a content-addressed store.
func New(objects objectstore.Store, bucket objectstore.Bucket, prefix string, metadata cache.Cache[string, objectstore.Object], backend coordination.Backend, owner string) (*Store, error) {
	if isNil(objects) {
		return nil, errors.New("cas: nil object store")
	}
	if isNil(backend) {
		return nil, errors.New("cas: nil coordination backend")
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateKey(objectstore.Key(prefix + "/digest")); err != nil || strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("cas: invalid prefix %q", prefix)
	}
	if owner == "" {
		return nil, errors.New("cas: empty owner")
	}
	if isNil(metadata) {
		metadata = nil
	}
	return &Store{objects: objects, bucket: bucket, prefix: prefix, metadata: metadata, coordination: backend, owner: owner}, nil
}

// Put streams content through an owner-only temporary file and stores it once.
func (store *Store) Put(ctx context.Context, reader io.Reader) (digest Digest, object objectstore.Object, returnErr error) {
	if err := ctx.Err(); err != nil {
		return Digest{}, objectstore.Object{}, err
	}
	if isNil(reader) {
		return Digest{}, objectstore.Object{}, errors.New("cas: nil reader")
	}
	temporary, err := os.CreateTemp("", "go-object-storage-cas-*")
	if err != nil {
		return Digest{}, objectstore.Object{}, fmt.Errorf("cas: create temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	defer func() {
		returnErr = errors.Join(returnErr, temporary.Close())
	}()

	hasher := sha256.New()
	if _, err := copyContext(ctx, io.MultiWriter(temporary, hasher), reader); err != nil {
		return Digest{}, objectstore.Object{}, err
	}
	copy(digest[:], hasher.Sum(nil))
	if _, err := temporary.Seek(0, io.SeekStart); err != nil {
		return Digest{}, objectstore.Object{}, fmt.Errorf("cas: rewind temporary file: %w", err)
	}

	lease, err := store.coordination.Acquire(ctx, coordination.AcquireRequest{
		Key:   "object-storage/cas/" + digest.String(),
		Owner: store.owner,
		TTL:   leaseTTL,
		Wait:  true,
	})
	if err != nil {
		return Digest{}, objectstore.Object{}, err
	}
	defer func() {
		if err := lease.Release(context.WithoutCancel(ctx)); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("cas: release lease: %w", err))
		}
	}()
	if _, err := lease.Renew(ctx, leaseTTL); err != nil {
		return Digest{}, objectstore.Object{}, err
	}
	fillCtx, stopFill := context.WithCancelCause(ctx)
	keepAlive, err := coordination.KeepAlive(fillCtx, lease, leaseTTL)
	if err != nil {
		stopFill(err)
		return Digest{}, objectstore.Object{}, err
	}
	keepAliveResult := make(chan error, 1)
	go func() {
		_, keepAliveErr := keepAlive.Await(context.Background())
		if keepAliveErr != nil && !errors.Is(keepAliveErr, errFillComplete) {
			stopFill(keepAliveErr)
		}
		keepAliveResult <- keepAliveErr
	}()
	defer func() {
		stopFill(errFillComplete)
		keepAliveErr := <-keepAliveResult
		if errors.Is(context.Cause(fillCtx), errFillComplete) && errors.Is(keepAliveErr, context.Canceled) {
			keepAliveErr = nil
		}
		if keepAliveErr != nil && !errors.Is(keepAliveErr, errFillComplete) {
			returnErr = errors.Join(returnErr, fmt.Errorf("cas: keep lease alive: %w", keepAliveErr))
		}
	}()

	key := store.key(digest)
	existing, err := store.objects.Stat(fillCtx, store.bucket, key)
	if err == nil {
		if !matchesDigest(existing, digest) {
			store.invalidate(ctx, digest)
			return Digest{}, objectstore.Object{}, ErrDigestMismatch
		}
		store.cache(ctx, digest, existing)
		return digest, existing.Clone(), nil
	}
	if !errors.Is(err, objectstore.ErrNotFound) {
		return Digest{}, objectstore.Object{}, err
	}

	sum := [32]byte(digest)
	object, err = store.objects.Put(fillCtx, store.bucket, key, temporary, objectstore.PutOptions{
		IfAbsent: true,
		SHA256:   &sum,
		Metadata: map[string]string{"object-storage-fencing-token": strconv.FormatUint(uint64(lease.Info().Token), 10)},
	})
	if errors.Is(err, objectstore.ErrPrecondition) {
		object, err = store.objects.Stat(fillCtx, store.bucket, key)
	}
	if err != nil {
		return Digest{}, objectstore.Object{}, err
	}
	if !matchesDigest(object, digest) {
		store.invalidate(ctx, digest)
		return Digest{}, objectstore.Object{}, ErrDigestMismatch
	}
	store.cache(ctx, digest, object)
	return digest, object.Clone(), nil
}

// matchesDigest reports whether object may be served for digest. Objects this
// package writes always carry a known digest that must match. An object with
// an unknown digest (written by another tool, or by a backend that cannot
// report it cheaply) is accepted on the strength of its content address:
// full reads through Open still verify every byte, but Stat and ranged reads
// of such objects are not verified.
func matchesDigest(object objectstore.Object, digest Digest) bool {
	known, ok := object.Digest()
	return !ok || known == [32]byte(digest)
}

// Open opens a digest. Full reads are always verified while streaming; ranged
// reads are checked against the stored digest when the backend reports one.
func (store *Store) Open(ctx context.Context, digest Digest, byteRange *objectstore.ByteRange) (io.ReadCloser, objectstore.Object, error) {
	body, object, err := store.objects.Get(ctx, store.bucket, store.key(digest), objectstore.GetOptions{Range: byteRange})
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			store.invalidate(ctx, digest)
		}
		return nil, objectstore.Object{}, err
	}
	if !matchesDigest(object, digest) {
		_ = body.Close()
		store.invalidate(ctx, digest)
		return nil, objectstore.Object{}, ErrDigestMismatch
	}
	store.cache(ctx, digest, object)
	if byteRange != nil {
		return body, object.Clone(), nil
	}
	verified := &verifyingReadCloser{
		body: body,
		hash: sha256.New(),
		want: digest,
		onMismatch: func() {
			store.invalidate(context.WithoutCancel(ctx), digest)
		},
	}
	return verified, object.Clone(), nil
}

// Stat returns cached or authoritative metadata for a digest.
func (store *Store) Stat(ctx context.Context, digest Digest) (objectstore.Object, error) {
	if store.metadata != nil {
		if object, ok, err := store.metadata.Get(ctx, digest.String()); err == nil && ok && object.SHA256Known && object.SHA256 == [32]byte(digest) {
			return object.Clone(), nil
		}
	}
	object, err := store.objects.Stat(ctx, store.bucket, store.key(digest))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			store.invalidate(ctx, digest)
		}
		return objectstore.Object{}, err
	}
	if !matchesDigest(object, digest) {
		store.invalidate(ctx, digest)
		return objectstore.Object{}, ErrDigestMismatch
	}
	store.cache(ctx, digest, object)
	return object.Clone(), nil
}

func (store *Store) key(digest Digest) objectstore.Key {
	return objectstore.Key(store.prefix + "/" + digest.hex())
}

func (store *Store) cache(ctx context.Context, digest Digest, object objectstore.Object) {
	if store.metadata != nil && object.SHA256Known {
		_ = store.metadata.Set(context.WithoutCancel(ctx), digest.String(), object.Clone())
	}
}

func (store *Store) invalidate(ctx context.Context, digest Digest) {
	if store.metadata != nil {
		_ = store.metadata.Invalidate(context.WithoutCancel(ctx), digest.String())
	}
}

type verifyingReadCloser struct {
	body       io.ReadCloser
	hash       hash.Hash
	want       Digest
	verified   bool
	result     error
	onMismatch func()
}

func (reader *verifyingReadCloser) Read(buffer []byte) (int, error) {
	if reader.verified {
		return 0, reader.result
	}
	count, err := reader.body.Read(buffer)
	if count > 0 {
		_, _ = reader.hash.Write(buffer[:count])
	}
	if errors.Is(err, io.EOF) {
		reader.verified = true
		var got Digest
		copy(got[:], reader.hash.Sum(nil))
		if got != reader.want {
			reader.result = ErrDigestMismatch
			reader.onMismatch()
			return count, reader.result
		}
		reader.result = io.EOF
	}
	return count, err
}

func (reader *verifyingReadCloser) Close() error {
	var verifyErr error
	if !reader.verified {
		_, verifyErr = io.Copy(io.Discard, reader)
	} else if errors.Is(reader.result, ErrDigestMismatch) {
		verifyErr = reader.result
	}
	return errors.Join(verifyErr, reader.body.Close())
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
