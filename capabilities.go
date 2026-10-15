package objectstore

import (
	"context"
	"maps"
	"net/http"
	"time"
)

// Optional capabilities are discovered with type assertions on a Store:
//
//	if copier, ok := store.(objectstore.Copier); ok { ... }
//
// Every capability keeps the Store error vocabulary: ErrNotFound,
// ErrPrecondition, ErrInvalidBucket, ErrInvalidKey, ErrInvalidRange, and
// ErrInvalidPart, wrapped with backend detail where useful.

// StatResult is the outcome of one key in a batch metadata lookup.
type StatResult struct {
	Object Object
	Err    error
}

// BatchStater returns metadata for many keys with backend-native batching or
// bounded parallelism.
type BatchStater interface {
	// StatMany returns exactly one result per key, in input order. Per-key
	// failures such as ErrNotFound are reported in StatResult.Err; the error
	// return is reserved for whole-call failures such as cancellation or an
	// invalid bucket. Successful results carry the fields Stat returns.
	StatMany(context.Context, Bucket, []Key) ([]StatResult, error)
}

// BatchDeleter removes many objects with backend-native batching.
type BatchDeleter interface {
	// DeleteMany unconditionally deletes keys and returns exactly one error
	// per key, in input order. Deleting an absent key succeeds, because batch
	// deletion is idempotent on every supported backend. The error return is
	// reserved for whole-call failures such as cancellation or an invalid
	// bucket.
	DeleteMany(context.Context, Bucket, []Key) ([]error, error)
}

// CopyOptions configures one server-side copy.
type CopyOptions struct {
	// SourceVersion, when non-empty, requires the source's current Version.
	SourceVersion string
	// IfAbsent and IfMatchVersion are atomic destination write conditions
	// with the same meaning as in PutOptions.
	IfAbsent       bool
	IfMatchVersion string
	// Metadata, when non-nil, replaces the destination user metadata; nil
	// copies the source metadata. ContentType, when non-empty, replaces the
	// source content type.
	Metadata    map[string]string
	ContentType string
}

// Clone returns options with independently owned reference fields.
func (options CopyOptions) Clone() CopyOptions {
	options.Metadata = maps.Clone(options.Metadata)
	return options
}

// Copier copies objects without routing bytes through the caller.
type Copier interface {
	// Copy publishes the source bytes under the destination key. The
	// destination keeps the source SHA-256 digest when it is known. A missing
	// source returns ErrNotFound; a failed SourceVersion or destination
	// condition returns ErrPrecondition. Source and destination may be in
	// different buckets of the same store.
	Copy(ctx context.Context, srcBucket Bucket, srcKey Key, dstBucket Bucket, dstKey Key, options CopyOptions) (Object, error)
}

// PresignedRequest is an HTTP request that a third party can execute without
// credentials until ExpiresAt.
type PresignedRequest struct {
	Method string
	URL    string
	// Header lists the headers the client must send verbatim. Content-Length,
	// when present, is informational: HTTP clients derive it from the body.
	Header    http.Header
	ExpiresAt time.Time
}

// Clone returns a request with independently owned headers.
func (request PresignedRequest) Clone() PresignedRequest {
	request.Header = request.Header.Clone()
	return request
}

// Presigner issues time-limited URLs for direct client transfers.
type Presigner interface {
	// PresignGet authorizes reading an object or exact byte range.
	PresignGet(ctx context.Context, bucket Bucket, key Key, expires time.Duration, byteRange *ByteRange) (PresignedRequest, error)
	// PresignPut authorizes one object write. ContentType, Metadata,
	// IfAbsent, IfMatchVersion, SHA256, and Size are bound into the request;
	// the backend enforces a supplied SHA-256 digest before publication.
	// Objects written without SHA256 may later report SHA256Known false.
	PresignPut(ctx context.Context, bucket Bucket, key Key, expires time.Duration, options PutOptions) (PresignedRequest, error)
	// PresignUploadPart authorizes uploading one multipart part of an exact
	// size and SHA-256 digest. The ETag response header of the executed
	// request identifies the part: complete the upload with
	// CompletedPart{Number: number, ETag: etag, Size: size, SHA256: digest}.
	PresignUploadPart(ctx context.Context, upload Upload, number int, size int64, digest [32]byte, expires time.Duration) (PresignedRequest, error)
}
