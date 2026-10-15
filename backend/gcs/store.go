// Package gcs adapts Google Cloud Storage clients (cloud.google.com/go/storage)
// to the backend-neutral object API.
//
// # Versions
//
// Version is the object generation as a decimal string. Generations are
// assigned by GCS on every publication and never repeat for a key, so they
// are a reliable compare-and-swap token even when identical bytes are written
// again. ETag is the GCS entity tag of the object resource.
//
// # Digests
//
// Every object written through this adapter records its SHA-256 digest in the
// user metadata key "go-object-storage-sha256" (lowercase hex) at creation
// time. Objects without that key (written by other tools) are reported with
// SHA256Known false; Stat, Get, and List never download object bytes to
// compute a digest.
//
// Put streams bodies through a storage.Writer without holding them in memory:
//
//   - With a known PutOptions.SHA256 the body streams directly. It is hashed
//     and counted while being sent, and the digest and PutOptions.Size are
//     verified before Writer.Close. On a mismatch the writer's context is
//     canceled instead of closing it, which abandons the upload before
//     finalization, so the object is never published.
//   - Without a known digest, bodies up to BufferSize are buffered and hashed
//     in memory. Larger bodies are hashed in a first pass when the body is an
//     io.Seeker (then rewound), or otherwise spooled once to a temporary file
//     in SpoolDirectory while hashing. The digest is then known and the
//     upload proceeds as above, because GCS object metadata must be supplied
//     when an upload starts. Callers that know the digest never spool.
//
// # Conditional writes
//
// IfAbsent maps to the ifGenerationMatch=0 precondition
// (storage.Conditions{DoesNotExist: true}) and IfMatchVersion to
// ifGenerationMatch=<generation> on uploads, compose, rewrite (Copy), and
// delete. GCS evaluates these preconditions atomically with publication; they
// are its compare-and-swap primitive. A version that is not a canonical
// positive decimal generation can never match and fails with ErrPrecondition
// without a request.
//
// Get reads object attributes first (the GCS readers do not expose user
// metadata), validates the requested range against the object size, and then
// opens a reader pinned to that generation. When a concurrent writer replaced
// the generation in between, Get starts over, unless IfMatchVersion was given,
// in which case it returns ErrPrecondition.
//
// # Multipart uploads
//
// GCS has no S3-style multipart uploads, so explicit multipart uploads are
// built from compose. BeginMultipart writes a small marker object (holding the
// upload's PutOptions as JSON) under MultipartPrefix, by default
// ".go-object-storage/multipart/<id>/upload"; PutPart streams each part to
// "<prefix><id>/<number>" while hashing it and records its SHA-256 digest in
// the part's metadata; CompleteMultipart verifies every part's size, recorded
// digest, and ETag, composes the parts (hierarchically through intermediate
// objects when there are more than 32) into the final key with the upload's
// IfAbsent/IfMatchVersion precondition, and deletes the marker and the
// staging objects. Keys under MultipartPrefix are reserved: they are rejected
// with ErrInvalidKey and hidden from listings.
//
// GCS cannot hash the composed object, so the whole-object digest is the one
// declared in PutOptions.SHA256 at BeginMultipart; per-part digests are
// verified client-side while each part streams. Uploads begun without a
// digest complete with SHA256Known false. CompleteMultipart is idempotent:
// when an earlier completion succeeded but its response was lost, the upload
// identity recorded in the object metadata lets a retry return the published
// object.
//
// # Presigning
//
// Presigned URLs are GCS V4 signed URLs from BucketHandle.SignedURL. Signing
// needs a service account identity: either Options.GoogleAccessID together
// with Options.PrivateKey or Options.SignBytes, or credentials the client can
// sign with (a service-account JSON key, or the IAM Credentials signBlob API
// via the client's credentials and the service account's
// iam.serviceAccounts.signBlob permission). GCS does not verify SHA-256
// digests of uploads, so PresignPut and PresignUploadPart bind the declared
// digest only as signed metadata: the uploader cannot change the recorded
// digest, but GCS does not reject bytes that do not match it. Size is
// enforced with a signed x-goog-content-length-range header.
package gcs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	objectstore "github.com/justusschock/go-object-storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	sha256MetadataKey = "go-object-storage-sha256"
	uploadMetadataKey = "go-object-storage-upload"

	// DefaultMultipartPrefix is the reserved key prefix for multipart
	// staging objects.
	DefaultMultipartPrefix = ".go-object-storage/multipart/"

	defaultChunkSize   = 16 << 20
	defaultBufferSize  = int64(16 << 20)
	defaultConcurrency = 16
	readRetryLimit     = 16
	listPageSize       = 1000
	// cleanupTimeout bounds best-effort deletion of staging objects.
	cleanupTimeout = 30 * time.Second
)

// Options configures transfer sizing, parallelism, the reserved multipart
// prefix, and URL signing. Zero values select defaults.
type Options struct {
	// ChunkSize is storage.Writer.ChunkSize for uploads: bodies larger than
	// one chunk use resumable uploads sent in chunks of this size, smaller
	// bodies one request. Default 16 MiB (the client default); the client
	// rounds it up to a multiple of 256 KiB. Each in-flight upload buffers
	// one chunk.
	ChunkSize int
	// BufferSize bounds the in-memory buffer used to hash bodies whose
	// digest is not known in advance; larger bodies are hashed in a first
	// pass (seekable bodies) or spooled to SpoolDirectory. Default 16 MiB.
	BufferSize int64
	// SpoolDirectory receives temporary files for large unseekable bodies
	// without a known digest; default os.TempDir().
	SpoolDirectory string
	// Concurrency bounds parallel requests in StatMany, DeleteMany, and
	// multipart part verification and cleanup; default 16.
	Concurrency int
	// MultipartPrefix is the reserved key prefix for multipart staging
	// objects; default DefaultMultipartPrefix. It must end with "/". Keys
	// under it are rejected and hidden from listings.
	MultipartPrefix string

	// GoogleAccessID is the service account email used to sign URLs. When
	// empty it is detected from the client's credentials.
	GoogleAccessID string
	// PrivateKey is the PEM or PKCS#12 service account key used to sign
	// URLs locally. When empty, SignBytes is used, or the key is taken from
	// the client's service-account credentials, or signing falls back to the
	// IAM Credentials signBlob API.
	PrivateKey []byte
	// SignBytes signs URLs with a custom signer, for example a KMS key or
	// the IAM Credentials API.
	SignBytes func([]byte) ([]byte, error)
	// Hostname overrides the host of signed URLs; default the client's
	// endpoint host (storage.googleapis.com unless configured).
	Hostname string
	// Insecure signs http:// instead of https:// URLs, for emulators.
	Insecure bool
	// URLStyle selects path-style (default) or virtual-hosted URLs.
	URLStyle storage.URLStyle
}

func (options Options) withDefaults() (Options, error) {
	if options.ChunkSize == 0 {
		options.ChunkSize = defaultChunkSize
	}
	if options.BufferSize == 0 {
		options.BufferSize = defaultBufferSize
	}
	if options.Concurrency == 0 {
		options.Concurrency = defaultConcurrency
	}
	if options.MultipartPrefix == "" {
		options.MultipartPrefix = DefaultMultipartPrefix
	}
	if options.URLStyle == nil {
		options.URLStyle = storage.PathStyle()
	}
	switch {
	case options.ChunkSize < 0:
		return Options{}, errors.New("gcs store: negative chunk size")
	case options.BufferSize < 0:
		return Options{}, errors.New("gcs store: negative buffer size")
	case options.Concurrency < 0:
		return Options{}, errors.New("gcs store: negative concurrency")
	case !strings.HasSuffix(options.MultipartPrefix, "/") || objectstore.ValidateKey(objectstore.Key(options.MultipartPrefix+"x")) != nil:
		return Options{}, errors.New("gcs store: multipart prefix must be a valid key prefix ending in /")
	}
	return options, nil
}

// Store maps portable object operations to Google Cloud Storage requests.
// It is safe for concurrent use.
type Store struct {
	client  *storage.Client
	options Options
	// composeFanIn is the number of sources per compose request; tests
	// lower it to exercise hierarchical composition cheaply.
	composeFanIn int
}

// New constructs a GCS store from a caller-owned client. The store does not
// close the client.
func New(client *storage.Client, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("gcs store: nil client")
	}
	options, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Store{client: client, options: options, composeFanIn: maximumComposeSources}, nil
}

func (store *Store) object(bucket objectstore.Bucket, key objectstore.Key) *storage.ObjectHandle {
	return store.client.Bucket(string(bucket)).Object(string(key))
}

// Put uploads one object with atomic GCS preconditions and verified SHA-256
// metadata, streaming the body through a storage.Writer.
func (store *Store) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := store.validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Object{}, err
	}
	if body == nil {
		return objectstore.Object{}, errors.New("gcs store: nil reader")
	}
	if _, err := metadataWithOptionalDigest(options.Metadata, nil); err != nil {
		return objectstore.Object{}, err
	}
	conditions, err := writeConditions(options.IfAbsent, options.IfMatchVersion)
	if err != nil {
		return objectstore.Object{}, err
	}
	target := write{bucket: bucket, key: key, options: options, conditions: conditions}
	if options.SHA256 != nil {
		return store.write(ctx, target, body, *options.SHA256)
	}
	start, seekable := seekStart(body)
	head, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: body}, store.options.BufferSize+1))
	if err != nil {
		return objectstore.Object{}, err
	}
	if int64(len(head)) <= store.options.BufferSize {
		return store.write(ctx, target, bytes.NewReader(head), sha256.Sum256(head))
	}
	if options.Size > 0 && int64(len(head)) > options.Size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	if seekable {
		digest, err := hashRemainder(ctx, head, body)
		if err != nil {
			return objectstore.Object{}, err
		}
		if _, err := body.(io.Seeker).Seek(start, io.SeekStart); err != nil {
			return objectstore.Object{}, fmt.Errorf("gcs store: rewind body: %w", err)
		}
		return store.write(ctx, target, body, digest)
	}
	spooled, digest, err := spool(ctx, store.options.SpoolDirectory, head, body)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer spooled.close()
	return store.write(ctx, target, spooled.file, digest)
}

// write describes one verified upload.
type write struct {
	bucket     objectstore.Bucket
	key        objectstore.Key
	options    objectstore.PutOptions
	conditions *storage.Conditions
	// extra is reserved adapter metadata added after validation.
	extra map[string]string
}

// write streams body to a storage.Writer while hashing it. The size and
// digest are verified before Close; on a mismatch, a read error, or
// cancellation the writer's context is canceled so that GCS never finalizes
// the upload.
func (store *Store) write(ctx context.Context, target write, body io.Reader, digest [32]byte) (objectstore.Object, error) {
	metadata, err := metadataWithDigest(target.options.Metadata, digest)
	if err != nil {
		return objectstore.Object{}, err
	}
	maps.Copy(metadata, target.extra)
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	handle := store.object(target.bucket, target.key)
	if target.conditions != nil {
		handle = handle.If(*target.conditions)
	}
	writer := handle.NewWriter(writeCtx)
	writer.ChunkSize = store.options.ChunkSize
	writer.ContentType = target.options.ContentType
	writer.Metadata = metadata
	abandon := func(err error) (objectstore.Object, error) {
		cancel()
		_ = writer.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return objectstore.Object{}, ctxErr
		}
		return objectstore.Object{}, err
	}
	source := &recordingReader{reader: contextReader{ctx: ctx, reader: body}}
	reader := io.Reader(source)
	if target.options.Size > 0 {
		reader = io.LimitReader(reader, target.options.Size+1)
	}
	hash := sha256.New()
	count, err := io.Copy(writer, io.TeeReader(reader, hash))
	if err != nil {
		if source.err == nil {
			// The upload request failed, for example on a precondition.
			return abandon(mapError(err))
		}
		return abandon(err)
	}
	if target.options.Size > 0 && count != target.options.Size {
		return abandon(fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition))
	}
	var got [32]byte
	copy(got[:], hash.Sum(nil))
	if got != digest {
		return abandon(fmt.Errorf("%w: SHA-256 mismatch", objectstore.ErrPrecondition))
	}
	if err := writer.Close(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return objectstore.Object{}, ctxErr
		}
		return objectstore.Object{}, mapError(err)
	}
	return objectFromAttrs(target.bucket, writer.Attrs())
}

// Get opens an object or exact byte range of one generation. It never reads
// beyond the requested bytes and never hashes object contents.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := store.validateObjectInput(ctx, bucket, key); err != nil {
		return nil, objectstore.Object{}, err
	}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
	}
	if options.IfMatchVersion != "" {
		if _, ok := parseVersion(options.IfMatchVersion); !ok {
			return nil, objectstore.Object{}, objectstore.ErrPrecondition
		}
	}
	handle := store.object(bucket, key)
	for range readRetryLimit {
		attrs, err := handle.Attrs(ctx)
		if err != nil {
			return nil, objectstore.Object{}, mapError(err)
		}
		object, err := objectFromAttrs(bucket, attrs)
		if err != nil {
			return nil, objectstore.Object{}, err
		}
		if options.IfMatchVersion != "" && object.Version != options.IfMatchVersion {
			return nil, objectstore.Object{}, objectstore.ErrPrecondition
		}
		offset, length := int64(0), object.Size
		if options.Range != nil {
			if options.Range.Offset >= object.Size || options.Range.Length > object.Size-options.Range.Offset {
				return nil, objectstore.Object{}, objectstore.ErrInvalidRange
			}
			offset, length = options.Range.Offset, options.Range.Length
		}
		if length == 0 {
			return io.NopCloser(bytes.NewReader(nil)), object, nil
		}
		reader, err := handle.Generation(attrs.Generation).NewRangeReader(ctx, offset, length)
		if err != nil {
			mapped := mapError(err)
			if errors.Is(mapped, objectstore.ErrNotFound) || errors.Is(mapped, objectstore.ErrPrecondition) {
				// The generation was replaced or deleted after Attrs.
				if options.IfMatchVersion != "" {
					return nil, objectstore.Object{}, fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err)
				}
				continue
			}
			return nil, objectstore.Object{}, mapped
		}
		return &exactReader{body: reader, remaining: length}, object, nil
	}
	return nil, objectstore.Object{}, errors.New("gcs store: object generation changed repeatedly during Get")
}

// Stat returns object metadata from one object attributes request.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := store.validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	attrs, err := store.object(bucket, key).Attrs(ctx)
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	return objectFromAttrs(bucket, attrs)
}

// Delete deletes the object, conditionally on its generation when version is
// non-empty. Deleting an absent object returns ErrNotFound.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := store.validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	handle := store.object(bucket, key)
	if version != "" {
		generation, ok := parseVersion(version)
		if !ok {
			return objectstore.ErrPrecondition
		}
		handle = handle.If(storage.Conditions{GenerationMatch: generation})
	}
	return mapError(handle.Delete(ctx))
}

// List returns a stable key-ordered page. GCS listings carry user metadata,
// so listed objects always include Metadata and the recorded SHA-256 digest
// (IncludeMetadata is implied). Keys under MultipartPrefix are hidden.
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
	query := &storage.Query{Prefix: options.Prefix, StartOffset: string(options.After), Projection: storage.ProjectionNoACL}
	if err := query.SetAttrSelection([]string{"Name", "Generation", "Etag", "Size", "Updated", "Created", "Metadata"}); err != nil {
		return objectstore.Page{}, err
	}
	iter := store.client.Bucket(string(bucket)).Objects(ctx, query)
	iter.PageInfo().MaxSize = listPageSize
	if options.Limit > 0 && options.Limit < listPageSize {
		iter.PageInfo().MaxSize = options.Limit + 1
	}
	var page objectstore.Page
	for {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return objectstore.Page{}, mapError(err)
		}
		if attrs.Prefix != "" || attrs.Name <= string(options.After) || store.reserved(attrs.Name) {
			continue
		}
		if options.Limit > 0 && len(page.Objects) == options.Limit {
			page.NextAfter = page.Objects[len(page.Objects)-1].Key
			break
		}
		object, err := objectFromAttrs(bucket, attrs)
		if err != nil {
			return objectstore.Page{}, err
		}
		page.Objects = append(page.Objects, object)
	}
	if err := ctx.Err(); err != nil {
		return objectstore.Page{}, err
	}
	return page, nil
}

// objectFromAttrs converts GCS object attributes. It is the single
// conversion used by every operation, so results of Put, Get, Stat, List,
// Copy, and CompleteMultipart for the same generation compare equal. The
// reserved adapter metadata keys are removed from the returned metadata.
func objectFromAttrs(bucket objectstore.Bucket, attrs *storage.ObjectAttrs) (objectstore.Object, error) {
	if attrs == nil || attrs.Name == "" || attrs.Generation <= 0 || attrs.Size < 0 {
		return objectstore.Object{}, errors.New("gcs store: incomplete object attributes")
	}
	modified := attrs.Updated
	if modified.IsZero() {
		modified = attrs.Created
	}
	metadata := maps.Clone(attrs.Metadata)
	encoded, hasDigest := "", false
	for name, value := range metadata {
		switch strings.ToLower(name) {
		case sha256MetadataKey:
			encoded, hasDigest = value, true
			delete(metadata, name)
		case uploadMetadataKey:
			delete(metadata, name)
		}
	}
	if len(metadata) == 0 {
		metadata = nil
	}
	object := objectstore.Object{
		Bucket: bucket, Key: objectstore.Key(attrs.Name), Version: strconv.FormatInt(attrs.Generation, 10), ETag: attrs.Etag,
		Size: attrs.Size, ModifiedAt: modified.UTC(), Metadata: metadata,
	}
	if !hasDigest {
		return object, nil
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return objectstore.Object{}, errors.New("gcs store: invalid SHA-256 metadata")
	}
	copy(object.SHA256[:], decoded)
	object.SHA256Known = true
	return object, nil
}

// recordedDigest returns the SHA-256 digest recorded in raw GCS metadata.
func recordedDigest(raw map[string]string) ([32]byte, bool) {
	for name, value := range raw {
		if strings.EqualFold(name, sha256MetadataKey) {
			decoded, err := hex.DecodeString(value)
			if err != nil || len(decoded) != sha256.Size {
				return [32]byte{}, false
			}
			var digest [32]byte
			copy(digest[:], decoded)
			return digest, true
		}
	}
	return [32]byte{}, false
}

func metadataWithDigest(metadata map[string]string, digest [sha256.Size]byte) (map[string]string, error) {
	return metadataWithOptionalDigest(metadata, &digest)
}

// metadataWithOptionalDigest rejects reserved user keys and adds the digest.
// The result is never nil.
func metadataWithOptionalDigest(metadata map[string]string, digest *[sha256.Size]byte) (map[string]string, error) {
	result := make(map[string]string, len(metadata)+1)
	for key, value := range metadata {
		if strings.EqualFold(key, sha256MetadataKey) || strings.EqualFold(key, uploadMetadataKey) {
			return nil, fmt.Errorf("gcs store: reserved metadata key %q", key)
		}
		result[key] = value
	}
	if digest != nil {
		result[sha256MetadataKey] = hex.EncodeToString(digest[:])
	}
	return result, nil
}

// writeConditions converts portable write conditions to GCS preconditions.
func writeConditions(ifAbsent bool, ifMatchVersion string) (*storage.Conditions, error) {
	switch {
	case ifAbsent && ifMatchVersion != "":
		// No object can be both absent and at a generation.
		return nil, objectstore.ErrPrecondition
	case ifAbsent:
		return &storage.Conditions{DoesNotExist: true}, nil
	case ifMatchVersion != "":
		generation, ok := parseVersion(ifMatchVersion)
		if !ok {
			return nil, objectstore.ErrPrecondition
		}
		return &storage.Conditions{GenerationMatch: generation}, nil
	}
	return nil, nil
}

// parseVersion parses a canonical positive decimal generation.
func parseVersion(version string) (int64, bool) {
	generation, err := strconv.ParseInt(version, 10, 64)
	if err != nil || generation <= 0 || strconv.FormatInt(generation, 10) != version {
		return 0, false
	}
	return generation, true
}

func (store *Store) reserved(key string) bool {
	return strings.HasPrefix(key, store.options.MultipartPrefix)
}

func (store *Store) validateObjectInput(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := errors.Join(objectstore.ValidateBucket(bucket), objectstore.ValidateKey(key)); err != nil {
		return err
	}
	if store.reserved(string(key)) {
		return fmt.Errorf("%w: reserved multipart prefix %q", objectstore.ErrInvalidKey, store.options.MultipartPrefix)
	}
	return nil
}

func mapError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
		return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err)
	}
	var apiError *googleapi.Error
	if errors.As(err, &apiError) {
		switch apiError.Code {
		case http.StatusNotFound:
			return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err)
		case http.StatusConflict, http.StatusPreconditionFailed:
			return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err)
		case http.StatusRequestedRangeNotSatisfiable:
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err)
		}
		return err
	}
	if grpcStatus, ok := status.FromError(err); ok {
		switch grpcStatus.Code() {
		case codes.NotFound:
			return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err)
		case codes.FailedPrecondition, codes.Aborted:
			return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err)
		case codes.OutOfRange:
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err)
		}
	}
	return err
}

var (
	_ objectstore.MultipartStore = (*Store)(nil)
	_ objectstore.BatchStater    = (*Store)(nil)
	_ objectstore.BatchDeleter   = (*Store)(nil)
	_ objectstore.Copier         = (*Store)(nil)
	_ objectstore.Presigner      = (*Store)(nil)
)
