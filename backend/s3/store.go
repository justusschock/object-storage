// Package s3 adapts AWS SDK v2 S3 clients to the backend-neutral object API.
//
// # Digests
//
// Every object written through this adapter records its SHA-256 digest in the
// user metadata key "go-object-storage-sha256" at creation time, because S3
// metadata cannot change after publication. Objects without that key (written
// by other tools) are reported with SHA256Known false; Stat, Get, and List
// never download object bytes to compute a digest.
//
// Put streams bodies without holding them in memory:
//
//   - Bodies up to PartSize are buffered (bounded by PartSize), hashed, and
//     sent with one PutObject whose SigV4 payload hash and
//     x-amz-checksum-sha256 header let S3 reject corrupted transfers.
//   - Larger bodies with a known PutOptions.SHA256 stream through a
//     multipart upload created with the digest metadata. Parts are hashed
//     while being read, at most Concurrency parts are in flight, and the
//     running whole-object digest is compared before CompleteMultipartUpload,
//     so a mismatch aborts the upload and never publishes the key.
//   - Larger bodies without a known digest are hashed in a first pass when
//     the body is an io.Seeker (then rewound), or otherwise spooled once to a
//     temporary file in SpoolDirectory while hashing; the digest is then
//     known and the upload proceeds as above. Callers that know the digest
//     (content-addressed writers) never spool.
//
// Explicit multipart uploads (BeginMultipart/PutPart/CompleteMultipart) are
// created directly on the final key and completed in place: no staging key,
// no re-download, no re-upload. S3 verifies every part against its SHA-256
// (payload hash and x-amz-checksum-sha256). S3 cannot hash the assembled
// object, so the whole-object digest is the one declared in
// PutOptions.SHA256 at BeginMultipart and recorded as metadata at creation;
// multipart.Uploader computes and verifies it from the source before
// beginning. Uploads begun without a digest complete with SHA256Known false.
// CompleteMultipart is idempotent: when an earlier completion succeeded but
// its response was lost, the upload's identity recorded in object metadata
// lets a retry return the published object.
//
// # Conditional writes
//
// IfAbsent maps to If-None-Match: * and IfMatchVersion to If-Match on
// PutObject, CompleteMultipartUpload, CopyObject, and DeleteObject; Version is
// the S3 entity tag. These conditions are S3's atomic compare-and-swap
// primitive. Get additionally checks the returned entity tag client-side.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	objectstore "github.com/justusschock/go-object-storage"
)

const (
	sha256MetadataKey = "go-object-storage-sha256"
	uploadMetadataKey = "go-object-storage-upload"
	// legacyStagingPrefix hides staging objects left by v0.2.0 multipart
	// uploads from listings.
	legacyStagingPrefix = "/.go-object-storage/multipart/"
	cleanupTimeout      = 10 * time.Second

	minimumPartSize            = int64(5 << 20)
	maximumPartCount           = 10_000
	defaultPartSize            = int64(16 << 20)
	defaultConcurrency         = 4
	defaultMetadataConcurrency = 16
	maximumCopyObjectSize      = int64(5 << 30)
	defaultCopyPartSize        = int64(1 << 30)
	deleteBatchSize            = 1000
)

type clientAPI interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	DeleteObjects(context.Context, *awss3.DeleteObjectsInput, ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error)
	CopyObject(context.Context, *awss3.CopyObjectInput, ...func(*awss3.Options)) (*awss3.CopyObjectOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	UploadPartCopy(context.Context, *awss3.UploadPartCopyInput, ...func(*awss3.Options)) (*awss3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
	ListParts(context.Context, *awss3.ListPartsInput, ...func(*awss3.Options)) (*awss3.ListPartsOutput, error)
}

// Options configures transfer sizing and parallelism. Zero values select
// defaults.
type Options struct {
	// PartSize is the single-request threshold and the multipart part size
	// for Put; default 16 MiB, minimum 5 MiB. Put holds at most
	// (Concurrency+1)*PartSize bytes in memory. When PutOptions.Size is
	// known the part size grows as needed to stay within 10,000 parts.
	PartSize int64
	// Concurrency bounds in-flight part uploads per Put and part copies per
	// Copy; default 4.
	Concurrency int
	// MetadataConcurrency bounds parallel HEAD requests for StatMany and
	// List with IncludeMetadata; default 16.
	MetadataConcurrency int
	// SpoolDirectory receives temporary files for large unseekable bodies
	// without a known digest; default os.TempDir().
	SpoolDirectory string
	// CopyPartSize is the part size for multipart copies of sources larger
	// than 5 GiB; default 1 GiB.
	CopyPartSize int64
	// MultipartCopyThreshold is the largest source copied with a single
	// CopyObject request; default and maximum 5 GiB.
	MultipartCopyThreshold int64
	// NoHeadAfterWrite builds the Object that single-request writes (Put
	// below PartSize, Copy below MultipartCopyThreshold) return from S3's
	// acknowledgment instead of a HEAD request, saving one request per
	// write. Every field matches what a HEAD reports (Version is the entity
	// tag) except ModifiedAt, which is the local clock: the acknowledgment
	// carries no Last-Modified. Multipart writes, and writes acknowledged
	// without an entity tag, still read the HEAD.
	NoHeadAfterWrite bool
}

func (options Options) withDefaults() (Options, error) {
	if options.PartSize == 0 {
		options.PartSize = defaultPartSize
	}
	if options.Concurrency == 0 {
		options.Concurrency = defaultConcurrency
	}
	if options.MetadataConcurrency == 0 {
		options.MetadataConcurrency = defaultMetadataConcurrency
	}
	if options.CopyPartSize == 0 {
		options.CopyPartSize = defaultCopyPartSize
	}
	if options.MultipartCopyThreshold == 0 {
		options.MultipartCopyThreshold = maximumCopyObjectSize
	}
	switch {
	case options.PartSize < minimumPartSize:
		return Options{}, fmt.Errorf("s3 store: part size must be at least %d", minimumPartSize)
	case options.Concurrency < 0 || options.MetadataConcurrency < 0:
		return Options{}, errors.New("s3 store: negative concurrency")
	case options.CopyPartSize < minimumPartSize || options.CopyPartSize > maximumCopyObjectSize:
		return Options{}, errors.New("s3 store: copy part size must be between 5 MiB and 5 GiB")
	case options.MultipartCopyThreshold < 0 || options.MultipartCopyThreshold > maximumCopyObjectSize:
		return Options{}, errors.New("s3 store: multipart copy threshold must be at most 5 GiB")
	}
	return options, nil
}

// Store maps portable object operations to S3 requests. It is safe for
// concurrent use.
type Store struct {
	client    clientAPI
	presigner presignAPI
	options   Options
}

// NewWithOptions constructs an S3 store from a caller-owned SDK client.
func NewWithOptions(client *awss3.Client, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("s3 store: nil client")
	}
	options, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Store{client: client, presigner: awss3.NewPresignClient(client), options: options}, nil
}

// New constructs an S3 store from caller-owned SDK clients. The uploader
// contributes its PartSize (raised to the 16 MiB default when smaller) and
// Concurrency; the adapter performs its own verified multipart streaming.
//
// Deprecated: Use NewWithOptions; the feature/s3/manager package is
// deprecated upstream.
func New(client *awss3.Client, uploader *manager.Uploader) (*Store, error) {
	if client == nil || uploader == nil {
		return nil, errors.New("s3 store: nil client or uploader")
	}
	return NewWithOptions(client, Options{PartSize: max(uploader.PartSize, defaultPartSize), Concurrency: uploader.Concurrency})
}

// Put uploads one object with atomic S3 write preconditions and verified
// SHA-256 metadata, streaming large bodies through bounded memory.
func (store *Store) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Object{}, err
	}
	if body == nil {
		return objectstore.Object{}, errors.New("s3 store: nil reader")
	}
	if _, err := metadataWithOptionalDigest(options.Metadata, nil); err != nil {
		return objectstore.Object{}, err
	}
	start, seekable := seekStart(body)
	partSize := store.partSize(options.Size)
	head, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: body}, partSize+1))
	if err != nil {
		return objectstore.Object{}, err
	}
	if int64(len(head)) <= partSize {
		return store.putBuffered(ctx, bucket, key, head, options)
	}
	if options.Size > 0 && int64(len(head)) > options.Size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	if options.SHA256 != nil {
		return store.putStream(ctx, bucket, key, io.MultiReader(bytes.NewReader(head), body), *options.SHA256, options)
	}
	if seekable {
		digest, err := hashRemainder(ctx, head, body)
		if err != nil {
			return objectstore.Object{}, err
		}
		if _, err := body.(io.Seeker).Seek(start, io.SeekStart); err != nil {
			return objectstore.Object{}, fmt.Errorf("s3 store: rewind body: %w", err)
		}
		return store.putStream(ctx, bucket, key, body, digest, options)
	}
	spooled, digest, err := spool(ctx, store.options.SpoolDirectory, head, body)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer spooled.close()
	return store.putStream(ctx, bucket, key, spooled.file, digest, options)
}

func (store *Store) putBuffered(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, data []byte, options objectstore.PutOptions) (objectstore.Object, error) {
	digest := sha256.Sum256(data)
	if options.SHA256 != nil && *options.SHA256 != digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != int64(len(data)) {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	metadata, err := metadataWithDigest(options.Metadata, digest)
	if err != nil {
		return objectstore.Object{}, err
	}
	input := &awss3.PutObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), Body: bytes.NewReader(data),
		ContentLength: int64Pointer(int64(len(data))), ContentType: optionalString(options.ContentType), Metadata: metadata,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ChecksumSHA256: stringPointer(base64.StdEncoding.EncodeToString(digest[:])),
	}
	if options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	input.IfMatch = optionalString(options.IfMatchVersion)
	output, err := store.client.PutObject(ctx, input, withPayloadHash(digest))
	if err != nil {
		return objectstore.Object{}, conditionalWriteError(err, options.IfMatchVersion)
	}
	return store.published(ctx, bucket, key, publication{etag: optionalDereference(output.ETag), size: int64(len(data)), digest: &digest, metadata: options.Metadata})
}

// publication describes a write this store just completed.
type publication struct {
	// multipart writes are completed from parts; some S3-compatible stores
	// acknowledge them with an entity tag that differs from what HEAD
	// reports, so their result always comes from the HEAD.
	multipart bool
	etag      string
	size      int64
	digest    *[32]byte
	nonce     string
	metadata  map[string]string
}

// published returns metadata for a just-written object. It never fails a
// write that S3 acknowledged: the HEAD result is used when it is provably the
// same write (same entity tag, or the upload nonce recorded at creation);
// when a concurrent writer already replaced the object, the result is built
// from the acknowledged write with the local time as ModifiedAt.
func (store *Store) published(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, write publication) (objectstore.Object, error) {
	if store.options.NoHeadAfterWrite && !write.multipart && write.etag != "" {
		return acknowledged(bucket, key, write), nil
	}
	output, err := store.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key))})
	if err == nil {
		object, convertErr := objectFromHead(bucket, key, output)
		if convertErr != nil {
			return objectstore.Object{}, convertErr
		}
		sameNonce := false
		for name, value := range output.Metadata {
			if strings.EqualFold(name, uploadMetadataKey) && write.nonce != "" && value == write.nonce {
				sameNonce = true
			}
		}
		if object.ETag == write.etag || sameNonce {
			return object, nil
		}
	} else if mapped := mapError(err); !errors.Is(mapped, objectstore.ErrNotFound) {
		return objectstore.Object{}, mapped
	}
	if write.etag == "" {
		return objectstore.Object{}, errors.New("s3 store: write acknowledged without an entity tag")
	}
	return acknowledged(bucket, key, write), nil
}

// acknowledged builds the Object of a write from its acknowledgment, with
// the local time as ModifiedAt.
func acknowledged(bucket objectstore.Bucket, key objectstore.Key, write publication) objectstore.Object {
	object := objectstore.Object{
		Bucket: bucket, Key: key, Version: write.etag, ETag: write.etag, Size: write.size,
		ModifiedAt: time.Now().UTC(), Metadata: maps.Clone(write.metadata),
	}
	if len(object.Metadata) == 0 {
		object.Metadata = nil
	}
	if write.digest != nil {
		object.SHA256, object.SHA256Known = *write.digest, true
	}
	return object
}

// Get opens an object or exact byte range. It never reads beyond the
// requested bytes and never hashes object contents.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return nil, objectstore.Object{}, err
	}
	input := &awss3.GetObjectInput{Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), IfMatch: optionalString(options.IfMatchVersion)}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
		input.Range = stringPointer(rangeHeader(*options.Range))
	}
	output, err := store.client.GetObject(ctx, input)
	if err != nil {
		return nil, objectstore.Object{}, mapError(err)
	}
	fail := func(err error) (io.ReadCloser, objectstore.Object, error) {
		if output.Body != nil {
			_ = output.Body.Close()
		}
		return nil, objectstore.Object{}, err
	}
	expected := int64(-1)
	if options.Range != nil {
		if err := validateRangeResponse(output, *options.Range); err != nil {
			return fail(err)
		}
		expected = options.Range.Length
	}
	object, err := objectFromGet(bucket, key, output)
	if err != nil {
		return fail(err)
	}
	if options.IfMatchVersion != "" && object.Version != options.IfMatchVersion {
		return fail(objectstore.ErrPrecondition)
	}
	if expected < 0 {
		expected = object.Size
	}
	return &exactReader{body: output.Body, remaining: expected}, object, nil
}

// Stat returns object metadata from one HEAD request.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	return store.head(ctx, bucket, key, "")
}

func (store *Store) head(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, ifMatch string) (objectstore.Object, error) {
	output, err := store.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), IfMatch: optionalString(ifMatch),
	})
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	object, err := objectFromHead(bucket, key, output)
	if err != nil {
		return objectstore.Object{}, err
	}
	if ifMatch != "" && object.Version != ifMatch {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	return object, nil
}

// Delete conditionally deletes the matching S3 entity tag. Deleting an absent
// key succeeds, as in S3.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	_, err := store.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), IfMatch: optionalString(version),
	})
	return mapError(err)
}

// List returns a stable key-ordered page from ListObjectsV2 metadata: key,
// size, entity tag, and modification time. User metadata and SHA-256 digests
// require one HEAD per key and are fetched, with bounded parallelism, only
// when IncludeMetadata is set.
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

	var objects []objectstore.Object
	var token *string
	truncated := false
	for {
		maximum := int32(1000)
		if options.Limit > 0 && options.Limit-len(objects) < int(maximum) {
			maximum = int32(options.Limit - len(objects))
		}
		input := &awss3.ListObjectsV2Input{
			Bucket: stringPointer(string(bucket)), Prefix: optionalString(options.Prefix), MaxKeys: int32Pointer(maximum), ContinuationToken: token,
		}
		if token == nil && options.After != "" {
			input.StartAfter = stringPointer(string(options.After))
		}
		output, err := store.client.ListObjectsV2(ctx, input)
		if err != nil {
			return objectstore.Page{}, mapError(err)
		}
		for _, listed := range output.Contents {
			if listed.Key == nil || strings.HasPrefix(*listed.Key, legacyStagingPrefix) {
				continue
			}
			object, err := objectFromListing(bucket, listed)
			if err != nil {
				return objectstore.Page{}, err
			}
			objects = append(objects, object)
		}
		truncated = output.IsTruncated != nil && *output.IsTruncated
		if options.Limit > 0 && len(objects) >= options.Limit {
			objects = objects[:options.Limit]
			break
		}
		if !truncated {
			break
		}
		if output.NextContinuationToken == nil || *output.NextContinuationToken == "" {
			return objectstore.Page{}, errors.New("s3 store: truncated listing without continuation token")
		}
		token = output.NextContinuationToken
	}
	sortObjects(objects)
	page := objectstore.Page{Objects: objects}
	if truncated && len(objects) > 0 {
		page.NextAfter = objects[len(objects)-1].Key
	}
	if options.IncludeMetadata && len(objects) > 0 {
		keys := make([]objectstore.Key, len(objects))
		for index, object := range objects {
			keys[index] = object.Key
		}
		results, err := store.StatMany(ctx, bucket, keys)
		if err != nil {
			return objectstore.Page{}, err
		}
		page.Objects = page.Objects[:0]
		for _, result := range results {
			switch {
			case result.Err == nil:
				page.Objects = append(page.Objects, result.Object)
			case errors.Is(result.Err, objectstore.ErrNotFound):
				// Deleted between listing and HEAD.
			default:
				return objectstore.Page{}, result.Err
			}
		}
	}
	return page, nil
}

func objectFromListing(bucket objectstore.Bucket, listed types.Object) (objectstore.Object, error) {
	if listed.Size == nil || listed.ETag == nil || *listed.ETag == "" || listed.LastModified == nil || *listed.Size < 0 {
		return objectstore.Object{}, errors.New("s3 store: incomplete listing entry")
	}
	return objectstore.Object{
		Bucket: bucket, Key: objectstore.Key(*listed.Key), Version: *listed.ETag, ETag: *listed.ETag,
		Size: *listed.Size, ModifiedAt: *listed.LastModified,
	}, nil
}

func validateRangeResponse(output *awss3.GetObjectOutput, requested objectstore.ByteRange) error {
	if output == nil || output.Body == nil || output.ContentLength == nil || output.ContentRange == nil || *output.ContentLength != requested.Length {
		return objectstore.ErrInvalidRange
	}
	value := *output.ContentRange
	if !strings.HasPrefix(value, "bytes ") {
		return objectstore.ErrInvalidRange
	}
	span, totalValue, found := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	startValue, endValue, spanFound := strings.Cut(span, "-")
	start, startErr := strconv.ParseInt(startValue, 10, 64)
	end, endErr := strconv.ParseInt(endValue, 10, 64)
	total, totalErr := strconv.ParseInt(totalValue, 10, 64)
	wantEnd := requested.Offset + requested.Length - 1
	if !found || !spanFound || startErr != nil || endErr != nil || totalErr != nil ||
		start != requested.Offset || end != wantEnd || total <= end {
		return objectstore.ErrInvalidRange
	}
	return nil
}

func objectFromHead(bucket objectstore.Bucket, key objectstore.Key, output *awss3.HeadObjectOutput) (objectstore.Object, error) {
	if output == nil || output.ContentLength == nil {
		return objectstore.Object{}, errors.New("s3 store: incomplete head response")
	}
	return objectFromMetadata(bucket, key, *output.ContentLength, output.ETag, output.LastModified, output.Metadata)
}

func objectFromGet(bucket objectstore.Bucket, key objectstore.Key, output *awss3.GetObjectOutput) (objectstore.Object, error) {
	if output == nil || output.Body == nil || output.ContentLength == nil {
		return objectstore.Object{}, errors.New("s3 store: incomplete get response")
	}
	size := *output.ContentLength
	if output.ContentRange != nil {
		separator := strings.LastIndexByte(*output.ContentRange, '/')
		if separator < 0 {
			return objectstore.Object{}, errors.New("s3 store: invalid content range")
		}
		parsed, err := strconv.ParseInt((*output.ContentRange)[separator+1:], 10, 64)
		if err != nil || parsed < 0 {
			return objectstore.Object{}, errors.New("s3 store: invalid content range")
		}
		size = parsed
	}
	return objectFromMetadata(bucket, key, size, output.ETag, output.LastModified, output.Metadata)
}

// objectFromMetadata converts S3 metadata; the reserved adapter keys are
// removed from the returned user metadata.
func objectFromMetadata(bucket objectstore.Bucket, key objectstore.Key, size int64, etag *string, modified *time.Time, raw map[string]string) (objectstore.Object, error) {
	if size < 0 || etag == nil || *etag == "" || modified == nil {
		return objectstore.Object{}, errors.New("s3 store: incomplete object metadata")
	}
	metadata := maps.Clone(raw)
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
	object := objectstore.Object{Bucket: bucket, Key: key, Version: *etag, ETag: *etag, Size: size, ModifiedAt: *modified, Metadata: metadata}
	if !hasDigest {
		return object, nil
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return objectstore.Object{}, errors.New("s3 store: invalid SHA-256 metadata")
	}
	copy(object.SHA256[:], decoded)
	object.SHA256Known = true
	return object, nil
}

func metadataWithDigest(metadata map[string]string, digest [sha256.Size]byte) (map[string]string, error) {
	return metadataWithOptionalDigest(metadata, &digest)
}

// metadataWithOptionalDigest rejects reserved user keys and adds the digest.
func metadataWithOptionalDigest(metadata map[string]string, digest *[sha256.Size]byte) (map[string]string, error) {
	result := maps.Clone(metadata)
	for key := range result {
		if strings.EqualFold(key, sha256MetadataKey) || strings.EqualFold(key, uploadMetadataKey) {
			return nil, errors.New("s3 store: reserved metadata key")
		}
	}
	if digest != nil {
		if result == nil {
			result = make(map[string]string)
		}
		result[sha256MetadataKey] = hex.EncodeToString(digest[:])
	}
	return result, nil
}

func validateObjectInput(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(objectstore.ValidateBucket(bucket), objectstore.ValidateKey(key))
}

func mapError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NoSuchKey", "NoSuchUpload", "NoSuchBucket", "NotFound":
			return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		case "PreconditionFailed", "ConditionalRequestConflict", "BadDigest", "XAmzContentSHA256Mismatch":
			return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		case "InvalidRange", "RequestedRangeNotSatisfiable":
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		case "InvalidPart", "InvalidPartOrder", "EntityTooSmall", "TooManyParts":
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidPart, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		}
	}
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) {
		switch responseError.HTTPStatusCode() {
		case 404:
			return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		case 409, 412:
			return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		case 416:
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		}
	}
	return err
}

// conditionalWriteError maps S3's 404 for an If-Match write on an absent key
// to ErrPrecondition, matching the portable IfMatchVersion contract.
func conditionalWriteError(err error, ifMatch string) error {
	mapped := mapError(err)
	var apiError smithy.APIError
	missingUpload := errors.As(err, &apiError) && apiError.ErrorCode() == "NoSuchUpload"
	if ifMatch != "" && errors.Is(mapped, objectstore.ErrNotFound) && !missingUpload {
		return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	}
	return mapped
}

func rangeHeader(byteRange objectstore.ByteRange) string {
	return fmt.Sprintf("bytes=%d-%d", byteRange.Offset, byteRange.Offset+byteRange.Length-1)
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func optionalDereference(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }
func int32Pointer(value int32) *int32    { return &value }

var (
	_ objectstore.MultipartStore = (*Store)(nil)
	_ objectstore.BatchStater    = (*Store)(nil)
	_ objectstore.BatchDeleter   = (*Store)(nil)
	_ objectstore.Copier         = (*Store)(nil)
	_ objectstore.Presigner      = (*Store)(nil)
)
