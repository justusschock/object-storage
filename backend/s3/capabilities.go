package s3

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	objectstore "github.com/justusschock/go-object-storage"
)

type presignAPI interface {
	PresignGetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignPutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignUploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// StatMany issues bounded-parallel HEAD requests (MetadataConcurrency).
func (store *Store) StatMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]objectstore.StatResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]objectstore.StatResult, len(keys))
	err := runParallel(ctx, len(keys), store.options.MetadataConcurrency, func(ctx context.Context, index int) error {
		object, err := store.Stat(ctx, bucket, keys[index])
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		results[index] = objectstore.StatResult{Object: object, Err: err}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, ctx.Err()
}

// DeleteMany deletes keys with DeleteObjects requests of up to 1,000 keys.
// Keys S3 reports as failed carry mapped per-key errors.
func (store *Store) DeleteMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]error, len(keys))
	indexes := make(map[string][]int, len(keys))
	var pending []types.ObjectIdentifier
	for index, key := range keys {
		if err := objectstore.ValidateKey(key); err != nil {
			results[index] = err
			continue
		}
		if _, seen := indexes[string(key)]; !seen {
			pending = append(pending, types.ObjectIdentifier{Key: stringPointer(string(key))})
		}
		indexes[string(key)] = append(indexes[string(key)], index)
	}
	for start := 0; start < len(pending); start += deleteBatchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := pending[start:min(start+deleteBatchSize, len(pending))]
		output, err := store.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: stringPointer(string(bucket)), Delete: &types.Delete{Objects: batch, Quiet: boolPointer(true)},
		})
		if err != nil {
			return nil, mapError(err)
		}
		for _, failure := range output.Errors {
			err := deleteError(failure)
			for _, index := range indexes[optionalDereference(failure.Key)] {
				results[index] = err
			}
		}
	}
	return results, nil
}

func deleteError(failure types.Error) error {
	code, message := optionalDereference(failure.Code), optionalDereference(failure.Message)
	detail := fmt.Sprintf("s3 store: delete %q: %s: %s", optionalDereference(failure.Key), code, message)
	switch code {
	case "NoSuchKey":
		return nil
	case "PreconditionFailed":
		return fmt.Errorf("%w: %s", objectstore.ErrPrecondition, detail)
	}
	return errors.New(detail)
}

// Copy copies server-side. Sources up to MultipartCopyThreshold use one
// CopyObject request; larger sources use a multipart upload of UploadPartCopy
// ranges pinned to the source entity tag. Destination conditions are
// evaluated atomically by S3. A known source digest is preserved.
func (store *Store) Copy(ctx context.Context, srcBucket objectstore.Bucket, srcKey objectstore.Key, dstBucket objectstore.Bucket, dstKey objectstore.Key, options objectstore.CopyOptions) (objectstore.Object, error) {
	if err := errors.Join(validateObjectInput(ctx, srcBucket, srcKey), validateObjectInput(ctx, dstBucket, dstKey)); err != nil {
		return objectstore.Object{}, err
	}
	if options.Metadata != nil {
		if _, err := metadataWithOptionalDigest(options.Metadata, nil); err != nil {
			return objectstore.Object{}, err
		}
	}
	source, err := store.head(ctx, srcBucket, srcKey, options.SourceVersion)
	if err != nil {
		return objectstore.Object{}, err
	}
	if source.Size > store.options.MultipartCopyThreshold {
		return store.copyMultipart(ctx, source, dstBucket, dstKey, options)
	}
	input := &awss3.CopyObjectInput{
		Bucket: stringPointer(string(dstBucket)), Key: stringPointer(string(dstKey)), CopySource: stringPointer(copySource(srcBucket, srcKey)),
		CopySourceIfMatch: optionalString(options.SourceVersion), IfMatch: optionalString(options.IfMatchVersion),
	}
	if options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	if options.Metadata != nil || options.ContentType != "" {
		// Replacing metadata must restate the digest, so the copy is pinned
		// to the source version whose digest was read.
		metadata, err := copyMetadata(source, options)
		if err != nil {
			return objectstore.Object{}, err
		}
		input.MetadataDirective = types.MetadataDirectiveReplace
		input.Metadata = metadata
		input.ContentType = optionalString(options.ContentType)
		input.CopySourceIfMatch = stringPointer(source.Version)
	}
	output, err := store.client.CopyObject(ctx, input)
	if err != nil {
		return objectstore.Object{}, conditionalWriteError(err, options.IfMatchVersion)
	}
	etag := ""
	if output.CopyObjectResult != nil {
		etag = optionalDereference(output.CopyObjectResult.ETag)
	}
	return store.published(ctx, dstBucket, dstKey, copyPublication(source, options, etag, ""))
}

func copyPublication(source objectstore.Object, options objectstore.CopyOptions, etag, nonce string) publication {
	// Multipart copies carry an upload nonce; single CopyObject calls do not.
	write := publication{multipart: nonce != "", etag: etag, size: source.Size, nonce: nonce, metadata: source.Metadata}
	if options.Metadata != nil {
		write.metadata = options.Metadata
	}
	if digest, ok := source.Digest(); ok {
		write.digest = &digest
	}
	return write
}

func (store *Store) copyMultipart(ctx context.Context, source objectstore.Object, dstBucket objectstore.Bucket, dstKey objectstore.Key, options objectstore.CopyOptions) (objectstore.Object, error) {
	metadata, err := copyMetadata(source, options)
	if err != nil {
		return objectstore.Object{}, err
	}
	nonce, err := newNonce()
	if err != nil {
		return objectstore.Object{}, err
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[uploadMetadataKey] = nonce
	created, err := store.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: stringPointer(string(dstBucket)), Key: stringPointer(string(dstKey)), Metadata: metadata, ContentType: optionalString(options.ContentType),
	})
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	uploadID := optionalDereference(created.UploadId)
	if uploadID == "" {
		return objectstore.Object{}, errors.New("s3 store: missing multipart upload ID")
	}
	abort := func(primary error) (objectstore.Object, error) {
		if abortErr := store.abortUpload(ctx, string(dstBucket), string(dstKey), uploadID); abortErr != nil && !errors.Is(abortErr, objectstore.ErrNotFound) {
			return objectstore.Object{}, errors.Join(primary, abortErr)
		}
		return objectstore.Object{}, primary
	}
	partSize := max(store.options.CopyPartSize, (source.Size+maximumPartCount-1)/maximumPartCount)
	count := int((source.Size + partSize - 1) / partSize)
	parts := make([]types.CompletedPart, count)
	var mu sync.Mutex
	err = runParallel(ctx, count, store.options.Concurrency, func(ctx context.Context, index int) error {
		offset := int64(index) * partSize
		length := min(partSize, source.Size-offset)
		output, err := store.client.UploadPartCopy(ctx, &awss3.UploadPartCopyInput{
			Bucket: stringPointer(string(dstBucket)), Key: stringPointer(string(dstKey)), UploadId: stringPointer(uploadID),
			PartNumber: int32Pointer(int32(index + 1)), CopySource: stringPointer(copySource(source.Bucket, source.Key)),
			CopySourceRange:   stringPointer(rangeHeader(objectstore.ByteRange{Offset: offset, Length: length})),
			CopySourceIfMatch: stringPointer(source.Version),
		})
		if err != nil {
			return mapError(err)
		}
		if output.CopyPartResult == nil || optionalDereference(output.CopyPartResult.ETag) == "" {
			return errors.New("s3 store: missing copied part ETag")
		}
		mu.Lock()
		parts[index] = types.CompletedPart{PartNumber: int32Pointer(int32(index + 1)), ETag: output.CopyPartResult.ETag}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return abort(err)
	}
	input := &awss3.CompleteMultipartUploadInput{
		Bucket: stringPointer(string(dstBucket)), Key: stringPointer(string(dstKey)), UploadId: stringPointer(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, IfMatch: optionalString(options.IfMatchVersion),
	}
	if options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	output, err := store.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return abort(conditionalWriteError(err, options.IfMatchVersion))
	}
	return store.published(ctx, dstBucket, dstKey, copyPublication(source, options, optionalDereference(output.ETag), nonce))
}

// copyMetadata returns the destination metadata: the replacement or source
// user metadata plus the known source digest.
func copyMetadata(source objectstore.Object, options objectstore.CopyOptions) (map[string]string, error) {
	metadata := source.Metadata
	if options.Metadata != nil {
		metadata = options.Metadata
	}
	if digest, ok := source.Digest(); ok {
		return metadataWithDigest(metadata, digest)
	}
	return metadataWithOptionalDigest(metadata, nil)
}

// copySource URL-encodes bucket/key for x-amz-copy-source, escaping every
// byte outside the unreserved set except the path separator.
func copySource(bucket objectstore.Bucket, key objectstore.Key) string {
	segments := strings.Split(string(bucket)+"/"+string(key), "/")
	for index, segment := range segments {
		segments[index] = strings.ReplaceAll(url.PathEscape(segment), "+", "%2B")
	}
	return strings.Join(segments, "/")
}

// PresignGet presigns a GetObject request; a range is sent as a signed Range
// header.
func (store *Store) PresignGet(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, byteRange *objectstore.ByteRange) (objectstore.PresignedRequest, error) {
	if err := store.validatePresign(ctx, bucket, key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	input := &awss3.GetObjectInput{Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key))}
	if byteRange != nil {
		if err := byteRange.Validate(); err != nil {
			return objectstore.PresignedRequest{}, err
		}
		input.Range = stringPointer(rangeHeader(*byteRange))
	}
	request, err := store.presigner.PresignGetObject(ctx, input, awss3.WithPresignExpires(expires))
	return presigned(request, expires, err)
}

// PresignPut presigns a PutObject request. A supplied SHA256 is bound as the
// signed x-amz-checksum-sha256 header, which S3 verifies, and recorded as the
// adapter's digest metadata. Conditions and Size become signed headers.
func (store *Store) PresignPut(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, options objectstore.PutOptions) (objectstore.PresignedRequest, error) {
	if err := store.validatePresign(ctx, bucket, key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	metadata, err := metadataWithOptionalDigest(options.Metadata, options.SHA256)
	if err != nil {
		return objectstore.PresignedRequest{}, err
	}
	input := &awss3.PutObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), ContentType: optionalString(options.ContentType),
		Metadata: metadata, IfMatch: optionalString(options.IfMatchVersion),
	}
	if options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	if options.SHA256 != nil {
		input.ChecksumSHA256 = stringPointer(base64.StdEncoding.EncodeToString(options.SHA256[:]))
	}
	if options.Size > 0 {
		input.ContentLength = int64Pointer(options.Size)
	}
	request, err := store.presigner.PresignPutObject(ctx, input, awss3.WithPresignExpires(expires))
	return presigned(request, expires, err)
}

// PresignUploadPart presigns one UploadPart request bound to the part's size
// and SHA-256 digest.
func (store *Store) PresignUploadPart(ctx context.Context, upload objectstore.Upload, number int, size int64, digest [32]byte, expires time.Duration) (objectstore.PresignedRequest, error) {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if err := store.validatePresign(ctx, custody.Bucket, custody.Key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if number <= 0 || number > maximumPartCount || size < 0 {
		return objectstore.PresignedRequest{}, objectstore.ErrInvalidPart
	}
	request, err := store.presigner.PresignUploadPart(ctx, &awss3.UploadPartInput{
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(string(custody.Key)), UploadId: stringPointer(custody.UploadID),
		PartNumber: int32Pointer(int32(number)), ContentLength: int64Pointer(size),
		ChecksumSHA256: stringPointer(base64.StdEncoding.EncodeToString(digest[:])),
	}, awss3.WithPresignExpires(expires))
	return presigned(request, expires, err)
}

func (store *Store) validatePresign(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	if store.presigner == nil {
		return errors.New("s3 store: presigning requires a store constructed from an *s3.Client")
	}
	if expires <= 0 || expires > 7*24*time.Hour {
		return errors.New("s3 store: presign expiry must be within (0, 7 days]")
	}
	return nil
}

func presigned(request *v4.PresignedHTTPRequest, expires time.Duration, err error) (objectstore.PresignedRequest, error) {
	if err != nil {
		return objectstore.PresignedRequest{}, mapError(err)
	}
	return objectstore.PresignedRequest{
		Method: request.Method, URL: request.URL, Header: request.SignedHeader.Clone(), ExpiresAt: time.Now().Add(expires),
	}, nil
}

func boolPointer(value bool) *bool { return &value }
