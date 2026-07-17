// Package s3 adapts AWS SDK v2 S3 clients to the backend-neutral object API.
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
	"sort"
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

const sha256MetadataKey = "go-object-storage-sha256"

type clientAPI interface {
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
}

type uploaderAPI interface {
	Upload(context.Context, *awss3.PutObjectInput, ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

// Store maps portable object operations to S3 requests.
type Store struct {
	client   clientAPI
	uploader uploaderAPI
}

// New constructs an S3 store from caller-owned SDK clients.
func New(client *awss3.Client, uploader *manager.Uploader) (*Store, error) {
	if client == nil || uploader == nil {
		return nil, errors.New("s3 store: nil client or uploader")
	}
	return &Store{client: client, uploader: uploader}, nil
}

// Put uploads one object with atomic S3 write preconditions and SHA-256 checking.
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
	metadata, err := metadataWithDigest(options.Metadata, digest)
	if err != nil {
		return objectstore.Object{}, err
	}
	input := &awss3.PutObjectInput{
		Bucket:            stringPointer(string(bucket)),
		Key:               stringPointer(string(key)),
		Body:              bytes.NewReader(data),
		ContentLength:     int64Pointer(int64(len(data))),
		ContentType:       optionalString(options.ContentType),
		Metadata:          metadata,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    stringPointer(base64.StdEncoding.EncodeToString(digest[:])),
	}
	applyPutConditions(input, options)
	if _, err := store.uploader.Upload(ctx, input); err != nil {
		return objectstore.Object{}, mapError(err)
	}
	object, err := store.Stat(ctx, bucket, key)
	if err != nil {
		return objectstore.Object{}, err
	}
	if object.Size != int64(len(data)) || object.SHA256 != digest {
		return objectstore.Object{}, errors.New("s3 store: uploaded object metadata mismatch")
	}
	return object, nil
}

// Get opens an object or exact inclusive S3 range while returning full metadata.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return nil, objectstore.Object{}, err
	}
	input := &awss3.GetObjectInput{Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), IfMatch: optionalString(options.IfMatchVersion)}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
		end := options.Range.Offset + options.Range.Length - 1
		input.Range = stringPointer(fmt.Sprintf("bytes=%d-%d", options.Range.Offset, end))
	}
	output, err := store.client.GetObject(ctx, input)
	if err != nil {
		return nil, objectstore.Object{}, mapError(err)
	}
	object, hasDigest, err := objectFromGet(bucket, key, output)
	if err != nil {
		_ = output.Body.Close()
		return nil, objectstore.Object{}, err
	}
	if !hasDigest {
		if options.Range == nil {
			data, readErr := io.ReadAll(output.Body)
			closeErr := output.Body.Close()
			if readErr != nil || closeErr != nil {
				return nil, objectstore.Object{}, errors.Join(readErr, closeErr)
			}
			object.SHA256 = sha256.Sum256(data)
			output.Body = io.NopCloser(bytes.NewReader(data))
		} else {
			digest, digestErr := store.loadDigest(ctx, bucket, key, object.Version)
			if digestErr != nil {
				_ = output.Body.Close()
				return nil, objectstore.Object{}, digestErr
			}
			object.SHA256 = digest
		}
	}
	return output.Body, object, nil
}

// Stat returns full portable object metadata.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	output, err := store.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	object, hasDigest, err := objectFromHead(bucket, key, output)
	if err != nil {
		return objectstore.Object{}, err
	}
	if !hasDigest {
		object.SHA256, err = store.loadDigest(ctx, bucket, key, object.Version)
	}
	return object, err
}

// Delete conditionally deletes the matching S3 entity tag.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	_, err := store.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), IfMatch: optionalString(version),
	})
	return mapError(err)
}

// List returns a stable key-ordered page and follows S3 continuation tokens as needed.
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

	var keys []objectstore.Key
	var token *string
	truncated := false
	for {
		maximum := int32(1000)
		if options.Limit > 0 && options.Limit-len(keys) < int(maximum) {
			maximum = int32(options.Limit - len(keys))
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
			if listed.Key != nil {
				keys = append(keys, objectstore.Key(*listed.Key))
			}
		}
		truncated = output.IsTruncated != nil && *output.IsTruncated
		if options.Limit > 0 && len(keys) >= options.Limit {
			keys = keys[:options.Limit]
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

	sort.Slice(keys, func(left, right int) bool { return keys[left] < keys[right] })
	page := objectstore.Page{Objects: make([]objectstore.Object, len(keys))}
	for index, key := range keys {
		object, err := store.Stat(ctx, bucket, key)
		if err != nil {
			return objectstore.Page{}, err
		}
		page.Objects[index] = object
	}
	if truncated && len(keys) > 0 {
		page.NextAfter = keys[len(keys)-1]
	}
	return page, nil
}

// BeginMultipart creates an S3 multipart upload with immutable option copies.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	metadata, err := metadataWithOptionalDigest(options.Metadata, options.SHA256)
	if err != nil {
		return objectstore.Upload{}, err
	}
	output, err := store.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), ContentType: optionalString(options.ContentType),
		Metadata: metadata, ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return objectstore.Upload{}, mapError(err)
	}
	if output.UploadId == nil || *output.UploadId == "" {
		return objectstore.Upload{}, errors.New("s3 store: missing multipart upload ID")
	}
	return objectstore.Upload{ID: *output.UploadId, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart uploads and verifies one numbered multipart part.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	if err := validateUpload(ctx, upload); err != nil {
		return objectstore.CompletedPart{}, err
	}
	if number <= 0 || number > 10000 || size < 0 {
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
	encoded := base64.StdEncoding.EncodeToString(digest[:])
	output, err := store.client.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket: stringPointer(string(upload.Bucket)), Key: stringPointer(string(upload.Key)), UploadId: stringPointer(upload.ID),
		PartNumber: int32Pointer(int32(number)), Body: bytes.NewReader(data), ContentLength: int64Pointer(size),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ChecksumSHA256: stringPointer(encoded),
	})
	if err != nil {
		return objectstore.CompletedPart{}, mapError(err)
	}
	if output.ETag == nil || *output.ETag == "" {
		return objectstore.CompletedPart{}, errors.New("s3 store: missing part ETag")
	}
	return objectstore.CompletedPart{Number: number, ETag: *output.ETag, Size: size, SHA256: digest}, nil
}

// CompleteMultipart validates ordering and atomically applies write preconditions.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	if err := validateUpload(ctx, upload); err != nil {
		return objectstore.Object{}, err
	}
	if len(completed) == 0 || len(completed) > 10000 {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	parts := make([]types.CompletedPart, len(completed))
	var total int64
	for index, part := range completed {
		if part.Number != index+1 || part.ETag == "" || part.Size < 0 || total > int64(^uint64(0)>>1)-part.Size {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
		total += part.Size
		parts[index] = types.CompletedPart{
			PartNumber: int32Pointer(int32(part.Number)), ETag: stringPointer(part.ETag),
			ChecksumSHA256: stringPointer(base64.StdEncoding.EncodeToString(part.SHA256[:])),
		}
	}
	input := &awss3.CompleteMultipartUploadInput{
		Bucket: stringPointer(string(upload.Bucket)), Key: stringPointer(string(upload.Key)), UploadId: stringPointer(upload.ID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, MpuObjectSize: int64Pointer(total),
	}
	if upload.Options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	input.IfMatch = optionalString(upload.Options.IfMatchVersion)
	output, err := store.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	object, err := store.Stat(ctx, upload.Bucket, upload.Key)
	if err != nil {
		return objectstore.Object{}, err
	}
	if upload.Options.SHA256 != nil {
		actual, verifyErr := store.loadDigest(ctx, upload.Bucket, upload.Key, object.Version)
		if verifyErr != nil {
			return objectstore.Object{}, verifyErr
		}
		if actual != *upload.Options.SHA256 {
			if output.ETag != nil {
				_ = store.Delete(ctx, upload.Bucket, upload.Key, *output.ETag)
			}
			return objectstore.Object{}, objectstore.ErrPrecondition
		}
		object.SHA256 = actual
	}
	return object, nil
}

// AbortMultipart removes one active S3 upload.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	if err := validateUpload(ctx, upload); err != nil {
		return err
	}
	_, err := store.client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket: stringPointer(string(upload.Bucket)), Key: stringPointer(string(upload.Key)), UploadId: stringPointer(upload.ID),
	})
	return mapError(err)
}

func (store *Store) loadDigest(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) ([sha256.Size]byte, error) {
	output, err := store.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(string(key)), IfMatch: optionalString(version),
	})
	if err != nil {
		return [sha256.Size]byte{}, mapError(err)
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, contextReader{ctx: ctx, reader: output.Body})
	closeErr := output.Body.Close()
	if readErr != nil || closeErr != nil {
		return [sha256.Size]byte{}, errors.Join(readErr, closeErr)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, ctx.Err()
}

func objectFromHead(bucket objectstore.Bucket, key objectstore.Key, output *awss3.HeadObjectOutput) (objectstore.Object, bool, error) {
	if output == nil || output.ContentLength == nil {
		return objectstore.Object{}, false, errors.New("s3 store: incomplete head response")
	}
	return objectFromMetadata(bucket, key, *output.ContentLength, output.ETag, output.LastModified, output.Metadata)
}

func objectFromGet(bucket objectstore.Bucket, key objectstore.Key, output *awss3.GetObjectOutput) (objectstore.Object, bool, error) {
	if output == nil || output.Body == nil || output.ContentLength == nil {
		return objectstore.Object{}, false, errors.New("s3 store: incomplete get response")
	}
	size := *output.ContentLength
	if output.ContentRange != nil {
		separator := strings.LastIndexByte(*output.ContentRange, '/')
		if separator < 0 {
			return objectstore.Object{}, false, errors.New("s3 store: invalid content range")
		}
		parsed, err := strconv.ParseInt((*output.ContentRange)[separator+1:], 10, 64)
		if err != nil || parsed < 0 {
			return objectstore.Object{}, false, errors.New("s3 store: invalid content range")
		}
		size = parsed
	}
	return objectFromMetadata(bucket, key, size, output.ETag, output.LastModified, output.Metadata)
}

func objectFromMetadata(bucket objectstore.Bucket, key objectstore.Key, size int64, etag *string, modified *time.Time, raw map[string]string) (objectstore.Object, bool, error) {
	if size < 0 || etag == nil || *etag == "" || modified == nil {
		return objectstore.Object{}, false, errors.New("s3 store: incomplete object metadata")
	}
	metadata := maps.Clone(raw)
	encoded, hasDigest := metadata[sha256MetadataKey]
	delete(metadata, sha256MetadataKey)
	object := objectstore.Object{Bucket: bucket, Key: key, Version: *etag, ETag: *etag, Size: size, ModifiedAt: *modified, Metadata: metadata}
	if !hasDigest {
		return object, false, nil
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return objectstore.Object{}, false, errors.New("s3 store: invalid SHA-256 metadata")
	}
	copy(object.SHA256[:], decoded)
	return object, true, nil
}

func metadataWithDigest(metadata map[string]string, digest [sha256.Size]byte) (map[string]string, error) {
	return metadataWithOptionalDigest(metadata, &digest)
}

func metadataWithOptionalDigest(metadata map[string]string, digest *[sha256.Size]byte) (map[string]string, error) {
	result := maps.Clone(metadata)
	for key := range result {
		if strings.EqualFold(key, sha256MetadataKey) {
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

func applyPutConditions(input *awss3.PutObjectInput, options objectstore.PutOptions) {
	if options.IfAbsent {
		input.IfNoneMatch = stringPointer("*")
	}
	input.IfMatch = optionalString(options.IfMatchVersion)
}

func validateObjectInput(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(objectstore.ValidateBucket(bucket), objectstore.ValidateKey(key))
}

func validateUpload(ctx context.Context, upload objectstore.Upload) error {
	if err := validateObjectInput(ctx, upload.Bucket, upload.Key); err != nil {
		return err
	}
	if upload.ID == "" {
		return objectstore.ErrInvalidPart
	}
	return nil
}

func mapError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NoSuchKey", "NoSuchUpload", "NoSuchBucket", "NotFound":
			return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err)
		case "PreconditionFailed", "ConditionalRequestConflict", "BadDigest":
			return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err)
		case "InvalidRange", "RequestedRangeNotSatisfiable":
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err)
		}
	}
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) {
		switch responseError.HTTPStatusCode() {
		case 404:
			return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err)
		case 409, 412:
			return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err)
		case 416:
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err)
		}
	}
	return err
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
		return nil, errors.New("s3 store: nil reader")
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

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }
func int32Pointer(value int32) *int32    { return &value }

var _ objectstore.MultipartStore = (*Store)(nil)
