// Package s3 adapts AWS SDK v2 S3 clients to the backend-neutral object API.
package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	objectstore "github.com/justusschock/go-object-storage"
)

const (
	sha256MetadataKey      = "go-object-storage-sha256"
	multipartStagingPrefix = "/.go-object-storage/multipart/"
	multipartTokenVersion  = 1
	cleanupTimeout         = 10 * time.Second
)

type clientAPI interface {
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
	ListParts(context.Context, *awss3.ListPartsInput, ...func(*awss3.Options)) (*awss3.ListPartsOutput, error)
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
	}
	applyPutConditions(input, options)
	if _, err := store.upload(ctx, input); err != nil {
		return objectstore.Object{}, err
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
	if options.Range != nil {
		if err := validateRangeResponse(output, *options.Range); err != nil {
			_ = output.Body.Close()
			return nil, objectstore.Object{}, err
		}
		data, readErr := io.ReadAll(contextReader{ctx: ctx, reader: output.Body})
		closeErr := output.Body.Close()
		if readErr != nil || closeErr != nil {
			return nil, objectstore.Object{}, errors.Join(readErr, closeErr)
		}
		if int64(len(data)) != options.Range.Length {
			return nil, objectstore.Object{}, objectstore.ErrInvalidRange
		}
		output.Body = io.NopCloser(bytes.NewReader(data))
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
			if listed.Key != nil && !strings.HasPrefix(*listed.Key, multipartStagingPrefix) {
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

// BeginMultipart creates an S3 multipart upload in an internal staging namespace.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	options = options.Clone()
	if err := validateMultipartInputStrings(bucket, key, options); err != nil {
		return objectstore.Upload{}, err
	}
	metadata, err := metadataWithOptionalDigest(options.Metadata, options.SHA256)
	if err != nil {
		return objectstore.Upload{}, err
	}
	stagingKey, err := newMultipartStagingKey()
	if err != nil {
		return objectstore.Upload{}, err
	}
	output, err := store.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(stagingKey), ContentType: optionalString(options.ContentType),
		Metadata: metadata, ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return objectstore.Upload{}, mapError(err)
	}
	if output.UploadId == nil || *output.UploadId == "" {
		return objectstore.Upload{}, errors.New("s3 store: missing multipart upload ID")
	}
	custody := multipartCustody{
		Version: multipartTokenVersion, Bucket: bucket, Key: key, StagingKey: stagingKey,
		UploadID: *output.UploadId, Options: options,
	}
	token, err := encodeMultipartCustody(custody)
	if err != nil {
		abortErr := store.abortUpload(ctx, bucket, stagingKey, *output.UploadId)
		return objectstore.Upload{}, errors.Join(err, abortErr)
	}
	return objectstore.Upload{ID: token, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart uploads and verifies one numbered multipart part.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
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
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(custody.StagingKey), UploadId: stringPointer(custody.UploadID),
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

// CompleteMultipart verifies staged bytes before atomically publishing the final key.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
		return objectstore.Object{}, err
	}
	active := true
	stagingETag := ""
	if _, err := store.client.ListParts(ctx, &awss3.ListPartsInput{
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(custody.StagingKey), UploadId: stringPointer(custody.UploadID), MaxParts: int32Pointer(1),
	}); err != nil {
		mapped := mapError(err)
		if !errors.Is(mapped, objectstore.ErrNotFound) {
			return objectstore.Object{}, mapped
		}
		active = false
		stagingETag, err = store.stagedObjectETag(ctx, custody)
		if err != nil {
			return objectstore.Object{}, err
		}
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
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(custody.StagingKey), UploadId: stringPointer(custody.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, MpuObjectSize: int64Pointer(total),
	}
	if active {
		output, completeErr := store.client.CompleteMultipartUpload(ctx, input)
		if completeErr != nil {
			mapped := mapError(completeErr)
			if !errors.Is(mapped, objectstore.ErrNotFound) {
				return objectstore.Object{}, mapped
			}
			stagingETag, err = store.stagedObjectETag(ctx, custody)
			if err != nil {
				return objectstore.Object{}, err
			}
		} else {
			stagingETag = optionalDereference(output.ETag)
			if stagingETag == "" {
				stagingETag, err = store.stagedObjectETag(ctx, custody)
				if err != nil {
					return objectstore.Object{}, err
				}
			}
		}
	}
	file, size, digest, err := store.spoolStagedObject(ctx, custody, stagingETag)
	if err != nil {
		return objectstore.Object{}, err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()
	if size != total {
		return objectstore.Object{}, errors.New("s3 store: staged multipart size mismatch")
	}
	if custody.Options.SHA256 != nil && digest != *custody.Options.SHA256 {
		cleanupErr := store.deleteStagedObject(ctx, custody, stagingETag)
		return objectstore.Object{}, errors.Join(objectstore.ErrPrecondition, cleanupErr)
	}
	metadata, err := metadataWithDigest(custody.Options.Metadata, digest)
	if err != nil {
		return objectstore.Object{}, err
	}
	finalInput := &awss3.PutObjectInput{
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(string(custody.Key)), Body: file,
		ContentLength: int64Pointer(size), ContentType: optionalString(custody.Options.ContentType), Metadata: metadata,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}
	applyPutConditions(finalInput, custody.Options)
	if _, err := store.upload(ctx, finalInput); err != nil {
		return objectstore.Object{}, err
	}
	object, err := store.Stat(ctx, custody.Bucket, custody.Key)
	if err != nil {
		return objectstore.Object{}, err
	}
	if object.Size != size || object.SHA256 != digest {
		return objectstore.Object{}, errors.New("s3 store: published multipart metadata mismatch")
	}
	if err := store.deleteStagedObject(ctx, custody, stagingETag); err != nil {
		return objectstore.Object{}, err
	}
	return object, nil
}

// AbortMultipart removes one active S3 upload.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
		return err
	}
	return store.abortUpload(ctx, custody.Bucket, custody.StagingKey, custody.UploadID)
}

type multipartCustody struct {
	Version    int                    `json:"version"`
	Bucket     objectstore.Bucket     `json:"bucket"`
	Key        objectstore.Key        `json:"key"`
	StagingKey string                 `json:"staging_key"`
	UploadID   string                 `json:"upload_id"`
	Options    objectstore.PutOptions `json:"options"`
}

func newMultipartStagingKey() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("s3 store: create multipart identity: %w", err)
	}
	return multipartStagingPrefix + hex.EncodeToString(nonce[:]), nil
}

func encodeMultipartCustody(custody multipartCustody) (string, error) {
	if err := validateMultipartInputStrings(custody.Bucket, custody.Key, custody.Options); err != nil ||
		!utf8.ValidString(custody.StagingKey) || !utf8.ValidString(custody.UploadID) {
		return "", errors.New("s3 store: multipart custody contains invalid UTF-8")
	}
	data, err := json.Marshal(custody)
	if err != nil {
		return "", fmt.Errorf("s3 store: encode multipart custody: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeMultipartCustody(ctx context.Context, upload objectstore.Upload) (multipartCustody, error) {
	if err := validateUpload(ctx, upload); err != nil {
		return multipartCustody{}, err
	}
	data, err := base64.RawURLEncoding.DecodeString(upload.ID)
	if err != nil {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	var custody multipartCustody
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&custody); err != nil {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	canonical, err := encodeMultipartCustody(custody)
	if err != nil || canonical != upload.ID || custody.Version != multipartTokenVersion ||
		custody.Bucket != upload.Bucket || custody.Key != upload.Key || custody.UploadID == "" ||
		!strings.HasPrefix(custody.StagingKey, multipartStagingPrefix) {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	if _, err := metadataWithOptionalDigest(custody.Options.Metadata, custody.Options.SHA256); err != nil {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	if err := validateMultipartInputStrings(custody.Bucket, custody.Key, custody.Options); err != nil ||
		!utf8.ValidString(custody.StagingKey) || !utf8.ValidString(custody.UploadID) {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	custody.Options = custody.Options.Clone()
	return custody, nil
}

func validateMultipartInputStrings(bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) error {
	if !utf8.ValidString(string(bucket)) {
		return fmt.Errorf("%w: invalid UTF-8", objectstore.ErrInvalidBucket)
	}
	if !utf8.ValidString(string(key)) {
		return fmt.Errorf("%w: invalid UTF-8", objectstore.ErrInvalidKey)
	}
	if !utf8.ValidString(options.ContentType) || !utf8.ValidString(options.IfMatchVersion) {
		return errors.New("s3 store: multipart options contain invalid UTF-8")
	}
	for key, value := range options.Metadata {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return errors.New("s3 store: multipart metadata contains invalid UTF-8")
		}
	}
	return nil
}

func (store *Store) upload(ctx context.Context, input *awss3.PutObjectInput) (*manager.UploadOutput, error) {
	output, err := store.uploader.Upload(ctx, input)
	if err == nil {
		return output, nil
	}
	mapped := mapError(err)
	var failure manager.MultiUploadFailure
	if !errors.As(err, &failure) || failure.UploadID() == "" {
		return nil, mapped
	}
	abortErr := store.abortUpload(ctx, objectstore.Bucket(*input.Bucket), *input.Key, failure.UploadID())
	if abortErr == nil || errors.Is(abortErr, objectstore.ErrNotFound) {
		return nil, mapped
	}
	return nil, errors.Join(mapped, fmt.Errorf("s3 store: abort failed multipart upload: %w", abortErr))
}

func (store *Store) abortUpload(ctx context.Context, bucket objectstore.Bucket, key string, uploadID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	_, err := store.client.AbortMultipartUpload(cleanupCtx, &awss3.AbortMultipartUploadInput{
		Bucket: stringPointer(string(bucket)), Key: stringPointer(key), UploadId: stringPointer(uploadID),
	})
	return mapError(err)
}

func (store *Store) deleteStagedObject(ctx context.Context, custody multipartCustody, etag string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	_, err := store.client.DeleteObject(cleanupCtx, &awss3.DeleteObjectInput{
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(custody.StagingKey), IfMatch: optionalString(etag),
	})
	err = mapError(err)
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil
	}
	return err
}

func (store *Store) stagedObjectETag(ctx context.Context, custody multipartCustody) (string, error) {
	output, err := store.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(custody.StagingKey),
	})
	if err != nil {
		return "", mapError(err)
	}
	if output == nil || output.ETag == nil || *output.ETag == "" {
		return "", objectstore.ErrNotFound
	}
	return *output.ETag, nil
}

func (store *Store) spoolStagedObject(ctx context.Context, custody multipartCustody, etag string) (*os.File, int64, [sha256.Size]byte, error) {
	output, err := store.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(custody.StagingKey), IfMatch: optionalString(etag),
	})
	if err != nil {
		return nil, 0, [sha256.Size]byte{}, mapError(err)
	}
	if output == nil || output.Body == nil {
		return nil, 0, [sha256.Size]byte{}, errors.New("s3 store: incomplete staged object response")
	}
	file, err := os.CreateTemp("", "go-object-storage-s3-*")
	if err != nil {
		_ = output.Body.Close()
		return nil, 0, [sha256.Size]byte{}, err
	}
	fail := func(err error) (*os.File, int64, [sha256.Size]byte, error) {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, 0, [sha256.Size]byte{}, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), contextReader{ctx: ctx, reader: output.Body})
	closeErr := output.Body.Close()
	if copyErr != nil || closeErr != nil {
		return fail(errors.Join(copyErr, closeErr))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return file, size, digest, nil
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
		case "InvalidPart", "InvalidPartOrder", "EntityTooSmall", "TooManyParts":
			return fmt.Errorf("%w: %v", objectstore.ErrInvalidPart, err)
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

func optionalDereference(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }
func int32Pointer(value int32) *int32    { return &value }

var _ objectstore.MultipartStore = (*Store)(nil)
