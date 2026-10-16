package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	objectstore "github.com/justusschock/go-object-storage"
)

// multipartTokenVersion 2 identifies uploads created directly on the final
// key. Version 1 tokens (staging-key uploads from v0.2.0) are rejected with
// ErrInvalidPart.
const multipartTokenVersion = 2

type multipartCustody struct {
	Version  int                    `json:"version"`
	Bucket   objectstore.Bucket     `json:"bucket"`
	Key      objectstore.Key        `json:"key"`
	UploadID string                 `json:"upload_id"`
	Nonce    string                 `json:"nonce"`
	Options  objectstore.PutOptions `json:"options"`
}

// BeginMultipart creates an S3 multipart upload on the final key. A declared
// PutOptions.SHA256 is recorded as object metadata now, because S3 metadata
// cannot be changed at completion.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	if err := options.ValidateSize(); err != nil {
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
	nonce, err := newNonce()
	if err != nil {
		return objectstore.Upload{}, err
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[uploadMetadataKey] = nonce
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
	custody := multipartCustody{Version: multipartTokenVersion, Bucket: bucket, Key: key, UploadID: *output.UploadId, Nonce: nonce, Options: options}
	token, err := encodeMultipartCustody(custody)
	if err != nil {
		abortErr := store.abortUpload(ctx, string(bucket), string(key), *output.UploadId)
		return objectstore.Upload{}, errors.Join(err, abortErr)
	}
	return objectstore.Upload{ID: token, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart streams one numbered part without buffering it. The part digest is
// the SigV4 payload hash and the x-amz-checksum-sha256 value, so S3 rejects
// mismatching bytes; the adapter also hashes the stream and rejects bodies
// with a different digest or length.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
		return objectstore.CompletedPart{}, err
	}
	if number <= 0 || number > maximumPartCount || size < 0 || body == nil {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	verifier := newVerifyingReader(io.LimitReader(contextReader{ctx: ctx, reader: body}, size))
	etag, err := store.uploadPart(ctx, string(custody.Bucket), string(custody.Key), custody.UploadID, number, verifier, size, digest)
	if err != nil {
		if errors.Is(err, objectstore.ErrPrecondition) || (verifier.eof && verifier.count < size) {
			return objectstore.CompletedPart{}, fmt.Errorf("%w: %v", objectstore.ErrInvalidPart, err)
		}
		return objectstore.CompletedPart{}, err
	}
	var extra [1]byte
	if count, _ := body.Read(extra[:]); count > 0 || verifier.count != size || verifier.digest() != digest {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	return objectstore.CompletedPart{Number: number, ETag: etag, Size: size, SHA256: digest}, nil
}

// CompleteMultipart completes the upload in place on the final key with the
// upload's put conditions. When the upload no longer exists because an
// earlier completion succeeded, it returns that published object.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
		return objectstore.Object{}, err
	}
	if len(completed) == 0 {
		// Report a missing (aborted or completed) upload before the part
		// list, as the reference backends do.
		if _, err := store.client.ListParts(ctx, &awss3.ListPartsInput{
			Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(string(custody.Key)), UploadId: stringPointer(custody.UploadID), MaxParts: int32Pointer(1),
		}); err != nil {
			if mapped := mapError(err); errors.Is(mapped, objectstore.ErrNotFound) {
				return objectstore.Object{}, objectstore.ErrNotFound
			} else if errors.Is(mapped, context.Canceled) || errors.Is(mapped, context.DeadlineExceeded) {
				return objectstore.Object{}, mapped
			}
		}
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	if len(completed) > maximumPartCount {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	parts := make([]types.CompletedPart, len(completed))
	var total int64
	for index, part := range completed {
		if part.Number != index+1 || part.ETag == "" || part.Size < 0 || total > int64(^uint64(0)>>1)-part.Size {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
		total += part.Size
		parts[index] = completedPart(part.Number, part.ETag, part.SHA256)
	}
	if custody.Options.Size > 0 && custody.Options.Size != total {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	output, err := store.complete(ctx, string(custody.Bucket), string(custody.Key), custody.UploadID, parts, total, custody.Options)
	if errors.Is(err, objectstore.ErrNotFound) || errors.Is(err, objectstore.ErrPrecondition) {
		// A retried completion may find its upload gone, or its own earlier
		// publication failing the IfAbsent/IfMatchVersion condition.
		object, earlierErr := store.completedEarlier(ctx, custody, total)
		if earlierErr == nil {
			return object, nil
		}
		if errors.Is(err, objectstore.ErrPrecondition) && errors.Is(earlierErr, objectstore.ErrNotFound) {
			return objectstore.Object{}, err
		}
		return objectstore.Object{}, earlierErr
	}
	if err != nil {
		return objectstore.Object{}, err
	}
	return store.published(ctx, custody.Bucket, custody.Key, publication{
		multipart: true, etag: optionalDereference(output.ETag), size: total, digest: custody.Options.SHA256, nonce: custody.Nonce, metadata: custody.Options.Metadata,
	})
}

// completedEarlier resolves a missing upload: either a previous completion
// published this upload's object (its nonce is in the object metadata) or
// the upload was aborted.
func (store *Store) completedEarlier(ctx context.Context, custody multipartCustody, total int64) (objectstore.Object, error) {
	output, err := store.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: stringPointer(string(custody.Bucket)), Key: stringPointer(string(custody.Key))})
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	for name, value := range output.Metadata {
		if strings.EqualFold(name, uploadMetadataKey) && value == custody.Nonce {
			object, err := objectFromHead(custody.Bucket, custody.Key, output)
			if err == nil && object.Size != total {
				err = errors.New("s3 store: completed multipart size mismatch")
			}
			return object, err
		}
	}
	return objectstore.Object{}, objectstore.ErrNotFound
}

// AbortMultipart removes one active S3 upload.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	custody, err := decodeMultipartCustody(ctx, upload)
	if err != nil {
		return err
	}
	return store.abortUpload(ctx, string(custody.Bucket), string(custody.Key), custody.UploadID)
}

func newNonce() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("s3 store: create multipart identity: %w", err)
	}
	return hex.EncodeToString(nonce[:]), nil
}

func encodeMultipartCustody(custody multipartCustody) (string, error) {
	if err := validateMultipartInputStrings(custody.Bucket, custody.Key, custody.Options); err != nil ||
		!utf8.ValidString(custody.UploadID) || !utf8.ValidString(custody.Nonce) {
		return "", errors.New("s3 store: multipart custody contains invalid UTF-8")
	}
	data, err := json.Marshal(custody)
	if err != nil {
		return "", fmt.Errorf("s3 store: encode multipart custody: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeMultipartCustody(ctx context.Context, upload objectstore.Upload) (multipartCustody, error) {
	if err := validateObjectInput(ctx, upload.Bucket, upload.Key); err != nil {
		return multipartCustody{}, err
	}
	if upload.ID == "" {
		return multipartCustody{}, objectstore.ErrInvalidPart
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
		custody.Bucket != upload.Bucket || custody.Key != upload.Key || custody.UploadID == "" || custody.Nonce == "" {
		return multipartCustody{}, objectstore.ErrInvalidPart
	}
	if _, err := metadataWithOptionalDigest(custody.Options.Metadata, custody.Options.SHA256); err != nil {
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
