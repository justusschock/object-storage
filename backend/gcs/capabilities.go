package gcs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	objectstore "github.com/justusschock/object-storage"
)

const maximumPresignExpiry = 7 * 24 * time.Hour

// StatMany issues bounded-parallel object attribute requests (Concurrency).
func (store *Store) StatMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]objectstore.StatResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]objectstore.StatResult, len(keys))
	err := runParallel(ctx, len(keys), store.options.Concurrency, func(ctx context.Context, index int) error {
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

// DeleteMany issues bounded-parallel unconditional deletes (Concurrency);
// absent keys succeed.
func (store *Store) DeleteMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]error, len(keys))
	err := runParallel(ctx, len(keys), store.options.Concurrency, func(ctx context.Context, index int) error {
		err := store.Delete(ctx, bucket, keys[index], "")
		switch {
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			return err
		case errors.Is(err, objectstore.ErrNotFound):
			err = nil
		}
		results[index] = err
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, ctx.Err()
}

// Copy copies server-side with the GCS rewrite API (storage.Copier), which
// handles objects of any size. Destination preconditions are evaluated
// atomically by GCS. SourceVersion is checked against the source generation,
// and the rewrite is pinned to that generation with ifSourceGenerationMatch.
// Without metadata overrides GCS copies the source metadata, including the
// recorded digest. Replacing Metadata or ContentType restates the source
// digest, so such copies are pinned to the source generation whose digest
// was read.
func (store *Store) Copy(ctx context.Context, srcBucket objectstore.Bucket, srcKey objectstore.Key, dstBucket objectstore.Bucket, dstKey objectstore.Key, options objectstore.CopyOptions) (objectstore.Object, error) {
	if err := errors.Join(store.validateObjectInput(ctx, srcBucket, srcKey), store.validateObjectInput(ctx, dstBucket, dstKey)); err != nil {
		return objectstore.Object{}, err
	}
	if _, err := metadataWithOptionalDigest(options.Metadata, nil); err != nil {
		return objectstore.Object{}, err
	}
	conditions, err := writeConditions(options.IfAbsent, options.IfMatchVersion)
	if err != nil {
		return objectstore.Object{}, err
	}
	var sourceGeneration int64
	if options.SourceVersion != "" {
		generation, ok := parseVersion(options.SourceVersion)
		if !ok {
			return objectstore.Object{}, objectstore.ErrPrecondition
		}
		sourceGeneration = generation
	}
	override := options.Metadata != nil || options.ContentType != ""
	for range readRetryLimit {
		source := store.object(srcBucket, srcKey)
		var sourceAttrs *storage.ObjectAttrs
		if override || sourceGeneration != 0 {
			sourceAttrs, err = source.Attrs(ctx)
			if err != nil {
				return objectstore.Object{}, mapError(err)
			}
			if sourceGeneration != 0 && sourceAttrs.Generation != sourceGeneration {
				return objectstore.Object{}, objectstore.ErrPrecondition
			}
			source = source.If(storage.Conditions{GenerationMatch: sourceAttrs.Generation})
		}
		destination := store.object(dstBucket, dstKey)
		if conditions != nil {
			destination = destination.If(*conditions)
		}
		copier := destination.CopierFrom(source)
		if override {
			sourceObject, err := objectFromAttrs(srcBucket, sourceAttrs)
			if err != nil {
				return objectstore.Object{}, err
			}
			metadata := sourceObject.Metadata
			if options.Metadata != nil {
				metadata = options.Metadata
			}
			var digest *[32]byte
			if value, ok := sourceObject.Digest(); ok {
				digest = &value
			}
			if copier.Metadata, err = metadataWithOptionalDigest(metadata, digest); err != nil {
				return objectstore.Object{}, err
			}
			copier.ContentType = sourceAttrs.ContentType
			if options.ContentType != "" {
				copier.ContentType = options.ContentType
			}
		}
		attrs, err := copier.Run(ctx)
		if err != nil {
			mapped := mapError(err)
			if errors.Is(mapped, objectstore.ErrPrecondition) && sourceAttrs != nil && sourceGeneration == 0 && store.sourceChanged(ctx, srcBucket, srcKey, sourceAttrs.Generation) {
				// The pinned source generation was replaced; read it again.
				continue
			}
			return objectstore.Object{}, mapped
		}
		return objectFromAttrs(dstBucket, attrs)
	}
	return objectstore.Object{}, errors.New("gcs store: source generation changed repeatedly during Copy")
}

func (store *Store) sourceChanged(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, generation int64) bool {
	attrs, err := store.object(bucket, key).Attrs(ctx)
	return err == nil && attrs.Generation != generation
}

// PresignGet presigns a V4 GET URL; a range is bound as a signed Range
// header.
func (store *Store) PresignGet(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, byteRange *objectstore.ByteRange) (objectstore.PresignedRequest, error) {
	if err := store.validatePresign(ctx, bucket, key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	header := http.Header{}
	if byteRange != nil {
		if err := byteRange.Validate(); err != nil {
			return objectstore.PresignedRequest{}, err
		}
		header.Set("Range", fmt.Sprintf("bytes=%d-%d", byteRange.Offset, byteRange.Offset+byteRange.Length-1))
	}
	return store.presign(bucket, string(key), http.MethodGet, expires, "", header)
}

// PresignPut presigns a V4 XML API PUT URL. ContentType, user metadata, and
// the SHA-256 digest metadata are bound as signed headers (Content-Type and
// x-goog-meta-*); IfAbsent and IfMatchVersion as x-goog-if-generation-match;
// Size as x-goog-content-length-range. GCS records but does not verify the
// digest. Metadata sent through the XML API is stored with lowercase keys.
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
	conditions, err := writeConditions(options.IfAbsent, options.IfMatchVersion)
	if err != nil {
		return objectstore.PresignedRequest{}, err
	}
	header := http.Header{}
	for name, value := range metadata {
		if err := validateHeaderMetadata(name, value); err != nil {
			return objectstore.PresignedRequest{}, err
		}
		header.Set("X-Goog-Meta-"+name, value)
	}
	if conditions != nil {
		header.Set("X-Goog-If-Generation-Match", strconv.FormatInt(conditions.GenerationMatch, 10))
	}
	if options.Size > 0 {
		header.Set("X-Goog-Content-Length-Range", fmt.Sprintf("%d,%d", options.Size, options.Size))
	}
	return store.presign(bucket, string(key), http.MethodPut, expires, options.ContentType, header)
}

// PresignUploadPart presigns a V4 PUT of one part's staging object, bound to
// the part's exact size (x-goog-content-length-range) and recording its
// SHA-256 digest as signed metadata. Complete the upload with the ETag
// response header; CompleteMultipart verifies the recorded digest and size.
func (store *Store) PresignUploadPart(ctx context.Context, upload objectstore.Upload, number int, size int64, digest [32]byte, expires time.Duration) (objectstore.PresignedRequest, error) {
	id, err := store.validateUpload(ctx, upload)
	if err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if err := store.validatePresign(ctx, upload.Bucket, upload.Key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if number <= 0 || number > maximumPartCount || size < 0 {
		return objectstore.PresignedRequest{}, objectstore.ErrInvalidPart
	}
	metadata, err := metadataWithDigest(nil, digest)
	if err != nil {
		return objectstore.PresignedRequest{}, err
	}
	header := http.Header{}
	for name, value := range metadata {
		header.Set("X-Goog-Meta-"+name, value)
	}
	header.Set("X-Goog-Content-Length-Range", fmt.Sprintf("%d,%d", size, size))
	return store.presign(upload.Bucket, store.partKey(id, number), http.MethodPut, expires, "application/octet-stream", header)
}

func (store *Store) presign(bucket objectstore.Bucket, name, method string, expires time.Duration, contentType string, header http.Header) (objectstore.PresignedRequest, error) {
	options := &storage.SignedURLOptions{
		GoogleAccessID: store.options.GoogleAccessID, PrivateKey: store.options.PrivateKey, SignBytes: store.options.SignBytes,
		Method: method, Expires: time.Now().Add(expires), ContentType: contentType, Style: store.options.URLStyle,
		Insecure: store.options.Insecure, Scheme: storage.SigningSchemeV4, Hostname: store.options.Hostname,
	}
	for name, values := range header {
		for _, value := range values {
			options.Headers = append(options.Headers, strings.ToLower(name)+":"+value)
		}
	}
	signed, err := store.client.Bucket(string(bucket)).SignedURL(name, options)
	if err != nil {
		return objectstore.PresignedRequest{}, fmt.Errorf("gcs store: sign URL: %w", err)
	}
	header = header.Clone()
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return objectstore.PresignedRequest{Method: method, URL: signed, Header: header, ExpiresAt: options.Expires}, nil
}

func (store *Store) validatePresign(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration) error {
	if err := store.validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	if expires <= 0 || expires > maximumPresignExpiry {
		return errors.New("gcs store: presign expiry must be within (0, 7 days]")
	}
	return nil
}

// validateHeaderMetadata rejects metadata that cannot be sent as an
// x-goog-meta-* header.
func validateHeaderMetadata(name, value string) error {
	if name == "" {
		return errors.New("gcs store: empty metadata key")
	}
	for _, char := range name {
		if char > 0x7e || char <= 0x20 || strings.ContainsRune("\"(),/:;<=>?@[\\]{}", char) {
			return fmt.Errorf("gcs store: metadata key %q is not a valid header name", name)
		}
	}
	for _, char := range value {
		if char == '\r' || char == '\n' || char == 0 {
			return fmt.Errorf("gcs store: metadata value for %q contains a control character", name)
		}
	}
	return nil
}
