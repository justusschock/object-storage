package azure

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	objectstore "github.com/justusschock/go-object-storage"
)

// StatMany issues bounded-parallel Get Blob Properties requests
// (BatchConcurrency).
func (store *Store) StatMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]objectstore.StatResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]objectstore.StatResult, len(keys))
	err := runParallel(ctx, len(keys), store.options.BatchConcurrency, func(ctx context.Context, index int) error {
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

// DeleteMany issues bounded-parallel unconditional Delete Blob requests
// (BatchConcurrency). Absent keys succeed.
func (store *Store) DeleteMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]error, len(keys))
	err := runParallel(ctx, len(keys), store.options.BatchConcurrency, func(ctx context.Context, index int) error {
		err := store.Delete(ctx, bucket, keys[index], "")
		switch {
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			return err
		case errors.Is(err, objectstore.ErrNotFound) && validateKey(keys[index]) == nil:
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

// Copy copies server-side with Copy Blob from the source blob's URL, which
// the client's own credential authorizes within one storage account (Shared
// Key, or Microsoft Entra ID with read access to the source). Destination
// conditions are evaluated atomically by Azure. A known source digest is
// preserved: without replacement metadata Azure copies the source metadata,
// and replacement metadata restates the digest with the copy pinned to the
// source version it was read from. Copies that Azure reports as pending are
// polled every CopyPollInterval until they finish.
//
// Copy Blob cannot set a content type, and cannot replace metadata with an
// empty set: a ContentType override (Set Blob Properties, which also resets
// the other standard HTTP properties) and an emptied metadata set (Set Blob
// Metadata) are applied by follow-up requests conditioned on the copied
// entity tag. These follow-ups are not atomic with the copy: readers may
// briefly observe the copy with the source content type or metadata, and a
// concurrent writer makes them fail with ErrPrecondition.
func (store *Store) Copy(ctx context.Context, srcBucket objectstore.Bucket, srcKey objectstore.Key, dstBucket objectstore.Bucket, dstKey objectstore.Key, options objectstore.CopyOptions) (objectstore.Object, error) {
	if err := errors.Join(validateObjectInput(ctx, srcBucket, srcKey), validateObjectInput(ctx, dstBucket, dstKey)); err != nil {
		return objectstore.Object{}, err
	}
	replacement, err := normalizeMetadata(options.Metadata)
	if err != nil {
		return objectstore.Object{}, err
	}
	source, err := store.properties(ctx, srcBucket, srcKey, options.SourceVersion)
	if err != nil {
		return objectstore.Object{}, err
	}
	request := &blob.StartCopyFromURLOptions{AccessConditions: writeConditions(options.IfAbsent, options.IfMatchVersion)}
	sourceVersion := options.SourceVersion
	metadata := source.Metadata
	clearMetadata := false
	if options.Metadata != nil {
		metadata = replacement
		reserved := map[string]string{}
		if digest, ok := source.Digest(); ok {
			reserved[sha256MetadataKey] = fmt.Sprintf("%x", digest)
		}
		request.Metadata = sdkMetadata(replacement, reserved)
		clearMetadata = request.Metadata == nil && len(source.Metadata) > 0
		sourceVersion = source.Version
	}
	if sourceVersion != "" {
		request.SourceModifiedAccessConditions = &blob.SourceModifiedAccessConditions{SourceIfMatch: to(azcore.ETag(sourceVersion))}
	}
	destination := store.containerClient(dstBucket).NewBlobClient(string(dstKey))
	sourceURL := store.containerClient(srcBucket).NewBlobClient(string(srcKey)).URL()
	response, err := destination.StartCopyFromURL(ctx, sourceURL, request)
	if err != nil {
		if bloberror.HasCode(err, bloberror.CannotVerifyCopySource) {
			var responseError *azcore.ResponseError
			if errors.As(err, &responseError) && responseError.StatusCode == http.StatusNotFound {
				return objectstore.Object{}, fmt.Errorf("%w: %v", objectstore.ErrNotFound, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
			}
		}
		return objectstore.Object{}, conditionalWriteError(err, options.IfMatchVersion)
	}
	etag, modified := response.ETag, response.LastModified
	if response.CopyStatus != nil && *response.CopyStatus == blob.CopyStatusTypePending {
		if etag, modified, err = store.awaitCopy(ctx, destination, response.CopyID); err != nil {
			return objectstore.Object{}, err
		}
	}
	if clearMetadata {
		updated, err := destination.SetMetadata(ctx, map[string]*string{}, &blob.SetMetadataOptions{AccessConditions: matchCondition(string(deref(etag)))})
		if err != nil {
			return objectstore.Object{}, mapError(err)
		}
		etag, modified = updated.ETag, updated.LastModified
	}
	if options.ContentType != "" {
		updated, err := destination.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentType: to(options.ContentType)}, &blob.SetHTTPHeadersOptions{AccessConditions: matchCondition(string(deref(etag)))})
		if err != nil {
			return objectstore.Object{}, mapError(err)
		}
		etag, modified = updated.ETag, updated.LastModified
	}
	// Report the destination as stored; the source may have changed after
	// it was read unless the copy was pinned to its version.
	if object, err := store.properties(ctx, dstBucket, dstKey, ""); err == nil && etag != nil && object.Version == string(*etag) {
		return object, nil
	}
	var digest *[32]byte
	if value, ok := source.Digest(); ok {
		digest = &value
	}
	return written(dstBucket, dstKey, etag, modified, source.Size, maps.Clone(metadata), digest)
}

// awaitCopy polls a pending copy until it succeeds, fails, or is replaced.
func (store *Store) awaitCopy(ctx context.Context, destination *blob.Client, copyID *string) (*azcore.ETag, *time.Time, error) {
	for {
		timer := time.NewTimer(store.options.CopyPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
		properties, err := destination.GetProperties(ctx, nil)
		if err != nil {
			return nil, nil, mapError(err)
		}
		if copyID != nil && (properties.CopyID == nil || *properties.CopyID != *copyID) {
			return nil, nil, fmt.Errorf("%w: destination replaced while the copy was pending", objectstore.ErrPrecondition)
		}
		switch deref(properties.CopyStatus) {
		case blob.CopyStatusTypeSuccess:
			return properties.ETag, properties.LastModified, nil
		case blob.CopyStatusTypePending:
		default:
			return nil, nil, fmt.Errorf("azure store: copy %s: %s", deref(properties.CopyStatus), deref(properties.CopyStatusDescription))
		}
	}
}

// PresignGet returns a read-only service SAS URL for the blob. A byte range
// is sent as the x-ms-range header; a SAS cannot bind headers, so the client
// may read the whole blob instead.
func (store *Store) PresignGet(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, byteRange *objectstore.ByteRange) (objectstore.PresignedRequest, error) {
	if err := validatePresign(ctx, bucket, key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	header := http.Header{}
	if byteRange != nil {
		if err := byteRange.Validate(); err != nil {
			return objectstore.PresignedRequest{}, err
		}
		header.Set("x-ms-range", fmt.Sprintf("bytes=%d-%d", byteRange.Offset, byteRange.Offset+byteRange.Length-1))
	}
	return store.presign(bucket, key, http.MethodGet, sas.BlobPermissions{Read: true}, expires, nil, header)
}

// PresignPut returns a Put Blob request authorized by a service SAS. The
// returned headers carry x-ms-blob-type, the content type, the metadata
// (including the declared SHA-256 digest metadata), and the If-None-Match /
// If-Match conditions, but a SAS cannot bind headers: a holder of the URL may
// omit or change them, and Azure cannot verify the SHA-256 digest, which is
// recorded as declared. IfAbsent is additionally enforced by granting only
// the SAS create permission, which cannot overwrite an existing blob.
// PutOptions.Size is reported as an informational Content-Length header.
func (store *Store) PresignPut(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration, options objectstore.PutOptions) (objectstore.PresignedRequest, error) {
	if err := validatePresign(ctx, bucket, key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	metadata, err := normalizeMetadata(options.Metadata)
	if err != nil {
		return objectstore.PresignedRequest{}, err
	}
	header := http.Header{"x-ms-blob-type": {"BlockBlob"}}
	for name, value := range metadata {
		header["x-ms-meta-"+name] = []string{value}
	}
	if options.SHA256 != nil {
		header["x-ms-meta-"+sha256MetadataKey] = []string{fmt.Sprintf("%x", *options.SHA256)}
	}
	if options.ContentType != "" {
		header.Set("x-ms-blob-content-type", options.ContentType)
	}
	if options.IfAbsent {
		header.Set("If-None-Match", "*")
	}
	if options.IfMatchVersion != "" {
		header.Set("If-Match", options.IfMatchVersion)
	}
	if options.Size > 0 {
		header.Set("Content-Length", fmt.Sprint(options.Size))
	}
	permissions := sas.BlobPermissions{Create: true, Write: !options.IfAbsent}
	return store.presign(bucket, key, http.MethodPut, permissions, expires, nil, header)
}

// PresignUploadPart returns a Put Block request for one part, authorized by
// a service SAS with write permission on the final blob. The block ID is
// derived from the upload ID and part number. Azure's Put Block response has
// no ETag header: complete the upload with CompletedPart{Number: number,
// Size: size, SHA256: digest} and an empty ETag (or the block ID). A SAS
// cannot bind the block ID, size, or digest, and the write permission also
// authorizes other writes to the blob until the URL expires; Azure verifies
// neither the part's size nor its SHA-256 digest, but CompleteMultipart
// checks every staged block's size.
func (store *Store) PresignUploadPart(ctx context.Context, upload objectstore.Upload, number int, size int64, digest [32]byte, expires time.Duration) (objectstore.PresignedRequest, error) {
	if err := validateUpload(ctx, upload); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if err := validatePresign(ctx, upload.Bucket, upload.Key, expires); err != nil {
		return objectstore.PresignedRequest{}, err
	}
	if number <= 0 || number > maximumBlockCount || size < 0 || size > maximumBlockSize {
		return objectstore.PresignedRequest{}, objectstore.ErrInvalidPart
	}
	_ = digest
	query := url.Values{"comp": {"block"}, "blockid": {blockID(upload.ID, number)}}
	header := http.Header{"Content-Length": {fmt.Sprint(size)}}
	return store.presign(upload.Bucket, upload.Key, http.MethodPut, sas.BlobPermissions{Write: true}, expires, query, header)
}

func validatePresign(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, expires time.Duration) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	if expires <= 0 {
		return errors.New("azure store: presign expiry must be positive")
	}
	return nil
}

// presign signs a blob service SAS (sr=b) with whole-second expiry and
// appends the extra query parameters.
func (store *Store) presign(bucket objectstore.Bucket, key objectstore.Key, method string, permissions sas.BlobPermissions, expires time.Duration, query url.Values, header http.Header) (objectstore.PresignedRequest, error) {
	expiry := time.Now().Add(expires).UTC().Truncate(time.Second)
	client := store.containerClient(bucket).NewBlobClient(string(key))
	var signed string
	if store.options.SharedKey != nil {
		parameters, err := sas.BlobSignatureValues{
			ContainerName: string(bucket), BlobName: string(key), Permissions: permissions.String(), ExpiryTime: expiry,
		}.SignWithSharedKey(store.options.SharedKey)
		if err != nil {
			return objectstore.PresignedRequest{}, fmt.Errorf("azure store: sign SAS: %w", err)
		}
		signed = client.URL() + "?" + parameters.Encode()
	} else {
		var err error
		if signed, err = client.GetSASURL(permissions, expiry, nil); err != nil {
			if errors.Is(err, bloberror.MissingSharedKeyCredential) {
				return objectstore.PresignedRequest{}, ErrPresignUnavailable
			}
			return objectstore.PresignedRequest{}, fmt.Errorf("azure store: sign SAS: %w", err)
		}
	}
	if len(query) > 0 {
		separator := "?"
		if strings.Contains(signed, "?") {
			separator = "&"
		}
		signed += separator + query.Encode()
	}
	return objectstore.PresignedRequest{Method: method, URL: signed, Header: header, ExpiresAt: expiry}, nil
}

func deref[T any](value *T) T {
	var zero T
	if value == nil {
		return zero
	}
	return *value
}
