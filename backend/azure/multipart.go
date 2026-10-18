package azure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	objectstore "github.com/justusschock/object-storage"
)

// markerVersion identifies the marker blob format.
const markerVersion = 1

// uploadMarker is the JSON content of a multipart marker blob. It is the
// authoritative record of the upload's target and options.
type uploadMarker struct {
	Version int                    `json:"version"`
	Bucket  objectstore.Bucket     `json:"bucket"`
	Key     objectstore.Key        `json:"key"`
	Options objectstore.PutOptions `json:"options"`
}

// BeginMultipart validates the options and creates the upload's marker blob
// "<multipartPrefix><upload ID>" in the bucket. Nothing is written to the
// final key until CompleteMultipart.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Upload{}, err
	}
	options = options.Clone()
	if _, err := normalizeMetadata(options.Metadata); err != nil {
		return objectstore.Upload{}, err
	}
	id, err := newSessionID()
	if err != nil {
		return objectstore.Upload{}, err
	}
	data, err := json.Marshal(uploadMarker{Version: markerVersion, Bucket: bucket, Key: key, Options: options})
	if err != nil {
		return objectstore.Upload{}, fmt.Errorf("azure store: encode multipart marker: %w", err)
	}
	_, err = store.blockBlob(bucket, objectstore.Key(multipartPrefix+id)).Upload(ctx, streaming.NopCloser(bytes.NewReader(data)), &blockblob.UploadOptions{
		HTTPHeaders:      contentHeaders("application/json"),
		Metadata:         sdkMetadata(nil, map[string]string{targetMetadataKey: targetHash(key)}),
		AccessConditions: writeConditions(true, ""),
	})
	if err != nil {
		return objectstore.Upload{}, mapError(err)
	}
	return objectstore.Upload{ID: id, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart streams one numbered part to Put Block on the final blob without
// buffering it. The block ID is derived from the upload ID and part number;
// the returned CompletedPart.ETag is that block ID. The part's length and
// SHA-256 digest are verified while streaming, before the last byte is
// sent, so a mismatching part is never staged. A streamed part cannot be
// resent, so Put Block is not retried.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	if err := validateUpload(ctx, upload); err != nil {
		return objectstore.CompletedPart{}, err
	}
	if number <= 0 || number > maximumBlockCount || size < 0 || size > maximumBlockSize || body == nil {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	if err := store.checkMarker(ctx, upload); err != nil {
		return objectstore.CompletedPart{}, err
	}
	id := blockID(upload.ID, number)
	part := newPartBody(ctx, contextReader{ctx: ctx, reader: body}, size, digest)
	if size == 0 {
		if err := part.verify(); err != nil {
			return objectstore.CompletedPart{}, err
		}
	}
	noRetry := policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
	_, err := store.blockBlob(upload.Bucket, upload.Key).StageBlock(noRetry, id, part, nil)
	if err != nil {
		if part.failure != nil {
			return objectstore.CompletedPart{}, part.failure
		}
		return objectstore.CompletedPart{}, mapError(err)
	}
	if !part.verified {
		return objectstore.CompletedPart{}, fmt.Errorf("%w: part body was not fully sent", objectstore.ErrInvalidPart)
	}
	return objectstore.CompletedPart{Number: number, ETag: id, Size: size, SHA256: digest}, nil
}

// CompleteMultipart commits the parts' blocks on the final blob with the
// upload's conditions, content type, and metadata, then deletes the marker.
// Parts must be numbered 1..n in order. CompletedPart.ETag may be empty
// (for example after a presigned part upload, whose Azure response carries
// no entity tag) or must equal the block ID PutPart returned. Each part's
// staged size is checked against CompletedPart.Size before committing.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	if err := validateUpload(ctx, upload); err != nil {
		return objectstore.Object{}, err
	}
	marker, err := store.readMarker(ctx, upload)
	if errors.Is(err, objectstore.ErrNotFound) {
		// An earlier completion may have published the object and deleted
		// the marker before its response was lost.
		if object, earlierErr := store.completedEarlier(ctx, upload); earlierErr == nil {
			return object, nil
		} else if !errors.Is(earlierErr, objectstore.ErrNotFound) {
			return objectstore.Object{}, earlierErr
		}
		return objectstore.Object{}, err
	}
	if err != nil {
		return objectstore.Object{}, err
	}
	if len(completed) == 0 || len(completed) > maximumBlockCount {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	ids := make([]string, len(completed))
	var total int64
	for index, part := range completed {
		id := blockID(upload.ID, index+1)
		if part.Number != index+1 || (part.ETag != "" && part.ETag != id) || part.Size < 0 || part.Size > maximumBlockSize {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
		ids[index] = id
		total += part.Size
	}
	if marker.Options.Size > 0 && marker.Options.Size != total {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	client := store.blockBlob(upload.Bucket, upload.Key)
	if err := store.checkStagedBlocks(ctx, client, ids, completed); err != nil {
		if object, earlierErr := store.completedEarlier(ctx, upload); earlierErr == nil {
			store.deleteMarker(ctx, upload)
			return object, nil
		}
		return objectstore.Object{}, err
	}
	metadata, err := normalizeMetadata(marker.Options.Metadata)
	if err != nil {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	reserved := map[string]string{uploadMetadataKey: upload.ID}
	if marker.Options.SHA256 != nil {
		reserved[sha256MetadataKey] = hex.EncodeToString(marker.Options.SHA256[:])
	}
	response, err := client.CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{
		HTTPHeaders:      contentHeaders(marker.Options.ContentType),
		Metadata:         sdkMetadata(metadata, reserved),
		AccessConditions: writeConditions(marker.Options.IfAbsent, marker.Options.IfMatchVersion),
	})
	if err != nil {
		if object, earlierErr := store.completedEarlier(ctx, upload); earlierErr == nil {
			store.deleteMarker(ctx, upload)
			return object, nil
		}
		return objectstore.Object{}, conditionalWriteError(err, marker.Options.IfMatchVersion)
	}
	store.deleteMarker(ctx, upload)
	return written(upload.Bucket, upload.Key, response.ETag, response.LastModified, total, metadata, marker.Options.SHA256)
}

// checkStagedBlocks verifies that every part's block is staged with the
// declared size.
func (store *Store) checkStagedBlocks(ctx context.Context, client *blockblob.Client, ids []string, completed []objectstore.CompletedPart) error {
	response, err := client.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
	if err != nil {
		if mapped := mapError(err); errors.Is(mapped, objectstore.ErrNotFound) {
			return fmt.Errorf("%w: no staged parts", objectstore.ErrInvalidPart)
		} else {
			return mapped
		}
	}
	staged := make(map[string]int64, len(response.UncommittedBlocks))
	for _, staging := range response.UncommittedBlocks {
		if staging != nil && staging.Name != nil && staging.Size != nil {
			staged[*staging.Name] = *staging.Size
		}
	}
	for index, id := range ids {
		size, ok := staged[id]
		if !ok || size != completed[index].Size {
			return fmt.Errorf("%w: part %d is not staged with %d bytes", objectstore.ErrInvalidPart, index+1, completed[index].Size)
		}
	}
	return nil
}

// completedEarlier returns the object an earlier completion of upload
// published, identified by the upload ID in its metadata, or ErrNotFound.
func (store *Store) completedEarlier(ctx context.Context, upload objectstore.Upload) (objectstore.Object, error) {
	response, err := store.blockBlob(upload.Bucket, upload.Key).GetProperties(ctx, nil)
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	for name, value := range response.Metadata {
		if strings.EqualFold(name, uploadMetadataKey) && value != nil && *value == upload.ID && response.ContentLength != nil {
			return objectFromMetadata(upload.Bucket, upload.Key, *response.ContentLength, response.ETag, response.LastModified, response.Metadata)
		}
	}
	return objectstore.Object{}, objectstore.ErrNotFound
}

// AbortMultipart deletes the upload's marker; later PutPart and
// CompleteMultipart calls fail with ErrNotFound. Azure offers no request to
// discard individual uncommitted blocks: blocks already staged on the final
// blob are garbage collected after seven days or when the blob is next
// written or deleted.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	if err := validateUpload(ctx, upload); err != nil {
		return err
	}
	return store.deleteBlob(ctx, upload.Bucket, multipartPrefix+upload.ID, "")
}

func (store *Store) deleteMarker(ctx context.Context, upload objectstore.Upload) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	// A leftover marker is hidden from listings and harmless.
	_ = store.deleteBlob(cleanupCtx, upload.Bucket, multipartPrefix+upload.ID, "")
}

// checkMarker verifies that the upload is active and targets upload.Key.
func (store *Store) checkMarker(ctx context.Context, upload objectstore.Upload) error {
	response, err := store.blockBlob(upload.Bucket, objectstore.Key(multipartPrefix+upload.ID)).GetProperties(ctx, nil)
	if err != nil {
		return mapError(err)
	}
	for name, value := range response.Metadata {
		if strings.EqualFold(name, targetMetadataKey) && value != nil && *value == targetHash(upload.Key) {
			return nil
		}
	}
	return fmt.Errorf("%w: upload belongs to a different key", objectstore.ErrInvalidPart)
}

// readMarker downloads and decodes the upload's marker.
func (store *Store) readMarker(ctx context.Context, upload objectstore.Upload) (uploadMarker, error) {
	response, err := store.blockBlob(upload.Bucket, objectstore.Key(multipartPrefix+upload.ID)).DownloadStream(ctx, nil)
	if err != nil {
		return uploadMarker{}, mapError(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return uploadMarker{}, err
	}
	var marker uploadMarker
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil || marker.Version != markerVersion || marker.Bucket != upload.Bucket || marker.Key != upload.Key {
		return uploadMarker{}, fmt.Errorf("%w: multipart marker does not match the upload", objectstore.ErrInvalidPart)
	}
	return marker, nil
}

// validateUpload checks the upload's target and ID format (32 lower-case
// hexadecimal characters).
func validateUpload(ctx context.Context, upload objectstore.Upload) error {
	if err := validateObjectInput(ctx, upload.Bucket, upload.Key); err != nil {
		return err
	}
	if decoded, err := hex.DecodeString(upload.ID); err != nil || len(decoded) != 16 || strings.ToLower(upload.ID) != upload.ID {
		return fmt.Errorf("%w: malformed upload ID", objectstore.ErrInvalidPart)
	}
	return nil
}

// targetHash identifies a multipart target key in marker metadata, which
// must be ASCII.
func targetHash(key objectstore.Key) string {
	sum := sha256.Sum256([]byte(key))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
