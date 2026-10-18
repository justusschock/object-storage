package gcs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	objectstore "github.com/justusschock/object-storage"
	"google.golang.org/api/iterator"
)

const (
	markerVersion = 1
	// maximumPartCount matches the S3 limit; part objects are named with at
	// least five digits.
	maximumPartCount = 10_000
	// maximumComposeSources is the GCS limit of source objects per compose.
	maximumComposeSources = 32
	maximumMarkerSize     = 1 << 20
	uploadIDLength        = 32
)

// uploadMarker is the JSON content of the marker object written by
// BeginMultipart. It is the authoritative record of the upload's target and
// options.
type uploadMarker struct {
	Version int                    `json:"version"`
	Bucket  objectstore.Bucket     `json:"bucket"`
	Key     objectstore.Key        `json:"key"`
	Options objectstore.PutOptions `json:"options"`
}

// BeginMultipart writes the upload marker object under MultipartPrefix. The
// returned Upload.ID is a random 32-character hex identifier.
func (store *Store) BeginMultipart(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	if err := store.validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Upload{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Upload{}, err
	}
	options = options.Clone()
	if _, err := metadataWithOptionalDigest(options.Metadata, nil); err != nil {
		return objectstore.Upload{}, err
	}
	data, err := json.Marshal(uploadMarker{Version: markerVersion, Bucket: bucket, Key: key, Options: options})
	if err != nil {
		return objectstore.Upload{}, fmt.Errorf("gcs store: encode multipart marker: %w", err)
	}
	var random [uploadIDLength / 2]byte
	if _, err := rand.Read(random[:]); err != nil {
		return objectstore.Upload{}, fmt.Errorf("gcs store: create multipart upload ID: %w", err)
	}
	id := hex.EncodeToString(random[:])
	writer := store.object(bucket, store.markerKey(id)).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ContentType = "application/json"
	writer.ChunkSize = 0
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return objectstore.Upload{}, mapError(err)
	}
	if err := writer.Close(); err != nil {
		return objectstore.Upload{}, mapError(err)
	}
	return objectstore.Upload{ID: id, Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

// PutPart streams one numbered part to a staging object while hashing it.
// The part's SHA-256 digest is recorded in the part object's metadata. A
// body with a different length or digest is never finalized and fails with
// ErrInvalidPart. The returned ETag is the part object's generation.
func (store *Store) PutPart(ctx context.Context, upload objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	id, err := store.validateUpload(ctx, upload)
	if err != nil {
		return objectstore.CompletedPart{}, err
	}
	if number <= 0 || number > maximumPartCount || size < 0 || body == nil {
		return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
	}
	if _, err := store.object(upload.Bucket, store.markerKey(id)).Attrs(ctx); err != nil {
		return objectstore.CompletedPart{}, mapError(err)
	}
	if size == 0 {
		// A zero Size means "unknown" to write; check the body is empty.
		var probe [1]byte
		if count, _ := io.ReadFull(contextReader{ctx: ctx, reader: body}, probe[:]); count > 0 {
			return objectstore.CompletedPart{}, objectstore.ErrInvalidPart
		}
		if err := ctx.Err(); err != nil {
			return objectstore.CompletedPart{}, err
		}
	}
	object, err := store.write(ctx, write{
		bucket: upload.Bucket, key: objectstore.Key(store.partKey(id, number)),
		options: objectstore.PutOptions{Size: size, ContentType: "application/octet-stream"},
	}, body, digest)
	if err != nil {
		if errors.Is(err, objectstore.ErrPrecondition) {
			return objectstore.CompletedPart{}, fmt.Errorf("%w: %v", objectstore.ErrInvalidPart, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
		}
		return objectstore.CompletedPart{}, err
	}
	return objectstore.CompletedPart{Number: number, ETag: object.Version, Size: size, SHA256: digest}, nil
}

// CompleteMultipart verifies every part against its staging object (size,
// recorded SHA-256 digest, and ETag) and composes the parts into the final
// key with the upload's preconditions. A part ETag may be the generation
// returned by PutPart, the GCS entity tag, or the hex MD5 entity tag returned
// by a presigned XML API upload. When the upload no longer exists because an
// earlier completion succeeded, it returns that published object.
func (store *Store) CompleteMultipart(ctx context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	id, err := store.validateUpload(ctx, upload)
	if err != nil {
		return objectstore.Object{}, err
	}
	marker, err := store.readMarker(ctx, upload, id)
	if errors.Is(err, objectstore.ErrNotFound) {
		return store.completedEarlier(ctx, upload, id)
	}
	if err != nil {
		return objectstore.Object{}, err
	}
	if len(completed) == 0 || len(completed) > maximumPartCount {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	var total int64
	for index, part := range completed {
		if part.Number != index+1 || part.ETag == "" || part.Size < 0 || total > math.MaxInt64-part.Size {
			return objectstore.Object{}, objectstore.ErrInvalidPart
		}
		total += part.Size
	}
	options := marker.Options
	if options.Size > 0 && options.Size != total {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	conditions, err := writeConditions(options.IfAbsent, options.IfMatchVersion)
	if err != nil {
		return objectstore.Object{}, err
	}
	metadata, err := metadataWithOptionalDigest(options.Metadata, options.SHA256)
	if err != nil {
		return objectstore.Object{}, objectstore.ErrInvalidPart
	}
	metadata[uploadMetadataKey] = id

	generations := make([]int64, len(completed))
	err = runParallel(ctx, len(completed), store.options.Concurrency, func(ctx context.Context, index int) error {
		part := completed[index]
		attrs, err := store.object(upload.Bucket, objectstore.Key(store.partKey(id, part.Number))).Attrs(ctx)
		if err != nil {
			mapped := mapError(err)
			if errors.Is(mapped, objectstore.ErrNotFound) {
				return fmt.Errorf("%w: part %d is missing", objectstore.ErrInvalidPart, part.Number)
			}
			return mapped
		}
		digest, ok := recordedDigest(attrs.Metadata)
		if !ok || digest != part.SHA256 || attrs.Size != part.Size || !partETagMatches(part.ETag, attrs) {
			return fmt.Errorf("%w: part %d does not match its upload", objectstore.ErrInvalidPart, part.Number)
		}
		generations[index] = attrs.Generation
		return nil
	})
	if err != nil {
		return objectstore.Object{}, err
	}
	sources := make([]*storage.ObjectHandle, len(completed))
	for index, part := range completed {
		sources[index] = store.object(upload.Bucket, objectstore.Key(store.partKey(id, part.Number))).Generation(generations[index])
	}
	sources, err = store.composeLevels(ctx, upload.Bucket, id, sources)
	if err != nil {
		return objectstore.Object{}, err
	}
	destination := store.object(upload.Bucket, upload.Key)
	if conditions != nil {
		destination = destination.If(*conditions)
	}
	composer := destination.ComposerFrom(sources...)
	composer.ContentType = options.ContentType
	composer.Metadata = metadata
	attrs, err := composer.Run(ctx)
	if err != nil {
		mapped := mapError(err)
		if errors.Is(mapped, objectstore.ErrPrecondition) {
			// A retried completion may find its own earlier publication
			// failing the IfAbsent/IfMatchVersion condition.
			if object, earlierErr := store.completedEarlier(ctx, upload, id); earlierErr == nil {
				store.cleanup(ctx, upload.Bucket, id)
				return object, nil
			}
		}
		return objectstore.Object{}, mapped
	}
	object, err := objectFromAttrs(upload.Bucket, attrs)
	if err != nil {
		return objectstore.Object{}, err
	}
	store.cleanup(ctx, upload.Bucket, id)
	return object, nil
}

// composeLevels reduces sources to at most 32 objects by composing groups of
// 32 into intermediate staging objects, level by level.
func (store *Store) composeLevels(ctx context.Context, bucket objectstore.Bucket, id string, sources []*storage.ObjectHandle) ([]*storage.ObjectHandle, error) {
	fanIn := store.composeFanIn
	for level := 1; len(sources) > fanIn; level++ {
		groups := (len(sources) + fanIn - 1) / fanIn
		next := make([]*storage.ObjectHandle, groups)
		err := runParallel(ctx, groups, store.options.Concurrency, func(ctx context.Context, group int) error {
			start := group * fanIn
			name := fmt.Sprintf("%s%s/compose/%d/%05d", store.options.MultipartPrefix, id, level, group)
			composer := store.object(bucket, objectstore.Key(name)).ComposerFrom(sources[start:min(start+fanIn, len(sources))]...)
			composer.ContentType = "application/octet-stream"
			attrs, err := composer.Run(ctx)
			if err != nil {
				return mapError(err)
			}
			next[group] = store.object(bucket, objectstore.Key(name)).Generation(attrs.Generation)
			return nil
		})
		if err != nil {
			return nil, err
		}
		sources = next
	}
	return sources, nil
}

// completedEarlier resolves a missing upload marker: either a previous
// completion published this upload's object (its ID is recorded in the
// object metadata) or the upload was aborted.
func (store *Store) completedEarlier(ctx context.Context, upload objectstore.Upload, id string) (objectstore.Object, error) {
	attrs, err := store.object(upload.Bucket, upload.Key).Attrs(ctx)
	if err != nil {
		mapped := mapError(err)
		if errors.Is(mapped, objectstore.ErrNotFound) {
			return objectstore.Object{}, objectstore.ErrNotFound
		}
		return objectstore.Object{}, mapped
	}
	for name, value := range attrs.Metadata {
		if strings.EqualFold(name, uploadMetadataKey) && value == id {
			return objectFromAttrs(upload.Bucket, attrs)
		}
	}
	return objectstore.Object{}, objectstore.ErrNotFound
}

// AbortMultipart deletes the upload marker and every staging object of the
// upload. Later PutPart and CompleteMultipart calls return ErrNotFound.
// Aborting an unknown upload returns ErrNotFound.
func (store *Store) AbortMultipart(ctx context.Context, upload objectstore.Upload) error {
	id, err := store.validateUpload(ctx, upload)
	if err != nil {
		return err
	}
	markerErr := mapError(store.object(upload.Bucket, store.markerKey(id)).Delete(ctx))
	if markerErr != nil && !errors.Is(markerErr, objectstore.ErrNotFound) {
		return markerErr
	}
	names, err := store.stagingObjects(ctx, upload.Bucket, id)
	if err != nil {
		return err
	}
	if err := store.deleteAll(ctx, upload.Bucket, names); err != nil {
		return err
	}
	if markerErr != nil && len(names) == 0 {
		return markerErr
	}
	return nil
}

// cleanup best-effort deletes the marker first (so a retried completion
// resolves through completedEarlier) and then every staging object.
func (store *Store) cleanup(ctx context.Context, bucket objectstore.Bucket, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := store.object(bucket, store.markerKey(id)).Delete(ctx); err != nil && !errors.Is(mapError(err), objectstore.ErrNotFound) {
		return
	}
	if names, err := store.stagingObjects(ctx, bucket, id); err == nil {
		_ = store.deleteAll(ctx, bucket, names)
	}
}

func (store *Store) stagingObjects(ctx context.Context, bucket objectstore.Bucket, id string) ([]string, error) {
	query := &storage.Query{Prefix: store.options.MultipartPrefix + id + "/"}
	if err := query.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, err
	}
	iter := store.client.Bucket(string(bucket)).Objects(ctx, query)
	var names []string
	for {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return names, nil
		}
		if err != nil {
			return nil, mapError(err)
		}
		names = append(names, attrs.Name)
	}
}

func (store *Store) deleteAll(ctx context.Context, bucket objectstore.Bucket, names []string) error {
	return runParallel(ctx, len(names), store.options.Concurrency, func(ctx context.Context, index int) error {
		err := mapError(store.client.Bucket(string(bucket)).Object(names[index]).Delete(ctx))
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil
		}
		return err
	})
}

func (store *Store) readMarker(ctx context.Context, upload objectstore.Upload, id string) (uploadMarker, error) {
	reader, err := store.object(upload.Bucket, store.markerKey(id)).NewReader(ctx)
	if err != nil {
		return uploadMarker{}, mapError(err)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maximumMarkerSize+1))
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return uploadMarker{}, mapError(err)
	}
	var marker uploadMarker
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil || len(data) > maximumMarkerSize {
		return uploadMarker{}, fmt.Errorf("%w: invalid multipart marker", objectstore.ErrInvalidPart)
	}
	if marker.Version != markerVersion || marker.Bucket != upload.Bucket || marker.Key != upload.Key {
		return uploadMarker{}, fmt.Errorf("%w: upload does not match its marker", objectstore.ErrInvalidPart)
	}
	marker.Options = marker.Options.Clone()
	return marker, nil
}

func (store *Store) validateUpload(ctx context.Context, upload objectstore.Upload) (string, error) {
	if err := store.validateObjectInput(ctx, upload.Bucket, upload.Key); err != nil {
		return "", err
	}
	if len(upload.ID) != uploadIDLength {
		return "", objectstore.ErrInvalidPart
	}
	if _, err := hex.DecodeString(upload.ID); err != nil || strings.ToLower(upload.ID) != upload.ID {
		return "", objectstore.ErrInvalidPart
	}
	return upload.ID, nil
}

func (store *Store) markerKey(id string) objectstore.Key {
	return objectstore.Key(store.options.MultipartPrefix + id + "/upload")
}

func (store *Store) partKey(id string, number int) string {
	return fmt.Sprintf("%s%s/%05d", store.options.MultipartPrefix, id, number)
}

// partETagMatches accepts the generation returned by PutPart, the JSON API
// entity tag, or the (optionally quoted) hex MD5 entity tag that the XML API
// returns for presigned part uploads.
func partETagMatches(etag string, attrs *storage.ObjectAttrs) bool {
	if etag == strconv.FormatInt(attrs.Generation, 10) || etag == attrs.Etag {
		return true
	}
	unquoted := strings.Trim(etag, `"`)
	return len(attrs.MD5) > 0 && strings.EqualFold(unquoted, hex.EncodeToString(attrs.MD5))
}
