// Package azure adapts Azure Blob Storage clients (azblob) to the
// backend-neutral object API. A Bucket is a container and a Key is a block
// blob name.
//
// # Digests
//
// Every object written through this adapter records its SHA-256 digest in
// the blob metadata name "goobjectstoragesha256" when the blob is committed.
// Objects without that name (written by other tools) are reported with
// SHA256Known false; Stat, Get, and List never download object bytes to
// compute a digest.
//
// Put streams bodies without holding them in memory:
//
//   - Bodies up to BlockSize are buffered (bounded by BlockSize), hashed,
//     and sent with one Put Blob request carrying a transactional
//     Content-MD5, so Azure rejects corrupted transfers.
//   - Larger or unsized bodies are read sequentially in BlockSize chunks
//     that are hashed while being read and staged with Put Block (each with
//     a transactional Content-MD5), with at most Concurrency blocks in
//     flight. Size and SHA-256 are verified before Put Block List commits
//     the blocks with the digest metadata, so a mismatching body never
//     publishes the key. Staged but uncommitted blocks are invisible and are
//     garbage collected by Azure (after seven days, or when the blob is next
//     written or deleted).
//
// Explicit multipart uploads map to block blobs: BeginMultipart creates a
// small marker blob under the reserved prefix ".go-object-storage/multipart/"
// that holds the upload's options, PutPart stages one block on the final blob
// with a block ID derived from the upload ID and part number, and
// CompleteMultipart commits the block list in place with the upload's
// conditions and metadata, then deletes the marker. Azure cannot hash the
// assembled object, so the whole-object digest is the one declared in
// PutOptions.SHA256 at BeginMultipart; uploads begun without a digest
// complete with SHA256Known false. CompleteMultipart is idempotent: a retry
// after a lost response finds the upload ID recorded in the blob metadata and
// returns the published object.
//
// # Metadata
//
// Azure metadata names must be C# identifiers (ASCII letters, digits, and
// underscores, not starting with a digit) and are case-insensitive: Put
// rejects other names with ErrInvalidMetadata, and the adapter stores and
// returns every name in lower case, so two names differing only in case are
// rejected too. Values must be printable ASCII without leading or trailing
// spaces, because they travel as HTTP headers. Names starting with
// "goobjectstorage" are reserved for the adapter. Azure limits the total
// metadata size to 8 KiB.
//
// # Conditional writes
//
// Version is the blob's entity tag, quoted exactly as Azure returns it.
// Azure assigns a new entity tag on every write, even of identical bytes.
// IfAbsent maps to If-None-Match: * and IfMatchVersion to If-Match on Put
// Blob, Put Block List, Copy Blob, and Delete Blob; Azure evaluates these
// conditions atomically with the write, which makes them its compare-and-swap
// primitive. Get and Stat check the returned entity tag as well.
//
// Azure discards all uncommitted blocks of a blob whenever the blob is
// written or deleted. Large streamed Puts and multipart uploads therefore
// lose their staged blocks when another writer publishes the same key first:
// such a streamed Put fails with ErrPrecondition (another writer won), and
// such a multipart upload fails to complete with ErrInvalidPart.
package azure

import (
	"bytes"
	"context"
	"crypto/md5"
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
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	objectstore "github.com/justusschock/go-object-storage"
)

const (
	// reservedMetadataPrefix starts every metadata name the adapter owns.
	reservedMetadataPrefix = "goobjectstorage"
	sha256MetadataKey      = reservedMetadataPrefix + "sha256"
	uploadMetadataKey      = reservedMetadataPrefix + "upload"
	targetMetadataKey      = reservedMetadataPrefix + "target"
	// multipartPrefix holds multipart marker blobs; it is hidden from
	// listings and rejected as a user key prefix.
	multipartPrefix = ".go-object-storage/multipart/"

	cleanupTimeout          = 10 * time.Second
	mebibyte                = int64(1 << 20)
	minimumBlockSize        = 64 << 10
	maximumBlockSize        = 4000 * mebibyte
	maximumBlockCount       = 50_000
	maximumKeyLength        = 1024
	maximumListResults      = 5000
	defaultBlockSize        = 16 * mebibyte
	defaultConcurrency      = 4
	defaultBatchConcurrency = 16
	defaultCopyPollInterval = 500 * time.Millisecond
)

var (
	// ErrInvalidMetadata identifies user metadata that Azure cannot store:
	// names that are not C# identifiers, names differing only in case,
	// reserved names, or values that are not printable ASCII.
	ErrInvalidMetadata = errors.New("azure store: invalid metadata")
	// ErrPresignUnavailable reports that the store has no Shared Key
	// credential to sign service SAS URLs with.
	ErrPresignUnavailable = errors.New("azure store: presigning requires a shared key credential")
)

// Options configures transfer sizing, parallelism, and presigning. Zero
// values select defaults.
type Options struct {
	// BlockSize is the single-request threshold and the staged block size
	// for Put; default 16 MiB, minimum 64 KiB, maximum 4000 MiB. Put holds
	// at most (Concurrency+1)*BlockSize bytes in memory. When PutOptions.Size
	// is known the block size grows as needed to stay within 50,000 blocks.
	BlockSize int64
	// Concurrency bounds in-flight Put Block requests per Put; default 4.
	Concurrency int
	// BatchConcurrency bounds parallel requests for StatMany and
	// DeleteMany; default 16.
	BatchConcurrency int
	// CopyPollInterval is the delay between status checks of a copy that
	// Azure reports as pending; default 500ms.
	CopyPollInterval time.Duration
	// SharedKey signs presigned (service SAS) URLs. When nil, the store
	// signs with the client's own credential if the client was created with
	// azblob.NewClientWithSharedKeyCredential or from a connection string
	// with an account key; otherwise the Presign methods return
	// ErrPresignUnavailable.
	SharedKey *azblob.SharedKeyCredential
}

func (options Options) withDefaults() (Options, error) {
	if options.BlockSize == 0 {
		options.BlockSize = defaultBlockSize
	}
	if options.Concurrency == 0 {
		options.Concurrency = defaultConcurrency
	}
	if options.BatchConcurrency == 0 {
		options.BatchConcurrency = defaultBatchConcurrency
	}
	if options.CopyPollInterval == 0 {
		options.CopyPollInterval = defaultCopyPollInterval
	}
	switch {
	case options.BlockSize < minimumBlockSize || options.BlockSize > maximumBlockSize:
		return Options{}, fmt.Errorf("azure store: block size must be between %d and %d bytes", minimumBlockSize, maximumBlockSize)
	case options.Concurrency < 0 || options.BatchConcurrency < 0:
		return Options{}, errors.New("azure store: negative concurrency")
	case options.CopyPollInterval < 0:
		return Options{}, errors.New("azure store: negative copy poll interval")
	}
	return options, nil
}

// Store maps portable object operations to Azure Blob Storage requests.
type Store struct {
	client  *azblob.Client
	options Options
}

// New constructs an Azure store from a caller-owned client. Buckets are
// containers, which must already exist.
func New(client *azblob.Client, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("azure store: nil client")
	}
	options, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Store{client: client, options: options}, nil
}

func (store *Store) containerClient(bucket objectstore.Bucket) *container.Client {
	return store.client.ServiceClient().NewContainerClient(string(bucket))
}

func (store *Store) blockBlob(bucket objectstore.Bucket, key objectstore.Key) *blockblob.Client {
	return store.containerClient(bucket).NewBlockBlobClient(string(key))
}

// Put uploads one object with atomic Azure write conditions and verified
// SHA-256 metadata, streaming large bodies through bounded memory.
func (store *Store) Put(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, body io.Reader, options objectstore.PutOptions) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	if err := options.ValidateSize(); err != nil {
		return objectstore.Object{}, err
	}
	if body == nil {
		return objectstore.Object{}, errors.New("azure store: nil reader")
	}
	metadata, err := normalizeMetadata(options.Metadata)
	if err != nil {
		return objectstore.Object{}, err
	}
	blockSize := store.blockSize(options.Size)
	head, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: body}, blockSize+1))
	if err != nil {
		return objectstore.Object{}, err
	}
	if int64(len(head)) <= blockSize {
		return store.putSingle(ctx, bucket, key, head, metadata, options)
	}
	if options.Size > 0 && int64(len(head)) > options.Size {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	return store.putBlocks(ctx, bucket, key, io.MultiReader(bytes.NewReader(head), body), blockSize, metadata, options)
}

func (store *Store) putSingle(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, data []byte, metadata map[string]string, options objectstore.PutOptions) (objectstore.Object, error) {
	digest := sha256.Sum256(data)
	if options.SHA256 != nil && *options.SHA256 != digest {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	if options.Size > 0 && options.Size != int64(len(data)) {
		return objectstore.Object{}, fmt.Errorf("%w: size mismatch", objectstore.ErrPrecondition)
	}
	checksum := md5.Sum(data)
	response, err := store.blockBlob(bucket, key).Upload(ctx, streaming.NopCloser(bytes.NewReader(data)), &blockblob.UploadOptions{
		HTTPHeaders:             contentHeaders(options.ContentType),
		Metadata:                sdkMetadata(metadata, map[string]string{sha256MetadataKey: hex.EncodeToString(digest[:])}),
		AccessConditions:        writeConditions(options.IfAbsent, options.IfMatchVersion),
		TransactionalValidation: blob.TransferValidationTypeMD5(checksum[:]),
	})
	if err != nil {
		return objectstore.Object{}, conditionalWriteError(err, options.IfMatchVersion)
	}
	return written(bucket, key, response.ETag, response.LastModified, int64(len(data)), metadata, &digest)
}

// written builds the object a write just published from its response, so
// no extra request is needed and the result equals a later Stat.
func written(bucket objectstore.Bucket, key objectstore.Key, etag *azcore.ETag, modified *time.Time, size int64, metadata map[string]string, digest *[32]byte) (objectstore.Object, error) {
	if etag == nil || *etag == "" || modified == nil {
		return objectstore.Object{}, errors.New("azure store: write acknowledged without an entity tag or modification time")
	}
	object := objectstore.Object{
		Bucket: bucket, Key: key, Version: string(*etag), ETag: string(*etag), Size: size,
		ModifiedAt: modified.UTC(), Metadata: maps.Clone(metadata),
	}
	if len(object.Metadata) == 0 {
		object.Metadata = nil
	}
	if digest != nil {
		object.SHA256, object.SHA256Known = *digest, true
	}
	return object, nil
}

// Get opens an object or exact byte range with one Get Blob request, whose
// response carries the metadata. It never reads beyond the requested bytes
// and never hashes object contents.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return nil, objectstore.Object{}, err
	}
	download := &blob.DownloadStreamOptions{AccessConditions: matchCondition(options.IfMatchVersion)}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
		download.Range = blob.HTTPRange{Offset: options.Range.Offset, Count: options.Range.Length}
	}
	response, err := store.blockBlob(bucket, key).DownloadStream(ctx, download)
	if err != nil {
		return nil, objectstore.Object{}, mapError(err)
	}
	fail := func(err error) (io.ReadCloser, objectstore.Object, error) {
		if response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, objectstore.Object{}, err
	}
	if response.Body == nil || response.ContentLength == nil {
		return fail(errors.New("azure store: incomplete download response"))
	}
	size := *response.ContentLength
	expected := size
	if options.Range != nil {
		total, err := validateRangeResponse(response.ContentRange, *response.ContentLength, *options.Range)
		if err != nil {
			return fail(err)
		}
		size, expected = total, options.Range.Length
	}
	object, err := objectFromMetadata(bucket, key, size, response.ETag, response.LastModified, response.Metadata)
	if err != nil {
		return fail(err)
	}
	if options.IfMatchVersion != "" && object.Version != options.IfMatchVersion {
		return fail(objectstore.ErrPrecondition)
	}
	return &exactReader{body: response.Body, remaining: expected}, object, nil
}

// validateRangeResponse checks that Azure returned exactly the requested
// range. Azure truncates ranges that extend past the end of a blob instead of
// failing, which the portable contract reports as ErrInvalidRange. It
// returns the total object size.
func validateRangeResponse(contentRange *string, contentLength int64, requested objectstore.ByteRange) (int64, error) {
	if contentRange == nil || contentLength != requested.Length {
		return 0, objectstore.ErrInvalidRange
	}
	span, totalValue, found := strings.Cut(strings.TrimPrefix(*contentRange, "bytes "), "/")
	startValue, endValue, spanFound := strings.Cut(span, "-")
	start, startErr := strconv.ParseInt(startValue, 10, 64)
	end, endErr := strconv.ParseInt(endValue, 10, 64)
	total, totalErr := strconv.ParseInt(totalValue, 10, 64)
	if !strings.HasPrefix(*contentRange, "bytes ") || !found || !spanFound || startErr != nil || endErr != nil || totalErr != nil ||
		start != requested.Offset || end != requested.Offset+requested.Length-1 || total <= end {
		return 0, objectstore.ErrInvalidRange
	}
	return total, nil
}

// Stat returns object metadata from one Get Blob Properties request.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return objectstore.Object{}, err
	}
	return store.properties(ctx, bucket, key, "")
}

func (store *Store) properties(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, ifMatch string) (objectstore.Object, error) {
	response, err := store.blockBlob(bucket, key).GetProperties(ctx, &blob.GetPropertiesOptions{AccessConditions: matchCondition(ifMatch)})
	if err != nil {
		return objectstore.Object{}, mapError(err)
	}
	if response.ContentLength == nil {
		return objectstore.Object{}, errors.New("azure store: incomplete properties response")
	}
	object, err := objectFromMetadata(bucket, key, *response.ContentLength, response.ETag, response.LastModified, response.Metadata)
	if err != nil {
		return objectstore.Object{}, err
	}
	if ifMatch != "" && object.Version != ifMatch {
		return objectstore.Object{}, objectstore.ErrPrecondition
	}
	return object, nil
}

// Delete deletes the blob, conditionally on its entity tag when version is
// non-empty, together with its snapshots. Deleting an absent key returns
// ErrNotFound, as Azure does.
func (store *Store) Delete(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, version string) error {
	if err := validateObjectInput(ctx, bucket, key); err != nil {
		return err
	}
	return store.deleteBlob(ctx, bucket, string(key), version)
}

func (store *Store) deleteBlob(ctx context.Context, bucket objectstore.Bucket, name, version string) error {
	_, err := store.containerClient(bucket).NewBlobClient(name).Delete(ctx, &blob.DeleteOptions{
		DeleteSnapshots: to(blob.DeleteSnapshotsOptionTypeInclude), AccessConditions: matchCondition(version),
	})
	return mapError(err)
}

// List returns a stable key-ordered page from flat List Blobs requests.
// Listings always include metadata (include=metadata), so every listed object
// carries the fields Stat returns, including a recorded digest, without
// per-key requests. Pages start at After with the List Blobs startFrom
// parameter; keys up to and including After are also skipped client-side.
// Multipart marker blobs are hidden.
func (store *Store) List(ctx context.Context, bucket objectstore.Bucket, options objectstore.ListOptions) (objectstore.Page, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Page{}, err
	}
	if err := validateBucket(bucket); err != nil {
		return objectstore.Page{}, err
	}
	if options.Limit < 0 {
		return objectstore.Page{}, objectstore.ErrInvalidRange
	}
	request := &container.ListBlobsFlatOptions{Include: container.ListBlobsInclude{Metadata: true}}
	if options.Prefix != "" {
		request.Prefix = to(options.Prefix)
	}
	if options.After != "" {
		request.StartFrom = to(string(options.After))
	}
	if options.Limit > 0 {
		// One extra entry reveals whether more keys remain, another covers
		// After itself, which startFrom includes.
		request.MaxResults = to(int32(min(options.Limit+2, maximumListResults)))
	}
	var objects []objectstore.Object
	truncated := false
	pager := store.containerClient(bucket).NewListBlobsFlatPager(request)
	for !truncated && pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return objectstore.Page{}, mapError(err)
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil || strings.HasPrefix(*item.Name, multipartPrefix) ||
				!strings.HasPrefix(*item.Name, options.Prefix) || objectstore.Key(*item.Name) <= options.After {
				continue
			}
			if options.Limit > 0 && len(objects) == options.Limit {
				truncated = true
				break
			}
			object, err := objectFromListing(bucket, item)
			if err != nil {
				return objectstore.Page{}, err
			}
			objects = append(objects, object)
		}
	}
	page := objectstore.Page{Objects: objects}
	if truncated {
		page.NextAfter = objects[len(objects)-1].Key
	}
	return page, nil
}

func objectFromListing(bucket objectstore.Bucket, item *container.BlobItem) (objectstore.Object, error) {
	properties := item.Properties
	if properties == nil || properties.ContentLength == nil || properties.ETag == nil {
		return objectstore.Object{}, errors.New("azure store: incomplete listing entry")
	}
	// List Blobs returns entity tags without the quotes of the ETag header.
	etag := string(*properties.ETag)
	if !strings.HasPrefix(etag, "\"") {
		etag = strconv.Quote(etag)
	}
	quoted := azcore.ETag(etag)
	return objectFromMetadata(bucket, objectstore.Key(*item.Name), *properties.ContentLength, &quoted, properties.LastModified, item.Metadata)
}

// objectFromMetadata converts Azure metadata: names are lower-cased and the
// reserved adapter names are removed from the returned user metadata.
func objectFromMetadata(bucket objectstore.Bucket, key objectstore.Key, size int64, etag *azcore.ETag, modified *time.Time, raw map[string]*string) (objectstore.Object, error) {
	if size < 0 || etag == nil || *etag == "" || modified == nil {
		return objectstore.Object{}, errors.New("azure store: incomplete object metadata")
	}
	metadata := make(map[string]string, len(raw))
	encoded, hasDigest := "", false
	for name, value := range raw {
		if value == nil {
			continue
		}
		lower := strings.ToLower(name)
		switch {
		case lower == sha256MetadataKey:
			encoded, hasDigest = *value, true
		case strings.HasPrefix(lower, reservedMetadataPrefix):
		default:
			metadata[lower] = *value
		}
	}
	if len(metadata) == 0 {
		metadata = nil
	}
	object := objectstore.Object{
		Bucket: bucket, Key: key, Version: string(*etag), ETag: string(*etag), Size: size, ModifiedAt: modified.UTC(), Metadata: metadata,
	}
	if !hasDigest {
		return object, nil
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return objectstore.Object{}, errors.New("azure store: invalid SHA-256 metadata")
	}
	copy(object.SHA256[:], decoded)
	object.SHA256Known = true
	return object, nil
}

// normalizeMetadata validates user metadata for Azure and returns it with
// lower-case names, or nil when empty.
func normalizeMetadata(metadata map[string]string) (map[string]string, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(metadata))
	for name, value := range metadata {
		if err := validateMetadataName(name); err != nil {
			return nil, err
		}
		lower := strings.ToLower(name)
		if _, duplicate := result[lower]; duplicate {
			return nil, fmt.Errorf("%w: names %q differ only in case", ErrInvalidMetadata, lower)
		}
		if err := validateMetadataValue(name, value); err != nil {
			return nil, err
		}
		result[lower] = value
	}
	return result, nil
}

func validateMetadataName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", ErrInvalidMetadata)
	}
	for index, character := range name {
		letter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_'
		if !letter && (index == 0 || character < '0' || character > '9') {
			return fmt.Errorf("%w: name %q is not a C# identifier (ASCII letters, digits, and underscores, not starting with a digit)", ErrInvalidMetadata, name)
		}
	}
	if strings.HasPrefix(strings.ToLower(name), reservedMetadataPrefix) {
		return fmt.Errorf("%w: name %q is reserved", ErrInvalidMetadata, name)
	}
	return nil
}

func validateMetadataValue(name, value string) error {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] > 0x7e {
			return fmt.Errorf("%w: value of %q is not printable ASCII", ErrInvalidMetadata, name)
		}
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: value of %q has leading or trailing spaces", ErrInvalidMetadata, name)
	}
	return nil
}

// sdkMetadata merges validated user metadata and adapter metadata into the
// SDK representation.
func sdkMetadata(user map[string]string, reserved map[string]string) map[string]*string {
	if len(user)+len(reserved) == 0 {
		return nil
	}
	result := make(map[string]*string, len(user)+len(reserved))
	for name, value := range user {
		result[name] = to(value)
	}
	for name, value := range reserved {
		result[name] = to(value)
	}
	return result
}

func validateBucket(bucket objectstore.Bucket) error {
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return err
	}
	name := string(bucket)
	switch name {
	case "$root", "$web", "$logs":
		return nil
	}
	valid := len(name) >= 3 && len(name) <= 63 && name[0] != '-' && name[len(name)-1] != '-' && !strings.Contains(name, "--")
	for _, character := range name {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			valid = false
		}
	}
	if !valid {
		return fmt.Errorf("%w: Azure container names have 3-63 lower-case letters, digits, and single hyphens", objectstore.ErrInvalidBucket)
	}
	return nil
}

func validateKey(key objectstore.Key) error {
	if err := objectstore.ValidateKey(key); err != nil {
		return err
	}
	switch {
	case !utf8.ValidString(string(key)):
		return fmt.Errorf("%w: invalid UTF-8", objectstore.ErrInvalidKey)
	case utf8.RuneCountInString(string(key)) > maximumKeyLength:
		return fmt.Errorf("%w: longer than %d characters", objectstore.ErrInvalidKey, maximumKeyLength)
	case strings.HasPrefix(string(key), multipartPrefix):
		return fmt.Errorf("%w: reserved prefix %q", objectstore.ErrInvalidKey, multipartPrefix)
	}
	return nil
}

func validateObjectInput(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(validateBucket(bucket), validateKey(key))
}

func contentHeaders(contentType string) *blob.HTTPHeaders {
	if contentType == "" {
		return nil
	}
	return &blob.HTTPHeaders{BlobContentType: to(contentType)}
}

func writeConditions(ifAbsent bool, ifMatch string) *blob.AccessConditions {
	if !ifAbsent && ifMatch == "" {
		return nil
	}
	conditions := &blob.ModifiedAccessConditions{}
	if ifAbsent {
		conditions.IfNoneMatch = to(azcore.ETagAny)
	}
	if ifMatch != "" {
		conditions.IfMatch = to(azcore.ETag(ifMatch))
	}
	return &blob.AccessConditions{ModifiedAccessConditions: conditions}
}

func matchCondition(version string) *blob.AccessConditions {
	return writeConditions(false, version)
}

// mapError translates Azure error codes and statuses to the portable error
// vocabulary, keeping the Azure error as detail.
func mapError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var responseError *azcore.ResponseError
	if !errors.As(err, &responseError) {
		return err
	}
	switch bloberror.Code(responseError.ErrorCode) {
	case bloberror.BlobNotFound, bloberror.ContainerNotFound, bloberror.ResourceNotFound:
		return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	case bloberror.ConditionNotMet, bloberror.SourceConditionNotMet, bloberror.TargetConditionNotMet,
		bloberror.BlobAlreadyExists, bloberror.MD5Mismatch, bloberror.CRC64Mismatch:
		return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	case bloberror.InvalidRange:
		return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	case bloberror.InvalidBlockList, bloberror.InvalidBlockID, bloberror.InvalidBlobOrBlock:
		return fmt.Errorf("%w: %v", objectstore.ErrInvalidPart, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	}
	switch responseError.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %v", objectstore.ErrNotFound, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	case http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	case http.StatusRequestedRangeNotSatisfiable:
		return fmt.Errorf("%w: %v", objectstore.ErrInvalidRange, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	}
	return err
}

// conditionalWriteError maps a not-found blob on an If-Match write to
// ErrPrecondition, matching the portable IfMatchVersion contract; a missing
// container stays ErrNotFound.
func conditionalWriteError(err error, ifMatch string) error {
	mapped := mapError(err)
	if ifMatch != "" && bloberror.HasCode(err, bloberror.BlobNotFound) {
		return fmt.Errorf("%w: %v", objectstore.ErrPrecondition, err) //nolint:errorlint // one classification per error: the cause is detail, not a second sentinel (ADR-0005)
	}
	return mapped
}

// blockSize returns the configured block size, grown so that an object of
// the expected size fits within Azure's 50,000 committed blocks.
func (store *Store) blockSize(expected int64) int64 {
	size := store.options.BlockSize
	if expected > 0 {
		needed := (expected + maximumBlockCount - 1) / maximumBlockCount
		needed = (needed + mebibyte - 1) / mebibyte * mebibyte
		size = min(max(size, needed), maximumBlockSize)
	}
	return size
}

func to[T any](value T) *T { return &value }

var (
	_ objectstore.MultipartStore = (*Store)(nil)
	_ objectstore.BatchStater    = (*Store)(nil)
	_ objectstore.BatchDeleter   = (*Store)(nil)
	_ objectstore.Copier         = (*Store)(nil)
	_ objectstore.Presigner      = (*Store)(nil)
)
