// Package http provides a read-only object store over plain HTTP(S), for
// example a static web server, an object-storage website endpoint, or a CDN
// serving a directory of objects.
//
// # URLs
//
// An object lives at <base URL>/<bucket>/<key>, where the bucket and every
// key segment are path-escaped. Options.OmitBucket drops the bucket segment
// (<base URL>/<key>) and Options.ObjectURL replaces the mapping entirely.
//
// # Reads
//
// Get issues one GET request, with an exact Range header for ranged reads,
// and Stat issues one HEAD request. Every request asks for the identity
// encoding, so Content-Length and Content-Range describe the stored bytes.
// A 206 response must carry exactly the requested Content-Range; servers
// that ignore Range and answer 200 are handled by discarding the bytes
// before the range and verifying the object size, so ranges stay exact.
// Ranges that extend past the end of the object fail with ErrInvalidRange.
// Readers yield exactly the reported length and fail with
// io.ErrUnexpectedEOF on truncated bodies.
//
// Objects report:
//
//   - Version and ETag: the ETag response header, verbatim (empty when the
//     server sends none). GetOptions.IfMatchVersion is sent as If-Match and
//     also checked against the returned ETag, so it holds even when the
//     server ignores conditional headers.
//   - Size: the Content-Length of a full response or the complete length of
//     a Content-Range; -1 when the server reports neither.
//   - ModifiedAt: the Last-Modified header in UTC (zero when absent).
//   - Metadata: x-amz-meta-*, x-goog-meta-*, and x-ms-meta-* headers with
//     lower-cased names and the prefix removed (S3, GCS, and Azure public
//     endpoints), nil when there are none.
//   - SHA256: the digest recorded by this library's S3, GCS, and Azure
//     adapters ("go-object-storage-sha256" or "goobjectstoragesha256"
//     metadata), or an RFC 9530 Repr-Digest / RFC 3230 Digest sha-256 value.
//     Otherwise SHA256Known is false; the store never hashes object bytes.
//
// # Read-only contract
//
// Put and Delete fail with ErrReadOnly and List with ErrNotSupported (plain
// HTTP has no portable listing). ErrReadOnly wraps ErrNotSupported. The store
// implements objectstore.Store and objectstore.BatchStater, not
// MultipartStore or the copy and presign capabilities. Status codes map to
// the objectstore errors: 404 and 410 to ErrNotFound, 412 to
// ErrPrecondition, 416 to ErrInvalidRange; other non-success statuses are
// returned as *StatusError.
package http

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	nethttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	objectstore "github.com/justusschock/object-storage"
)

const defaultStatConcurrency = 16

var (
	// ErrNotSupported reports an operation that plain HTTP cannot provide,
	// such as List.
	ErrNotSupported = errors.New("http store: operation not supported")
	// ErrReadOnly reports a write (Put or Delete) against the read-only
	// store. It wraps ErrNotSupported.
	ErrReadOnly = fmt.Errorf("%w: read-only store", ErrNotSupported)
)

// metadataPrefixes are the response header prefixes that carry user metadata
// on public S3, GCS, and Azure Blob endpoints.
var metadataPrefixes = []string{"x-amz-meta-", "x-goog-meta-", "x-ms-meta-"}

// digestMetadataKeys are the metadata keys under which this library's
// object-storage adapters record SHA-256 digests.
var digestMetadataKeys = []string{"go-object-storage-sha256", "goobjectstoragesha256"}

// Options configures requests. Zero values select defaults.
type Options struct {
	// Client performs requests; default http.DefaultClient. Its transport
	// controls TLS, proxies, timeouts, and retries.
	Client *nethttp.Client
	// Header is added to every request, for example API keys or cookies.
	Header nethttp.Header
	// BearerToken, when non-empty, is sent as "Authorization: Bearer
	// <token>" and overrides an Authorization entry in Header.
	BearerToken string
	// OmitBucket maps objects to <base URL>/<key> instead of
	// <base URL>/<bucket>/<key>.
	OmitBucket bool
	// ObjectURL, when non-nil, replaces the default URL mapping. It returns
	// an absolute http or https URL for one object.
	ObjectURL func(bucket objectstore.Bucket, key objectstore.Key) (string, error)
	// StatConcurrency bounds parallel HEAD requests in StatMany; default 16.
	StatConcurrency int
}

// Store reads objects over HTTP(S). It is safe for concurrent use.
type Store struct {
	base    *url.URL
	options Options
}

// StatusError reports an unexpected HTTP response status. URL omits the
// query string, which may carry credentials.
type StatusError struct {
	Method     string
	URL        string
	StatusCode int
	Status     string
}

func (err *StatusError) Error() string {
	return fmt.Sprintf("http store: %s %s: %s", err.Method, err.URL, err.Status)
}

// New constructs a read-only store for objects below baseURL, an absolute
// http or https URL. baseURL may be empty when Options.ObjectURL is set.
func New(baseURL string, options Options) (*Store, error) {
	if options.StatConcurrency < 0 {
		return nil, errors.New("http store: negative stat concurrency")
	}
	if options.StatConcurrency == 0 {
		options.StatConcurrency = defaultStatConcurrency
	}
	if options.Client == nil {
		options.Client = nethttp.DefaultClient
	}
	options.Header = options.Header.Clone()
	store := &Store{options: options}
	if baseURL == "" && options.ObjectURL != nil {
		return store, nil
	}
	base, err := parseHTTPURL(baseURL)
	if err != nil {
		return nil, err
	}
	base.Path = strings.TrimSuffix(base.Path, "/")
	base.RawPath = ""
	store.base = base
	return store, nil
}

// URL returns the URL that serves one object.
func (store *Store) URL(bucket objectstore.Bucket, key objectstore.Key) (string, error) {
	if err := errors.Join(objectstore.ValidateBucket(bucket), objectstore.ValidateKey(key)); err != nil {
		return "", err
	}
	if store.options.ObjectURL != nil {
		raw, err := store.options.ObjectURL(bucket, key)
		if err != nil {
			return "", err
		}
		parsed, err := parseHTTPURL(raw)
		if err != nil {
			return "", err
		}
		return parsed.String(), nil
	}
	segments := strings.Split(string(key), "/")
	if !store.options.OmitBucket {
		segments = append([]string{string(bucket)}, segments...)
	}
	for index, segment := range segments {
		segments[index] = url.PathEscape(segment)
	}
	target := *store.base
	target.RawPath = store.base.EscapedPath() + "/" + strings.Join(segments, "/")
	unescaped, err := url.PathUnescape(target.RawPath)
	if err != nil {
		return "", fmt.Errorf("http store: object path: %w", err)
	}
	target.Path = unescaped
	return target.String(), nil
}

// Put always fails with ErrReadOnly.
func (store *Store) Put(context.Context, objectstore.Bucket, objectstore.Key, io.Reader, objectstore.PutOptions) (objectstore.Object, error) {
	return objectstore.Object{}, ErrReadOnly
}

// Delete always fails with ErrReadOnly.
func (store *Store) Delete(context.Context, objectstore.Bucket, objectstore.Key, string) error {
	return ErrReadOnly
}

// List always fails with ErrNotSupported: plain HTTP has no portable
// listing. Callers that need enumeration keep an index object (for example a
// manifest of keys) and read it with Get.
func (store *Store) List(context.Context, objectstore.Bucket, objectstore.ListOptions) (objectstore.Page, error) {
	return objectstore.Page{}, ErrNotSupported
}

// Get opens an object or exact byte range with one GET request.
func (store *Store) Get(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.GetOptions) (io.ReadCloser, objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.Object{}, err
	}
	if options.Range != nil {
		if err := options.Range.Validate(); err != nil {
			return nil, objectstore.Object{}, err
		}
	}
	request, err := store.request(ctx, nethttp.MethodGet, bucket, key)
	if err != nil {
		return nil, objectstore.Object{}, err
	}
	if options.Range != nil {
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", options.Range.Offset, options.Range.Offset+options.Range.Length-1))
	}
	if options.IfMatchVersion != "" {
		request.Header.Set("If-Match", options.IfMatchVersion)
	}
	response, err := store.options.Client.Do(request)
	if err != nil {
		return nil, objectstore.Object{}, requestError(ctx, err)
	}
	fail := func(err error) (io.ReadCloser, objectstore.Object, error) {
		_ = response.Body.Close()
		return nil, objectstore.Object{}, err
	}
	if err := statusError(request, response, options.Range != nil); err != nil {
		return fail(err)
	}
	object, err := objectFromResponse(bucket, key, response)
	if err != nil {
		return fail(err)
	}
	if options.IfMatchVersion != "" && object.Version != options.IfMatchVersion {
		return fail(objectstore.ErrPrecondition)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if options.Range == nil {
		if object.Size < 0 {
			return response.Body, object, nil
		}
		return &exactReader{body: response.Body, remaining: object.Size}, object, nil
	}
	byteRange := *options.Range
	if response.StatusCode == nethttp.StatusPartialContent {
		if err := validateContentRange(response, byteRange); err != nil {
			return fail(err)
		}
		return &exactReader{body: response.Body, remaining: byteRange.Length}, object, nil
	}
	// The server ignored Range and sent the whole object.
	if object.Size >= 0 && byteRange.Offset+byteRange.Length > object.Size {
		return fail(objectstore.ErrInvalidRange)
	}
	if skipped, err := io.CopyN(io.Discard, response.Body, byteRange.Offset); err != nil {
		if skipped < byteRange.Offset && errors.Is(err, io.EOF) {
			return fail(objectstore.ErrInvalidRange)
		}
		return fail(requestError(ctx, err))
	}
	return &exactReader{body: response.Body, remaining: byteRange.Length}, object, nil
}

// Stat returns object metadata from one HEAD request.
func (store *Store) Stat(ctx context.Context, bucket objectstore.Bucket, key objectstore.Key) (objectstore.Object, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.Object{}, err
	}
	request, err := store.request(ctx, nethttp.MethodHead, bucket, key)
	if err != nil {
		return objectstore.Object{}, err
	}
	response, err := store.options.Client.Do(request)
	if err != nil {
		return objectstore.Object{}, requestError(ctx, err)
	}
	defer func() { _ = response.Body.Close() }()
	if err := statusError(request, response, false); err != nil {
		return objectstore.Object{}, err
	}
	object, err := objectFromResponse(bucket, key, response)
	if err != nil {
		return objectstore.Object{}, err
	}
	return object, ctx.Err()
}

// StatMany issues bounded-parallel HEAD requests (StatConcurrency).
func (store *Store) StatMany(ctx context.Context, bucket objectstore.Bucket, keys []objectstore.Key) ([]objectstore.StatResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := objectstore.ValidateBucket(bucket); err != nil {
		return nil, err
	}
	results := make([]objectstore.StatResult, len(keys))
	slots := make(chan struct{}, store.options.StatConcurrency)
	var wait sync.WaitGroup
	for index, key := range keys {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer func() { <-slots }()
			object, err := store.Stat(ctx, bucket, key)
			results[index] = objectstore.StatResult{Object: object, Err: err}
		}()
	}
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (store *Store) request(ctx context.Context, method string, bucket objectstore.Bucket, key objectstore.Key) (*nethttp.Request, error) {
	target, err := store.URL(bucket, key)
	if err != nil {
		return nil, err
	}
	request, err := nethttp.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, fmt.Errorf("http store: build request: %w", err)
	}
	for name, values := range store.options.Header {
		request.Header[name] = append([]string(nil), values...)
	}
	if store.options.BearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+store.options.BearerToken)
	}
	// Identity encoding keeps Content-Length and Content-Range in terms of
	// stored bytes and disables transparent decompression.
	request.Header.Set("Accept-Encoding", "identity")
	return request, nil
}

func parseHTTPURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("http store: invalid URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("http store: URL must be absolute http or https")
	}
	return parsed, nil
}

func requestError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func statusError(request *nethttp.Request, response *nethttp.Response, ranged bool) error {
	switch response.StatusCode {
	case nethttp.StatusOK:
		return nil
	case nethttp.StatusPartialContent:
		if ranged {
			return nil
		}
	case nethttp.StatusNotFound, nethttp.StatusGone:
		return fmt.Errorf("%w: %s", objectstore.ErrNotFound, redacted(request.URL))
	case nethttp.StatusPreconditionFailed:
		return objectstore.ErrPrecondition
	case nethttp.StatusRequestedRangeNotSatisfiable:
		return objectstore.ErrInvalidRange
	}
	return &StatusError{Method: request.Method, URL: redacted(request.URL), StatusCode: response.StatusCode, Status: response.Status}
}

func redacted(target *url.URL) string {
	clean := *target
	clean.RawQuery, clean.Fragment, clean.User = "", "", nil
	return clean.String()
}

func objectFromResponse(bucket objectstore.Bucket, key objectstore.Key, response *nethttp.Response) (objectstore.Object, error) {
	etag := response.Header.Get("ETag")
	object := objectstore.Object{Bucket: bucket, Key: key, Version: etag, ETag: etag, Size: response.ContentLength}
	if response.StatusCode == nethttp.StatusPartialContent {
		object.Size = -1
		if _, _, total, err := parseContentRange(response.Header.Get("Content-Range")); err == nil {
			object.Size = total
		}
	}
	if modified := response.Header.Get("Last-Modified"); modified != "" {
		if parsed, err := nethttp.ParseTime(modified); err == nil {
			object.ModifiedAt = parsed.UTC()
		}
	}
	metadata := map[string]string{}
	var recorded string
	for name, values := range response.Header {
		lower := strings.ToLower(name)
		for _, prefix := range metadataPrefixes {
			if !strings.HasPrefix(lower, prefix) || len(lower) == len(prefix) || len(values) == 0 {
				continue
			}
			metadataName := lower[len(prefix):]
			isDigest := false
			for _, digestKey := range digestMetadataKeys {
				if metadataName == digestKey {
					recorded, isDigest = values[0], true
				}
			}
			if !isDigest {
				metadata[metadataName] = values[0]
			}
		}
	}
	if len(metadata) > 0 {
		object.Metadata = maps.Clone(metadata)
	}
	if recorded != "" {
		decoded, err := hex.DecodeString(recorded)
		if err != nil || len(decoded) != sha256.Size {
			return objectstore.Object{}, errors.New("http store: invalid SHA-256 metadata")
		}
		copy(object.SHA256[:], decoded)
		object.SHA256Known = true
		return object, nil
	}
	if digest, ok := headerDigest(response.Header); ok {
		object.SHA256, object.SHA256Known = digest, true
	}
	return object, nil
}

// headerDigest parses a sha-256 value from an RFC 9530 Repr-Digest header
// (sha-256=:<base64>:) or a legacy RFC 3230 Digest header (SHA-256=<base64>).
func headerDigest(header nethttp.Header) ([32]byte, bool) {
	for _, name := range []string{"Repr-Digest", "Digest"} {
		for _, value := range header.Values(name) {
			for _, member := range strings.Split(value, ",") {
				algorithm, encoded, found := strings.Cut(strings.TrimSpace(member), "=")
				if !found || !strings.EqualFold(strings.TrimSpace(algorithm), "sha-256") {
					continue
				}
				encoded = strings.Trim(strings.TrimSpace(encoded), ":")
				decoded, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil || len(decoded) != sha256.Size {
					continue
				}
				var digest [32]byte
				copy(digest[:], decoded)
				return digest, true
			}
		}
	}
	return [32]byte{}, false
}

func validateContentRange(response *nethttp.Response, requested objectstore.ByteRange) error {
	start, end, total, err := parseContentRange(response.Header.Get("Content-Range"))
	if err != nil || start != requested.Offset || end != requested.Offset+requested.Length-1 || (total >= 0 && total <= end) {
		return objectstore.ErrInvalidRange
	}
	if response.ContentLength >= 0 && response.ContentLength != requested.Length {
		return objectstore.ErrInvalidRange
	}
	return nil
}

// parseContentRange parses "bytes <start>-<end>/<total>"; total is -1 for
// "*".
func parseContentRange(value string) (start, end, total int64, err error) {
	invalid := errors.New("http store: invalid Content-Range")
	if !strings.HasPrefix(value, "bytes ") {
		return 0, 0, 0, invalid
	}
	span, totalValue, found := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	startValue, endValue, spanFound := strings.Cut(span, "-")
	if !found || !spanFound {
		return 0, 0, 0, invalid
	}
	start, startErr := strconv.ParseInt(startValue, 10, 64)
	end, endErr := strconv.ParseInt(endValue, 10, 64)
	total = -1
	var totalErr error
	if totalValue != "*" {
		total, totalErr = strconv.ParseInt(totalValue, 10, 64)
	}
	if startErr != nil || endErr != nil || totalErr != nil || start < 0 || end < start {
		return 0, 0, 0, invalid
	}
	return start, end, total, nil
}

// exactReader yields exactly remaining bytes and reports a short body as
// io.ErrUnexpectedEOF instead of a silent truncation.
type exactReader struct {
	body      io.ReadCloser
	remaining int64
}

func (reader *exactReader) Read(buffer []byte) (int, error) {
	if reader.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(buffer)) > reader.remaining {
		buffer = buffer[:reader.remaining]
	}
	count, err := reader.body.Read(buffer)
	reader.remaining -= int64(count)
	if errors.Is(err, io.EOF) {
		if reader.remaining > 0 {
			return count, io.ErrUnexpectedEOF
		}
		return count, io.EOF
	}
	if err == nil && reader.remaining == 0 {
		return count, io.EOF
	}
	return count, err
}

func (reader *exactReader) Close() error { return reader.body.Close() }

var (
	_ objectstore.Store       = (*Store)(nil)
	_ objectstore.BatchStater = (*Store)(nil)
)
