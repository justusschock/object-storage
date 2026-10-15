package s3

import (
	"bytes"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// fakeS3 is an in-process S3 endpoint backed by gofakes3. gofakes3 v1.2.0
// implements conditional PutObject (If-None-Match/If-Match) but not the
// other S3 conditional requests this adapter relies on, nor UploadPartCopy.
// conditionalHandler fills exactly those gaps in front of gofakes3 and
// serializes requests so that every emulated condition is atomic:
//
//   - If-Match on GetObject, HeadObject, and DeleteObject;
//   - If-None-Match/If-Match on CompleteMultipartUpload and CopyObject, and
//     x-amz-copy-source-if-match on CopyObject and UploadPartCopy;
//   - UploadPartCopy, emulated as UploadPart of the source range.
//
// gofakes3 also does not verify x-amz-checksum-sha256 or SigV4 payload
// hashes; the adapter's client-side digest checks cover those cases here.
type fakeS3 struct {
	client  *awss3.Client
	server  *httptest.Server
	backend *s3mem.Backend
	handler *conditionalHandler
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	backend := s3mem.New()
	for _, bucket := range []string{"objects", "objects-copy"} {
		if err := backend.CreateBucket(bucket); err != nil {
			t.Fatal(err)
		}
	}
	faker := gofakes3.New(backend, gofakes3.WithTimeSkewLimit(0))
	handler := &conditionalHandler{next: faker.Server(), backend: backend}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test-access-key", "test-secret-key", ""),
		HTTPClient:  server.Client(),
	}, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	return &fakeS3{client: client, server: server, backend: backend, handler: handler}
}

func (fake *fakeS3) store(t *testing.T, options Options) *Store {
	t.Helper()
	store, err := NewWithOptions(fake.client, options)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type conditionalHandler struct {
	mu       sync.Mutex
	next     http.Handler
	backend  gofakes3.Backend
	requests []recordedRequest
}

// recordedRequest is one request seen by the fake, for request-shape tests.
type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
}

func (request recordedRequest) is(method string, query string) bool {
	_, ok := request.Query[query]
	return request.Method == method && (query == "" || ok)
}

func (handler *conditionalHandler) recorded() []recordedRequest {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return append([]recordedRequest(nil), handler.requests...)
}

func (handler *conditionalHandler) reset() {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.requests = nil
}

func (handler *conditionalHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.requests = append(handler.requests, recordedRequest{
		Method: request.Method, Path: request.URL.Path, Query: request.URL.Query(), Header: request.Header.Clone(),
	})
	bucket, key, _ := strings.Cut(strings.TrimPrefix(request.URL.Path, "/"), "/")
	query := request.URL.Query()
	_, hasUploadID := query["uploadId"]
	copySource := request.Header.Get("X-Amz-Copy-Source")
	if key == "" {
		handler.next.ServeHTTP(writer, request)
		return
	}
	switch {
	case request.Method == http.MethodGet || request.Method == http.MethodHead || (request.Method == http.MethodDelete && !hasUploadID):
		if !handler.check(writer, bucket, key, request.Header.Get("If-Match"), "") {
			return
		}
	case request.Method == http.MethodPost && hasUploadID:
		if !handler.check(writer, bucket, key, request.Header.Get("If-Match"), request.Header.Get("If-None-Match")) {
			return
		}
	case request.Method == http.MethodPut && copySource != "":
		sourceBucket, sourceKey, ok := parseCopySource(copySource)
		if !ok {
			writeError(writer, http.StatusBadRequest, "InvalidArgument")
			return
		}
		if !handler.check(writer, sourceBucket, sourceKey, request.Header.Get("X-Amz-Copy-Source-If-Match"), "") {
			return
		}
		if hasUploadID {
			handler.uploadPartCopy(writer, request, sourceBucket, sourceKey)
			return
		}
		if !handler.check(writer, bucket, key, request.Header.Get("If-Match"), request.Header.Get("If-None-Match")) {
			return
		}
		request.Header.Del("If-Match")
		request.Header.Del("If-None-Match")
	}
	handler.next.ServeHTTP(writer, request)
}

// check enforces If-Match and If-None-Match: * against the current object.
func (handler *conditionalHandler) check(writer http.ResponseWriter, bucket, key, ifMatch, ifNoneMatch string) bool {
	if ifMatch == "" && ifNoneMatch == "" {
		return true
	}
	object, err := handler.backend.HeadObject(bucket, key)
	exists := err == nil && object != nil
	if exists {
		_ = object.Contents.Close()
	}
	switch {
	case ifNoneMatch == "*" && exists:
		writeError(writer, http.StatusPreconditionFailed, "PreconditionFailed")
		return false
	case ifMatch != "" && !exists:
		writeError(writer, http.StatusNotFound, "NoSuchKey")
		return false
	case ifMatch != "" && strings.Trim(ifMatch, `"`) != hex.EncodeToString(object.Hash):
		writeError(writer, http.StatusPreconditionFailed, "PreconditionFailed")
		return false
	}
	return true
}

func (handler *conditionalHandler) uploadPartCopy(writer http.ResponseWriter, request *http.Request, bucket, key string) {
	var byteRange *gofakes3.ObjectRangeRequest
	if value := request.Header.Get("X-Amz-Copy-Source-Range"); value != "" {
		start, end, ok := strings.Cut(strings.TrimPrefix(value, "bytes="), "-")
		first, firstErr := strconv.ParseInt(start, 10, 64)
		last, lastErr := strconv.ParseInt(end, 10, 64)
		if !ok || firstErr != nil || lastErr != nil {
			writeError(writer, http.StatusBadRequest, "InvalidArgument")
			return
		}
		byteRange = &gofakes3.ObjectRangeRequest{Start: first, End: last}
	}
	object, err := handler.backend.GetObject(bucket, key, byteRange)
	if err != nil {
		writeError(writer, http.StatusNotFound, "NoSuchKey")
		return
	}
	data, err := io.ReadAll(object.Contents)
	_ = object.Contents.Close()
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "InternalError")
		return
	}
	part := request.Clone(request.Context())
	part.Body = io.NopCloser(bytes.NewReader(data))
	part.ContentLength = int64(len(data))
	part.Header = http.Header{"Content-Length": {strconv.Itoa(len(data))}}
	recorder := httptest.NewRecorder()
	handler.next.ServeHTTP(recorder, part)
	if recorder.Code != http.StatusOK {
		writer.WriteHeader(recorder.Code)
		_, _ = writer.Write(recorder.Body.Bytes())
		return
	}
	writer.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(writer).Encode(struct {
		XMLName      xml.Name `xml:"CopyPartResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}{ETag: recorder.Header().Get("ETag"), LastModified: time.Now().UTC().Format(time.RFC3339)})
}

func parseCopySource(value string) (string, string, bool) {
	value = strings.SplitN(strings.TrimPrefix(value, "/"), "?", 2)[0]
	bucket, key, ok := strings.Cut(value, "/")
	if !ok {
		return "", "", false
	}
	bucket, bucketErr := url.PathUnescape(bucket)
	key, keyErr := url.PathUnescape(key)
	return bucket, key, bucketErr == nil && keyErr == nil
}

func writeError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/xml")
	writer.WriteHeader(status)
	_, _ = fmt.Fprintf(writer, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}
