// Package gcstest runs an in-memory Google Cloud Storage server in-process,
// so that code using cloud.google.com/go/storage (including the gcs object
// store adapter) can be integration-tested without network access or
// credentials.
//
// The server is github.com/fsouza/fake-gcs-server/fakestorage behind a small
// HTTP middleware that makes the emulation faithful where the adapter
// depends on it. The middleware serializes every request, which makes every
// emulated precondition atomic, and adds:
//
//   - ifGenerationMatch / ifGenerationNotMatch on simple (uploadType=media)
//     and resumable uploads, evaluated when a resumable session finalizes;
//     the fake only honors them on multipart uploads;
//   - ifGenerationMatch / ifGenerationNotMatch (query parameters or
//     x-goog-if-generation-match / x-goog-if-generation-not-match headers) on
//     reads, deletes, metadata updates, and XML API (signed URL) PUTs, and
//     the generation parameter on deletes;
//   - ifGenerationMatch / ifGenerationNotMatch and ifSourceGenerationMatch /
//     ifSourceGenerationNotMatch on rewrite and copyTo;
//   - a complete compose implementation: the fake ignores destination
//     preconditions and source generations, keeps the previous generation
//     when composing onto an existing object, and fails with 500 instead of
//     404 for missing sources;
//   - x-goog-content-length-range on signed PUTs, and the ETag (quoted hex
//     MD5) and x-goog-generation response headers that the XML API returns
//     for PUTs;
//   - strictly increasing generations: the fake derives generations from the
//     wall clock in microseconds, so the middleware waits for the clock to
//     advance between mutations.
//
// The server speaks plain HTTP on 127.0.0.1. Signed URLs are accepted
// without verifying signatures; sign them with http (Insecure) URLs and any
// RSA key.
package gcstest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/option"
)

// NewServer starts an in-memory GCS server with the named buckets and
// returns a client connected to it, plus an idempotent cleanup function that
// closes the client and stops the server. Cleanup is also registered with
// tb.Cleanup.
//
// The client uses the server as its JSON API endpoint and XML API host, so
// reads, writes, and signed URLs (BucketHandle.SignedURL with Insecure set)
// all reach the fake. It sends no credentials.
func NewServer(tb testing.TB, buckets ...string) (*storage.Client, func()) {
	tb.Helper()
	listener := httptest.NewUnstartedServer(nil)
	fake, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		NoListener: true,
		Scheme:     "http",
		PublicHost: listener.Listener.Addr().String(),
	})
	if err != nil {
		listener.Close()
		tb.Fatalf("gcstest: start fake server: %v", err)
	}
	for _, bucket := range buckets {
		fake.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
	}
	listener.Config.Handler = newHandler(fake)
	listener.Start()
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint(listener.URL+"/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(listener.Client()),
	)
	if err != nil {
		listener.Close()
		fake.Stop()
		tb.Fatalf("gcstest: create client: %v", err)
	}
	cleanup := sync.OnceFunc(func() {
		_ = client.Close()
		listener.Close()
		fake.Stop()
	})
	tb.Cleanup(cleanup)
	return client, cleanup
}

// conditions holds GCS generation preconditions.
type conditions struct {
	match    *int64
	notMatch *int64
}

func (c conditions) empty() bool { return c.match == nil && c.notMatch == nil }

// met evaluates the conditions against the live generation, 0 when absent.
func (c conditions) met(generation int64) bool {
	return (c.match == nil || *c.match == generation) && (c.notMatch == nil || *c.notMatch != generation)
}

type session struct {
	bucket, name string
	conditions   conditions
}

type handler struct {
	mu           sync.Mutex
	next         http.Handler
	fake         *fakestorage.Server
	lastMutation int64
	sessions     map[string]session
}

func newHandler(fake *fakestorage.Server) *handler {
	return &handler{next: fake.HTTPHandler(), fake: fake, sessions: make(map[string]session)}
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	segments := pathSegments(request)
	query := request.URL.Query()
	switch {
	case len(segments) >= 1 && segments[0] == "_internal", len(segments) >= 1 && segments[0] == "batch":
		h.next.ServeHTTP(writer, request)
	case len(segments) >= 6 && segments[0] == "upload" && segments[3] == "b" && segments[5] == "o":
		h.upload(writer, request, segments[4], query)
	case len(segments) == 3 && segments[0] == "upload" && segments[1] == "resumable":
		h.uploadChunk(writer, request, segments[2])
	case len(segments) >= 6 && segments[0] == "download" && segments[3] == "b" && segments[5] == "o":
		h.objectRequest(writer, request, segments[4], joinName(segments[6:]), false)
	case len(segments) >= 4 && segments[0] == "storage" && segments[1] == "v1" && segments[2] == "b":
		h.jsonAPI(writer, request, segments[3:])
	case len(segments) >= 2:
		h.xmlAPI(writer, request, segments[0], joinName(segments[1:]))
	default:
		h.next.ServeHTTP(writer, request)
	}
}

// jsonAPI handles /storage/v1/b/{bucket}/... requests.
func (h *handler) jsonAPI(writer http.ResponseWriter, request *http.Request, segments []string) {
	bucket := segments[0]
	if len(segments) < 3 || segments[1] != "o" {
		h.next.ServeHTTP(writer, request)
		return
	}
	rest := segments[2:]
	for index, segment := range rest {
		if (segment == "rewriteTo" || segment == "copyTo") && index > 0 && len(rest) > index+3 && rest[index+1] == "b" && rest[index+3] == "o" {
			h.rewrite(writer, request, bucket, joinName(rest[:index]), rest[index+2], joinName(rest[index+4:]))
			return
		}
	}
	if len(rest) > 1 && rest[len(rest)-1] == "compose" && request.Method == http.MethodPost {
		h.compose(writer, request, bucket, joinName(rest[:len(rest)-1]))
		return
	}
	if len(rest) > 1 && rest[len(rest)-2] == "acl" || len(rest) > 2 && rest[len(rest)-3] == "acl" {
		h.next.ServeHTTP(writer, request)
		return
	}
	h.objectRequest(writer, request, bucket, joinName(rest), false)
}

// xmlAPI handles path-style XML API requests, including signed URLs.
func (h *handler) xmlAPI(writer http.ResponseWriter, request *http.Request, bucket, name string) {
	if request.Method == http.MethodPut && request.URL.Query().Get("X-Goog-Algorithm") != "" {
		h.signedPut(writer, request, bucket, name)
		return
	}
	h.objectRequest(writer, request, bucket, name, true)
}

// objectRequest enforces preconditions on reads, deletes, and metadata
// updates of one object.
func (h *handler) objectRequest(writer http.ResponseWriter, request *http.Request, bucket, name string, xmlAPI bool) {
	preconditions, err := requestConditions(request, "")
	if err != nil {
		writeError(writer, xmlAPI, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	mutation := request.Method != http.MethodGet && request.Method != http.MethodHead
	if !preconditions.empty() || (request.Method == http.MethodDelete && request.URL.Query().Get("generation") != "") {
		generation := h.generation(bucket, name)
		if value := request.URL.Query().Get("generation"); value != "" && request.Method == http.MethodDelete {
			wanted, err := strconv.ParseInt(value, 10, 64)
			if err != nil || wanted != generation {
				writeError(writer, xmlAPI, http.StatusNotFound, "notFound", "No such object generation.")
				return
			}
		}
		if generation == 0 && mutation {
			writeError(writer, xmlAPI, http.StatusNotFound, "notFound", "No such object.")
			return
		}
		if !preconditions.met(generation) {
			if generation == 0 {
				writeError(writer, xmlAPI, http.StatusNotFound, "notFound", "No such object.")
				return
			}
			writeError(writer, xmlAPI, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
			return
		}
	}
	if mutation {
		h.mutate(func() { h.next.ServeHTTP(writer, request) })
		return
	}
	h.next.ServeHTTP(writer, request)
}

// upload handles /upload/storage/v1/b/{bucket}/o requests.
func (h *handler) upload(writer http.ResponseWriter, request *http.Request, bucket string, query url.Values) {
	preconditions, err := requestConditions(request, "")
	if err != nil {
		writeError(writer, false, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if query.Get("upload_id") != "" {
		h.uploadChunk(writer, request, query.Get("upload_id"))
		return
	}
	name := query.Get("name")
	uploadType := query.Get("uploadType")
	if uploadType == "" && request.Header.Get("X-Goog-Upload-Protocol") == "resumable" {
		uploadType = "resumable"
	}
	if uploadType == "resumable" && request.Method == http.MethodPost {
		recorder := httptest.NewRecorder()
		h.next.ServeHTTP(recorder, request)
		if recorder.Code/100 == 2 {
			if location, err := url.Parse(recorder.Header().Get("Location")); err == nil {
				if id := location.Query().Get("upload_id"); id != "" {
					h.sessions[id] = session{bucket: bucket, name: name, conditions: preconditions}
				}
			}
		}
		copyResponse(writer, recorder)
		return
	}
	if name != "" && !preconditions.empty() && !preconditions.met(h.generation(bucket, name)) {
		writeError(writer, false, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
		return
	}
	h.mutate(func() { h.next.ServeHTTP(writer, request) })
}

// uploadChunk evaluates a resumable session's preconditions when the chunk
// finalizes the upload.
func (h *handler) uploadChunk(writer http.ResponseWriter, request *http.Request, id string) {
	upload, known := h.sessions[id]
	if known && finalChunk(request.Header.Get("Content-Range")) {
		delete(h.sessions, id)
		if !upload.conditions.empty() && !upload.conditions.met(h.generation(upload.bucket, upload.name)) {
			writeError(writer, false, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
			return
		}
	}
	if request.Method == http.MethodDelete {
		delete(h.sessions, id)
	}
	h.mutate(func() { h.next.ServeHTTP(writer, request) })
}

// rewrite enforces destination and source preconditions on rewrite and
// copyTo.
func (h *handler) rewrite(writer http.ResponseWriter, request *http.Request, sourceBucket, sourceName, bucket, name string) {
	destination, err := requestConditions(request, "")
	if err != nil {
		writeError(writer, false, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	source, err := requestConditions(request, "Source")
	if err != nil {
		writeError(writer, false, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if !source.empty() {
		generation := h.generation(sourceBucket, sourceName)
		if generation == 0 {
			writeError(writer, false, http.StatusNotFound, "notFound", "No such object.")
			return
		}
		if !source.met(generation) {
			writeError(writer, false, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
			return
		}
	}
	if !destination.empty() && !destination.met(h.generation(bucket, name)) {
		writeError(writer, false, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
		return
	}
	h.mutate(func() { h.next.ServeHTTP(writer, request) })
}

type composeRequest struct {
	SourceObjects []struct {
		Name                string `json:"name"`
		Generation          int64  `json:"generation,string"`
		ObjectPreconditions *struct {
			IfGenerationMatch int64 `json:"ifGenerationMatch,string"`
		} `json:"objectPreconditions"`
	} `json:"sourceObjects"`
	Destination *struct {
		ContentType        string            `json:"contentType"`
		ContentEncoding    string            `json:"contentEncoding"`
		ContentDisposition string            `json:"contentDisposition"`
		ContentLanguage    string            `json:"contentLanguage"`
		CacheControl       string            `json:"cacheControl"`
		Metadata           map[string]string `json:"metadata"`
	} `json:"destination"`
}

// compose implements objects.compose with source generations, source and
// destination preconditions, and a fresh destination generation.
func (h *handler) compose(writer http.ResponseWriter, request *http.Request, bucket, name string) {
	preconditions, err := requestConditions(request, "")
	if err != nil {
		writeError(writer, false, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	var input composeRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		writeError(writer, false, http.StatusBadRequest, "invalid", "Error parsing request body")
		return
	}
	if len(input.SourceObjects) == 0 || len(input.SourceObjects) > 32 {
		writeError(writer, false, http.StatusBadRequest, "invalid", "The number of source components must be between 1 and 32.")
		return
	}
	if _, err := h.fake.Backend().GetBucket(bucket); err != nil {
		writeError(writer, false, http.StatusNotFound, "notFound", "No such bucket.")
		return
	}
	var data bytes.Buffer
	for _, source := range input.SourceObjects {
		var object fakestorage.Object
		var err error
		if source.Generation > 0 {
			object, err = h.fake.GetObjectWithGeneration(bucket, source.Name, source.Generation)
		} else {
			object, err = h.fake.GetObject(bucket, source.Name)
		}
		if err != nil {
			writeError(writer, false, http.StatusNotFound, "notFound", fmt.Sprintf("Object %s not found.", source.Name))
			return
		}
		if source.ObjectPreconditions != nil && source.ObjectPreconditions.IfGenerationMatch != object.Generation {
			writeError(writer, false, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
			return
		}
		data.Write(object.Content)
	}
	if !preconditions.empty() && !preconditions.met(h.generation(bucket, name)) {
		writeError(writer, false, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
		return
	}
	attrs := fakestorage.ObjectAttrs{BucketName: bucket, Name: name}
	if destination := input.Destination; destination != nil {
		attrs.ContentType, attrs.ContentEncoding, attrs.ContentDisposition = destination.ContentType, destination.ContentEncoding, destination.ContentDisposition
		attrs.ContentLanguage, attrs.CacheControl, attrs.Metadata = destination.ContentLanguage, destination.CacheControl, destination.Metadata
	}
	h.mutate(func() {
		err = h.fake.CreateObjectStreaming(fakestorage.Object{ObjectAttrs: attrs, Content: data.Bytes()}.StreamingObject())
	})
	if err != nil {
		writeError(writer, false, http.StatusInternalServerError, "internalError", err.Error())
		return
	}
	// Respond with the object resource, as objects.compose does.
	get, err := http.NewRequestWithContext(request.Context(), http.MethodGet, "/storage/v1/b/"+url.PathEscape(bucket)+"/o/"+url.PathEscape(name), nil)
	if err != nil {
		writeError(writer, false, http.StatusInternalServerError, "internalError", err.Error())
		return
	}
	get.Host = request.Host
	h.next.ServeHTTP(writer, get)
}

// signedPut handles XML API object PUTs (signed URL uploads).
func (h *handler) signedPut(writer http.ResponseWriter, request *http.Request, bucket, name string) {
	preconditions, err := requestConditions(request, "")
	if err != nil {
		writeError(writer, true, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	if value := request.Header.Get("X-Goog-Content-Length-Range"); value != "" {
		minimum, maximum, ok := parseLengthRange(value)
		if !ok {
			writeError(writer, true, http.StatusBadRequest, "InvalidArgument", "Invalid x-goog-content-length-range.")
			return
		}
		data, err := io.ReadAll(io.LimitReader(request.Body, maximum+1))
		if err != nil {
			writeError(writer, true, http.StatusBadRequest, "IncompleteBody", err.Error())
			return
		}
		if int64(len(data)) < minimum || int64(len(data)) > maximum {
			writeError(writer, true, http.StatusBadRequest, "EntityTooLarge", "Your proposed upload is outside the allowed content length range.")
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(data))
		request.ContentLength = int64(len(data))
	}
	if !preconditions.empty() && !preconditions.met(h.generation(bucket, name)) {
		writeError(writer, true, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold.")
		return
	}
	recorder := httptest.NewRecorder()
	h.mutate(func() { h.next.ServeHTTP(recorder, request) })
	if recorder.Code/100 != 2 {
		copyResponse(writer, recorder)
		return
	}
	object, err := h.fake.GetObjectStreaming(bucket, name)
	if err != nil {
		writeError(writer, true, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	_ = object.Close()
	if md5, err := base64.StdEncoding.DecodeString(object.Md5Hash); err == nil {
		writer.Header().Set("ETag", strconv.Quote(hex.EncodeToString(md5)))
	}
	writer.Header().Set("X-Goog-Generation", strconv.FormatInt(object.Generation, 10))
	writer.Header().Set("X-Goog-Metageneration", "1")
	writer.Header().Set("X-Goog-Hash", "crc32c="+object.Crc32c+",md5="+object.Md5Hash)
	writer.WriteHeader(http.StatusOK)
}

// mutate runs one state change so that the fake, which derives generations
// from the wall clock in microseconds, assigns a strictly newer generation
// than every earlier change.
func (h *handler) mutate(change func()) {
	for now := time.Now().UnixMicro(); now <= h.lastMutation; now = time.Now().UnixMicro() {
		time.Sleep(time.Duration(h.lastMutation-now+1) * time.Microsecond)
	}
	change()
	h.lastMutation = time.Now().UnixMicro()
}

// generation returns the live generation of an object, or 0 when absent.
func (h *handler) generation(bucket, name string) int64 {
	object, err := h.fake.GetObjectStreaming(bucket, name)
	if err != nil {
		return 0
	}
	_ = object.Close()
	return object.Generation
}

// requestConditions reads if{prefix}GenerationMatch and
// if{prefix}GenerationNotMatch from the query string or from the matching
// x-goog-if-* headers.
func requestConditions(request *http.Request, prefix string) (conditions, error) {
	var result conditions
	query := request.URL.Query()
	for _, condition := range []struct {
		parameter, header string
		target            **int64
	}{
		{"if" + prefix + "GenerationMatch", "X-Goog-If-" + headerPrefix(prefix) + "Generation-Match", &result.match},
		{"if" + prefix + "GenerationNotMatch", "X-Goog-If-" + headerPrefix(prefix) + "Generation-Not-Match", &result.notMatch},
	} {
		value := query.Get(condition.parameter)
		if value == "" {
			value = request.Header.Get(condition.header)
		}
		if value == "" {
			continue
		}
		generation, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return conditions{}, fmt.Errorf("invalid %s %q", condition.parameter, value)
		}
		*condition.target = &generation
	}
	return result, nil
}

func headerPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	return prefix + "-"
}

// finalChunk reports whether a resumable upload chunk's Content-Range
// declares the total size and reaches it.
func finalChunk(contentRange string) bool {
	value, ok := strings.CutPrefix(contentRange, "bytes ")
	if !ok {
		return contentRange == ""
	}
	span, total, ok := strings.Cut(value, "/")
	if !ok || total == "*" {
		return false
	}
	size, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return false
	}
	if span == "*" {
		return true
	}
	_, end, ok := strings.Cut(span, "-")
	last, err := strconv.ParseInt(end, 10, 64)
	return ok && err == nil && last+1 >= size
}

func parseLengthRange(value string) (int64, int64, bool) {
	first, second, ok := strings.Cut(value, ",")
	minimum, minimumErr := strconv.ParseInt(strings.TrimSpace(first), 10, 64)
	maximum, maximumErr := strconv.ParseInt(strings.TrimSpace(second), 10, 64)
	return minimum, maximum, ok && minimumErr == nil && maximumErr == nil && minimum >= 0 && maximum >= minimum
}

// pathSegments splits the escaped request path into unescaped segments.
// Escaped slashes stay inside their segment.
func pathSegments(request *http.Request) []string {
	path := strings.TrimPrefix(request.URL.EscapedPath(), "/")
	if path == "" {
		return nil
	}
	segments := strings.Split(path, "/")
	for index, segment := range segments {
		if unescaped, err := url.PathUnescape(segment); err == nil {
			segments[index] = unescaped
		}
	}
	return segments
}

func joinName(segments []string) string { return strings.Join(segments, "/") }

func copyResponse(writer http.ResponseWriter, recorder *httptest.ResponseRecorder) {
	for name, values := range recorder.Header() {
		writer.Header()[name] = values
	}
	writer.WriteHeader(recorder.Code)
	_, _ = writer.Write(recorder.Body.Bytes())
}

// writeError writes a JSON API or XML API error response.
func writeError(writer http.ResponseWriter, xmlAPI bool, status int, reason, message string) {
	if xmlAPI {
		writer.Header().Set("Content-Type", "application/xml; charset=UTF-8")
		writer.WriteHeader(status)
		_ = xml.NewEncoder(writer).Encode(struct {
			XMLName xml.Name `xml:"Error"`
			Code    string   `xml:"Code"`
			Message string   `xml:"Message"`
		}{Code: reason, Message: message})
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]any{
		"code": status, "message": message,
		"errors": []map[string]any{{"domain": "global", "reason": reason, "message": message}},
	}})
}
