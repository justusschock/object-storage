package http_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
	httpstore "github.com/justusschock/go-object-storage/backend/http"
)

var modified = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// dirServer serves files below root with http.ServeContent, which implements
// Range, If-Match, and HEAD. Every response carries a strong ETag derived
// from the file contents; digest controls whether a Repr-Digest header is
// sent, and metadata adds x-amz-meta-* headers.
type dirServer struct {
	root        string
	digest      bool
	ignoreRange bool
	noETag      bool
	metadata    map[string]string

	mu       sync.Mutex
	requests []*nethttp.Request
}

func (server *dirServer) ServeHTTP(writer nethttp.ResponseWriter, request *nethttp.Request) {
	server.mu.Lock()
	server.requests = append(server.requests, request.Clone(context.Background()))
	server.mu.Unlock()
	if request.Method != nethttp.MethodGet && request.Method != nethttp.MethodHead {
		nethttp.Error(writer, "read only", nethttp.StatusMethodNotAllowed)
		return
	}
	data, err := os.ReadFile(filepath.Join(server.root, filepath.FromSlash(strings.TrimPrefix(request.URL.Path, "/"))))
	if err != nil {
		nethttp.NotFound(writer, request)
		return
	}
	digest := sha256.Sum256(data)
	if !server.noETag {
		writer.Header().Set("ETag", `"`+hex.EncodeToString(digest[:16])+`"`)
	}
	if server.digest {
		writer.Header().Set("Repr-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(digest[:])+":")
	}
	for name, value := range server.metadata {
		writer.Header().Set("X-Amz-Meta-"+name, value)
	}
	if server.ignoreRange {
		request.Header.Del("Range")
	}
	nethttp.ServeContent(writer, request, "", modified, bytes.NewReader(data))
}

func (server *dirServer) lastRequest() *nethttp.Request {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.requests[len(server.requests)-1]
}

func newServer(t *testing.T, server *dirServer, files map[string]string) (*httpstore.Store, *httptest.Server) {
	t.Helper()
	server.root = t.TempDir()
	for name, contents := range files {
		path := filepath.Join(server.root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	store, err := httpstore.New(httpServer.URL+"/remote/", httpstore.Options{Client: httpServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return store, httpServer
}

func read(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestReadContract runs the read subset of the store contract (put/get/stat
// consistency, conditional reads, exact ranges, not-found, cancellation)
// against a static file server.
// FR-OBJ-17
func TestReadContract(t *testing.T) {
	ctx := context.Background()
	server := &dirServer{digest: true, metadata: map[string]string{"owner": "caller"}}
	store, _ := newServer(t, server, map[string]string{"remote/objects/path/value": "immutable object", "remote/objects/range": "abcdefghij"})

	t.Run("get and stat", func(t *testing.T) {
		stat, err := store.Stat(ctx, "objects", "path/value")
		if err != nil {
			t.Fatal(err)
		}
		wantDigest := sha256.Sum256([]byte("immutable object"))
		if stat.Bucket != "objects" || stat.Key != "path/value" || stat.Version == "" || stat.ETag != stat.Version || stat.Size != 16 ||
			!stat.SHA256Known || stat.SHA256 != wantDigest || !stat.ModifiedAt.Equal(modified) || stat.Metadata["owner"] != "caller" {
			t.Fatalf("Stat = %#v", stat)
		}
		body, got, err := store.Get(ctx, "objects", "path/value", objectstore.GetOptions{IfMatchVersion: stat.Version})
		if err != nil {
			t.Fatal(err)
		}
		if data := read(t, body); string(data) != "immutable object" || !reflect.DeepEqual(got, stat) {
			t.Fatalf("Get = (%q, %#v), want %#v", data, got, stat)
		}
		if _, _, err := store.Get(ctx, "objects", "path/value", objectstore.GetOptions{IfMatchVersion: `"wrong"`}); !errors.Is(err, objectstore.ErrPrecondition) {
			t.Fatalf("Get wrong version error = %v, want ErrPrecondition", err)
		}
		if header := server.lastRequest().Header; header.Get("If-Match") != `"wrong"` || header.Get("Accept-Encoding") != "identity" {
			t.Fatalf("conditional request headers = %v", header)
		}
	})

	t.Run("exact range", func(t *testing.T) {
		body, object, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 2, Length: 4}})
		if err != nil {
			t.Fatal(err)
		}
		if data := read(t, body); string(data) != "cdef" || object.Size != 10 {
			t.Fatalf("ranged Get = (%q, %#v)", data, object)
		}
		for _, byteRange := range []objectstore.ByteRange{{Offset: 10, Length: 1}, {Offset: 8, Length: 3}, {Offset: -1, Length: 1}, {Offset: 0, Length: 0}} {
			body, _, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{Range: &byteRange})
			if body != nil || !errors.Is(err, objectstore.ErrInvalidRange) {
				t.Fatalf("Get range %+v = (%#v, %v), want nil ErrInvalidRange", byteRange, body, err)
			}
		}
	})

	t.Run("not found", func(t *testing.T) {
		if _, err := store.Stat(ctx, "objects", "missing"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("Stat missing error = %v", err)
		}
		if body, _, err := store.Get(ctx, "objects", "missing", objectstore.GetOptions{}); body != nil || !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("Get missing = (%#v, %v)", body, err)
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		if _, err := store.Stat(ctx, "objects", "../escape"); !errors.Is(err, objectstore.ErrInvalidKey) {
			t.Fatalf("Stat invalid key error = %v", err)
		}
		if _, _, err := store.Get(ctx, "", "key", objectstore.GetOptions{}); !errors.Is(err, objectstore.ErrInvalidBucket) {
			t.Fatalf("Get invalid bucket error = %v", err)
		}
	})

	t.Run("batch stat", func(t *testing.T) {
		keys := []objectstore.Key{"range", "missing", "path/value"}
		results, err := store.StatMany(ctx, "objects", keys)
		if err != nil || len(results) != len(keys) {
			t.Fatalf("StatMany = (%#v, %v)", results, err)
		}
		for index, key := range keys {
			if key == "missing" {
				if !errors.Is(results[index].Err, objectstore.ErrNotFound) {
					t.Fatalf("StatMany missing error = %v", results[index].Err)
				}
				continue
			}
			want, err := store.Stat(ctx, "objects", key)
			if err != nil || results[index].Err != nil || !reflect.DeepEqual(results[index].Object, want) {
				t.Fatalf("StatMany[%d] = %#v, want %#v (%v)", index, results[index], want, err)
			}
		}
		if results, err := store.StatMany(ctx, "objects", nil); err != nil || len(results) != 0 {
			t.Fatalf("StatMany(nil) = (%#v, %v)", results, err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if body, _, err := store.Get(canceled, "objects", "range", objectstore.GetOptions{}); body != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Get = (%#v, %v)", body, err)
		}
		if _, err := store.Stat(canceled, "objects", "range"); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Stat error = %v", err)
		}
		if _, err := store.StatMany(canceled, "objects", []objectstore.Key{"range"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled StatMany error = %v", err)
		}
	})
}

// FR-OBJ-17
func TestReadOnly(t *testing.T) {
	store, err := httpstore.New("https://example.com", httpstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Put(ctx, "objects", "key", strings.NewReader("value"), objectstore.PutOptions{}); !errors.Is(err, httpstore.ErrReadOnly) || !errors.Is(err, httpstore.ErrNotSupported) {
		t.Fatalf("Put error = %v, want ErrReadOnly", err)
	}
	if err := store.Delete(ctx, "objects", "key", ""); !errors.Is(err, httpstore.ErrReadOnly) {
		t.Fatalf("Delete error = %v, want ErrReadOnly", err)
	}
	if _, err := store.List(ctx, "objects", objectstore.ListOptions{}); !errors.Is(err, httpstore.ErrNotSupported) || errors.Is(err, httpstore.ErrReadOnly) {
		t.Fatalf("List error = %v, want ErrNotSupported", err)
	}
	var multipart any = store
	if _, ok := multipart.(objectstore.MultipartStore); ok {
		t.Fatal("read-only store claims multipart support")
	}
}

// FR-OBJ-17
func TestServerIgnoringRange(t *testing.T) {
	store, _ := newServer(t, &dirServer{ignoreRange: true}, map[string]string{"remote/objects/range": "abcdefghij"})
	ctx := context.Background()
	body, object, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 3, Length: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if data := read(t, body); string(data) != "defgh" || object.Size != 10 || object.SHA256Known {
		t.Fatalf("range over full response = (%q, %#v)", data, object)
	}
	if body, _, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 8, Length: 3}}); body != nil || !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("past-end range over full response = (%#v, %v)", body, err)
	}
}

func TestServerWithoutETag(t *testing.T) {
	store, _ := newServer(t, &dirServer{noETag: true}, map[string]string{"remote/objects/key": "value"})
	ctx := context.Background()
	object, err := store.Stat(ctx, "objects", "key")
	if err != nil || object.Version != "" || object.Size != 5 || object.Metadata != nil {
		t.Fatalf("Stat = (%#v, %v)", object, err)
	}
	// Without an ETag no version can match, even if the server ignores If-Match.
	if _, _, err := store.Get(ctx, "objects", "key", objectstore.GetOptions{IfMatchVersion: `"any"`}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Get IfMatchVersion without ETag error = %v", err)
	}
}

func TestHeadersAndStatusErrors(t *testing.T) {
	var seen nethttp.Header
	var path string
	server := httptest.NewServer(nethttp.HandlerFunc(func(writer nethttp.ResponseWriter, request *nethttp.Request) {
		seen, path = request.Header.Clone(), request.URL.EscapedPath()
		writer.WriteHeader(nethttp.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)
	store, err := httpstore.New(server.URL+"/base path?token=secret", httpstore.Options{
		Client:      server.Client(),
		Header:      nethttp.Header{"X-Api-Key": {"key"}, "Authorization": {"Basic overridden"}},
		BearerToken: "token",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Stat(context.Background(), "my bucket", "dir/file #1")
	var statusErr *httpstore.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != nethttp.StatusUnauthorized || statusErr.Method != nethttp.MethodHead || strings.Contains(statusErr.Error(), "secret") {
		t.Fatalf("Stat error = %#v", err)
	}
	if seen.Get("Authorization") != "Bearer token" || seen.Get("X-Api-Key") != "key" {
		t.Fatalf("request headers = %v", seen)
	}
	if path != "/base%20path/my%20bucket/dir/file%20%231" {
		t.Fatalf("request path = %q", path)
	}
}

func TestURLMapping(t *testing.T) {
	for _, test := range []struct {
		name    string
		base    string
		options httpstore.Options
		want    string
	}{
		{name: "bucket", base: "https://cdn.example.com/remote", want: "https://cdn.example.com/remote/objects/a/b%3Fc"},
		{name: "trailing slash", base: "https://cdn.example.com/", want: "https://cdn.example.com/objects/a/b%3Fc"},
		{name: "omit bucket", base: "http://cdn.example.com/r", options: httpstore.Options{OmitBucket: true}, want: "http://cdn.example.com/r/a/b%3Fc"},
		{name: "custom", options: httpstore.Options{ObjectURL: func(bucket objectstore.Bucket, key objectstore.Key) (string, error) {
			return "https://" + string(bucket) + ".example.com/" + string(key), nil
		}}, want: "https://objects.example.com/a/b?c"}, // ObjectURL owns escaping
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := httpstore.New(test.base, test.options)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.URL("objects", "a/b?c")
			if err != nil || got != test.want {
				t.Fatalf("URL = (%q, %v), want %q", got, err, test.want)
			}
		})
	}
	for _, base := range []string{"", "ftp://example.com", "/relative", "https://"} {
		if _, err := httpstore.New(base, httpstore.Options{}); err == nil {
			t.Fatalf("New(%q) succeeded", base)
		}
	}
	if _, err := httpstore.New("https://example.com", httpstore.Options{StatConcurrency: -1}); err == nil {
		t.Fatal("New accepted negative concurrency")
	}
}

// roundTripper returns canned responses for transport-level edge cases.
type roundTripper func(*nethttp.Request) (*nethttp.Response, error)

func (function roundTripper) RoundTrip(request *nethttp.Request) (*nethttp.Response, error) {
	return function(request)
}

func TestTruncatedAndMalformedResponses(t *testing.T) {
	respond := func(status int, header nethttp.Header, body string, length int64) *httpstore.Store {
		client := &nethttp.Client{Transport: roundTripper(func(request *nethttp.Request) (*nethttp.Response, error) {
			return &nethttp.Response{
				StatusCode: status, Status: nethttp.StatusText(status), Header: header, ContentLength: length,
				Body: io.NopCloser(strings.NewReader(body)), Request: request,
			}, nil
		})}
		store, err := httpstore.New("https://example.com", httpstore.Options{Client: client})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	ctx := context.Background()

	body, object, err := respond(nethttp.StatusOK, nethttp.Header{}, "short", 10).Get(ctx, "objects", "key", objectstore.GetOptions{})
	if err != nil || object.Size != 10 {
		t.Fatalf("Get = (%#v, %v)", object, err)
	}
	if _, err := io.ReadAll(body); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body error = %v, want io.ErrUnexpectedEOF", err)
	}

	wrongRange := nethttp.Header{"Content-Range": {"bytes 1-3/10"}}
	if _, _, err := respond(nethttp.StatusPartialContent, wrongRange, "bcd", 3).Get(ctx, "objects", "key", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 2, Length: 3}}); !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("mismatched Content-Range error = %v", err)
	}
	if _, _, err := respond(nethttp.StatusPartialContent, nethttp.Header{}, "bcd", 3).Get(ctx, "objects", "key", objectstore.GetOptions{}); err == nil {
		t.Fatal("unrequested partial content accepted")
	}
	unknownTotal := nethttp.Header{"Content-Range": {"bytes 2-4/*"}}
	body, object, err = respond(nethttp.StatusPartialContent, unknownTotal, "cde", 3).Get(ctx, "objects", "key", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 2, Length: 3}})
	if err != nil || object.Size != -1 || string(read(t, body)) != "cde" {
		t.Fatalf("unknown total = (%#v, %v)", object, err)
	}
	badDigest := nethttp.Header{"X-Goog-Meta-Go-Object-Storage-Sha256": {"zz"}}
	if _, err := respond(nethttp.StatusOK, badDigest, "", 0).Stat(ctx, "objects", "key"); err == nil {
		t.Fatal("invalid digest metadata accepted")
	}
	for _, status := range []int{nethttp.StatusGone, nethttp.StatusPreconditionFailed, nethttp.StatusRequestedRangeNotSatisfiable} {
		want := map[int]error{nethttp.StatusGone: objectstore.ErrNotFound, nethttp.StatusPreconditionFailed: objectstore.ErrPrecondition, nethttp.StatusRequestedRangeNotSatisfiable: objectstore.ErrInvalidRange}[status]
		if _, err := respond(status, nethttp.Header{}, "", 0).Stat(ctx, "objects", "key"); !errors.Is(err, want) {
			t.Fatalf("status %d error = %v, want %v", status, err, want)
		}
	}
}

func TestDigestAndMetadataHeaders(t *testing.T) {
	data := []byte("payload")
	digest := sha256.Sum256(data)
	encoded := base64.StdEncoding.EncodeToString(digest[:])
	for _, test := range []struct {
		name     string
		header   nethttp.Header
		known    bool
		metadata map[string]string
	}{
		{name: "repr-digest", header: nethttp.Header{"Repr-Digest": {"sha-512=:AAAA:, sha-256=:" + encoded + ":"}}, known: true},
		{name: "legacy digest", header: nethttp.Header{"Digest": {"MD5=abc,SHA-256=" + encoded}}, known: true},
		{name: "s3 metadata", header: nethttp.Header{"X-Amz-Meta-Go-Object-Storage-Sha256": {hex.EncodeToString(digest[:])}, "X-Amz-Meta-Owner": {"me"}}, known: true, metadata: map[string]string{"owner": "me"}},
		{name: "azure metadata", header: nethttp.Header{"X-Ms-Meta-Goobjectstoragesha256": {hex.EncodeToString(digest[:])}, "X-Ms-Meta-Tier": {"hot"}}, known: true, metadata: map[string]string{"tier": "hot"}},
		{name: "malformed digest", header: nethttp.Header{"Repr-Digest": {"sha-256=:short:"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(nethttp.HandlerFunc(func(writer nethttp.ResponseWriter, request *nethttp.Request) {
				for name, values := range test.header {
					writer.Header()[name] = values
				}
				nethttp.ServeContent(writer, request, "", time.Time{}, bytes.NewReader(data))
			}))
			t.Cleanup(server.Close)
			store, err := httpstore.New(server.URL, httpstore.Options{Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			object, err := store.Stat(context.Background(), "objects", "key")
			if err != nil {
				t.Fatal(err)
			}
			if object.SHA256Known != test.known || (test.known && object.SHA256 != digest) || !reflect.DeepEqual(object.Metadata, test.metadata) || !object.ModifiedAt.IsZero() {
				t.Fatalf("object = %#v", object)
			}
		})
	}
}
