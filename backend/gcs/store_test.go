package gcs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/storage"
	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/gcs/gcstest"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FR-OBJ-25
func TestMapError(t *testing.T) {
	plain := errors.New("plain")
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{"nil", nil, nil},
		{"canceled", fmt.Errorf("wrapped: %w", context.Canceled), context.Canceled},
		{"deadline", context.DeadlineExceeded, context.DeadlineExceeded},
		{"object not exist", storage.ErrObjectNotExist, objectstore.ErrNotFound},
		{"bucket not exist", fmt.Errorf("op: %w", storage.ErrBucketNotExist), objectstore.ErrNotFound},
		{"http 404", &googleapi.Error{Code: http.StatusNotFound}, objectstore.ErrNotFound},
		{"http 409", &googleapi.Error{Code: http.StatusConflict}, objectstore.ErrPrecondition},
		{"http 412", fmt.Errorf("wrapped: %w", &googleapi.Error{Code: http.StatusPreconditionFailed}), objectstore.ErrPrecondition},
		{"http 416", &googleapi.Error{Code: http.StatusRequestedRangeNotSatisfiable}, objectstore.ErrInvalidRange},
		{"http 500", &googleapi.Error{Code: http.StatusInternalServerError}, nil},
		{"grpc not found", status.Error(codes.NotFound, "missing"), objectstore.ErrNotFound},
		{"grpc failed precondition", status.Error(codes.FailedPrecondition, "condition"), objectstore.ErrPrecondition},
		{"grpc aborted", status.Error(codes.Aborted, "conflict"), objectstore.ErrPrecondition},
		{"grpc out of range", status.Error(codes.OutOfRange, "range"), objectstore.ErrInvalidRange},
		{"grpc unavailable", status.Error(codes.Unavailable, "down"), nil},
		{"plain", plain, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := mapError(test.err)
			switch {
			case test.err == nil:
				if got != nil {
					t.Fatalf("mapError(nil) = %v", got)
				}
			case test.want == nil:
				for _, sentinel := range []error{objectstore.ErrNotFound, objectstore.ErrPrecondition, objectstore.ErrInvalidRange} {
					if errors.Is(got, sentinel) {
						t.Fatalf("mapError(%v) = %v, want unmapped", test.err, got)
					}
				}
				if got != test.err { //nolint:errorlint // identity: unmapped errors are returned unchanged, not wrapped
					t.Fatalf("mapError(%v) = %v, want the original error", test.err, got)
				}
			case !errors.Is(got, test.want):
				t.Fatalf("mapError(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("New accepted a nil client")
	}
	client, _ := gcstest.NewServer(t)
	for _, options := range []Options{
		{ChunkSize: -1}, {BufferSize: -1}, {Concurrency: -1},
		{MultipartPrefix: "no-slash"}, {MultipartPrefix: "/leading/"}, {MultipartPrefix: "a/../"},
	} {
		if _, err := New(client, options); err == nil {
			t.Fatalf("New accepted %+v", options)
		}
	}
	store, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if store.options.ChunkSize != defaultChunkSize || store.options.BufferSize != defaultBufferSize ||
		store.options.Concurrency != defaultConcurrency || store.options.MultipartPrefix != DefaultMultipartPrefix || store.options.URLStyle == nil {
		t.Fatalf("defaults = %+v", store.options)
	}
}

func TestParseVersion(t *testing.T) {
	for version, want := range map[string]int64{"1": 1, "1700000000000000": 1700000000000000} {
		if got, ok := parseVersion(version); !ok || got != want {
			t.Fatalf("parseVersion(%q) = (%d, %v)", version, got, ok)
		}
	}
	for _, version := range []string{"", "0", "-1", "+1", "01", "1.0", "abc", "99999999999999999999"} {
		if _, ok := parseVersion(version); ok {
			t.Fatalf("parseVersion(%q) accepted a non-canonical generation", version)
		}
	}
}

// FR-OBJ-14
func TestVersionIsGeneration(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	first := mustPut(t, store, "objects", "generation", []byte("same"), objectstore.PutOptions{})
	second := mustPut(t, store, "objects", "generation", []byte("same"), objectstore.PutOptions{})
	attrs, err := store.client.Bucket("objects").Object("generation").Attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != fmt.Sprint(attrs.Generation) || second.Version == first.Version || second.ETag != attrs.Etag {
		t.Fatalf("versions = %q then %q, attrs generation %d etag %q", first.Version, second.Version, attrs.Generation, attrs.Etag)
	}
	// Identical bytes get a new generation, so a stale version cannot match.
	if _, err := store.Put(ctx, "objects", "generation", bytes.NewReader([]byte("x")), objectstore.PutOptions{IfMatchVersion: first.Version}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("stale version after identical rewrite error = %v", err)
	}
	if err := store.Delete(ctx, "objects", "generation", "not-a-generation"); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Delete non-numeric version error = %v", err)
	}
	if err := store.Delete(ctx, "objects", "missing", ""); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Delete absent key error = %v, want ErrNotFound", err)
	}
	if _, err := store.Put(ctx, "objects", "both", bytes.NewReader(nil), objectstore.PutOptions{IfAbsent: true, IfMatchVersion: second.Version}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("IfAbsent with IfMatchVersion error = %v", err)
	}
}

// FR-OBJ-14
func TestPutBodyPaths(t *testing.T) {
	store := newTestStore(t, Options{ChunkSize: 256 << 10, BufferSize: 300 << 10})
	ctx := context.Background()
	data := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB: resumable, beyond the buffer.
	digest := sha256.Sum256(data)
	for name, body := range map[objectstore.Key]func() io.Reader{
		"seekable":   func() io.Reader { return bytes.NewReader(data) },
		"unseekable": func() io.Reader { return io.MultiReader(bytes.NewReader(data)) },
		"offset": func() io.Reader {
			reader := bytes.NewReader(append([]byte("skip"), data...))
			_, _ = reader.Seek(4, io.SeekStart)
			return reader
		},
	} {
		object, err := store.Put(ctx, "objects", name, body(), objectstore.PutOptions{})
		if err != nil {
			t.Fatalf("Put %s: %v", name, err)
		}
		if object.SHA256 != digest || !object.SHA256Known || object.Size != int64(len(data)) {
			t.Fatalf("Put %s = %#v", name, object)
		}
		if got := mustRead(t, store, "objects", name); !bytes.Equal(got, data) {
			t.Fatalf("Put %s stored %d different bytes", name, len(got))
		}
	}
	empty := mustPut(t, store, "objects", "empty", nil, objectstore.PutOptions{})
	if empty.Size != 0 || empty.SHA256 != sha256.Sum256(nil) {
		t.Fatalf("empty object = %#v", empty)
	}
	if got := mustRead(t, store, "objects", "empty"); len(got) != 0 {
		t.Fatalf("empty object read %q", got)
	}
	// A resumable upload that fails verification after chunks were sent is
	// never finalized.
	wrong := sha256.Sum256([]byte("wrong"))
	if _, err := store.Put(ctx, "objects", "rejected", bytes.NewReader(data), objectstore.PutOptions{SHA256: &wrong}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("wrong digest error = %v", err)
	}
	if _, err := store.Put(ctx, "objects", "rejected", bytes.NewReader(data), objectstore.PutOptions{SHA256: &digest, Size: int64(len(data)) - 1}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("long body error = %v", err)
	}
	failing := io.MultiReader(bytes.NewReader(data), errorReader{errors.New("disk failed")})
	if _, err := store.Put(ctx, "objects", "rejected", failing, objectstore.PutOptions{SHA256: &digest}); err == nil || !strings.Contains(err.Error(), "disk failed") {
		t.Fatalf("failing body error = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancelling := io.MultiReader(bytes.NewReader(data), cancelReader{cancel: cancel})
	if _, err := store.Put(canceled, "objects", "rejected", cancelling, objectstore.PutOptions{SHA256: &digest}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled body error = %v", err)
	}
	if _, err := store.Stat(ctx, "objects", "rejected"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("rejected uploads published an object: %v", err)
	}
	if _, err := store.Put(ctx, "objects", "reserved", bytes.NewReader(nil), objectstore.PutOptions{Metadata: map[string]string{"Go-Object-Storage-SHA256": "x"}}); err == nil {
		t.Fatal("Put accepted reserved metadata")
	}
	if _, err := store.Put(ctx, "objects", "nil", nil, objectstore.PutOptions{}); err == nil {
		t.Fatal("Put accepted a nil body")
	}
}

func TestForeignObjectsHaveUnknownDigest(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	writer := store.client.Bucket("objects").Object("foreign").NewWriter(ctx)
	writer.Metadata = map[string]string{"tool": "other"}
	if _, err := writer.Write([]byte("foreign bytes")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	object, err := store.Stat(ctx, "objects", "foreign")
	if err != nil {
		t.Fatal(err)
	}
	if object.SHA256Known || object.SHA256 != [32]byte{} || object.Metadata["tool"] != "other" || object.Size != 13 {
		t.Fatalf("foreign object = %#v", object)
	}
	page, err := store.List(ctx, "objects", objectstore.ListOptions{})
	if err != nil || len(page.Objects) != 1 || !reflect.DeepEqual(page.Objects[0], object) {
		t.Fatalf("List = (%#v, %v), want %#v", page, err, object)
	}
}

func TestGetRanges(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	object := mustPut(t, store, "objects", "range", []byte("0123456789"), objectstore.PutOptions{})
	for _, test := range []struct {
		byteRange objectstore.ByteRange
		want      string
	}{
		{objectstore.ByteRange{Offset: 0, Length: 10}, "0123456789"},
		{objectstore.ByteRange{Offset: 9, Length: 1}, "9"},
		{objectstore.ByteRange{Offset: 3, Length: 2}, "34"},
	} {
		body, got, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{Range: &test.byteRange, IfMatchVersion: object.Version})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil || string(data) != test.want || !reflect.DeepEqual(got, object) {
			t.Fatalf("Get %+v = (%q, %#v, %v)", test.byteRange, data, got, err)
		}
	}
	for _, byteRange := range []objectstore.ByteRange{{Offset: -1, Length: 1}, {Offset: 0, Length: 0}, {Offset: 0, Length: 11}, {Offset: 10, Length: 1}, {Offset: 5, Length: 6}} {
		if _, _, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{Range: &byteRange}); !errors.Is(err, objectstore.ErrInvalidRange) {
			t.Fatalf("Get %+v error = %v, want ErrInvalidRange", byteRange, err)
		}
	}
	if _, _, err := store.Get(ctx, "objects", "range", objectstore.GetOptions{IfMatchVersion: "garbage"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Get non-numeric version error = %v", err)
	}
	if _, _, err := store.Get(ctx, "objects", "missing", objectstore.GetOptions{}); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Get missing error = %v", err)
	}
}

func TestListPaging(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	keys := []objectstore.Key{"a", "b/1", "b/2", "b/3", "c", "d", "e"}
	for _, key := range keys {
		mustPut(t, store, "objects", key, []byte(key), objectstore.PutOptions{Metadata: map[string]string{"key": string(key)}})
	}
	// Staging objects of an active multipart upload stay hidden.
	upload, err := store.BeginMultipart(ctx, "objects", "b/multipart", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustPart(t, store, upload, 1, []byte("part"))

	for _, test := range []struct {
		name    string
		options objectstore.ListOptions
		want    []objectstore.Key
		next    objectstore.Key
	}{
		{"unlimited", objectstore.ListOptions{}, keys, ""},
		{"exact limit", objectstore.ListOptions{Limit: len(keys)}, keys, ""},
		{"over limit", objectstore.ListOptions{Limit: 100}, keys, ""},
		{"first page", objectstore.ListOptions{Limit: 3}, keys[:3], "b/2"},
		{"middle page", objectstore.ListOptions{Limit: 3, After: "b/2"}, keys[3:6], "d"},
		{"last page", objectstore.ListOptions{Limit: 3, After: "d"}, keys[6:], ""},
		{"after between keys", objectstore.ListOptions{After: "b/15"}, keys[2:], ""},
		{"after past end", objectstore.ListOptions{After: "z"}, nil, ""},
		{"prefix", objectstore.ListOptions{Prefix: "b/", Limit: 2}, keys[1:3], "b/2"},
		{"prefix after", objectstore.ListOptions{Prefix: "b/", After: "b/2"}, keys[3:4], ""},
		{"after before prefix", objectstore.ListOptions{Prefix: "c", After: "a"}, keys[4:5], ""},
		{"no match", objectstore.ListOptions{Prefix: "x"}, nil, ""},
		{"reserved prefix", objectstore.ListOptions{Prefix: DefaultMultipartPrefix}, nil, ""},
		{"dot prefix", objectstore.ListOptions{Prefix: "."}, nil, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			page, err := store.List(ctx, "objects", test.options)
			if err != nil {
				t.Fatal(err)
			}
			if got := listedKeys(page); !slices.Equal(got, test.want) || page.NextAfter != test.next {
				t.Fatalf("List %+v = %v next %q, want %v next %q", test.options, got, page.NextAfter, test.want, test.next)
			}
			for _, object := range page.Objects {
				if object.Metadata["key"] != string(object.Key) || !object.SHA256Known || object.SHA256 != sha256.Sum256([]byte(object.Key)) {
					t.Fatalf("listed object without metadata: %#v", object)
				}
			}
		})
	}
	// Walking pages of every size returns every key exactly once.
	for limit := 1; limit <= len(keys)+1; limit++ {
		var walked []objectstore.Key
		var after objectstore.Key
		for {
			page, err := store.List(ctx, "objects", objectstore.ListOptions{Limit: limit, After: after})
			if err != nil {
				t.Fatal(err)
			}
			walked = append(walked, listedKeys(page)...)
			if page.NextAfter == "" {
				break
			}
			after = page.NextAfter
		}
		if !slices.Equal(walked, keys) {
			t.Fatalf("walk with limit %d = %v", limit, walked)
		}
	}
	if _, err := store.List(ctx, "objects", objectstore.ListOptions{Limit: -1}); !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("negative limit error = %v", err)
	}
	if _, err := store.List(ctx, "", objectstore.ListOptions{}); !errors.Is(err, objectstore.ErrInvalidBucket) {
		t.Fatalf("empty bucket error = %v", err)
	}
	if _, err := store.List(ctx, "missing-bucket", objectstore.ListOptions{}); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("missing bucket error = %v", err)
	}
}

func TestReservedPrefixRejected(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	reserved := objectstore.Key(DefaultMultipartPrefix + "0123456789abcdef0123456789abcdef/upload")
	mustPut(t, store, "objects", "source", []byte("value"), objectstore.PutOptions{})
	checks := map[string]error{}
	_, checks["Put"] = store.Put(ctx, "objects", reserved, bytes.NewReader(nil), objectstore.PutOptions{})
	_, _, checks["Get"] = store.Get(ctx, "objects", reserved, objectstore.GetOptions{})
	_, checks["Stat"] = store.Stat(ctx, "objects", reserved)
	checks["Delete"] = store.Delete(ctx, "objects", reserved, "")
	_, checks["BeginMultipart"] = store.BeginMultipart(ctx, "objects", reserved, objectstore.PutOptions{})
	_, checks["Copy to"] = store.Copy(ctx, "objects", "source", "objects", reserved, objectstore.CopyOptions{})
	_, checks["Copy from"] = store.Copy(ctx, "objects", reserved, "objects", "destination", objectstore.CopyOptions{})
	_, checks["PresignGet"] = store.PresignGet(ctx, "objects", reserved, time.Minute, nil)
	_, checks["PresignPut"] = store.PresignPut(ctx, "objects", reserved, time.Minute, objectstore.PutOptions{})
	results, err := store.StatMany(ctx, "objects", []objectstore.Key{reserved})
	if err != nil {
		t.Fatal(err)
	}
	checks["StatMany"] = results[0].Err
	errs, err := store.DeleteMany(ctx, "objects", []objectstore.Key{reserved})
	if err != nil {
		t.Fatal(err)
	}
	checks["DeleteMany"] = errs[0]
	for name, err := range checks {
		if !errors.Is(err, objectstore.ErrInvalidKey) {
			t.Fatalf("%s on reserved key error = %v, want ErrInvalidKey", name, err)
		}
	}

	custom := newTestStore(t, Options{MultipartPrefix: "_staging/"})
	if _, err := custom.Put(ctx, "objects", "_staging/x", bytes.NewReader(nil), objectstore.PutOptions{}); !errors.Is(err, objectstore.ErrInvalidKey) {
		t.Fatalf("custom prefix Put error = %v", err)
	}
	mustPut(t, custom, "objects", objectstore.Key(DefaultMultipartPrefix+"x"), []byte("allowed"), objectstore.PutOptions{})
	upload, err := custom.BeginMultipart(ctx, "objects", "custom", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := custom.client.Bucket("objects").Object("_staging/" + upload.ID + "/upload").Attrs(ctx); err != nil {
		t.Fatalf("custom prefix marker: %v", err)
	}
}

// FR-OBJ-06, FR-OBJ-14
func TestMultipartHierarchicalCompose(t *testing.T) {
	for _, test := range []struct {
		parts, fanIn int
	}{
		{1, 32}, {32, 32}, {33, 32}, {70, 32}, // one and two compose levels at the GCS limit
		{30, 3}, {28, 3}, // four levels (30 -> 10 -> 4 -> 2) and an uneven tree
	} {
		count := test.parts
		t.Run(fmt.Sprintf("%d parts fan-in %d", test.parts, test.fanIn), func(t *testing.T) {
			store := newTestStore(t, Options{Concurrency: 8})
			store.composeFanIn = test.fanIn
			ctx := context.Background()
			var full []byte
			parts := make([][]byte, count)
			for index := range parts {
				parts[index] = []byte(fmt.Sprintf("<%d>", index+1))
				full = append(full, parts[index]...)
			}
			digest := sha256.Sum256(full)
			upload, err := store.BeginMultipart(ctx, "objects", "many", objectstore.PutOptions{SHA256: &digest, ContentType: "text/plain", IfAbsent: true})
			if err != nil {
				t.Fatal(err)
			}
			completed := make([]objectstore.CompletedPart, count)
			err = runParallel(ctx, count, 8, func(ctx context.Context, index int) error {
				part, err := store.PutPart(ctx, upload, index+1, bytes.NewReader(parts[index]), int64(len(parts[index])), sha256.Sum256(parts[index]))
				completed[index] = part
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			object, err := store.CompleteMultipart(ctx, upload, completed)
			if err != nil {
				t.Fatal(err)
			}
			if object.Size != int64(len(full)) || object.SHA256 != digest || !object.SHA256Known || object.Metadata != nil {
				t.Fatalf("composed object = %#v", object)
			}
			if got := mustRead(t, store, "objects", "many"); !bytes.Equal(got, full) {
				t.Fatalf("composed %d bytes, want %d", len(got), len(full))
			}
			stat, err := store.Stat(ctx, "objects", "many")
			if err != nil || !reflect.DeepEqual(stat, object) {
				t.Fatalf("Stat = (%#v, %v), want %#v", stat, err, object)
			}
			if staged := rawNames(t, store, DefaultMultipartPrefix); len(staged) != 0 {
				t.Fatalf("staging objects left behind: %v", staged)
			}
			// A retried completion returns the published object.
			again, err := store.CompleteMultipart(ctx, upload, completed)
			if err != nil || !reflect.DeepEqual(again, object) {
				t.Fatalf("retried CompleteMultipart = (%#v, %v), want %#v", again, err, object)
			}
			if err := store.AbortMultipart(ctx, upload); !errors.Is(err, objectstore.ErrNotFound) {
				t.Fatalf("Abort after completion error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestMultipartVerification(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "objects", "verified", objectstore.PutOptions{Size: 8, Metadata: map[string]string{"m": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	first := mustPart(t, store, upload, 1, []byte("abcd"))
	second := mustPart(t, store, upload, 2, []byte("efgh"))
	digest := sha256.Sum256([]byte("part"))
	for name, body := range map[string]struct {
		data []byte
		size int64
	}{"short": {[]byte("par"), 4}, "long": {[]byte("parts"), 4}, "wrong digest": {[]byte("PART"), 4}, "empty with data": {[]byte("x"), 0}} {
		if _, err := store.PutPart(ctx, upload, 3, bytes.NewReader(body.data), body.size, digest); !errors.Is(err, objectstore.ErrInvalidPart) {
			t.Fatalf("PutPart %s error = %v, want ErrInvalidPart", name, err)
		}
	}
	if _, err := store.client.Bucket("objects").Object(store.partKey(upload.ID, 3)).Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("rejected part was published: %v", err)
	}
	for _, number := range []int{0, -1, maximumPartCount + 1} {
		if _, err := store.PutPart(ctx, upload, number, bytes.NewReader(nil), 0, sha256.Sum256(nil)); !errors.Is(err, objectstore.ErrInvalidPart) {
			t.Fatalf("PutPart number %d error = %v", number, err)
		}
	}
	for name, parts := range map[string][]objectstore.CompletedPart{
		"empty":        nil,
		"gap":          {first, withNumber(second, 3)},
		"wrong etag":   {first, withETag(second, "123")},
		"wrong digest": {first, withDigest(second, digest)},
		"wrong size":   {withSize(first, 5), withSize(second, 3)},
		"missing part": {first, second, {Number: 3, ETag: "1", Size: 0, SHA256: sha256.Sum256(nil)}},
	} {
		if _, err := store.CompleteMultipart(ctx, upload, parts); !errors.Is(err, objectstore.ErrInvalidPart) {
			t.Fatalf("CompleteMultipart %s error = %v, want ErrInvalidPart", name, err)
		}
	}
	if _, err := store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{first}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("CompleteMultipart size mismatch error = %v, want ErrPrecondition", err)
	}
	for name, mutated := range map[string]objectstore.Upload{
		"bad id":      {ID: "not-hex", Bucket: upload.Bucket, Key: upload.Key},
		"upper id":    {ID: strings.ToUpper(upload.ID), Bucket: upload.Bucket, Key: upload.Key},
		"other key":   {ID: upload.ID, Bucket: upload.Bucket, Key: "other"},
		"unknown id":  {ID: strings.Repeat("0", 32), Bucket: upload.Bucket, Key: upload.Key},
		"other store": {ID: upload.ID, Bucket: "objects-copy", Key: upload.Key},
	} {
		_, err := store.CompleteMultipart(ctx, mutated, []objectstore.CompletedPart{first, second})
		want := objectstore.ErrInvalidPart
		if name == "unknown id" || name == "other store" {
			want = objectstore.ErrNotFound
		}
		if !errors.Is(err, want) {
			t.Fatalf("CompleteMultipart with %s error = %v, want %v", name, err, want)
		}
	}
	// The marker, not the caller's Upload.Options, is authoritative.
	tampered := upload.Clone()
	tampered.Options.Metadata = map[string]string{"m": "tampered"}
	object, err := store.CompleteMultipart(ctx, tampered, []objectstore.CompletedPart{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if object.SHA256Known || object.Metadata["m"] != "v" || object.Size != 8 {
		t.Fatalf("completed object without declared digest = %#v", object)
	}
	if got := mustRead(t, store, "objects", "verified"); string(got) != "abcdefgh" {
		t.Fatalf("completed bytes = %q", got)
	}
}

func TestMultipartConditions(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	existing := mustPut(t, store, "objects", "conditional", []byte("old"), objectstore.PutOptions{})
	complete := func(options objectstore.PutOptions) (objectstore.Object, error) {
		upload, err := store.BeginMultipart(ctx, "objects", "conditional", options)
		if err != nil {
			t.Fatal(err)
		}
		part := mustPart(t, store, upload, 1, []byte("new"))
		return store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{part})
	}
	if _, err := complete(objectstore.PutOptions{IfAbsent: true}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("IfAbsent completion error = %v", err)
	}
	if _, err := complete(objectstore.PutOptions{IfMatchVersion: "garbage"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("garbage IfMatchVersion completion error = %v", err)
	}
	replaced, err := complete(objectstore.PutOptions{IfMatchVersion: existing.Version})
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Version == existing.Version {
		t.Fatalf("replacement kept version %q", replaced.Version)
	}
	if _, err := complete(objectstore.PutOptions{IfMatchVersion: existing.Version}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("stale IfMatchVersion completion error = %v", err)
	}
	if got := mustRead(t, store, "objects", "conditional"); string(got) != "new" {
		t.Fatalf("object after conditional completions = %q", got)
	}
}

func TestCopyOverrides(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	source := mustPut(t, store, "objects", "source", []byte("bytes"), objectstore.PutOptions{ContentType: "text/plain", Metadata: map[string]string{"a": "1"}})
	typed, err := store.Copy(ctx, "objects", "source", "objects", "typed", objectstore.CopyOptions{ContentType: "application/json"})
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := store.client.Bucket("objects").Object("typed").Attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if attrs.ContentType != "application/json" || typed.Metadata["a"] != "1" || typed.SHA256 != source.SHA256 || !typed.SHA256Known {
		t.Fatalf("content type copy = %#v (%s)", typed, attrs.ContentType)
	}
	cleared, err := store.Copy(ctx, "objects", "source", "objects", "cleared", objectstore.CopyOptions{Metadata: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Metadata != nil || cleared.SHA256 != source.SHA256 || !cleared.SHA256Known {
		t.Fatalf("metadata-clearing copy = %#v", cleared)
	}
	if _, err := store.Copy(ctx, "objects", "source", "objects", "bad", objectstore.CopyOptions{Metadata: map[string]string{"go-object-storage-sha256": "00"}}); err == nil {
		t.Fatal("Copy accepted reserved metadata")
	}
	if _, err := store.Copy(ctx, "objects", "source", "objects", "bad", objectstore.CopyOptions{SourceVersion: "garbage"}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("garbage SourceVersion error = %v", err)
	}
}

// FR-OBJ-07, FR-OBJ-14
func TestPresignShape(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	digest := sha256.Sum256([]byte("payload"))
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		put, err := store.PresignPut(ctx, "objects", "dir/a b.txt", 15*time.Minute, objectstore.PutOptions{
			ContentType: "text/plain", Metadata: map[string]string{"owner": "me"}, IfMatchVersion: "42", SHA256: &digest, Size: 7,
		})
		if err != nil {
			t.Fatal(err)
		}
		if put.Method != http.MethodPut || !put.ExpiresAt.Equal(start.Add(15*time.Minute)) {
			t.Fatalf("PresignPut = %#v", put)
		}
		parsed, err := url.Parse(put.URL)
		if err != nil {
			t.Fatal(err)
		}
		query := parsed.Query()
		if parsed.Scheme != "http" || parsed.EscapedPath() != "/objects/dir/a%20b.txt" ||
			query.Get("X-Goog-Algorithm") != "GOOG4-RSA-SHA256" || query.Get("X-Goog-Expires") != "900" ||
			query.Get("X-Goog-Date") != start.UTC().Format("20060102T150405Z") ||
			!strings.HasPrefix(query.Get("X-Goog-Credential"), testAccessID+"/") || query.Get("X-Goog-Signature") == "" {
			t.Fatalf("PresignPut URL = %s", put.URL)
		}
		wantSigned := "content-type;host;x-goog-content-length-range;x-goog-if-generation-match;x-goog-meta-go-object-storage-sha256;x-goog-meta-owner"
		if got := query.Get("X-Goog-SignedHeaders"); got != wantSigned {
			t.Fatalf("signed headers = %q, want %q", got, wantSigned)
		}
		wantHeader := http.Header{
			"Content-Type":                         {"text/plain"},
			"X-Goog-Meta-Owner":                    {"me"},
			"X-Goog-Meta-Go-Object-Storage-Sha256": {fmt.Sprintf("%x", digest)},
			"X-Goog-If-Generation-Match":           {"42"},
			"X-Goog-Content-Length-Range":          {"7,7"},
		}
		if !reflect.DeepEqual(put.Header, wantHeader) {
			t.Fatalf("PresignPut header = %#v, want %#v", put.Header, wantHeader)
		}

		absent, err := store.PresignPut(ctx, "objects", "absent", time.Minute, objectstore.PutOptions{IfAbsent: true})
		if err != nil || absent.Header.Get("X-Goog-If-Generation-Match") != "0" {
			t.Fatalf("PresignPut IfAbsent = (%#v, %v)", absent, err)
		}

		get, err := store.PresignGet(ctx, "objects", "dir/a b.txt", time.Hour, &objectstore.ByteRange{Offset: 4, Length: 6})
		if err != nil {
			t.Fatal(err)
		}
		parsed, _ = url.Parse(get.URL)
		if get.Method != http.MethodGet || get.Header.Get("Range") != "bytes=4-9" || parsed.Query().Get("X-Goog-SignedHeaders") != "host;range" ||
			parsed.Query().Get("X-Goog-Expires") != "3600" {
			t.Fatalf("PresignGet = %#v", get)
		}

		upload := objectstore.Upload{ID: strings.Repeat("ab", 16), Bucket: "objects", Key: "multipart"}
		part, err := store.PresignUploadPart(ctx, upload, 7, 7, digest, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		parsed, _ = url.Parse(part.URL)
		if parsed.Path != "/objects/"+DefaultMultipartPrefix+upload.ID+"/00007" || part.Header.Get("X-Goog-Content-Length-Range") != "7,7" ||
			part.Header.Get("X-Goog-Meta-Go-Object-Storage-Sha256") != fmt.Sprintf("%x", digest) {
			t.Fatalf("PresignUploadPart = %#v", part)
		}
	})

	for name, call := range map[string]func() error{
		"zero expiry": func() error { _, err := store.PresignGet(ctx, "objects", "k", 0, nil); return err },
		"long expiry": func() error { _, err := store.PresignGet(ctx, "objects", "k", 8*24*time.Hour, nil); return err },
		"bad range": func() error {
			_, err := store.PresignGet(ctx, "objects", "k", time.Minute, &objectstore.ByteRange{Length: 0})
			return err
		},
		"bad version": func() error {
			_, err := store.PresignPut(ctx, "objects", "k", time.Minute, objectstore.PutOptions{IfMatchVersion: "v"})
			return err
		},
		"header-unsafe metadata": func() error {
			_, err := store.PresignPut(ctx, "objects", "k", time.Minute, objectstore.PutOptions{Metadata: map[string]string{"a b": "c"}})
			return err
		},
		"bad part": func() error {
			_, err := store.PresignUploadPart(ctx, objectstore.Upload{ID: strings.Repeat("ab", 16), Bucket: "objects", Key: "k"}, 0, 1, digest, time.Minute)
			return err
		},
	} {
		if call() == nil {
			t.Fatalf("presign with %s succeeded", name)
		}
	}

	signErr := errors.New("signer unavailable")
	failing, err := New(store.client, Options{GoogleAccessID: testAccessID, SignBytes: func([]byte) ([]byte, error) { return nil, signErr }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.PresignGet(ctx, "objects", "k", time.Minute, nil); !errors.Is(err, signErr) {
		t.Fatalf("presign with a failing signer error = %v", err)
	}
	virtual := newTestStore(t, Options{URLStyle: storage.VirtualHostedStyle(), Hostname: "storage.example.com"})
	request, err := virtual.PresignGet(ctx, "objects", "k", time.Minute, nil)
	if err != nil || !strings.HasPrefix(request.URL, "http://objects.storage.example.com/k?") {
		t.Fatalf("virtual-hosted PresignGet = (%#v, %v)", request, err)
	}
}

// FR-OBJ-07, FR-OBJ-14
func TestBatchOperations(t *testing.T) {
	store := newTestStore(t, Options{Concurrency: 3})
	ctx := context.Background()
	var keys []objectstore.Key
	for index := range 20 {
		key := objectstore.Key(fmt.Sprintf("batch/%02d", index))
		keys = append(keys, key)
		if index%2 == 0 {
			mustPut(t, store, "objects", key, []byte(key), objectstore.PutOptions{})
		}
	}
	keys = append(keys, "")
	results, err := store.StatMany(ctx, "objects", keys)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range results[:20] {
		if (index%2 == 0) != (result.Err == nil) || (index%2 == 1 && !errors.Is(result.Err, objectstore.ErrNotFound)) {
			t.Fatalf("StatMany[%d] = %#v", index, result)
		}
	}
	if !errors.Is(results[20].Err, objectstore.ErrInvalidKey) {
		t.Fatalf("StatMany invalid key = %#v", results[20])
	}
	errs, err := store.DeleteMany(ctx, "objects", keys)
	if err != nil {
		t.Fatal(err)
	}
	for index, err := range errs[:20] {
		if err != nil {
			t.Fatalf("DeleteMany[%d] = %v", index, err)
		}
	}
	if !errors.Is(errs[20], objectstore.ErrInvalidKey) {
		t.Fatalf("DeleteMany invalid key = %v", errs[20])
	}
	if names := rawNames(t, store, "batch/"); len(names) != 0 {
		t.Fatalf("objects left after DeleteMany: %v", names)
	}
	if _, err := store.StatMany(ctx, "", keys); !errors.Is(err, objectstore.ErrInvalidBucket) {
		t.Fatalf("StatMany empty bucket error = %v", err)
	}
	if _, err := store.DeleteMany(ctx, "", keys); !errors.Is(err, objectstore.ErrInvalidBucket) {
		t.Fatalf("DeleteMany empty bucket error = %v", err)
	}
}

// TestConcurrentGetDuringReplacement reads an object while another goroutine
// keeps replacing it. Get pins its reader to the generation whose metadata it
// returns, so bytes and metadata always belong to the same generation. (Real
// GCS limits writes to one object to about one per second; the writer pauses
// briefly so that Get's bounded retries are not starved.)
func TestConcurrentGetDuringReplacement(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	mustPut(t, store, "objects", "hot", []byte("value-0"), objectstore.PutOptions{})
	stop := make(chan struct{})
	writerErr := make(chan error, 1)
	go func() {
		for index := 1; ; index++ {
			select {
			case <-stop:
				writerErr <- nil
				return
			case <-time.After(time.Millisecond):
			}
			if _, err := store.Put(ctx, "objects", "hot", bytes.NewReader([]byte(fmt.Sprintf("value-%d", index))), objectstore.PutOptions{}); err != nil {
				writerErr <- err
				return
			}
		}
	}()
	var failure error
	for range 50 {
		body, object, err := store.Get(ctx, "objects", "hot", objectstore.GetOptions{})
		if err != nil {
			failure = err
			break
		}
		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil || sha256.Sum256(data) != object.SHA256 || int64(len(data)) != object.Size {
			failure = fmt.Errorf("get returned bytes %q that do not match %#v (%w)", data, object, err)
			break
		}
	}
	close(stop)
	if err := errors.Join(failure, <-writerErr); err != nil {
		t.Fatal(err)
	}
}

type errorReader struct{ err error }

func (reader errorReader) Read([]byte) (int, error) { return 0, reader.err }

type cancelReader struct{ cancel context.CancelFunc }

func (reader cancelReader) Read([]byte) (int, error) {
	reader.cancel()
	return 0, context.Canceled
}

func mustPut(t testing.TB, store *Store, bucket objectstore.Bucket, key objectstore.Key, data []byte, options objectstore.PutOptions) objectstore.Object {
	t.Helper()
	object, err := store.Put(context.Background(), bucket, key, bytes.NewReader(data), options)
	if err != nil {
		t.Fatal(err)
	}
	return object
}

func mustPart(t testing.TB, store *Store, upload objectstore.Upload, number int, data []byte) objectstore.CompletedPart {
	t.Helper()
	part, err := store.PutPart(context.Background(), upload, number, bytes.NewReader(data), int64(len(data)), sha256.Sum256(data))
	if err != nil {
		t.Fatal(err)
	}
	return part
}

func mustRead(t testing.TB, store *Store, bucket objectstore.Bucket, key objectstore.Key) []byte {
	t.Helper()
	body, _, err := store.Get(context.Background(), bucket, key, objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// rawNames lists object names in the "objects" bucket, including reserved
// staging objects.
func rawNames(t testing.TB, store *Store, prefix string) []string {
	t.Helper()
	iter := store.client.Bucket("objects").Objects(context.Background(), &storage.Query{Prefix: prefix})
	var names []string
	for {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, attrs.Name)
	}
}

func listedKeys(page objectstore.Page) []objectstore.Key {
	var keys []objectstore.Key
	for _, object := range page.Objects {
		keys = append(keys, object.Key)
	}
	return keys
}

func withNumber(part objectstore.CompletedPart, number int) objectstore.CompletedPart {
	part.Number = number
	return part
}

func withETag(part objectstore.CompletedPart, etag string) objectstore.CompletedPart {
	part.ETag = etag
	return part
}

func withDigest(part objectstore.CompletedPart, digest [32]byte) objectstore.CompletedPart {
	part.SHA256 = digest
	return part
}

func withSize(part objectstore.CompletedPart, size int64) objectstore.CompletedPart {
	part.Size = size
	return part
}
