package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	objectstore "github.com/justusschock/go-object-storage"
)

const testPartSize = 5 << 20

type onlyReader struct{ reader io.Reader }

func (reader onlyReader) Read(buffer []byte) (int, error) { return reader.reader.Read(buffer) }

func patterned(size int) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = byte(index*31 + index/1024)
	}
	return data
}

func countRequests(requests []recordedRequest, method, query string) int {
	count := 0
	for _, request := range requests {
		if request.is(method, query) {
			count++
		}
	}
	return count
}

func TestPutSignsSmallBodiesWithTheirDigest(t *testing.T) {
	fake := newFakeS3(t)
	store := fake.store(t, Options{})
	data := []byte("small body")
	digest := sha256.Sum256(data)
	if _, err := store.Put(context.Background(), "objects", "small", onlyReader{bytes.NewReader(data)}, objectstore.PutOptions{IfAbsent: true}); err != nil {
		t.Fatal(err)
	}
	var put *recordedRequest
	for _, request := range fake.handler.recorded() {
		if request.Method == "PUT" {
			put = &request
		}
	}
	if put == nil || put.Header.Get("X-Amz-Content-Sha256") != hex.EncodeToString(digest[:]) ||
		put.Header.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(digest[:]) || put.Header.Get("If-None-Match") != "*" ||
		put.Header.Get("X-Amz-Meta-Go-Object-Storage-Sha256") != hex.EncodeToString(digest[:]) {
		t.Fatalf("PutObject request = %#v", put)
	}
}

func TestPutStreamingPathsAvoidSpoolingWhenPossible(t *testing.T) {
	fake := newFakeS3(t)
	// An unusable spool directory proves which paths spool.
	store := fake.store(t, Options{PartSize: testPartSize, Concurrency: 2, SpoolDirectory: filepath.Join(t.TempDir(), "missing")})
	data := patterned(3*testPartSize + 17)
	digest := sha256.Sum256(data)
	ctx := context.Background()

	if _, err := store.Put(ctx, "objects", "known", onlyReader{bytes.NewReader(data)}, objectstore.PutOptions{SHA256: &digest}); err != nil {
		t.Fatalf("known-digest stream spooled or failed: %v", err)
	}
	if _, err := store.Put(ctx, "objects", "seekable", bytes.NewReader(data), objectstore.PutOptions{}); err != nil {
		t.Fatalf("seekable unknown-digest body spooled or failed: %v", err)
	}
	if _, err := store.Put(ctx, "objects", "spooled", onlyReader{bytes.NewReader(data)}, objectstore.PutOptions{}); err == nil {
		t.Fatal("unseekable unknown-digest body did not use the spool directory")
	}
	for _, key := range []objectstore.Key{"known", "seekable"} {
		object, err := store.Stat(ctx, "objects", key)
		if err != nil || !object.SHA256Known || object.SHA256 != digest || object.Size != int64(len(data)) {
			t.Fatalf("Stat %s = (%#v, %v)", key, object, err)
		}
	}
	requests := fake.handler.recorded()
	if parts := countRequests(requests, "PUT", "partNumber"); parts != 8 {
		t.Fatalf("uploaded %d parts, want 4 per streamed object", parts)
	}
	if gets := countRequests(requests, "GET", ""); gets != 0 {
		t.Fatalf("streaming put downloaded data with %d GET requests", gets)
	}
}

func TestPutStreamingDigestMismatchAbortsWithoutPublishing(t *testing.T) {
	fake := newFakeS3(t)
	store := fake.store(t, Options{PartSize: testPartSize})
	data := patterned(2*testPartSize + 1)
	wrong := sha256.Sum256([]byte("wrong"))
	if _, err := store.Put(context.Background(), "objects", "mismatch", onlyReader{bytes.NewReader(data)}, objectstore.PutOptions{SHA256: &wrong}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("Put error = %v, want ErrPrecondition", err)
	}
	requests := fake.handler.recorded()
	if countRequests(requests, "DELETE", "uploadId") != 1 || countRequests(requests, "POST", "uploadId") != 0 {
		t.Fatalf("mismatch did not abort exactly once without completing: %+v", requests)
	}
	if _, err := store.Stat(context.Background(), "objects", "mismatch"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("mismatch published object: %v", err)
	}
}

func TestForeignObjectsReportUnknownDigestWithoutDownloading(t *testing.T) {
	fake := newFakeS3(t)
	store := fake.store(t, Options{})
	ctx := context.Background()
	data := []byte("written by another tool")
	if _, err := fake.client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String("objects"), Key: aws.String("foreign"), Body: bytes.NewReader(data), Metadata: map[string]string{"owner": "other"}}); err != nil {
		t.Fatal(err)
	}
	fake.handler.reset()
	object, err := store.Stat(ctx, "objects", "foreign")
	if err != nil || object.SHA256Known || object.SHA256 != ([32]byte{}) || object.Size != int64(len(data)) || object.Metadata["owner"] != "other" {
		t.Fatalf("Stat foreign = (%#v, %v)", object, err)
	}
	body, ranged, err := store.Get(ctx, "objects", "foreign", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 11, Length: 7}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(got) != "another" || ranged.SHA256Known || ranged.Size != int64(len(data)) {
		t.Fatalf("ranged Get = (%q, %#v, %v)", got, ranged, err)
	}
	requests := fake.handler.recorded()
	if len(requests) != 2 || requests[1].Method != "GET" || requests[1].Header.Get("Range") != "bytes=11-17" {
		t.Fatalf("requests = %+v, want one HEAD and one ranged GET", requests)
	}
}

func TestListUsesListingMetadataWithoutHeadRequests(t *testing.T) {
	fake := newFakeS3(t)
	store := fake.store(t, Options{})
	ctx := context.Background()
	for _, key := range []objectstore.Key{"list/a", "list/b", "list/c"} {
		if _, err := store.Put(ctx, "objects", key, strings.NewReader(string(key)), objectstore.PutOptions{Metadata: map[string]string{"k": string(key)}}); err != nil {
			t.Fatal(err)
		}
	}
	fake.handler.reset()
	page, err := store.List(ctx, "objects", objectstore.ListOptions{Prefix: "list/", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 2 || page.NextAfter != "list/b" || page.Objects[0].Size != 6 || page.Objects[0].ETag == "" || page.Objects[0].SHA256Known || page.Objects[0].Metadata != nil {
		t.Fatalf("page = %#v", page)
	}
	if heads := countRequests(fake.handler.recorded(), "HEAD", ""); heads != 0 {
		t.Fatalf("List issued %d HEAD requests", heads)
	}
	fake.handler.reset()
	page, err = store.List(ctx, "objects", objectstore.ListOptions{Prefix: "list/", IncludeMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 3 || !page.Objects[2].SHA256Known || page.Objects[2].Metadata["k"] != "list/c" {
		t.Fatalf("detailed page = %#v", page)
	}
	if heads := countRequests(fake.handler.recorded(), "HEAD", ""); heads != 3 {
		t.Fatalf("detailed List issued %d HEAD requests, want 3", heads)
	}
}

func TestMultipartCompletesInPlaceAndIdempotently(t *testing.T) {
	fake := newFakeS3(t)
	store := fake.store(t, Options{})
	ctx := context.Background()
	first, second := patterned(testPartSize), []byte("tail")
	whole := sha256.Sum256(append(bytes.Clone(first), second...))
	upload, err := store.BeginMultipart(ctx, "objects", "assembled", objectstore.PutOptions{SHA256: &whole, IfAbsent: true})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]objectstore.CompletedPart, 2)
	for index, data := range [][]byte{first, second} {
		// Unseekable bodies stream without buffering.
		parts[index], err = store.PutPart(ctx, upload, index+1, onlyReader{bytes.NewReader(data)}, int64(len(data)), sha256.Sum256(data))
		if err != nil {
			t.Fatal(err)
		}
	}
	object, err := store.CompleteMultipart(ctx, upload, parts)
	if err != nil {
		t.Fatal(err)
	}
	if object.SHA256 != whole || !object.SHA256Known || object.Size != int64(len(first)+len(second)) {
		t.Fatalf("completed object = %#v", object)
	}
	for _, request := range fake.handler.recorded() {
		if request.Path != "/objects/assembled" && request.Method != "GET" {
			t.Fatalf("multipart touched a staging key: %s %s", request.Method, request.Path)
		}
		if request.Method == "GET" {
			t.Fatalf("multipart completion downloaded data: %s %s", request.Method, request.Path)
		}
		if request.Method == "POST" && request.Query.Has("uploadId") && request.Header.Get("If-None-Match") != "*" {
			t.Fatalf("completion lacks If-None-Match: %+v", request.Header)
		}
	}
	again, err := store.CompleteMultipart(ctx, upload, parts)
	if err != nil || again.Version != object.Version {
		t.Fatalf("repeated completion = (%#v, %v), want the published object", again, err)
	}
}

func TestMultipartWithoutDeclaredDigestHasUnknownDigest(t *testing.T) {
	store := newFakeS3(t).store(t, Options{})
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "objects", "undeclared", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("part")
	part, err := store.PutPart(ctx, upload, 1, bytes.NewReader(data), 4, sha256.Sum256(data))
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{part})
	if err != nil || object.SHA256Known || object.Size != 4 {
		t.Fatalf("completed = (%#v, %v), want unknown digest", object, err)
	}
}

func TestPutPartRejectsWrongBodies(t *testing.T) {
	store := newFakeS3(t).store(t, Options{})
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "objects", "parts", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("part"))
	for name, body := range map[string]io.Reader{
		"wrong bytes": strings.NewReader("pArt"),
		"short body":  onlyReader{strings.NewReader("par")},
		"long body":   strings.NewReader("parts"),
	} {
		if _, err := store.PutPart(ctx, upload, 1, body, 4, digest); !errors.Is(err, objectstore.ErrInvalidPart) {
			t.Fatalf("%s: PutPart error = %v, want ErrInvalidPart", name, err)
		}
	}
	if err := store.AbortMultipart(ctx, upload); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsLegacyStagingCustody(t *testing.T) {
	store := newFakeS3(t).store(t, Options{})
	legacy, err := json.Marshal(map[string]any{"version": 1, "bucket": "objects", "key": "k", "staging_key": legacyStagingPrefix + "00", "upload_id": "id", "options": objectstore.PutOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	upload := objectstore.Upload{ID: base64.RawURLEncoding.EncodeToString(legacy), Bucket: "objects", Key: "k"}
	if _, err := store.CompleteMultipart(context.Background(), upload, nil); !errors.Is(err, objectstore.ErrInvalidPart) {
		t.Fatalf("legacy custody error = %v, want ErrInvalidPart", err)
	}
}

func TestMultipartCopyForLargeSources(t *testing.T) {
	fake := newFakeS3(t)
	store := fake.store(t, Options{CopyPartSize: testPartSize, MultipartCopyThreshold: testPartSize, Concurrency: 2})
	ctx := context.Background()
	data := patterned(2*testPartSize + 9)
	digest := sha256.Sum256(data)
	source, err := store.Put(ctx, "objects", "large", bytes.NewReader(data), objectstore.PutOptions{SHA256: &digest, Metadata: map[string]string{"origin": "large"}})
	if err != nil {
		t.Fatal(err)
	}
	fake.handler.reset()
	copied, err := store.Copy(ctx, "objects", "large", "objects-copy", "large-copy", objectstore.CopyOptions{SourceVersion: source.Version, IfAbsent: true})
	if err != nil {
		t.Fatal(err)
	}
	if !copied.SHA256Known || copied.SHA256 != digest || copied.Size != int64(len(data)) || copied.Metadata["origin"] != "large" {
		t.Fatalf("copied = %#v", copied)
	}
	requests := fake.handler.recorded()
	partCopies := 0
	for _, request := range requests {
		if request.Method == "PUT" && request.Query.Has("partNumber") && request.Header.Get("X-Amz-Copy-Source-Range") != "" {
			partCopies++
			if request.Header.Get("X-Amz-Copy-Source-If-Match") != source.Version {
				t.Fatalf("part copy not pinned to source version: %+v", request.Header)
			}
		}
	}
	if partCopies != 3 {
		t.Fatalf("issued %d UploadPartCopy requests, want 3", partCopies)
	}
	body, _, err := store.Get(ctx, "objects-copy", "large-copy", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || sha256.Sum256(got) != digest {
		t.Fatalf("copied bytes differ: %v", err)
	}
	if _, err := store.Copy(ctx, "objects", "large", "objects-copy", "large-copy", objectstore.CopyOptions{IfAbsent: true}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("multipart copy IfAbsent error = %v, want ErrPrecondition", err)
	}
}

func TestCopySourceEscaping(t *testing.T) {
	for key, want := range map[objectstore.Key]string{
		"plain/key":    "bucket/plain/key",
		"a+b c/ü?x=1#": "bucket/a%2Bb%20c/%C3%BC%3Fx=1%23",
	} {
		if got := copySource("bucket", key); got != want {
			t.Fatalf("copySource(%q) = %q, want %q", key, got, want)
		}
	}
	store := newFakeS3(t).store(t, Options{})
	ctx := context.Background()
	if _, err := store.Put(ctx, "objects", "odd key+name", strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Copy(ctx, "objects", "odd key+name", "objects", "copied", objectstore.CopyOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestPresignRequiresSDKClient(t *testing.T) {
	store := &Store{client: &stubClient{}, options: Options{}}
	if _, err := store.PresignGet(context.Background(), "objects", "key", 0, nil); err == nil {
		t.Fatal("PresignGet accepted a zero expiry or missing presigner")
	}
}

func TestDeleteManyMapsPerKeyErrors(t *testing.T) {
	client := &stubClient{deleteErrors: []types.Error{
		{Key: aws.String("denied"), Code: aws.String("AccessDenied"), Message: aws.String("no")},
		{Key: aws.String("gone"), Code: aws.String("NoSuchKey")},
	}}
	store := &Store{client: client, options: Options{}}
	keys := []objectstore.Key{"ok", "denied", "gone", "denied", "../bad"}
	errs, err := store.DeleteMany(context.Background(), "objects", keys)
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != nil || errs[1] == nil || errs[2] != nil || errs[3] == nil || !errors.Is(errs[4], objectstore.ErrInvalidKey) {
		t.Fatalf("DeleteMany errors = %v", errs)
	}
	if len(client.deleteInputs) != 1 || len(client.deleteInputs[0].Delete.Objects) != 3 {
		t.Fatalf("DeleteObjects inputs = %#v, want one batch of 3 unique valid keys", client.deleteInputs)
	}
}

func TestMapsS3Errors(t *testing.T) {
	for code, want := range map[string]error{
		"NoSuchKey": objectstore.ErrNotFound, "NoSuchUpload": objectstore.ErrNotFound,
		"PreconditionFailed": objectstore.ErrPrecondition, "XAmzContentSHA256Mismatch": objectstore.ErrPrecondition,
		"InvalidRange": objectstore.ErrInvalidRange, "InvalidPart": objectstore.ErrInvalidPart, "InvalidPartOrder": objectstore.ErrInvalidPart,
	} {
		if got := mapError(&smithy.GenericAPIError{Code: code, Message: "failed"}); !errors.Is(got, want) {
			t.Fatalf("mapError(%q) = %v, want %v", code, got, want)
		}
	}
	if got := conditionalWriteError(&smithy.GenericAPIError{Code: "NoSuchKey"}, `"etag"`); !errors.Is(got, objectstore.ErrPrecondition) {
		t.Fatalf("If-Match on absent key = %v, want ErrPrecondition", got)
	}
	if got := conditionalWriteError(&smithy.GenericAPIError{Code: "NoSuchUpload"}, `"etag"`); !errors.Is(got, objectstore.ErrNotFound) {
		t.Fatalf("missing upload = %v, want ErrNotFound", got)
	}
}

func TestOptionsValidation(t *testing.T) {
	for _, options := range []Options{{PartSize: 1}, {Concurrency: -1}, {CopyPartSize: 1}, {MultipartCopyThreshold: 6 << 30}} {
		if _, err := options.withDefaults(); err == nil {
			t.Fatalf("withDefaults(%+v) accepted invalid options", options)
		}
	}
	if _, err := NewWithOptions(nil, Options{}); err == nil {
		t.Fatal("NewWithOptions accepted a nil client")
	}
}

// stubClient implements clientAPI for request-mapping tests; unimplemented
// methods panic through the nil embedded interface.
type stubClient struct {
	clientAPI
	deleteErrors []types.Error
	deleteInputs []*awss3.DeleteObjectsInput
}

func (client *stubClient) DeleteObjects(_ context.Context, input *awss3.DeleteObjectsInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error) {
	client.deleteInputs = append(client.deleteInputs, input)
	return &awss3.DeleteObjectsOutput{Errors: client.deleteErrors}, nil
}

func TestNoHeadAfterWriteReturnsTheAcknowledgedObject(t *testing.T) {
	fake := newFakeS3(t)
	ctx := context.Background()
	small := []byte("content-addressed")
	large := patterned(3*testPartSize + 5)
	for _, skip := range []bool{false, true} {
		store := fake.store(t, Options{PartSize: testPartSize, Concurrency: 2, NoHeadAfterWrite: skip})
		fake.handler.reset()
		written := map[objectstore.Key]objectstore.Object{}
		var err error
		if written["small"], err = store.Put(ctx, "objects", "small", bytes.NewReader(small), objectstore.PutOptions{Metadata: map[string]string{"k": "v"}}); err != nil {
			t.Fatal(err)
		}
		if written["large"], err = store.Put(ctx, "objects", "large", bytes.NewReader(large), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if written["copy"], err = store.Copy(ctx, "objects", "small", "objects-copy", "copy", objectstore.CopyOptions{}); err != nil {
			t.Fatal(err)
		}
		// Copy reads its source once; the multipart Put keeps its HEAD
		// (some stores acknowledge multipart writes with another entity tag).
		heads := countRequests(fake.handler.recorded(), "HEAD", "")
		if skip && heads != 2 {
			t.Fatalf("NoHeadAfterWrite: %d HEAD requests, want 2 (copy source, multipart Put)", heads)
		}
		if !skip && heads != 4 {
			t.Fatalf("default: %d HEAD requests, want 4", heads)
		}
		// The returned objects match what a HEAD reports, ModifiedAt aside.
		for key, got := range written {
			bucket := objectstore.Bucket("objects")
			if key == "copy" {
				bucket = "objects-copy"
			}
			want, err := store.Stat(ctx, bucket, key)
			if err != nil {
				t.Fatal(err)
			}
			got.ModifiedAt, want.ModifiedAt = time.Time{}, time.Time{}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("skip=%v %s: returned %#v, HEAD reports %#v", skip, key, got, want)
			}
		}
		// The returned Version works as a compare-and-swap token.
		if _, err := store.Put(ctx, "objects", "small", bytes.NewReader([]byte("v2")), objectstore.PutOptions{IfMatchVersion: written["small"].Version}); err != nil {
			t.Fatalf("skip=%v: CAS with the returned version: %v", skip, err)
		}
	}
}
