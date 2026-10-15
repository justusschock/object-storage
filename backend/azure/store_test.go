package azure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	objectstore "github.com/justusschock/go-object-storage"
)

func TestMapError(t *testing.T) {
	for _, test := range []struct {
		code   string
		status int
		want   error
	}{
		{"BlobNotFound", http.StatusNotFound, objectstore.ErrNotFound},
		{"ContainerNotFound", http.StatusNotFound, objectstore.ErrNotFound},
		{"", http.StatusNotFound, objectstore.ErrNotFound},
		{"ConditionNotMet", http.StatusPreconditionFailed, objectstore.ErrPrecondition},
		{"SourceConditionNotMet", http.StatusPreconditionFailed, objectstore.ErrPrecondition},
		{"TargetConditionNotMet", http.StatusPreconditionFailed, objectstore.ErrPrecondition},
		{"BlobAlreadyExists", http.StatusConflict, objectstore.ErrPrecondition},
		{"Md5Mismatch", http.StatusBadRequest, objectstore.ErrPrecondition},
		{"Crc64Mismatch", http.StatusBadRequest, objectstore.ErrPrecondition},
		{"InvalidRange", http.StatusRequestedRangeNotSatisfiable, objectstore.ErrInvalidRange},
		{"", http.StatusRequestedRangeNotSatisfiable, objectstore.ErrInvalidRange},
		{"InvalidBlockList", http.StatusBadRequest, objectstore.ErrInvalidPart},
		{"InvalidBlockId", http.StatusBadRequest, objectstore.ErrInvalidPart},
		{"InvalidBlobOrBlock", http.StatusBadRequest, objectstore.ErrInvalidPart},
		{"AuthenticationFailed", http.StatusForbidden, nil},
	} {
		source := &azcore.ResponseError{ErrorCode: test.code, StatusCode: test.status}
		mapped := mapError(fmt.Errorf("wrapped: %w", source))
		var responseError *azcore.ResponseError
		if !errors.As(mapped, &responseError) && test.want == nil {
			t.Errorf("mapError(%s) lost the Azure error", test.code)
		}
		if test.want != nil && !errors.Is(mapped, test.want) {
			t.Errorf("mapError(%s, %d) = %v, want %v", test.code, test.status, mapped, test.want)
		}
	}
	if mapError(nil) != nil || !errors.Is(mapError(context.Canceled), context.Canceled) {
		t.Fatal("mapError changed nil or cancellation")
	}
	absent := &azcore.ResponseError{ErrorCode: "BlobNotFound", StatusCode: http.StatusNotFound}
	if err := conditionalWriteError(absent, `"0x1"`); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("If-Match write on absent blob = %v, want ErrPrecondition", err)
	}
	missingContainer := &azcore.ResponseError{ErrorCode: "ContainerNotFound", StatusCode: http.StatusNotFound}
	if err := conditionalWriteError(missingContainer, `"0x1"`); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("If-Match write in missing container = %v, want ErrNotFound", err)
	}
}

func TestMetadataValidation(t *testing.T) {
	normalized, err := normalizeMetadata(map[string]string{"Owner": "caller", "_x1": "", "snake_Case": "a b"})
	if err != nil || !reflect.DeepEqual(normalized, map[string]string{"owner": "caller", "_x1": "", "snake_case": "a b"}) {
		t.Fatalf("normalizeMetadata = (%v, %v)", normalized, err)
	}
	if normalized, err := normalizeMetadata(map[string]string{}); err != nil || normalized != nil {
		t.Fatalf("empty metadata = (%v, %v), want nil", normalized, err)
	}
	for _, metadata := range []map[string]string{
		{"with-hyphen": "v"}, {"1digit": "v"}, {"": "v"}, {"ünicode": "v"}, {"dot.name": "v"},
		{"Name": "a", "name": "b"},
		{"goobjectstoragesha256": "v"}, {"GoObjectStorageAnything": "v"},
		{"value": "ü"}, {"value": "line\nbreak"}, {"value": " padded"}, {"value": "padded "},
	} {
		if _, err := normalizeMetadata(metadata); !errors.Is(err, ErrInvalidMetadata) {
			t.Errorf("normalizeMetadata(%q) error = %v, want ErrInvalidMetadata", metadata, err)
		}
	}
}

func TestBlockIDDerivation(t *testing.T) {
	session := strings.Repeat("ab", 16)
	first, again, other := blockID(session, 1), blockID(session, 1), blockID(session, 50_000)
	if first != again || first == other || len(first) != len(other) {
		t.Fatalf("block IDs %q %q %q are not deterministic, distinct, and equal-length", first, again, other)
	}
	decoded, err := base64.StdEncoding.DecodeString(other)
	if err != nil || string(decoded) != session+"00050000" || len(decoded) > 64 {
		t.Fatalf("decoded block ID = (%q, %v)", decoded, err)
	}
	generated, err := newSessionID()
	if err != nil || len(blockID(generated, 7)) != len(first) {
		t.Fatalf("session ID %q yields a different block ID length", generated)
	}
}

func TestOptions(t *testing.T) {
	defaults, err := Options{}.withDefaults()
	if err != nil || defaults.BlockSize != 16<<20 || defaults.Concurrency != 4 || defaults.BatchConcurrency != 16 || defaults.CopyPollInterval != 500*time.Millisecond {
		t.Fatalf("defaults = (%+v, %v)", defaults, err)
	}
	for _, options := range []Options{{BlockSize: 1024}, {BlockSize: 4001 << 20}, {Concurrency: -1}, {BatchConcurrency: -1}, {CopyPollInterval: -1}} {
		if _, err := options.withDefaults(); err == nil {
			t.Errorf("options %+v accepted", options)
		}
	}
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("New accepted a nil client")
	}
	store := &Store{options: defaults}
	if got := store.blockSize(0); got != 16<<20 {
		t.Fatalf("blockSize(0) = %d", got)
	}
	if got := store.blockSize(5 << 40); got != 105<<20 {
		t.Fatalf("blockSize(5 TiB) = %d, want 105 MiB to stay within 50,000 blocks", got)
	}
}

func TestValidateRangeResponse(t *testing.T) {
	requested := objectstore.ByteRange{Offset: 8, Length: 3}
	for _, test := range []struct {
		contentRange string
		length       int64
		ok           bool
	}{
		{"bytes 8-10/20", 3, true},
		{"bytes 8-9/10", 2, false}, // Azure truncates ranges past the end.
		{"bytes 7-9/20", 3, false},
		{"bytes 8-10/10", 3, false},
		{"8-10/20", 3, false},
		{"bytes 8-10/*", 3, false},
	} {
		total, err := validateRangeResponse(&test.contentRange, test.length, requested)
		if test.ok != (err == nil) || (test.ok && total != 20) {
			t.Errorf("validateRangeResponse(%q) = (%d, %v)", test.contentRange, total, err)
		}
	}
	if _, err := validateRangeResponse(nil, 3, requested); !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatal("missing Content-Range accepted")
	}
}

func TestInputValidation(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	for _, bucket := range []objectstore.Bucket{"", "ab", "Upper", "under_score", "double--hyphen", "-leading"} {
		if _, err := store.Stat(ctx, bucket, "key"); !errors.Is(err, objectstore.ErrInvalidBucket) {
			t.Errorf("Stat bucket %q error = %v", bucket, err)
		}
	}
	for _, key := range []objectstore.Key{"", "/leading", ".go-object-storage/multipart/x", objectstore.Key(strings.Repeat("k", 1025)), "bad\xffutf8"} {
		if _, err := store.Stat(ctx, "objects", key); !errors.Is(err, objectstore.ErrInvalidKey) {
			t.Errorf("Stat key %q error = %v", key, err)
		}
	}
	if _, err := store.Put(ctx, "objects", "key", strings.NewReader("v"), objectstore.PutOptions{Metadata: map[string]string{"bad-name": "v"}}); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("Put invalid metadata error = %v", err)
	}
	if _, err := store.Stat(ctx, "missing", "key"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Stat in missing container error = %v", err)
	}
	if err := store.Delete(ctx, "objects", "absent", ""); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Delete absent key error = %v, want ErrNotFound", err)
	}
	if _, err := store.List(ctx, "objects", objectstore.ListOptions{Limit: -1}); !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("negative List limit error = %v", err)
	}
}

func TestEmptyObject(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	object, err := store.Put(ctx, "objects", "empty", onlyReader{strings.NewReader("")}, objectstore.PutOptions{})
	if err != nil || object.Size != 0 || object.SHA256 != sha256.Sum256(nil) {
		t.Fatalf("empty Put = (%#v, %v)", object, err)
	}
	body, got, err := store.Get(ctx, "objects", "empty", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || len(data) != 0 || !reflect.DeepEqual(got, object) {
		t.Fatalf("empty Get = (%q, %#v, %v)", data, got, err)
	}
	if _, _, err := store.Get(ctx, "objects", "empty", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 0, Length: 1}}); !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("ranged Get of empty object error = %v", err)
	}
}

func TestMetadataNamesAreLowerCased(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	object, err := store.Put(ctx, "objects", "case", strings.NewReader("value"), objectstore.PutOptions{Metadata: map[string]string{"Owner": "Caller"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(object.Metadata, map[string]string{"owner": "Caller"}) {
		t.Fatalf("Put metadata = %v", object.Metadata)
	}
	body, got, err := store.Get(ctx, "objects", "case", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	page, err := store.List(ctx, "objects", objectstore.ListOptions{})
	if err != nil || len(page.Objects) != 1 || !reflect.DeepEqual(got, object) || !reflect.DeepEqual(page.Objects[0], object) {
		t.Fatalf("Get = %#v, List = %#v (%v), want %#v", got, page, err, object)
	}
}

func TestListHidesMultipartMarkersAndPaginates(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	if _, err := store.BeginMultipart(ctx, "objects", "pending", objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	var want []objectstore.Key
	for index := range 7 {
		key := objectstore.Key(fmt.Sprintf("key/%02d", index))
		want = append(want, key)
		if _, err := store.Put(ctx, "objects", key, strings.NewReader("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var got []objectstore.Key
	after := objectstore.Key("")
	for pages := 0; ; pages++ {
		page, err := store.List(ctx, "objects", objectstore.ListOptions{After: after, Limit: 3})
		if err != nil || pages > 3 {
			t.Fatalf("List page %d = (%v, %v)", pages, page, err)
		}
		for _, object := range page.Objects {
			got = append(got, object.Key)
		}
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	all, err := store.List(ctx, "objects", objectstore.ListOptions{Limit: 7})
	if err != nil || len(all.Objects) != 7 || all.NextAfter != "" {
		t.Fatalf("exact-limit page = (%d objects, NextAfter %q, %v)", len(all.Objects), all.NextAfter, err)
	}
}

func TestPutPartVerifiesBeforeStaging(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "objects", "verified", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("p"), 100_000)
	digest := sha256.Sum256(data)
	wrong := sha256.Sum256([]byte("wrong"))
	for name, test := range map[string]struct {
		body   []byte
		size   int64
		digest [32]byte
	}{
		"wrong digest": {data, int64(len(data)), wrong},
		"short body":   {data[:10], int64(len(data)), digest},
		"long body":    {append(bytes.Clone(data), 'x'), int64(len(data)), digest},
		"empty digest": {nil, 0, wrong},
	} {
		if _, err := store.PutPart(ctx, upload, 1, onlyReader{bytes.NewReader(test.body)}, test.size, test.digest); !errors.Is(err, objectstore.ErrInvalidPart) {
			t.Errorf("%s: PutPart error = %v, want ErrInvalidPart", name, err)
		}
	}
	blocks, err := store.blockBlob("objects", "verified").GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
	if err == nil && len(blocks.UncommittedBlocks) != 0 {
		t.Fatalf("rejected parts were staged: %+v", blocks.UncommittedBlocks)
	}
	part, err := store.PutPart(ctx, upload, 1, onlyReader{bytes.NewReader(data)}, int64(len(data)), digest)
	if err != nil || part.ETag != blockID(upload.ID, 1) {
		t.Fatalf("PutPart = (%+v, %v)", part, err)
	}
	for _, completed := range [][]objectstore.CompletedPart{
		{{Number: 1, ETag: "foreign", Size: part.Size}},
		{{Number: 1, Size: part.Size + 1}},
		{{Number: 2, Size: part.Size}},
	} {
		if _, err := store.CompleteMultipart(ctx, upload, completed); !errors.Is(err, objectstore.ErrInvalidPart) {
			t.Errorf("CompleteMultipart(%+v) error = %v, want ErrInvalidPart", completed, err)
		}
	}
	object, err := store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{part})
	if err != nil || object.SHA256Known || object.Size != int64(len(data)) {
		t.Fatalf("CompleteMultipart without declared digest = (%#v, %v)", object, err)
	}
	// A retried completion returns the published object.
	again, err := store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{part})
	if err != nil || !reflect.DeepEqual(again, object) {
		t.Fatalf("retried CompleteMultipart = (%#v, %v), want %#v", again, err, object)
	}
	if err := store.AbortMultipart(ctx, upload); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Abort after completion error = %v, want ErrNotFound", err)
	}
	if _, err := store.PutPart(ctx, objectstore.Upload{ID: "not-an-id", Bucket: "objects", Key: "verified"}, 1, bytes.NewReader(nil), 0, digest); !errors.Is(err, objectstore.ErrInvalidPart) {
		t.Fatalf("malformed upload ID error = %v", err)
	}
	other := upload
	other.Key = "other"
	if _, err := store.PutPart(ctx, other, 1, bytes.NewReader(data), int64(len(data)), digest); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("PutPart on completed upload error = %v, want ErrNotFound", err)
	}
}

func TestPutPartRejectsUploadForAnotherKey(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "objects", "target", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	upload.Key = "elsewhere"
	digest := sha256.Sum256([]byte("part"))
	if _, err := store.PutPart(ctx, upload, 1, strings.NewReader("part"), 4, digest); !errors.Is(err, objectstore.ErrInvalidPart) {
		t.Fatalf("PutPart for another key error = %v, want ErrInvalidPart", err)
	}
}

// TestStreamingPutLosesToConcurrentWrite shows Azure discarding a streamed
// Put's uncommitted blocks when another writer publishes the key first; the
// streamed Put fails with ErrPrecondition and the winner stays published.
func TestStreamingPutLosesToConcurrentWrite(t *testing.T) {
	store := newTestStore(t, Options{BlockSize: 64 << 10, Concurrency: 1})
	ctx := context.Background()
	reader, writer := io.Pipe()
	result := make(chan error, 1)
	go func() {
		_, err := store.Put(ctx, "objects", "contended", reader, objectstore.PutOptions{})
		result <- err
	}()
	if _, err := writer.Write(bytes.Repeat([]byte("a"), 3<<16)); err != nil {
		t.Fatal(err)
	}
	client := store.blockBlob("objects", "contended")
	for deadline := time.Now().Add(5 * time.Second); ; {
		blocks, err := client.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
		if err == nil && len(blocks.UncommittedBlocks) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("streamed Put staged no blocks")
		}
		time.Sleep(5 * time.Millisecond)
	}
	winner, err := store.Put(ctx, "objects", "contended", strings.NewReader("winner"), objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	if err := <-result; !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("losing streamed Put error = %v, want ErrPrecondition", err)
	}
	if stat, err := store.Stat(ctx, "objects", "contended"); err != nil || stat.Version != winner.Version {
		t.Fatalf("Stat = (%#v, %v), want the winner", stat, err)
	}
}

func TestCopyOverrides(t *testing.T) {
	store := newTestStore(t, Options{})
	ctx := context.Background()
	source, err := store.Put(ctx, "objects", "source", strings.NewReader("bytes"), objectstore.PutOptions{Metadata: map[string]string{"origin": "source"}})
	if err != nil {
		t.Fatal(err)
	}
	copied, err := store.Copy(ctx, "objects", "source", "objects-copy", "typed", objectstore.CopyOptions{
		ContentType: "text/plain", Metadata: map[string]string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if copied.Metadata != nil || !copied.SHA256Known || copied.SHA256 != source.SHA256 || copied.Size != source.Size {
		t.Fatalf("copy with emptied metadata = %#v", copied)
	}
	stat, err := store.Stat(ctx, "objects-copy", "typed")
	if err != nil || !reflect.DeepEqual(stat, copied) {
		t.Fatalf("Stat = (%#v, %v), want %#v", stat, err, copied)
	}
	properties, err := store.blockBlob("objects-copy", "typed").GetProperties(ctx, nil)
	if err != nil || *properties.ContentType != "text/plain" {
		t.Fatalf("copied content type = (%v, %v)", properties.ContentType, err)
	}
	if _, err := store.Copy(ctx, "objects", "source", "objects-copy", "typed", objectstore.CopyOptions{Metadata: map[string]string{"bad-name": "v"}}); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("Copy invalid metadata error = %v", err)
	}
}

func TestPutCancellationDuringStreaming(t *testing.T) {
	store := newTestStore(t, Options{BlockSize: 64 << 10})
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	go func() {
		_, _ = writer.Write(bytes.Repeat([]byte("a"), 2<<16))
		cancel()
		_ = writer.Close()
	}()
	if _, err := store.Put(ctx, "objects", "canceled", reader, objectstore.PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled streaming Put error = %v", err)
	}
	if _, err := store.Stat(context.Background(), "objects", "canceled"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("canceled streaming Put published: %v", err)
	}
}

type onlyReader struct{ reader io.Reader }

func (reader onlyReader) Read(buffer []byte) (int, error) { return reader.reader.Read(buffer) }
