package azure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/azure/azuretest"
)

type executed struct {
	status int
	header http.Header
	body   []byte
}

// execute performs a presigned request with http.DefaultClient, like a
// third party without credentials.
func execute(t *testing.T, presigned objectstore.PresignedRequest, body []byte) executed {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(context.Background(), presigned.Method, presigned.URL, reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range presigned.Header {
		if !strings.EqualFold(name, "Content-Length") {
			request.Header[name] = append([]string(nil), values...)
		}
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return executed{status: response.StatusCode, header: response.Header, body: data}
}

// TestPresign covers the storetest presign flow with Azure's one deviation:
// Put Block responses carry no ETag, so the presigned part is completed with
// an empty CompletedPart.ETag.
// FR-OBJ-07, FR-OBJ-15
func TestPresign(t *testing.T) {
	for name, options := range map[string]func(*azuretest.Server) Options{
		"client credential":   func(*azuretest.Server) Options { return Options{} },
		"explicit shared key": func(server *azuretest.Server) Options { return Options{SharedKey: server.Credential()} },
	} {
		t.Run(name, func(t *testing.T) {
			server := azuretest.NewServer(t, "objects")
			store, err := New(server.NewClient(t), options(server))
			if err != nil {
				t.Fatal(err)
			}
			testPresign(t, store)
		})
	}
}

func testPresign(t *testing.T, store *Store) {
	ctx := context.Background()
	data := []byte("presigned payload")
	digest := sha256.Sum256(data)
	put, err := store.PresignPut(ctx, "objects", "presigned/object", time.Minute, objectstore.PutOptions{
		IfAbsent: true, SHA256: &digest, Size: int64(len(data)), ContentType: "text/plain", Metadata: map[string]string{"Via": "presign"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if put.Method != http.MethodPut || !put.ExpiresAt.After(time.Now()) || put.Header.Get("If-None-Match") != "*" {
		t.Fatalf("PresignPut = %#v", put)
	}
	if response := execute(t, put, data); response.status != http.StatusCreated {
		t.Fatalf("presigned PUT = %d %s", response.status, response.body)
	}
	object, err := store.Stat(ctx, "objects", "presigned/object")
	if err != nil {
		t.Fatal(err)
	}
	if object.Size != int64(len(data)) || !object.SHA256Known || object.SHA256 != digest || object.Metadata["via"] != "presign" {
		t.Fatalf("presigned object = %#v", object)
	}
	if response := execute(t, put, data); response.status/100 == 2 {
		t.Fatalf("presigned IfAbsent PUT replaced an existing object: %d", response.status)
	}
	// IfAbsent grants only the SAS create permission, so dropping the
	// If-None-Match header does not allow an overwrite either.
	stripped := put.Clone()
	stripped.Header.Del("If-None-Match")
	if response := execute(t, stripped, data); response.status != http.StatusForbidden {
		t.Fatalf("create-only SAS overwrite = %d", response.status)
	}

	get, err := store.PresignGet(ctx, "objects", "presigned/object", time.Minute, &objectstore.ByteRange{Offset: 4, Length: 6})
	if err != nil {
		t.Fatal(err)
	}
	if response := execute(t, get, nil); response.status != http.StatusPartialContent || string(response.body) != string(data[4:10]) {
		t.Fatalf("presigned ranged GET = (%d, %q)", response.status, response.body)
	}

	upload, err := store.BeginMultipart(ctx, "objects", "presigned/multipart", objectstore.PutOptions{SHA256: &digest})
	if err != nil {
		t.Fatal(err)
	}
	part, err := store.PresignUploadPart(ctx, upload, 1, int64(len(data)), digest, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	response := execute(t, part, data)
	if response.status != http.StatusCreated || response.header.Get("ETag") != "" {
		t.Fatalf("presigned part upload = (%d, ETag %q)", response.status, response.header.Get("ETag"))
	}
	completed, err := store.CompleteMultipart(ctx, upload, []objectstore.CompletedPart{{Number: 1, Size: int64(len(data)), SHA256: digest}})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Size != int64(len(data)) || completed.SHA256 != digest || !completed.SHA256Known {
		t.Fatalf("presigned multipart object = %#v", completed)
	}
	body, _, err := store.Get(ctx, "objects", "presigned/multipart", objectstore.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("presigned multipart bytes = (%q, %v)", got, err)
	}
}

func TestPresignWithoutSharedKey(t *testing.T) {
	server := azuretest.NewServer(t, "objects")
	client, err := azblob.NewClientWithNoCredential(server.Endpoint(), nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PresignGet(context.Background(), "objects", "key", time.Minute, nil); !errors.Is(err, ErrPresignUnavailable) {
		t.Fatalf("PresignGet without shared key error = %v", err)
	}
	if _, err := store.PresignGet(context.Background(), "objects", "key", 0, nil); err == nil {
		t.Fatal("PresignGet accepted a zero expiry")
	}
}

func TestPresignedRequestExpires(t *testing.T) {
	store := newTestStore(t, Options{})
	put, err := store.PresignPut(context.Background(), "objects", "expired", time.Second, objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if put.ExpiresAt.After(time.Now().Add(time.Second)) || put.ExpiresAt.Nanosecond() != 0 {
		t.Fatalf("ExpiresAt = %v, want the signed whole-second expiry", put.ExpiresAt)
	}
	time.Sleep(time.Until(put.ExpiresAt) + 10*time.Millisecond)
	if response := execute(t, put, []byte("late")); response.status != http.StatusForbidden {
		t.Fatalf("expired presigned PUT = %d", response.status)
	}
}
