package azuretest

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

func blockClient(client *azblob.Client, containerName, name string) *blockblob.Client {
	return client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(name)
}

func upload(t *testing.T, client *azblob.Client, name, data string, options *blockblob.UploadOptions) blockblob.UploadResponse {
	t.Helper()
	response, err := blockClient(client, "objects", name).Upload(context.Background(), streaming.NopCloser(strings.NewReader(data)), options)
	if err != nil {
		t.Fatalf("upload %s: %v", name, err)
	}
	return response
}

func download(t *testing.T, client *azblob.Client, name string, options *blob.DownloadStreamOptions) (string, blob.DownloadStreamResponse, error) {
	t.Helper()
	response, err := blockClient(client, "objects", name).DownloadStream(context.Background(), options)
	if err != nil {
		return "", response, err
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	return string(data), response, err
}

func TestRoundTripWithMetadataAndProperties(t *testing.T) {
	client, _ := NewClient(t, "objects")
	metadata := map[string]*string{"owner": to.Ptr("caller"), "a_b": to.Ptr("1"), "ab": to.Ptr("2"), "a1": to.Ptr("3"), "Z": to.Ptr("4")}
	written := upload(t, client, "dir/with space/ü", "hello world", &blockblob.UploadOptions{
		Metadata: metadata, HTTPHeaders: &blob.HTTPHeaders{BlobContentType: to.Ptr("text/plain")},
		TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
	})
	if written.ETag == nil || !strings.HasPrefix(string(*written.ETag), "\"0x") || written.LastModified == nil {
		t.Fatalf("upload response = %+v", written)
	}
	data, response, err := download(t, client, "dir/with space/ü", nil)
	if err != nil || data != "hello world" || *response.ETag != *written.ETag || *response.ContentType != "text/plain" {
		t.Fatalf("download = (%q, %+v, %v)", data, response, err)
	}
	if got := response.Metadata["Owner"]; got == nil || *got != "caller" {
		t.Fatalf("metadata = %v", response.Metadata)
	}
	properties, err := blockClient(client, "objects", "dir/with space/ü").GetProperties(context.Background(), nil)
	if err != nil || *properties.ContentLength != 11 || !properties.LastModified.Equal(*written.LastModified) {
		t.Fatalf("properties = (%+v, %v)", properties, err)
	}
}

func TestWrongKeyIsRejected(t *testing.T) {
	server := NewServer(t, "objects")
	wrong, err := azblob.NewSharedKeyCredential(AccountName, base64.StdEncoding.EncodeToString([]byte("not the account key")))
	if err != nil {
		t.Fatal(err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(server.Endpoint(), wrong, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = blockClient(client, "objects", "key").Upload(context.Background(), streaming.NopCloser(strings.NewReader("x")), nil)
	if !bloberror.HasCode(err, bloberror.AuthenticationFailed) {
		t.Fatalf("wrong key error = %v, want AuthenticationFailed", err)
	}
	anonymous, err := azblob.NewClientWithNoCredential(server.Endpoint(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := anonymous.DownloadStream(context.Background(), "objects", "key", nil); !bloberror.HasCode(err, bloberror.NoAuthenticationInformation) {
		t.Fatalf("anonymous error = %v, want NoAuthenticationInformation", err)
	}
}

func TestConditionalHeaders(t *testing.T) {
	client, _ := NewClient(t, "objects")
	ctx := context.Background()
	ifMatch := func(etag azcore.ETag) *blob.AccessConditions {
		return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &etag}}
	}
	absent := &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}
	first := upload(t, client, "cas", "one", &blockblob.UploadOptions{AccessConditions: absent})
	_, err := blockClient(client, "objects", "cas").Upload(ctx, streaming.NopCloser(strings.NewReader("two")), &blockblob.UploadOptions{AccessConditions: absent})
	if !bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
		t.Fatalf("If-None-Match * on existing = %v", err)
	}
	_, err = blockClient(client, "objects", "missing").Upload(ctx, streaming.NopCloser(strings.NewReader("two")), &blockblob.UploadOptions{AccessConditions: ifMatch("\"0x1\"")})
	if !bloberror.HasCode(err, bloberror.ConditionNotMet) {
		t.Fatalf("If-Match on absent = %v", err)
	}
	second := upload(t, client, "cas", "two", &blockblob.UploadOptions{AccessConditions: ifMatch(*first.ETag)})
	if *second.ETag == *first.ETag {
		t.Fatal("rewrite kept its entity tag")
	}
	if _, _, err := download(t, client, "cas", &blob.DownloadStreamOptions{AccessConditions: ifMatch(*first.ETag)}); !bloberror.HasCode(err, bloberror.ConditionNotMet) {
		t.Fatalf("stale If-Match read = %v", err)
	}
	if _, err := blockClient(client, "objects", "cas").Delete(ctx, &blob.DeleteOptions{AccessConditions: ifMatch(*first.ETag)}); !bloberror.HasCode(err, bloberror.ConditionNotMet) {
		t.Fatalf("stale If-Match delete = %v", err)
	}
	if _, err := blockClient(client, "objects", "cas").GetProperties(ctx, &blob.GetPropertiesOptions{AccessConditions: ifMatch(*first.ETag)}); !bloberror.HasCode(err, bloberror.ConditionNotMet) {
		t.Fatalf("HEAD error = %v, want ConditionNotMet from x-ms-error-code", err)
	}
	if _, err := blockClient(client, "objects", "cas").Delete(ctx, &blob.DeleteOptions{AccessConditions: ifMatch(*second.ETag)}); err != nil {
		t.Fatal(err)
	}
	if _, err := blockClient(client, "objects", "cas").GetProperties(ctx, nil); !bloberror.HasCode(err, bloberror.BlobNotFound) {
		t.Fatalf("HEAD after delete = %v", err)
	}
	if _, err := blockClient(client, "missing-container", "cas").GetProperties(ctx, nil); !bloberror.HasCode(err, bloberror.ContainerNotFound) {
		t.Fatalf("HEAD in missing container = %v", err)
	}
}

func TestRanges(t *testing.T) {
	client, _ := NewClient(t, "objects")
	upload(t, client, "range", "abcdefghij", nil)
	data, response, err := download(t, client, "range", &blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset: 2, Count: 4}})
	if err != nil || data != "cdef" || *response.ContentRange != "bytes 2-5/10" || *response.ContentLength != 4 {
		t.Fatalf("range = (%q, %v, %v)", data, response.ContentRange, err)
	}
	data, response, err = download(t, client, "range", &blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset: 8, Count: 5}})
	if err != nil || data != "ij" || *response.ContentRange != "bytes 8-9/10" {
		t.Fatalf("range past end = (%q, %v, %v), want Azure's truncated range", data, response.ContentRange, err)
	}
	if _, _, err := download(t, client, "range", &blob.DownloadStreamOptions{Range: blob.HTTPRange{Offset: 10, Count: 1}}); !bloberror.HasCode(err, bloberror.InvalidRange) {
		t.Fatalf("range at end = %v, want InvalidRange", err)
	}
}

func TestListPaging(t *testing.T) {
	client, _ := NewClient(t, "objects")
	for _, name := range []string{"a/3", "b/1", "a/1", "a/2", "a/\x01control"} {
		upload(t, client, name, name, &blockblob.UploadOptions{Metadata: map[string]*string{"name": to.Ptr("v")}})
	}
	pager := client.NewListBlobsFlatPager("objects", &azblob.ListBlobsFlatOptions{Prefix: to.Ptr("a/"), MaxResults: to.Ptr[int32](2), Include: container.ListBlobsInclude{Metadata: true}})
	var names []string
	pages := 0
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, item := range page.Segment.BlobItems {
			names = append(names, *item.Name)
			if item.Metadata["name"] == nil || *item.Metadata["name"] != "v" || strings.HasPrefix(string(*item.Properties.ETag), "\"") {
				t.Fatalf("listed item = %+v", item)
			}
		}
	}
	if fmt.Sprint(names) != fmt.Sprint([]string{"a/\x01control", "a/1", "a/2", "a/3"}) || pages != 2 {
		t.Fatalf("names = %q over %d pages", names, pages)
	}
	page, err := client.NewListBlobsFlatPager("objects", &azblob.ListBlobsFlatOptions{StartFrom: to.Ptr("a/2")}).NextPage(context.Background())
	if err != nil || len(page.Segment.BlobItems) != 3 || *page.Segment.BlobItems[0].Name != "a/2" || page.Segment.BlobItems[0].Metadata != nil {
		t.Fatalf("startFrom page = (%+v, %v)", page.Segment, err)
	}
}

func TestBlockLists(t *testing.T) {
	client, _ := NewClient(t, "objects")
	ctx := context.Background()
	blocks := blockClient(client, "objects", "blocks")
	id := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	for _, staged := range []struct{ id, data string }{{"block-1", "hello "}, {"block-2", "world"}, {"block-1", "HELLO "}} {
		if _, err := blocks.StageBlock(ctx, id(staged.id), streaming.NopCloser(strings.NewReader(staged.data)), &blockblob.StageBlockOptions{
			TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := blocks.StageBlock(ctx, id("short"), streaming.NopCloser(strings.NewReader("x")), nil); !bloberror.HasCode(err, bloberror.InvalidBlobOrBlock) {
		t.Fatalf("different block ID length error = %v", err)
	}
	if _, err := blocks.StageBlock(ctx, id("block-3"), streaming.NopCloser(strings.NewReader("x")), &blockblob.StageBlockOptions{
		TransactionalValidation: blob.TransferValidationTypeMD5(bytes.Repeat([]byte{1}, 16)),
	}); !bloberror.HasCode(err, bloberror.MD5Mismatch) {
		t.Fatalf("MD5 mismatch error = %v", err)
	}
	if _, err := blocks.GetProperties(ctx, nil); !bloberror.HasCode(err, bloberror.BlobNotFound) {
		t.Fatalf("uncommitted blob is visible: %v", err)
	}
	list, err := blocks.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
	if err != nil || len(list.UncommittedBlocks) != 2 || *list.UncommittedBlocks[0].Size != 6 {
		t.Fatalf("uncommitted list = (%+v, %v)", list, err)
	}
	if _, err := blocks.CommitBlockList(ctx, []string{id("block-1"), id("block-9")}, nil); !bloberror.HasCode(err, bloberror.InvalidBlockList) {
		t.Fatalf("unknown block commit error = %v", err)
	}
	if _, err := blocks.CommitBlockList(ctx, []string{id("block-1"), id("block-2")}, &blockblob.CommitBlockListOptions{
		Metadata: map[string]*string{"k": to.Ptr("v")},
	}); err != nil {
		t.Fatal(err)
	}
	if data, _, err := download(t, client, "blocks", nil); err != nil || data != "HELLO world" {
		t.Fatalf("committed data = (%q, %v)", data, err)
	}
	// Committed blocks stay reusable; uncommitted blocks were discarded.
	if _, err := blocks.CommitBlockList(ctx, []string{id("block-2")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := blocks.StageBlock(ctx, id("block-4"), streaming.NopCloser(strings.NewReader("lost")), nil); err != nil {
		t.Fatal(err)
	}
	upload(t, client, "blocks", "replaced", nil)
	if _, err := blocks.CommitBlockList(ctx, []string{id("block-4")}, nil); !bloberror.HasCode(err, bloberror.InvalidBlockList) {
		t.Fatalf("Put Blob kept uncommitted blocks: %v", err)
	}
}

func TestCopy(t *testing.T) {
	server := NewServer(t, "objects", "objects-copy")
	client := server.NewClient(t)
	ctx := context.Background()
	source := upload(t, client, "source", "copied", &blockblob.UploadOptions{Metadata: map[string]*string{"origin": to.Ptr("source")}})
	destination := client.ServiceClient().NewContainerClient("objects-copy").NewBlobClient("destination")
	sourceURL := blockClient(client, "objects", "source").URL()
	stale := azcore.ETag("\"0x1\"")
	if _, err := destination.StartCopyFromURL(ctx, sourceURL, &blob.StartCopyFromURLOptions{
		SourceModifiedAccessConditions: &blob.SourceModifiedAccessConditions{SourceIfMatch: &stale},
	}); !bloberror.HasCode(err, bloberror.SourceConditionNotMet) {
		t.Fatalf("stale source copy error = %v", err)
	}
	copied, err := destination.StartCopyFromURL(ctx, sourceURL, &blob.StartCopyFromURLOptions{
		SourceModifiedAccessConditions: &blob.SourceModifiedAccessConditions{SourceIfMatch: source.ETag},
	})
	if err != nil || *copied.CopyStatus != blob.CopyStatusTypeSuccess {
		t.Fatalf("copy = (%+v, %v)", copied, err)
	}
	properties, err := destination.GetProperties(ctx, nil)
	if err != nil || *properties.Metadata["Origin"] != "source" || *properties.CopyStatus != blob.CopyStatusTypeSuccess || *properties.ETag != *copied.ETag {
		t.Fatalf("copied properties = (%+v, %v)", properties, err)
	}
	if _, err := destination.StartCopyFromURL(ctx, sourceURL, &blob.StartCopyFromURLOptions{
		AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &stale}},
	}); !bloberror.HasCode(err, bloberror.TargetConditionNotMet) {
		t.Fatalf("stale target copy error = %v", err)
	}
	if _, err := destination.StartCopyFromURL(ctx, sourceURL+"-missing", nil); !bloberror.HasCode(err, bloberror.CannotVerifyCopySource) {
		t.Fatalf("missing source copy error = %v", err)
	}
}

func TestSharedAccessSignatures(t *testing.T) {
	server := NewServer(t, "objects")
	client := server.NewClient(t)
	upload(t, client, "shared", "shared bytes", nil)
	signed, err := blockClient(client, "objects", "shared").GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, signed, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("x-ms-range", "bytes=0-5")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusPartialContent || string(data) != "shared" {
		t.Fatalf("SAS GET = (%d, %q)", response.StatusCode, data)
	}
	put, err := http.NewRequest(http.MethodPut, signed, strings.NewReader("overwrite"))
	if err != nil {
		t.Fatal(err)
	}
	put.Header.Set("x-ms-blob-type", "BlockBlob")
	response, err = http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden || response.Header.Get("x-ms-error-code") != "AuthorizationPermissionMismatch" {
		t.Fatalf("read-only SAS PUT = %d %s", response.StatusCode, response.Header.Get("x-ms-error-code"))
	}
	tampered := strings.Replace(signed, "sp=r", "sp=rw", 1)
	response, err = http.Get(tampered)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden || response.Header.Get("x-ms-error-code") != "AuthenticationFailed" {
		t.Fatalf("tampered SAS = %d %s", response.StatusCode, response.Header.Get("x-ms-error-code"))
	}
}

// TestSASExpiry drives the handler in a synctest bubble, so the fake's
// clock advances deterministically past the signed expiry.
func TestSASExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, err := newService(AccountName, AccountKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		if status, code := svc.createContainer("objects"); status != http.StatusCreated {
			t.Fatal(code)
		}
		credential, err := azblob.NewSharedKeyCredential(AccountName, AccountKey)
		if err != nil {
			t.Fatal(err)
		}
		values, err := sas.BlobSignatureValues{
			ContainerName: "objects", BlobName: "expiring", Permissions: (&sas.BlobPermissions{Create: true, Write: true}).String(),
			ExpiryTime: time.Now().Add(time.Minute).UTC(),
		}.SignWithSharedKey(credential)
		if err != nil {
			t.Fatal(err)
		}
		put := func() *httptest.ResponseRecorder {
			request := httptest.NewRequest(http.MethodPut, "/"+AccountName+"/objects/expiring?"+values.Encode(), strings.NewReader("value"))
			request.Header.Set("x-ms-blob-type", "BlockBlob")
			recorder := httptest.NewRecorder()
			svc.ServeHTTP(recorder, request)
			return recorder
		}
		if recorder := put(); recorder.Code != http.StatusCreated {
			t.Fatalf("SAS PUT before expiry = %d %s", recorder.Code, recorder.Body)
		}
		time.Sleep(time.Minute)
		if recorder := put(); recorder.Code != http.StatusForbidden || recorder.Header().Get("x-ms-error-code") != "AuthenticationFailed" {
			t.Fatalf("SAS PUT after expiry = %d %s", recorder.Code, recorder.Body)
		}
	})
}

func TestRequestDateSkew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, err := newService(AccountName, AccountKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodHead, "/"+AccountName+"/objects/blob", nil)
		request.Header.Set("x-ms-date", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
		request.Header.Set("Authorization", "SharedKey "+AccountName+":"+svc.sign(sharedKeyStringToSign(request, AccountName, "/"+AccountName+"/objects/blob")))
		recorder := httptest.NewRecorder()
		svc.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusForbidden || recorder.Header().Get("x-ms-error-code") != "AuthenticationFailed" {
			t.Fatalf("stale date = %d %s", recorder.Code, recorder.Header().Get("x-ms-error-code"))
		}
	})
}

func TestCreateContainer(t *testing.T) {
	server := NewServer(t)
	client := server.NewClient(t)
	if _, err := client.CreateContainer(context.Background(), "fresh", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateContainer(context.Background(), "fresh", nil); !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		t.Fatalf("duplicate container error = %v", err)
	}
	if err := server.CreateContainer("Invalid_Name"); err == nil {
		t.Fatal("invalid container name accepted")
	}
	var responseError *azcore.ResponseError
	_, err := client.CreateContainer(context.Background(), "x", nil)
	if !errors.As(err, &responseError) || responseError.StatusCode != http.StatusBadRequest {
		t.Fatalf("short container name error = %v", err)
	}
}

func TestHeaderCollation(t *testing.T) {
	for _, test := range []struct {
		left, right string
		want        int
	}{
		{"x-ms-blob-type", "x-ms-date", -1},
		{"x-ms-meta-a_b", "x-ms-meta-ab", -1},
		{"x-ms-meta-a1", "x-ms-meta-ab", -1},
		{"x-ms-meta-ab", "x-ms-meta-a-b", -1},
		{"x-ms-version", "x-ms-version", 0},
	} {
		if got := compareHeaderNames(test.left, test.right); got != test.want {
			t.Errorf("compare(%q, %q) = %d, want %d", test.left, test.right, got, test.want)
		}
	}
}
