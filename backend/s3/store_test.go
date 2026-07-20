package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	objectstore "github.com/justusschock/go-object-storage"
)

func TestPutMapsConditionalsChecksumAndMetadata(t *testing.T) {
	data := []byte("value")
	digest := sha256.Sum256(data)
	now := time.Unix(123, 0)
	for _, test := range []struct {
		name      string
		options   objectstore.PutOptions
		wantNone  string
		wantMatch string
	}{
		{name: "if absent", options: objectstore.PutOptions{IfAbsent: true, SHA256: &digest, Metadata: map[string]string{"owner": "caller"}}, wantNone: "*"},
		{name: "if match", options: objectstore.PutOptions{IfMatchVersion: `"old"`, SHA256: &digest, Metadata: map[string]string{"owner": "caller"}}, wantMatch: `"old"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{headOutput: &awss3.HeadObjectOutput{
				ContentLength: ptr(int64(len(data))), ETag: ptr(`"etag"`), LastModified: &now,
				Metadata: map[string]string{"owner": "caller", sha256MetadataKey: hex.EncodeToString(digest[:])},
			}}
			uploader := &fakeUploader{output: &manager.UploadOutput{ETag: ptr(`"etag"`)}}
			store := &Store{client: client, uploader: uploader}
			object, err := store.Put(context.Background(), "bucket", "path/value", bytes.NewReader(data), test.options)
			if err != nil {
				t.Fatal(err)
			}
			input := uploader.input
			if deref(input.IfNoneMatch) != test.wantNone || deref(input.IfMatch) != test.wantMatch ||
				input.ChecksumSHA256 != nil || input.ChecksumAlgorithm != types.ChecksumAlgorithmSha256 {
				t.Fatalf("PutObjectInput = %#v", input)
			}
			if !bytes.Equal(uploader.body, data) || input.Metadata["owner"] != "caller" || input.Metadata[sha256MetadataKey] != hex.EncodeToString(digest[:]) {
				t.Fatalf("uploaded body/metadata = %q, %#v", uploader.body, input.Metadata)
			}
			if object.Version != `"etag"` || object.SHA256 != digest || object.Metadata["owner"] != "caller" {
				t.Fatalf("object = %#v", object)
			}
		})
	}
}

func TestGetRejectsShortenedRange(t *testing.T) {
	digest := sha256.Sum256([]byte("0123456789"))
	now := time.Unix(456, 0)
	client := &fakeClient{getOutput: &awss3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader([]byte("89"))), ContentLength: ptr(int64(2)), ContentRange: ptr("bytes 8-9/10"),
		ETag: ptr(`"etag"`), LastModified: &now, Metadata: map[string]string{sha256MetadataKey: hex.EncodeToString(digest[:])},
	}}
	store := &Store{client: client, uploader: &fakeUploader{}}
	body, _, err := store.Get(context.Background(), "bucket", "key", objectstore.GetOptions{Range: &objectstore.ByteRange{Offset: 8, Length: 3}})
	if body != nil || !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("Get shortened range = (%#v, %v), want nil ErrInvalidRange", body, err)
	}
}

func TestGetMapsExactRangeAndVersion(t *testing.T) {
	digest := sha256.Sum256([]byte("full object payload"))
	now := time.Unix(456, 0)
	client := &fakeClient{getOutput: &awss3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader([]byte("0123456789"))), ContentLength: ptr(int64(10)), ContentRange: ptr("bytes 5-14/19"),
		ETag: ptr(`"etag"`), LastModified: &now, Metadata: map[string]string{sha256MetadataKey: hex.EncodeToString(digest[:])},
	}}
	store := &Store{client: client, uploader: &fakeUploader{}}
	body, object, err := store.Get(context.Background(), "bucket", "path/value", objectstore.GetOptions{
		Range: &objectstore.ByteRange{Offset: 5, Length: 10}, IfMatchVersion: `"etag"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if deref(client.getInput.Range) != "bytes=5-14" || deref(client.getInput.IfMatch) != `"etag"` {
		t.Fatalf("GetObjectInput = %#v", client.getInput)
	}
	if object.Size != 19 || object.Version != `"etag"` || object.SHA256 != digest {
		t.Fatalf("object = %#v", object)
	}
}

func TestListMapsStablePaginationAndSorts(t *testing.T) {
	now := time.Unix(789, 0)
	digest := sha256.Sum256([]byte("value"))
	client := &fakeClient{
		listOutput: &awss3.ListObjectsV2Output{
			Contents:    []types.Object{{Key: ptr("a/2")}, {Key: ptr("a/1")}},
			IsTruncated: ptr(true), NextContinuationToken: ptr("opaque"),
		},
		headOutput: &awss3.HeadObjectOutput{ContentLength: ptr(int64(5)), ETag: ptr(`"etag"`), LastModified: &now, Metadata: map[string]string{sha256MetadataKey: hex.EncodeToString(digest[:])}},
	}
	store := &Store{client: client, uploader: &fakeUploader{}}
	page, err := store.List(context.Background(), "bucket", objectstore.ListOptions{Prefix: "a/", After: "a/0", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if deref(client.listInput.Prefix) != "a/" || deref(client.listInput.StartAfter) != "a/0" || deref(client.listInput.MaxKeys) != 2 {
		t.Fatalf("ListObjectsV2Input = %#v", client.listInput)
	}
	if got := []objectstore.Key{page.Objects[0].Key, page.Objects[1].Key}; !reflect.DeepEqual(got, []objectstore.Key{"a/1", "a/2"}) || page.NextAfter != "a/2" {
		t.Fatalf("page = %#v", page)
	}
}

func TestMultipartMapsOrderingChecksumsCompletionAndAbort(t *testing.T) {
	partData := []byte("part")
	partDigest := sha256.Sum256(partData)
	fullDigest := partDigest
	now := time.Unix(999, 0)
	client := &fakeClient{
		createOutput:     &awss3.CreateMultipartUploadOutput{UploadId: ptr("upload-id")},
		uploadPartOutput: &awss3.UploadPartOutput{ETag: ptr(`"part"`), ChecksumSHA256: ptr(base64.StdEncoding.EncodeToString(partDigest[:]))},
		completeOutput:   &awss3.CompleteMultipartUploadOutput{ETag: ptr(`"staging"`)},
		getOutput:        &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(partData))},
		headOutput:       &awss3.HeadObjectOutput{ContentLength: ptr(int64(len(partData))), ETag: ptr(`"etag"`), LastModified: &now, Metadata: map[string]string{sha256MetadataKey: hex.EncodeToString(fullDigest[:])}},
		abortOutput:      &awss3.AbortMultipartUploadOutput{},
		listPartsOutput:  &awss3.ListPartsOutput{},
	}
	uploader := &fakeUploader{output: &manager.UploadOutput{ETag: ptr(`"etag"`)}}
	store := &Store{client: client, uploader: uploader}
	upload, err := store.BeginMultipart(context.Background(), "bucket", "key", objectstore.PutOptions{IfAbsent: true, SHA256: &fullDigest})
	if err != nil {
		t.Fatal(err)
	}
	part, err := store.PutPart(context.Background(), upload, 1, bytes.NewReader(partData), int64(len(partData)), partDigest)
	if err != nil {
		t.Fatal(err)
	}
	if deref(client.uploadPartInput.ChecksumSHA256) != base64.StdEncoding.EncodeToString(partDigest[:]) || client.uploadPartInput.ChecksumAlgorithm != types.ChecksumAlgorithmSha256 {
		t.Fatalf("UploadPartInput = %#v", client.uploadPartInput)
	}
	if _, err := store.CompleteMultipart(context.Background(), upload, []objectstore.CompletedPart{{Number: 2}, part}); !errors.Is(err, objectstore.ErrInvalidPart) {
		t.Fatalf("unsorted completion error = %v", err)
	}
	upload.Options.IfAbsent = false
	upload.Options.SHA256 = nil
	object, err := store.CompleteMultipart(context.Background(), upload, []objectstore.CompletedPart{part})
	if err != nil {
		t.Fatal(err)
	}
	if deref(client.completeInput.Key) == "key" || len(client.completeInput.MultipartUpload.Parts) != 1 ||
		deref(uploader.input.IfNoneMatch) != "*" || uploader.input.Metadata[sha256MetadataKey] != hex.EncodeToString(fullDigest[:]) || object.SHA256 != fullDigest {
		t.Fatalf("CompleteMultipartUploadInput/final Put/object = %#v, %#v, %#v", client.completeInput, uploader.input, object)
	}
	if err := store.AbortMultipart(context.Background(), upload); err != nil || deref(client.abortInput.UploadId) != "upload-id" {
		t.Fatalf("AbortMultipart = %v, input %#v", err, client.abortInput)
	}
}

func TestMultipartChecksumMismatchNeverPublishesFinalKey(t *testing.T) {
	want := sha256.Sum256([]byte("expected"))
	partDigest := sha256.Sum256([]byte("wrong"))
	now := time.Unix(1000, 0)
	client := &fakeClient{
		createOutput:     &awss3.CreateMultipartUploadOutput{UploadId: ptr("upload-id")},
		uploadPartOutput: &awss3.UploadPartOutput{ETag: ptr(`"part"`)},
		completeOutput:   &awss3.CompleteMultipartUploadOutput{ETag: ptr(`"staging"`)},
		getOutput:        &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("wrong")))},
		headOutput:       &awss3.HeadObjectOutput{ContentLength: ptr(int64(5)), ETag: ptr(`"staging"`), LastModified: &now, Metadata: map[string]string{sha256MetadataKey: hex.EncodeToString(want[:])}},
		listPartsOutput:  &awss3.ListPartsOutput{},
	}
	uploader := &fakeUploader{output: &manager.UploadOutput{}}
	store := &Store{client: client, uploader: uploader}
	upload, err := store.BeginMultipart(context.Background(), "bucket", "final", objectstore.PutOptions{SHA256: &want})
	if err != nil {
		t.Fatal(err)
	}
	part, err := store.PutPart(context.Background(), upload, 1, bytes.NewReader([]byte("wrong")), 5, partDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteMultipart(context.Background(), upload, []objectstore.CompletedPart{part}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("CompleteMultipart error = %v, want ErrPrecondition", err)
	}
	if uploader.input != nil || client.deleteInput == nil || deref(client.deleteInput.Key) == "final" {
		t.Fatalf("mismatch published or deleted final key: upload %#v, delete %#v", uploader.input, client.deleteInput)
	}
}

func TestCompleteAfterAbortReturnsNotFound(t *testing.T) {
	client := &fakeClient{createOutput: &awss3.CreateMultipartUploadOutput{UploadId: ptr("upload-id")}, abortOutput: &awss3.AbortMultipartUploadOutput{}, listPartsOutput: &awss3.ListPartsOutput{}}
	store := &Store{client: client, uploader: &fakeUploader{}}
	upload, err := store.BeginMultipart(context.Background(), "bucket", "key", objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AbortMultipart(context.Background(), upload); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteMultipart(context.Background(), upload, nil); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("CompleteMultipart after abort error = %v, want ErrNotFound", err)
	}
}

func TestCompleteResumesFromCompletedStagingObject(t *testing.T) {
	data := []byte("complete")
	digest := sha256.Sum256(data)
	now := time.Unix(1001, 0)
	client := &fakeClient{
		createOutput: &awss3.CreateMultipartUploadOutput{UploadId: ptr("upload-id")},
		listPartsErr: &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "completion response was lost"},
		getOutput:    &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))},
		headOutputs: []*awss3.HeadObjectOutput{
			{ContentLength: ptr(int64(len(data))), ETag: ptr(`"staging"`), LastModified: &now},
			{ContentLength: ptr(int64(len(data))), ETag: ptr(`"final"`), LastModified: &now, Metadata: map[string]string{sha256MetadataKey: hex.EncodeToString(digest[:])}},
		},
	}
	uploader := &fakeUploader{output: &manager.UploadOutput{ETag: ptr(`"final"`)}}
	store := &Store{client: client, uploader: uploader}
	upload, err := store.BeginMultipart(context.Background(), "bucket", "key", objectstore.PutOptions{SHA256: &digest})
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.CompleteMultipart(context.Background(), upload, []objectstore.CompletedPart{{Number: 1, ETag: `"part"`, Size: int64(len(data)), SHA256: digest}})
	if err != nil {
		t.Fatal(err)
	}
	if client.completeInput != nil || uploader.input == nil || object.SHA256 != digest {
		t.Fatalf("resume completed staging = complete %#v, upload %#v, object %#v", client.completeInput, uploader.input, object)
	}
}

func TestBeginMultipartRejectsLossyCustodyBeforeCreatingUpload(t *testing.T) {
	client := &fakeClient{}
	store := &Store{client: client, uploader: &fakeUploader{}}
	_, err := store.BeginMultipart(context.Background(), "bucket", "key", objectstore.PutOptions{Metadata: map[string]string{"owner": string([]byte{0xff})}})
	if err == nil || client.createInput != nil {
		t.Fatalf("BeginMultipart = (%v, create %#v), want rejection before side effect", err, client.createInput)
	}
}

func TestPutAbortsFailedManagerMultipartWithIndependentContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeClient{abortOutput: &awss3.AbortMultipartUploadOutput{}}
	uploader := &fakeUploader{
		err:      fakeMultiUploadError{id: "upload-id", err: context.Canceled},
		onUpload: cancel,
	}
	store := &Store{client: client, uploader: uploader}
	_, err := store.Put(ctx, "bucket", "key", bytes.NewReader([]byte("value")), objectstore.PutOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put error = %v, want context.Canceled", err)
	}
	if client.abortInput == nil || deref(client.abortInput.UploadId) != "upload-id" || client.abortContextErr != nil {
		t.Fatalf("AbortMultipartUpload = (%#v, %v), want upload-id with live context", client.abortInput, client.abortContextErr)
	}
}

func TestMapsS3Errors(t *testing.T) {
	for code, want := range map[string]error{
		"NoSuchKey": objectstore.ErrNotFound, "NoSuchUpload": objectstore.ErrNotFound,
		"PreconditionFailed": objectstore.ErrPrecondition, "InvalidRange": objectstore.ErrInvalidRange,
		"InvalidPart": objectstore.ErrInvalidPart, "InvalidPartOrder": objectstore.ErrInvalidPart,
	} {
		got := mapError(&smithy.GenericAPIError{Code: code, Message: "failed"})
		if !errors.Is(got, want) {
			t.Fatalf("mapError(%q) = %v, want %v", code, got, want)
		}
	}
}

type fakeUploader struct {
	input    *awss3.PutObjectInput
	body     []byte
	output   *manager.UploadOutput
	err      error
	onUpload func()
}

func (fake *fakeUploader) Upload(_ context.Context, input *awss3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	fake.input = input
	fake.body, _ = io.ReadAll(input.Body)
	if fake.onUpload != nil {
		fake.onUpload()
	}
	return fake.output, fake.err
}

type fakeMultiUploadError struct {
	id  string
	err error
}

func (err fakeMultiUploadError) Error() string    { return "multipart upload failed: " + err.err.Error() }
func (err fakeMultiUploadError) Unwrap() error    { return err.err }
func (err fakeMultiUploadError) UploadID() string { return err.id }

type fakeClient struct {
	getInput         *awss3.GetObjectInput
	getOutput        *awss3.GetObjectOutput
	headInput        *awss3.HeadObjectInput
	headOutput       *awss3.HeadObjectOutput
	headOutputs      []*awss3.HeadObjectOutput
	listInput        *awss3.ListObjectsV2Input
	listOutput       *awss3.ListObjectsV2Output
	deleteInput      *awss3.DeleteObjectInput
	createInput      *awss3.CreateMultipartUploadInput
	createOutput     *awss3.CreateMultipartUploadOutput
	uploadPartInput  *awss3.UploadPartInput
	uploadPartOutput *awss3.UploadPartOutput
	completeInput    *awss3.CompleteMultipartUploadInput
	completeOutput   *awss3.CompleteMultipartUploadOutput
	abortInput       *awss3.AbortMultipartUploadInput
	abortOutput      *awss3.AbortMultipartUploadOutput
	abortContextErr  error
	listPartsInput   *awss3.ListPartsInput
	listPartsOutput  *awss3.ListPartsOutput
	listPartsErr     error
	aborted          bool
}

func (fake *fakeClient) GetObject(_ context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	fake.getInput = input
	return fake.getOutput, nil
}
func (fake *fakeClient) HeadObject(_ context.Context, input *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	fake.headInput = input
	if len(fake.headOutputs) > 0 {
		output := fake.headOutputs[0]
		fake.headOutputs = fake.headOutputs[1:]
		return output, nil
	}
	return fake.headOutput, nil
}

func (fake *fakeClient) DeleteObject(_ context.Context, input *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	fake.deleteInput = input
	return &awss3.DeleteObjectOutput{}, nil
}
func (fake *fakeClient) ListObjectsV2(_ context.Context, input *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	fake.listInput = input
	return fake.listOutput, nil
}
func (fake *fakeClient) CreateMultipartUpload(_ context.Context, input *awss3.CreateMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	fake.createInput = input
	return fake.createOutput, nil
}
func (fake *fakeClient) UploadPart(_ context.Context, input *awss3.UploadPartInput, _ ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	fake.uploadPartInput = input
	return fake.uploadPartOutput, nil
}
func (fake *fakeClient) CompleteMultipartUpload(_ context.Context, input *awss3.CompleteMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	fake.completeInput = input
	return fake.completeOutput, nil
}
func (fake *fakeClient) AbortMultipartUpload(ctx context.Context, input *awss3.AbortMultipartUploadInput, _ ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	fake.abortInput = input
	fake.abortContextErr = ctx.Err()
	fake.aborted = true
	return fake.abortOutput, nil
}
func (fake *fakeClient) ListParts(_ context.Context, input *awss3.ListPartsInput, _ ...func(*awss3.Options)) (*awss3.ListPartsOutput, error) {
	fake.listPartsInput = input
	if fake.aborted {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "aborted"}
	}
	return fake.listPartsOutput, fake.listPartsErr
}

func ptr[T any](value T) *T { return &value }

func deref[T comparable](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}
