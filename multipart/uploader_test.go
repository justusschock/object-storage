package multipart

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	objectstore "github.com/justusschock/go-object-storage"
)

var (
	errInjected = errors.New("injected part failure")
	errAbort    = errors.New("injected abort failure")
)

func TestUploaderOrdersPartsAndBoundsParallelism(t *testing.T) {
	backend := &recordingMultipart{}
	uploader, err := newUploader(backend, Options{PartSize: 5, MaxParallel: 2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	object, err := uploader.UploadReaderAt(context.Background(), "objects", "key", bytes.NewReader([]byte("abcdefghijkl")), 12, objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if object.Key != "key" || backend.completeCalls != 1 || backend.abortCalls != 0 {
		t.Fatalf("object=%#v complete=%d abort=%d", object, backend.completeCalls, backend.abortCalls)
	}
	if got := partNumbers(backend.completed); !slicesEqual(got, []int{1, 2, 3}) {
		t.Fatalf("completed parts = %v", got)
	}
	if backend.maxActive != 2 {
		t.Fatalf("maximum active parts = %d, want 2", backend.maxActive)
	}
}

func TestUploaderAbortsOnceOnPartFailure(t *testing.T) {
	backend := &recordingMultipart{failPart: 3}
	uploader, err := newUploader(backend, Options{PartSize: 5, MaxParallel: 2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = uploader.UploadReaderAt(context.Background(), "objects", "key", bytes.NewReader([]byte("abcdefghijkl")), 12, objectstore.PutOptions{})
	if !errors.Is(err, errInjected) || backend.completeCalls != 0 || backend.abortCalls != 1 {
		t.Fatalf("error=%v complete=%d abort=%d", err, backend.completeCalls, backend.abortCalls)
	}
}

func TestUploaderJoinsAbortFailure(t *testing.T) {
	backend := &recordingMultipart{failPart: 1, abortErr: errAbort}
	uploader, err := newUploader(backend, Options{PartSize: 5, MaxParallel: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = uploader.UploadReaderAt(context.Background(), "objects", "key", bytes.NewReader([]byte("abc")), 3, objectstore.PutOptions{})
	if !errors.Is(err, errInjected) || !errors.Is(err, errAbort) || backend.abortCalls != 1 {
		t.Fatalf("error=%v abort=%d", err, backend.abortCalls)
	}
}

func TestNewUploaderValidatesOptions(t *testing.T) {
	backend := &recordingMultipart{}
	for _, options := range []Options{
		{PartSize: 5<<20 - 1, MaxParallel: 1},
		{PartSize: 5 << 20, MaxParallel: 0},
	} {
		if _, err := NewUploader(backend, options); err == nil {
			t.Fatalf("NewUploader(%+v) returned nil error", options)
		}
	}
	if _, err := NewUploader(backend, Options{PartSize: 5 << 20, MaxParallel: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestUploaderRejectsPartCountOverflowBeforeBegin(t *testing.T) {
	backend := &recordingMultipart{}
	uploader, err := newUploader(backend, Options{PartSize: 5, MaxParallel: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.UploadReaderAt(context.Background(), "objects", "key", bytes.NewReader(nil), math.MaxInt64, objectstore.PutOptions{}); err == nil {
		t.Fatal("UploadReaderAt returned nil error")
	}
	if backend.beginCalls != 0 {
		t.Fatalf("BeginMultipart calls = %d, want 0", backend.beginCalls)
	}
}

type recordingMultipart struct {
	objectstore.MultipartStore

	mu            sync.Mutex
	active        int
	maxActive     int
	failPart      int
	abortErr      error
	completed     []objectstore.CompletedPart
	beginOptions  objectstore.PutOptions
	beginCalls    int
	completeCalls int
	abortCalls    int
}

func (backend *recordingMultipart) BeginMultipart(_ context.Context, bucket objectstore.Bucket, key objectstore.Key, options objectstore.PutOptions) (objectstore.Upload, error) {
	backend.mu.Lock()
	backend.beginCalls++
	backend.beginOptions = options.Clone()
	backend.mu.Unlock()
	return objectstore.Upload{ID: "upload", Bucket: bucket, Key: key, Options: options.Clone()}, nil
}

func (backend *recordingMultipart) PutPart(ctx context.Context, _ objectstore.Upload, number int, body io.Reader, size int64, digest [32]byte) (objectstore.CompletedPart, error) {
	backend.mu.Lock()
	backend.active++
	if backend.active > backend.maxActive {
		backend.maxActive = backend.active
	}
	backend.mu.Unlock()
	defer func() {
		backend.mu.Lock()
		backend.active--
		backend.mu.Unlock()
	}()

	select {
	case <-ctx.Done():
		return objectstore.CompletedPart{}, context.Cause(ctx)
	case <-time.After(10 * time.Millisecond):
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return objectstore.CompletedPart{}, err
	}
	if number == backend.failPart {
		return objectstore.CompletedPart{}, errInjected
	}
	if int64(len(data)) != size || sha256.Sum256(data) != digest {
		return objectstore.CompletedPart{}, errors.New("invalid part payload")
	}
	return objectstore.CompletedPart{Number: number, ETag: string(rune('a' + number)), Size: size, SHA256: digest}, nil
}

func (backend *recordingMultipart) CompleteMultipart(_ context.Context, upload objectstore.Upload, completed []objectstore.CompletedPart) (objectstore.Object, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.completeCalls++
	backend.completed = append([]objectstore.CompletedPart(nil), completed...)
	return objectstore.Object{Bucket: upload.Bucket, Key: upload.Key}, nil
}

func (backend *recordingMultipart) AbortMultipart(_ context.Context, _ objectstore.Upload) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.abortCalls++
	return backend.abortErr
}

func partNumbers(parts []objectstore.CompletedPart) []int {
	numbers := make([]int, len(parts))
	for index, part := range parts {
		numbers[index] = part.Number
	}
	return numbers
}

func slicesEqual(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestUploaderDeclaresVerifiedWholeObjectDigest(t *testing.T) {
	data := bytes.Repeat([]byte("digest"), 3000)
	backend := &recordingMultipart{}
	uploader, err := newUploader(backend, Options{PartSize: 4096, MaxParallel: 2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.UploadReaderAt(context.Background(), "bucket", "key", bytes.NewReader(data), int64(len(data)), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(data)
	if backend.beginOptions.SHA256 == nil || *backend.beginOptions.SHA256 != want || backend.beginOptions.Size != int64(len(data)) {
		t.Fatalf("BeginMultipart options = %#v, want declared digest and size", backend.beginOptions)
	}

	wrong := sha256.Sum256([]byte("wrong"))
	rejecting := &recordingMultipart{}
	uploader, err = newUploader(rejecting, Options{PartSize: 4096, MaxParallel: 2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.UploadReaderAt(context.Background(), "bucket", "key", bytes.NewReader(data), int64(len(data)), objectstore.PutOptions{SHA256: &wrong}); !errors.Is(err, objectstore.ErrPrecondition) {
		t.Fatalf("UploadReaderAt wrong digest error = %v, want ErrPrecondition", err)
	}
	if rejecting.beginCalls != 0 {
		t.Fatalf("wrong digest began %d uploads", rejecting.beginCalls)
	}
}
