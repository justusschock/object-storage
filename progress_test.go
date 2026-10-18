package objectstore_test

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	objectstore "github.com/justusschock/object-storage"
)

// FR-OBJ-09
func TestWithProgressReportsCumulativeBytes(t *testing.T) {
	t.Parallel()

	var reports [][2]int64
	reader := objectstore.WithProgress(strings.NewReader("hello world"), 11, func(transferred, total int64) {
		reports = append(reports, [2]int64{transferred, total})
	})
	buffer := make([]byte, 4)
	var read []byte
	for {
		count, err := reader.Read(buffer)
		read = append(read, buffer[:count]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(read) != "hello world" || len(reports) != 3 || reports[2] != [2]int64{11, 11} || reports[0] != [2]int64{4, 11} {
		t.Fatalf("read %q with reports %v", read, reports)
	}
	if plain := strings.NewReader("x"); objectstore.WithProgress(plain, 1, nil) != io.Reader(plain) {
		t.Fatal("nil progress function wrapped the reader")
	}

	closed := false
	body := objectstore.WithProgressReadCloser(closeRecorder{Reader: strings.NewReader("abc"), closed: &closed}, -1, func(transferred, total int64) {
		if total != -1 {
			t.Errorf("total = %d, want -1", total)
		}
	})
	if data, err := io.ReadAll(body); err != nil || string(data) != "abc" {
		t.Fatalf("ReadAll = (%q, %v)", data, err)
	}
	if err := body.Close(); err != nil || !closed {
		t.Fatalf("Close = %v, closed %v", err, closed)
	}
}

type closeRecorder struct {
	io.Reader
	closed *bool
}

func (recorder closeRecorder) Close() error {
	*recorder.closed = true
	return nil
}

// FR-OBJ-05
func TestObjectDigestAndOptionHelpers(t *testing.T) {
	t.Parallel()

	digest := [32]byte{7}
	if got, ok := (objectstore.Object{SHA256: digest}).Digest(); ok || got != ([32]byte{}) {
		t.Fatalf("unknown Digest() = (%x, %v)", got, ok)
	}
	if got, ok := (objectstore.Object{SHA256: digest, SHA256Known: true}).Digest(); !ok || got != digest {
		t.Fatalf("known Digest() = (%x, %v)", got, ok)
	}
	if err := (objectstore.PutOptions{Size: -1}).ValidateSize(); !errors.Is(err, objectstore.ErrInvalidRange) {
		t.Fatalf("ValidateSize(-1) = %v", err)
	}
	if err := (objectstore.PutOptions{Size: 0}).ValidateSize(); err != nil {
		t.Fatalf("ValidateSize(0) = %v", err)
	}

	copyOptions := objectstore.CopyOptions{Metadata: map[string]string{"a": "b"}}
	copyClone := copyOptions.Clone()
	copyClone.Metadata["a"] = "mutated"
	if copyOptions.Metadata["a"] != "b" {
		t.Fatal("CopyOptions.Clone aliases metadata")
	}
	request := objectstore.PresignedRequest{Header: http.Header{"Range": {"bytes=0-1"}}}
	requestClone := request.Clone()
	requestClone.Header.Set("Range", "mutated")
	if request.Header.Get("Range") != "bytes=0-1" {
		t.Fatal("PresignedRequest.Clone aliases headers")
	}
}
