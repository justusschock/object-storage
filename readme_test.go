package objectstore_test

import (
	"os"
	"strings"
	"testing"
)

func TestREADMEReleaseContract(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(data)
	for _, required := range []string{
		"Go 1.22",
		"go get github.com/justusschock/go-object-storage@v0.2.0",
		"go get github.com/justusschock/go-object-storage/backend/s3@v0.2.0",
		"memory.New()", "PutOptions", "ByteRange", "IfAbsent", "SHA-256",
		"s3adapter.New", "manager.NewUploader", "S3_ENDPOINT", "S3_REGION", "S3_BUCKET",
		"go test -race ./...", "go test -tags=integration ./...",
	} {
		if !strings.Contains(readme, required) {
			t.Errorf("README is missing %q", required)
		}
	}
	if strings.Contains(readme, "Initial repository scaffold") {
		t.Error("README still describes the initial scaffold")
	}
}
