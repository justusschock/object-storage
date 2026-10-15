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
		"Go 1.25",
		"go get github.com/justusschock/go-object-storage@v0.3.0",
		"go get github.com/justusschock/go-object-storage/backend/s3@v0.3.0",
		"go get github.com/justusschock/go-object-storage/backend/gcs@v0.3.0",
		"go get github.com/justusschock/go-object-storage/backend/azure@v0.3.0",
		"go get github.com/justusschock/go-object-storage/backend/sftp@v0.3.0",
		"go get github.com/justusschock/go-object-storage/backend/http@v0.3.0",
		"| Backend |", "gcstest.NewServer", "azuretest.NewServer", "sftptest.NewServer", "ErrReadOnly",
		"memory.New()", "PutOptions", "ByteRange", "IfAbsent", "SHA-256", "SHA256Known",
		"IfMatchVersion", "BatchStater", "BatchDeleter", "Copier", "Presigner", "WithProgress",
		"fs.New", "lease.New", "conformance.Run", "testing/synctest", "gofakes3",
		"s3adapter.NewWithOptions", "S3_ENDPOINT", "S3_REGION", "S3_BUCKET",
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
