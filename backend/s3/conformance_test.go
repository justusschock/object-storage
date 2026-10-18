package s3

import (
	"testing"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/internal/storetest"
)

// TestConformance runs the complete store contract, including every optional
// capability, against an in-process gofakes3 endpoint. The 5 MiB part size
// drives the streaming and spooling multipart paths with the contract's
// 11 MiB bodies.
// FR-OBJ-13, FR-OBJ-19
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		return newFakeS3(t).store(t, Options{PartSize: 5 << 20, Concurrency: 2})
	})
}
