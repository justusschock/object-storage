package azure

import (
	"testing"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/azure/azuretest"
	"github.com/justusschock/object-storage/internal/storetest"
)

// withoutPresign hides the Presigner capability from storetest. The
// storetest presign case requires an ETag header on the presigned part
// upload response, which Azure's Put Block response never carries; the
// Azure presign flow is covered by TestPresign instead.
type withoutPresign struct {
	objectstore.MultipartStore
	objectstore.BatchStater
	objectstore.BatchDeleter
	objectstore.Copier
}

func newTestStore(t *testing.T, options Options) *Store {
	t.Helper()
	server := azuretest.NewServer(t, "objects", "objects-copy")
	store, err := New(server.NewClient(t), options)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// TestConformance runs the complete store contract and the batch and copy
// capabilities against the in-process Azure fake. The 4 MiB block size
// drives the staged-block streaming path with the contract's 11 MiB bodies.
// FR-OBJ-15, FR-OBJ-19
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		store := newTestStore(t, Options{BlockSize: 4 << 20, Concurrency: 2})
		return withoutPresign{MultipartStore: store, BatchStater: store, BatchDeleter: store, Copier: store}
	})
}
