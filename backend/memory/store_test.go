package memory_test

import (
	"testing"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/memory"
	"github.com/justusschock/object-storage/storetest"
)

// FR-OBJ-11
func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) objectstore.MultipartStore {
		return memory.New()
	})
}
