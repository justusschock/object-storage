package objectstore_test

import (
	"testing"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/memory"
	"github.com/justusschock/object-storage/storetest"
)

// FR-OBJ-01, FR-OBJ-02, FR-OBJ-03, FR-OBJ-04, FR-OBJ-05, FR-OBJ-06, FR-OBJ-07, FR-OBJ-10, FR-OBJ-19
func TestMemoryConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) objectstore.MultipartStore {
		return memory.New()
	})
}
