package memory_test

import (
	"testing"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/memory"
	"github.com/justusschock/go-object-storage/storetest"
)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) objectstore.MultipartStore {
		return memory.New()
	})
}
