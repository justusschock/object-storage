package sftp

import (
	"testing"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/sftp/sftptest"
	"github.com/justusschock/object-storage/internal/storetest"
)

// TestConformance runs the complete store contract, including every optional
// capability, against an in-process SFTP server with the OpenSSH extensions
// (atomic posix-rename publication and hard-link copies).
// FR-OBJ-16, FR-OBJ-19
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		return newTestStore(t, sftptest.Options{}, Options{})
	})
}

// TestConformanceWithoutExtensions runs the contract against a plain SFTP v3
// server: no extensions and a rename that refuses to replace its target, so
// publication uses the remove-then-rename fallback and Copy streams bytes.
// FR-OBJ-16
func TestConformanceWithoutExtensions(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		return newTestStore(t, sftptest.Options{DisableExtensions: true, StrictRename: true}, Options{})
	})
}

func newTestStore(t *testing.T, serverOptions sftptest.Options, options Options) *Store {
	t.Helper()
	server := sftptest.NewServerWithOptions(t, serverOptions)
	store, err := New(server.Client(t), "store", options)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
