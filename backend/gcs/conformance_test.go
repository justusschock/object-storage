package gcs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"sync"
	"testing"

	objectstore "github.com/justusschock/object-storage"
	"github.com/justusschock/object-storage/backend/gcs/gcstest"
	"github.com/justusschock/object-storage/storetest"
)

// TestConformance runs the complete store contract, including every optional
// capability, against the in-process gcstest server. The 1 MiB chunk and
// buffer sizes drive resumable uploads and the spooling path with the
// contract's 11 MiB bodies. Presigned requests are executed against the fake,
// which does not verify signatures; a generated RSA key signs them.
// FR-OBJ-14, FR-OBJ-19
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		return newTestStore(t, Options{ChunkSize: 1 << 20, BufferSize: 1 << 20, Concurrency: 4})
	})
}

const testAccessID = "signer@test-project.iam.gserviceaccount.com"

var testSigningKey = sync.OnceValue(func() []byte {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
})

// newTestStore returns a store on a fresh gcstest server with the buckets
// "objects" and "objects-copy" and test signing credentials.
func newTestStore(t testing.TB, options Options) *Store {
	t.Helper()
	client, _ := gcstest.NewServer(t, "objects", "objects-copy")
	options.Insecure = true
	if options.GoogleAccessID == "" {
		options.GoogleAccessID = testAccessID
		options.PrivateKey = testSigningKey()
	}
	store, err := New(client, options)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
