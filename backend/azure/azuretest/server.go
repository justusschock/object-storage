// Package azuretest provides a minimal in-process Azure Blob Storage REST
// endpoint for tests.
//
// The fake implements exactly the subset of the Blob service REST API that
// the azblob SDK issues for the go-object-storage Azure adapter, with
// Azure's status codes, x-ms-error-code headers, and XML error bodies:
//
//   - Create Container (PUT ?restype=container);
//   - List Blobs (GET ?restype=container&comp=list): flat listings with
//     prefix, startFrom, marker/maxresults paging (NextMarker), and
//     include=metadata;
//   - Put Blob (block blobs only), Get Blob (Range and x-ms-range, 206 with
//     Content-Range, truncation of ranges extending past the end, 416
//     InvalidRange when the offset is at or beyond the size), Get Blob
//     Properties (HEAD; errors carry no body), Set Blob Properties
//     (?comp=properties, content type), Set Blob Metadata (?comp=metadata),
//     and Delete Blob;
//   - Put Block, Put Block List (Committed/Uncommitted/Latest), and Get Block
//     List, with per-blob uncommitted block storage, the equal block-ID
//     length rule, and Azure's discarding of uncommitted blocks whenever a
//     blob is written or deleted;
//   - Copy Blob from a source in the same fake account, completed
//     synchronously (x-ms-copy-status: success);
//   - conditional headers If-Match, If-None-Match (including *),
//     If-Modified-Since, If-Unmodified-Since, and x-ms-source-if-match /
//     x-ms-source-if-none-match on copies, evaluated atomically under one
//     mutex;
//   - x-ms-meta-* metadata (names must be C# identifiers and are stored
//     lower-cased), Content-MD5 and x-ms-content-crc64 transactional
//     verification;
//   - Shared Key authentication, verified by recomputing the Blob service
//     string-to-sign exactly as the azblob SDK computes it (canonicalized
//     x-ms-* headers in the SDK's collation order, path-style canonicalized
//     resource, Content-Length "0" signed as empty) with a 15 minute clock
//     skew window; and service SAS (sr=b and sr=c, Shared Key signed),
//     verified by recomputing the signature from the query parameters and
//     checking st/se against the clock and sp against the operation.
//
// URLs are path-style like Azurite: http://127.0.0.1:port/<account>/
// <container>/<blob>. Account SAS, user delegation SAS, leases, snapshots,
// versions, tags, page and append blobs, hierarchical listings
// (delimiter), and Blob Batch are not implemented and fail with
// Azure-style errors.
package azuretest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const (
	// AccountName is the storage account served by the fake: the well-known
	// Azurite development account.
	AccountName = "devstoreaccount1"
	// AccountKey is the base64 Shared Key of AccountName: the well-known,
	// public Azurite development key.
	AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// Server is a running fake Blob service bound to a loopback port.
type Server struct {
	http       *httptest.Server
	service    *service
	credential *azblob.SharedKeyCredential
}

// NewServer starts a fake Blob service for AccountName/AccountKey, creates
// the given containers, and closes the server when tb finishes.
func NewServer(tb testing.TB, containers ...string) *Server {
	tb.Helper()
	credential, err := azblob.NewSharedKeyCredential(AccountName, AccountKey)
	if err != nil {
		tb.Fatal(err)
	}
	svc, err := newService(AccountName, AccountKey)
	if err != nil {
		tb.Fatal(err)
	}
	server := &Server{http: httptest.NewServer(svc), service: svc, credential: credential}
	tb.Cleanup(server.Close)
	for _, name := range containers {
		if err := server.CreateContainer(name); err != nil {
			tb.Fatal(err)
		}
	}
	return server
}

// Endpoint returns the service URL to pass to azblob client constructors,
// for example "http://127.0.0.1:53817/devstoreaccount1/".
func (server *Server) Endpoint() string {
	return server.http.URL + "/" + AccountName + "/"
}

// Credential returns a Shared Key credential for AccountName/AccountKey.
func (server *Server) Credential() *azblob.SharedKeyCredential {
	return server.credential
}

// NewClient returns an azblob client for Endpoint authenticated with
// Credential, using the server's HTTP client as transport.
func (server *Server) NewClient(tb testing.TB) *azblob.Client {
	tb.Helper()
	client, err := azblob.NewClientWithSharedKeyCredential(server.Endpoint(), server.credential, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: server.http.Client()},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return client
}

// CreateContainer creates a container directly, without a request. It fails
// when the name is invalid or the container exists.
func (server *Server) CreateContainer(name string) error {
	status, code := server.service.createContainer(name)
	if status != http.StatusCreated {
		return fmt.Errorf("azuretest: create container %q: %d %s", name, status, code)
	}
	return nil
}

// Close shuts the server down. It is safe to call more than once.
func (server *Server) Close() {
	server.http.Close()
}

// NewClient starts a server with the given containers (see NewServer) and
// returns an authenticated client for it together with its credential.
func NewClient(tb testing.TB, containers ...string) (*azblob.Client, *azblob.SharedKeyCredential) {
	tb.Helper()
	server := NewServer(tb, containers...)
	return server.NewClient(tb), server.Credential()
}
