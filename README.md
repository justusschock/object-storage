# go-object-storage

go-object-storage is a Go library of backend-neutral object storage contracts
with exact semantics — atomic conditional writes, exact byte ranges, verified
SHA-256 digests, explicit multipart uploads — and implementations for memory,
local filesystems, S3-compatible services, Google Cloud Storage, Azure Blob
Storage, SFTP servers and read-only HTTP(S) origins. On top of the contract
it provides content-addressed storage, immutable pack files, parallel
multipart uploads, fenced leases and lifecycle artifact handoff. It is for Go
services and tools that keep data or coordination state in object storage and
want the same behaviour, and the same tests, on every backend.

## Features

- One `Store` / `MultipartStore` contract with identical semantics on seven
  backends; optional `BatchStater`, `BatchDeleter`, `Copier` and `Presigner`
  capabilities discovered by type assertion.
- `IfAbsent` and `IfMatchVersion`: atomic create-only writes and
  compare-and-swap on every read/write backend, across processes for the
  filesystem store.
- Declared digests and sizes verified before publication; digests reported
  without downloading objects (`Object.SHA256Known`).
- Streaming writes with bounded memory; exact half-open ranged reads.
- Content-addressed storage with fenced duplicate suppression (`cas`), packs
  with one ranged read per blob (`pack`), bounded-parallel multipart uploads
  (`multipart`), fenced leases (`lease`) and lifecycle artifact envelopes
  (`handoff`) on any store.
- A public conformance suite (`storetest`) and in-process GCS, Azure Blob and
  SFTP servers (`gcstest`, `azuretest`, `sftptest`) for hermetic tests.
- Provider SDKs isolated in nested modules: the root module depends only on
  the standard library and four small sibling libraries.

## Installation

```bash
go get github.com/justusschock/go-object-storage@latest
```

Each remote backend is a nested module, so you add only the SDK you use:

```bash
go get github.com/justusschock/go-object-storage/backend/s3@latest    # AWS SDK v2, S3-compatible stores
go get github.com/justusschock/go-object-storage/backend/gcs@latest   # Google Cloud Storage
go get github.com/justusschock/go-object-storage/backend/azure@latest # Azure Blob Storage
go get github.com/justusschock/go-object-storage/backend/sftp@latest  # SFTP over SSH
go get github.com/justusschock/go-object-storage/backend/http@latest  # read-only HTTP(S)
```

Requires Go 1.25 or newer (the tests use `testing/synctest`).

## Quick start

This program (the package's `Example`) uses the in-memory reference
backend: it writes an immutable object with a create-only condition and a
declared digest, then reads an exact range. The same calls work on every
backend.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	objectstore "github.com/justusschock/go-object-storage"
	"github.com/justusschock/go-object-storage/backend/memory"
)

func main() {
	ctx := context.Background()
	store := memory.New()
	data := []byte("hello object storage")
	digest := sha256.Sum256(data)

	object, err := store.Put(ctx, "assets", "greeting.txt", bytes.NewReader(data), objectstore.PutOptions{
		IfAbsent: true,
		SHA256:   &digest,
		Metadata: map[string]string{"content": "example"},
	})
	if err != nil {
		panic(err)
	}

	body, metadata, err := store.Get(ctx, object.Bucket, object.Key, objectstore.GetOptions{
		Range: &objectstore.ByteRange{Offset: 6, Length: 6},
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = body.Close() }()

	rangeBytes, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s (%d-byte object)\n", rangeBytes, metadata.Size)
}
```

Output:

```text
object (20-byte object)
```

Remote backends take caller-owned clients, so credentials, endpoints,
transports and retries stay under your control:

```go
import (
	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	pkgsftp "github.com/pkg/sftp"

	objectstore "github.com/justusschock/go-object-storage"
	azureadapter "github.com/justusschock/go-object-storage/backend/azure"
	gcsadapter "github.com/justusschock/go-object-storage/backend/gcs"
	httpstore "github.com/justusschock/go-object-storage/backend/http"
	sftpadapter "github.com/justusschock/go-object-storage/backend/sftp"
)

func newStores(gcsClient *storage.Client, azureClient *azblob.Client, sftpClient *pkgsftp.Client, token string) ([]objectstore.Store, error) {
	gcsStore, err := gcsadapter.New(gcsClient, gcsadapter.Options{}) // Bucket = GCS bucket
	if err != nil {
		return nil, err
	}
	azureStore, err := azureadapter.New(azureClient, azureadapter.Options{}) // Bucket = existing container
	if err != nil {
		return nil, err
	}
	sftpStore, err := sftpadapter.New(sftpClient, "/srv/objects", sftpadapter.Options{}) // Bucket = directory below root
	if err != nil {
		return nil, err
	}
	httpStore, err := httpstore.New("https://cdn.example.com/remote", httpstore.Options{BearerToken: token}) // read-only
	if err != nil {
		return nil, err
	}
	return []objectstore.Store{gcsStore, azureStore, sftpStore, httpStore}, nil
}
```

The S3 adapter is constructed the same way with `s3adapter.NewWithOptions(client, s3adapter.Options{})` from an `*s3.Client` of the AWS SDK v2. Each backend's `example_test.go` has a complete program.

## Packages

| Package | Module | Purpose |
|---|---|---|
| `objectstore` (root) | root | Contract: `Store`, `MultipartStore`, optional capabilities, value types, sentinel errors, progress helpers |
| `backend/memory` | root | Race-safe in-memory reference store |
| `backend/fs` | root | Filesystem store, safe across goroutines and processes |
| `storetest` | root | Conformance suite for `Store` implementations |
| `cas` | root | SHA-256 content-addressed storage with fenced duplicate suppression |
| `pack` | root | Deterministic immutable pack and index files, one ranged read per blob |
| `multipart` | root | Bounded-parallel multipart uploads from an `io.ReaderAt` |
| `lease` | root | Fenced `go-distributed-coordination` leases stored as objects |
| `handoff` | root | `go-lifecycle` artifact envelopes stored as objects |
| `backend/s3` | `backend/s3` | AWS SDK v2 adapter for S3-compatible stores |
| `backend/gcs`, `backend/gcs/gcstest` | `backend/gcs` | Google Cloud Storage adapter; in-process GCS server |
| `backend/azure`, `backend/azure/azuretest` | `backend/azure` | Azure Blob Storage adapter; in-process Blob service |
| `backend/sftp`, `backend/sftp/sftptest` | `backend/sftp` | SFTP adapter; in-process SSH/SFTP server |
| `backend/http` | `backend/http` | Read-only HTTP(S) store |

Packages under `internal/` are not part of the API.

### Backend capabilities

| Backend | Constructor | CAS (`IfAbsent`, `IfMatchVersion`) | Multipart | Copy | Presign | Batch delete |
|---|---|---|---|---|---|---|
| memory | `memory.New()` | yes | yes | yes | no | yes |
| fs | `fs.New(directory)` | yes (OS file lock) | yes | yes (hard link) | no | yes |
| s3 | `s3adapter.NewWithOptions` | yes (`If-None-Match`/`If-Match`) | yes (native) | yes (`CopyObject`/`UploadPartCopy`) | yes (SigV4) | yes (`DeleteObjects`) |
| gcs | `gcs.New` | yes (generation preconditions) | yes (compose) | yes (rewrite) | yes (V4 signed URLs) | yes (parallel) |
| azure | `azure.New` | yes (ETag conditions) | yes (staged blocks) | yes (Copy Blob) | yes (service SAS) | yes (parallel) |
| sftp | `sftp.New` | yes (lock file; see below) | yes (assembled remotely) | yes (hard link or stream) | no | yes |
| http | `http.New` | no (read-only) | no | no | no | no |

Every backend supports exact ranged reads and implements `BatchStater`.

## Design and guarantees

- **Immutable versions and compare-and-swap.** `Put` publishes a new version
  atomically. `IfAbsent` and `IfMatchVersion` are evaluated with
  publication; `ErrPrecondition` means another writer won. Versions identify
  contents (S3 entity tags repeat for identical bytes), so encode a changing
  value such as a counter in CAS-managed objects.
- **Integrity.** `PutOptions.SHA256` and a positive `PutOptions.Size` are
  checked before an object becomes visible. `Object.SHA256Known` is false
  when a digest is not available cheaply (objects written by other tools,
  listings without `ListOptions.IncludeMetadata`, multipart uploads without a
  declared digest); backends never download an object to compute one. `cas`,
  `pack` and `handoff` verify the bytes they stream.
- **Ranges.** `ByteRange` is half-open, `[Offset, Offset+Length)`; stores
  return exactly those bytes or `ErrInvalidRange`.
- **Errors.** Failures are classified by `ErrNotFound`, `ErrPrecondition`,
  `ErrInvalidBucket`, `ErrInvalidKey`, `ErrInvalidRange` and
  `ErrInvalidPart`; test them with `errors.Is`. Each error has one class;
  provider detail is in the message. Context errors are returned unchanged.
- **Concurrency and cancellation.** Every store and layer is safe for
  concurrent use. Every operation takes a `context.Context` and honours it;
  `Get` bodies are bound to it. The library never logs.
- **Ownership.** Returned objects, options and pages own their mutable
  fields; `Clone` methods copy them.
- **Filesystem and SFTP.** Writes stream into a staging file and publish by
  renaming a metadata file that names an immutable data file, so readers
  never see partial objects. The filesystem store makes conditions atomic
  across processes with `flock` / `LockFileEx`. The SFTP store uses a
  heartbeated lock file created with `O_EXCL` and broken as stale after
  `sftp.Options.StaleLockAge`; its conditions are exact among goroutines
  sharing one store and across processes when the server implements
  exclusive create atomically.
- **Leases.** `lease.New(store, bucket, prefix, lease.Options{})` grants
  fenced leases with strictly increasing tokens. Expiry uses the
  participants' clocks, which must agree to well within one TTL; protected
  resources must still check fencing tokens. The backend passes the
  `go-distributed-coordination` `conformance.Run` suite.
- **Stability.** The module is `v0.x`: a minor release may change the API.
  Every change is listed in [CHANGELOG.md](CHANGELOG.md) and backed by an
  ADR.

### Testing your own backend

```go
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.MultipartStore {
		return newEmptyStore(t) // must accept the buckets "objects" and "objects-copy"
	})
}
```

`gcstest.NewServer`, `azuretest.NewServer` and `sftptest.NewServer` give
downstream tests real SDK clients against in-process servers, without Docker
or cloud accounts. The S3 module's tests use gofakes3; an opt-in conformance
run against a real endpoint is behind `-tags=integration` and reads
`S3_ENDPOINT`, `S3_REGION` and `S3_BUCKET` (credentials come from the default
AWS provider chain).

## Documentation

- API reference: [pkg.go.dev/github.com/justusschock/go-object-storage](https://pkg.go.dev/github.com/justusschock/go-object-storage)
  and the nested modules under `backend/`.
- Requirements and design decisions: [docs/](docs/README.md) — the PRD and
  the ADRs on the store contract, module layout, dependencies, errors,
  concurrency, persistence formats, conformance testing, API boundary, Go
  versions and testing.

## Contributing

Changes go through pull requests with green CI; see [AGENTS.md](AGENTS.md)
for the rules (docs first, ADRs for API and format changes, synctest, the
pre-PR checks for every module) and the release procedure for the nested
modules.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
