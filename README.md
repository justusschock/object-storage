# go-object-storage

`go-object-storage` provides backend-neutral object storage contracts and reusable implementations for conditional writes, exact ranged reads, multipart uploads, SHA-256 content addressing, immutable pack files, object-storage leases, and lifecycle handoff artifacts.

The library requires Go 1.25 or newer (its tests use `testing/synctest`). Its public contracts contain no cloud-provider, credential, or product resource types.

## Installation

The root module contains the neutral API, the memory and filesystem backends, and the reusable storage layers:

```bash
go get github.com/justusschock/go-object-storage@v0.3.0
```

Each remote backend is isolated in a nested module so root consumers do not acquire cloud SDK or SSH dependencies:

```bash
go get github.com/justusschock/go-object-storage/backend/s3@v0.3.0    # AWS SDK v2, S3-compatible stores
go get github.com/justusschock/go-object-storage/backend/gcs@v0.3.0   # Google Cloud Storage
go get github.com/justusschock/go-object-storage/backend/azure@v0.3.0 # Azure Blob Storage
go get github.com/justusschock/go-object-storage/backend/sftp@v0.3.0  # SFTP over SSH
go get github.com/justusschock/go-object-storage/backend/http@v0.3.0  # read-only HTTP(S)
```

All nested modules require the root `v0.3.0` release.

These are private modules. Configure standard Git authentication and set `GOPRIVATE=github.com/justusschock/*` and `GONOSUMDB=github.com/justusschock/*` before downloading them. The modules contain no `replace` directives and do not require a committed `go.work`.

## Basic use

The following complete program uses the in-memory reference backend, writes an immutable object, then reads an exact range. The same `Store` and `MultipartStore` contracts are implemented by the filesystem, S3, GCS, Azure, and SFTP adapters.

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
	defer body.Close()

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

`ByteRange` is half-open—`[Offset, Offset+Length)`—and implementations return the requested bytes exactly or `ErrInvalidRange`. Returned objects, options, pages, and metadata maps own their mutable storage.

## Digests

A supplied `PutOptions.SHA256` digest is checked before publication, and `PutOptions.Size` (when positive) is the exact expected length; both fail with `ErrPrecondition`. Objects report their digest only when it is known cheaply: `Object.SHA256Known` is false for objects written by other tools, for `List` results without `ListOptions.IncludeMetadata`, and for multipart uploads completed without a declared digest. Backends never download an object to compute a digest for `Stat`, `Get`, or `List`; `Object.Digest()` returns the digest and whether it is known. The `cas` package keeps verifying full reads of its own content-addressed objects and accepts unknown-digest objects at a content address only on the strength of the address, still verifying every byte it streams.

## Compare-and-swap

`IfAbsent` and `IfMatchVersion` are atomic write conditions and form the portable compare-and-swap primitive: read an object's `Version`, then write with `IfMatchVersion` set to it; `ErrPrecondition` means another writer won. Versions identify stored contents (S3 entity tags repeat for identical bytes), so encode a changing value such as a counter or token in CAS-managed objects. The conformance suite hammers concurrent CAS increments against every backend.

## Optional capabilities

Backends advertise extra operations through interfaces discovered with type assertions:

- `BatchStater` — `StatMany` returns one `StatResult` per key (S3: bounded-parallel HEAD requests).
- `BatchDeleter` — `DeleteMany` deletes many keys idempotently with per-key errors (S3: `DeleteObjects` in batches of 1,000).
- `Copier` — `Copy` copies server-side with source-version and destination conditions, preserving a known digest (S3: `CopyObject`, or `UploadPartCopy` above 5 GiB; filesystem: hard links).
- `Presigner` — `PresignGet`, `PresignPut`, and `PresignUploadPart` return a `PresignedRequest` (method, URL, required headers, expiry) for direct client transfers.

`WithProgress` and `WithProgressReadCloser` wrap upload bodies and download readers with a `ProgressFunc` callback.

## Backends

| Backend | Module / constructor | CAS (`IfAbsent`, `IfMatchVersion`) | Ranged reads | Multipart | Copy | Presign | Batch delete |
|---|---|---|---|---|---|---|---|
| memory | root, `memory.New()` | yes | yes | yes | yes | no | yes |
| fs | root, `fs.New(directory)` | yes (OS file lock) | yes | yes | yes (hard link) | no | yes |
| s3 | `backend/s3`, `s3adapter.NewWithOptions` | yes (`If-None-Match`/`If-Match`) | yes | yes (native) | yes (`CopyObject`/`UploadPartCopy`) | yes (SigV4) | yes (`DeleteObjects`) |
| gcs | `backend/gcs`, `gcs.New` | yes (generation preconditions) | yes | yes (compose) | yes (rewrite) | yes (V4 signed URLs) | yes (parallel) |
| azure | `backend/azure`, `azure.New` | yes (ETag conditions) | yes | yes (staged blocks) | yes (Copy Blob) | yes (service SAS, shared key) | yes (parallel) |
| sftp | `backend/sftp`, `sftp.New` | yes (lock file; see guarantees) | yes | yes (assembled remotely) | yes (hard link or stream) | no | yes |
| http | `backend/http`, `http.New` | no (read-only) | yes | no | no | no | no |

Every backend implements `BatchStater` (the remote ones with bounded-parallel metadata requests). Every read/write backend passes the full `internal/storetest` contract in-process—remote backends against in-memory test servers that ship as importable packages (`gcstest`, `azuretest`, `sftptest`) so downstream projects can run integration tests without Docker or cloud accounts.

The remote constructors take caller-owned clients, so credentials, endpoints, transports, and retries stay application concerns:

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

## Filesystem store

`fs.New(directory)` opens a store that is safe for concurrent use by many goroutines and many processes sharing one directory. Writes stream into a staging file while hashing and publish by renaming a metadata file that names an immutable data file, so readers never observe partial objects and crashes leave the old or the new version. Conditional writes hold an OS file lock (flock on Unix, `LockFileEx` on Windows), which makes `IfAbsent` and `IfMatchVersion` atomic across processes. Keys map to directories, so empty or `.` key segments are rejected and case-insensitive filesystems fold keys that differ only in case.

## Leases on object storage

`lease.New(store, bucket, prefix, lease.Options{})` implements the `go-distributed-coordination` `Backend` and `Lease` contracts on any store with conditional writes. A lease is one JSON object with the owner, fencing token, and expiry: acquisition creates it with `IfAbsent` or replaces an expired or released lease with `IfMatchVersion`, renewal and release are compare-and-swaps, and release keeps the object so fencing tokens increase strictly across release and expiry. Waiting acquisitions poll (`Options.PollInterval`, default one second) and wake at the holder's expiry. Expiry uses the participants' clocks, which must agree to well within one TTL; protected resources must still check fencing tokens. The backend passes `conformance.Run` with the memory and filesystem stores.

## S3-compatible stores

Construct the optional adapter from a caller-owned AWS SDK v2 client. Credentials, endpoints, retry policy, transport, encryption, IAM, and bucket provisioning remain application responsibilities.

```go
package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3adapter "github.com/justusschock/go-object-storage/backend/s3"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(os.Getenv("S3_REGION")))
	if err != nil {
		panic(err)
	}
	client := awss3.NewFromConfig(cfg, func(options *awss3.Options) {
		if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
			options.BaseEndpoint = &endpoint
			options.UsePathStyle = true
		}
	})
	store, err := s3adapter.NewWithOptions(client, s3adapter.Options{PartSize: 64 << 20, Concurrency: 8})
	if err != nil {
		panic(err)
	}
	_ = store
}
```

The adapter records each object's SHA-256 as metadata when the object is created and streams bodies through bounded memory. Bodies up to `PartSize` are sent with one `PutObject` signed with their SHA-256. Larger bodies with a known digest stream through a multipart upload whose running digest is compared before completion, so mismatches abort without publishing; larger bodies without a digest are hashed in a first pass when seekable or spooled once to a temporary file otherwise. Explicit multipart uploads are created and completed directly on the final key—no staging copy and no re-download—with S3 verifying every part's SHA-256; the whole-object digest is the one declared at `BeginMultipart`, which `multipart.Uploader` computes from its source. Completion is idempotent across lost responses. `List` uses `ListObjectsV2` metadata and issues HEAD requests only for `IncludeMetadata`. `IfAbsent` and `IfMatchVersion` map to `If-None-Match: *` and `If-Match` on `PutObject`, `CompleteMultipartUpload`, `CopyObject`, and `DeleteObject`. `s3adapter.New` remains for compatibility with `feature/s3/manager` uploaders, which AWS has deprecated.

## Google Cloud Storage

`gcs.New(client, gcs.Options{})` adapts a caller-owned `*storage.Client` (`cloud.google.com/go/storage`); a Bucket is a GCS bucket. `Version` is the object generation, `IfAbsent` and `IfMatchVersion` map to `DoesNotExist` and `GenerationMatch` preconditions on uploads, compose, rewrite, and delete, and the SHA-256 is recorded in the `go-object-storage-sha256` metadata key. Put streams through a `storage.Writer` while hashing and checks size and digest before `Close`; a mismatch cancels the upload so it is never finalized. Bodies without a declared digest are hashed in memory up to `BufferSize`, in a first pass when seekable, or spooled once. Get reads attributes, validates the range, then reads that exact generation. Listings always carry metadata. Multipart uploads stage parts as objects under the reserved `gcs.DefaultMultipartPrefix` (hidden from listings, rejected as user keys) and complete with one or more `Compose` calls (32 sources each) under the upload's preconditions; the whole-object digest is the one declared at `BeginMultipart`. `Copy` uses the rewrite API. Presigning uses V4 signed URLs (`BucketHandle.SignedURL`) and needs a service-account identity: `Options.GoogleAccessID` with `PrivateKey` or `SignBytes`, service-account JSON credentials on the client, or IAM `signBlob`. GCS cannot verify a SHA-256 on presigned uploads: the digest is bound as signed metadata and the size via `x-goog-content-length-range`.

`gcstest.NewServer(t, "bucket")` returns a `*storage.Client` for an in-process [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) plus an idempotent cleanup function. A middleware in front of the fake serializes requests and adds the generation preconditions, compose semantics, signed XML uploads, and strictly increasing generations that the fake lacks; presign against it with `gcs.Options{Insecure: true}` and any RSA key.

## Azure Blob Storage

`azure.New(client, azure.Options{})` adapts a caller-owned `*azblob.Client`; a Bucket is an existing container and a Key is a block blob name. `Version` is the ETag; `IfAbsent` and `IfMatchVersion` map to `If-None-Match: *` and `If-Match` on Put Blob, Put Block List, Copy Blob, and Delete Blob. The SHA-256 is recorded in the `goobjectstoragesha256` metadata name. Azure metadata names must be C# identifiers and are case-insensitive, so the adapter rejects other names with `azure.ErrInvalidMetadata` and returns names in lower case; values must be printable ASCII. Bodies up to `BlockSize` use one Put Blob with a transactional Content-MD5; larger or unsized bodies are staged as blocks while hashing and committed with Put Block List only after size and digest match. Multipart uploads stage blocks on the final blob (block IDs derived from the upload ID and part number) behind a marker blob under `.go-object-storage/multipart/`, and commit in place; Azure discards uncommitted blocks when another writer publishes the same key first, which surfaces as `ErrPrecondition` (streamed Put) or `ErrInvalidPart` (multipart). `Copy` uses same-account Copy Blob. Presigning issues service SAS URLs signed with `Options.SharedKey` or the client's shared key (otherwise `azure.ErrPresignUnavailable`). A SAS cannot bind headers, so conditions, metadata, and digests on presigned requests are not enforced by the signature (`IfAbsent` is enforced through create-only permission), and Put Block responses carry no ETag: complete presigned part uploads with an empty `CompletedPart.ETag`.

`azuretest.NewServer(t, "container")` starts a focused in-process Blob REST fake (Put/Get/HEAD/Delete Blob, ranges, flat List Blobs paging, Put Block/Put Block List, synchronous Copy Blob, conditional headers, Content-MD5/CRC64) that verifies Shared Key and service SAS signatures; `Server.NewClient(t)` returns an unmodified SDK client for it, and `azuretest.NewClient(t, "container")` does both in one call. It uses the public Azurite development account (`azuretest.AccountName`, `azuretest.AccountKey`).

## SFTP

`sftp.New(client, root, sftp.Options{})` stores objects below `root` through a caller-owned `*sftp.Client` (`github.com/pkg/sftp`). The layout matches the filesystem store: immutable data files plus a metadata sidecar per key, written to a staging file while hashing and published by renaming the sidecar with `posix-rename@openssh.com` (atomic replace). Servers without that extension fall back to plain rename and, when the target exists, a two-rename sequence that readers tolerate; `Store.AtomicReplace` reports which mode is active. Conditional writes are evaluated under a per-bucket lock file created with `O_EXCL`, heartbeated by its holder and broken as stale after `Options.StaleLockAge` (default one minute) of unchanged contents. They are exact among goroutines sharing one store, and across processes as long as the server implements exclusive create atomically and no live lock is broken as stale; otherwise they are best-effort. Multipart completion streams parts back through the client to assemble them, and `Copy` hard-links data files with `hardlink@openssh.com` when available. SFTP requests are not context-aware: the store checks contexts between requests and on every streamed read. There is no presigning.

`sftptest.NewServer(t)` starts an in-process SSH server on 127.0.0.1 serving a temporary directory over SFTP, with password (`Server.ClientConfig`) and public-key (`Server.PublicKeyClientConfig`) authentication and a `Server.Client(t)` convenience; `sftptest.Options` can disable the OpenSSH extensions or enforce strict SFTP v3 rename to exercise the fallbacks. It is not a chroot and is meant for trusted test code.

## Read-only HTTP(S)

`http.New(baseURL, http.Options{})` reads objects from `<baseURL>/<bucket>/<key>` on any static web server, object-storage website endpoint, or CDN (`Options.OmitBucket` drops the bucket segment, `Options.ObjectURL` replaces the mapping, `Options.Header` and `Options.BearerToken` authenticate). Get issues one GET with an exact Range header and validates the returned Content-Range; servers that ignore Range are handled by skipping to the offset. Stat is one HEAD request; `Version` and `ETag` are the ETag header, `Size` comes from Content-Length, and digests come from this library's S3/GCS/Azure metadata headers or an RFC 9530 `Repr-Digest`. Put and Delete return `http.ErrReadOnly` and List returns `http.ErrNotSupported` (`ErrReadOnly` wraps it); unexpected statuses are `*http.StatusError`. The storetest contract assumes writes, so the module tests the read subset—Get/Stat consistency, conditional reads, exact and invalid ranges, not-found, batch stat, and cancellation—against `http.ServeContent`.

## Packages

- `objectstore`: buckets, keys, immutable object metadata, conditions, ranges, listing, multipart, optional capability, and progress contracts.
- `backend/memory`: race-safe reference backend and conformance implementation.
- `backend/fs`: cross-process-safe filesystem backend.
- `multipart`: bounded parallel `io.ReaderAt` multipart orchestration with whole-object digest verification, ordered completion, and abort-on-failure.
- `cas`: SHA-256 content-addressed storage with fenced duplicate suppression and full-read verification.
- `pack`: deterministic immutable pack/index files with one exact ranged read per entry.
- `lease`: fenced `go-distributed-coordination` leases stored as objects.
- `handoff`: canonical `go-lifecycle/handoff` artifact envelopes stored as objects.
- `backend/s3`: isolated AWS SDK v2 S3-compatible adapter in its own module.
- `backend/gcs` and `backend/gcs/gcstest`: Google Cloud Storage adapter and in-memory GCS test server.
- `backend/azure` and `backend/azure/azuretest`: Azure Blob Storage adapter and in-process Blob REST fake.
- `backend/sftp` and `backend/sftp/sftptest`: SFTP adapter and in-process SSH/SFTP test server.
- `backend/http`: read-only HTTP(S) store.

## Verification

Run the root module checks:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Run the isolated adapter checks in each nested module (`backend/s3`, `backend/gcs`, `backend/azure`, `backend/sftp`, `backend/http`). All tests run in-process without Docker or cloud credentials; S3 tests use [gofakes3](https://github.com/johannesboyne/gofakes3), with a small test handler that adds the S3 conditional requests and `UploadPartCopy` that gofakes3 lacks:

```bash
cd backend/s3
go test ./...
go test -race ./...
go vet ./...
go test -tags=integration ./...
```

The tagged integration conformance test reads only `S3_ENDPOINT`, `S3_REGION`, and `S3_BUCKET`. Credentials come from the caller-configured AWS credential provider; the test does not accept or log raw access-key environment variables. It skips when the non-secret settings or provider credentials are unavailable and cleans up its run-scoped objects and recorded multipart uploads.

The nested modules require one root module release. Before that tag exists, `scripts/with-root-candidate.sh` serves the checked-out commit as the required version from an isolated mirror, which `scripts/verify-release-modules.sh` and CI use to verify every nested module.

GitHub Actions runs module download/verification, tidy cleanliness, unit and integration compilation, race tests, vet, filesystem, SFTP, and HTTP cross-compilation, exact private dependency checks, and dependency-boundary checks for the root module and every nested module on Go 1.25.x through 1.27.x.

## License

Apache License 2.0. See [LICENSE](LICENSE).
