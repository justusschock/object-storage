# go-object-storage

`go-object-storage` provides backend-neutral object storage contracts and reusable implementations for conditional writes, exact ranged reads, multipart uploads, SHA-256 content addressing, immutable pack files, object-storage leases, and lifecycle handoff artifacts.

The library requires Go 1.25 or newer (its tests use `testing/synctest`). Its public contracts contain no cloud-provider, credential, or product resource types.

## Installation

The root module contains the neutral API, the memory and filesystem backends, and the reusable storage layers:

```bash
go get github.com/justusschock/go-object-storage@v0.3.0
```

The AWS SDK v2 adapter is isolated in a nested module so root consumers do not acquire AWS dependencies:

```bash
go get github.com/justusschock/go-object-storage/backend/s3@v0.3.0
```

These are private modules. Configure standard Git authentication and set `GOPRIVATE=github.com/justusschock/*` and `GONOSUMDB=github.com/justusschock/*` before downloading them. The modules contain no `replace` directives and do not require a committed `go.work`.

## Basic use

The following complete program uses the in-memory reference backend, writes an immutable object, then reads an exact range. The same `Store` and `MultipartStore` contracts are implemented by the filesystem and S3 adapters.

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

## Verification

Run the root module checks:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Run the isolated adapter checks. Unit and conformance tests run in-process against [gofakes3](https://github.com/johannesboyne/gofakes3), with a small test handler that adds the S3 conditional requests and `UploadPartCopy` that gofakes3 lacks:

```bash
cd backend/s3
go test ./...
go test -race ./...
go vet ./...
go test -tags=integration ./...
```

The tagged integration conformance test reads only `S3_ENDPOINT`, `S3_REGION`, and `S3_BUCKET`. Credentials come from the caller-configured AWS credential provider; the test does not accept or log raw access-key environment variables. It skips when the non-secret settings or provider credentials are unavailable and cleans up its run-scoped objects and recorded multipart uploads.

`backend/s3` requires the root module release tagged from the same commit. Before that tag exists, `scripts/with-root-candidate.sh` serves the checked-out commit as the required version from an isolated mirror, which `scripts/verify-release-modules.sh` and CI use to verify the nested module.

GitHub Actions runs module download/verification, tidy cleanliness, unit and integration compilation, race tests, vet, filesystem-backend cross-compilation, exact private dependency checks, and dependency-boundary checks on Go 1.25.x through 1.27.x.

## License

Apache License 2.0. See [LICENSE](LICENSE).
