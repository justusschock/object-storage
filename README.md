# go-object-storage

`go-object-storage` provides backend-neutral object storage contracts and reusable implementations for conditional writes, exact ranged reads, multipart uploads, SHA-256 content addressing, immutable pack files, and lifecycle handoff artifacts.

The library requires Go 1.22 or newer. Its public contracts contain no cloud-provider, credential, or product resource types.

## Installation

The root module contains the neutral API and reusable storage layers:

```bash
go get github.com/justusschock/go-object-storage@v0.2.0
```

The AWS SDK v2 adapter is isolated in a nested module so root consumers do not acquire AWS dependencies:

```bash
go get github.com/justusschock/go-object-storage/backend/s3@v0.2.0
```

These are private modules. Configure standard Git authentication and set `GOPRIVATE=github.com/justusschock/*` and `GONOSUMDB=github.com/justusschock/*` before downloading them. The modules contain no `replace` directives and do not require a committed `go.work`.

## Basic use

The following complete program uses the in-memory reference backend, writes an immutable object, then reads an exact range. The same `Store` and `MultipartStore` contracts are implemented by the S3 adapter.

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

`IfAbsent` and `IfMatchVersion` are atomic write conditions. A supplied SHA-256 digest is checked before publication. Returned objects, options, pages, and metadata maps own their mutable storage. `ByteRange` is half-open—`[Offset, Offset+Length)`—and implementations return the requested bytes exactly or `ErrInvalidRange`.

## S3-compatible stores

Construct the optional adapter from caller-owned AWS SDK v2 clients. Credentials, endpoints, retry policy, transport, encryption, IAM, and bucket provisioning remain application responsibilities.

```go
package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
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
	store, err := s3adapter.New(client, manager.NewUploader(client))
	if err != nil {
		panic(err)
	}
	_ = store
}
```

The adapter validates exact `Content-Range` responses, maps portable precondition/not-found/range/part errors, and keeps AWS types out of the root module. Explicit multipart uploads assemble under an internal staging key, verify the complete SHA-256 and byte length, then apply the original conditions when publishing the final key. Failed manager uploads are aborted with a separate bounded cleanup context.

## Packages

- `objectstore`: buckets, keys, immutable object metadata, conditions, ranges, listing, and multipart contracts.
- `backend/memory`: race-safe reference backend and conformance implementation.
- `multipart`: bounded parallel `io.ReaderAt` multipart orchestration with ordered completion and abort-on-failure.
- `cas`: SHA-256 content-addressed storage with fenced duplicate suppression and full-read verification.
- `pack`: deterministic immutable pack/index files with one exact ranged read per entry.
- `handoff`: canonical `go-lifecycle/handoff` artifact envelopes stored as objects.
- `backend/s3`: isolated AWS SDK v2 S3-compatible adapter in its own module.

## Verification

Run the root module checks:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Run the isolated adapter checks:

```bash
cd backend/s3
go test ./...
go test -race ./...
go vet ./...
go test -tags=integration ./...
```

The tagged integration conformance test reads only `S3_ENDPOINT`, `S3_REGION`, and `S3_BUCKET`. Credentials come from the caller-configured AWS credential provider; the test does not accept or log raw access-key environment variables. It skips when the non-secret settings or provider credentials are unavailable and cleans up its run-scoped objects and recorded multipart staging uploads.

GitHub Actions runs module download/verification, tidy cleanliness, unit and integration compilation, race tests, vet, exact private dependency checks, and dependency-boundary checks on Go 1.22.x through 1.26.x.

## License

Apache License 2.0. See [LICENSE](LICENSE).
