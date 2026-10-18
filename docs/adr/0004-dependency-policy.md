# ADR 0004: Dependency policy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0003 |

## Context

Recorded retrospectively for the first public release. Storage code sits low
in application dependency graphs; every module it requires is inherited by
its consumers and must be audited, updated and trusted. The repository
nevertheless has to talk to provider APIs and reuses a few sibling
libraries for leases, caching, concurrency and lifecycle artifacts.

## Decision drivers

- D1: The root module stays small and auditable.
- D2: Provider integrations use the providers' maintained SDKs instead of
  hand-written protocol code, so authentication, retries and signing stay
  correct.
- D3: Tests run hermetically, without Docker or cloud accounts.
- D4: Shared concepts (leases, caches, futures, lifecycle artifacts) are not
  reimplemented when a sibling library already defines the contract.

## Considered options

1. **Standard library first; sibling libraries for shared contracts;
   provider SDKs and their fakes only in the nested backend modules**
   - Pro: the root graph contains only sibling libraries; provider code is
     maintained by the providers.
   - Con: the backends inherit the SDKs' large graphs.
2. **Hand-written HTTP clients for every provider**
   - Pro: tiny dependency graphs.
   - Con: re-implements SigV4, OAuth, SAS and Shared Key signing, retries and
     protocol edge cases; high maintenance and security risk.
3. **A multi-cloud abstraction library underneath**
   - Pro: fewer adapters to write.
   - Con: its semantics (conditional writes, digests, ranges) do not match
     this contract exactly; adds a large dependency to every backend.

## Decision

We will allow dependencies as follows. Adding a dependency to any module
requires a new ADR.

**Root module** — standard library, plus sibling libraries:

| Dependency | Used by | Why |
|---|---|---|
| `github.com/justusschock/go-distributed-coordination` | `lease`, `cas` | `lease` implements its `Backend`/`Lease` contract and passes its conformance suite; `cas` fences duplicate uploads with its leases and `KeepAlive`. |
| `github.com/justusschock/go-cache` | `cas` | optional metadata cache (`cache.Cache[string, objectstore.Object]`). |
| `github.com/justusschock/go-concurrency` | `multipart` | bounded parallel part uploads. |
| `github.com/justusschock/go-lifecycle` | `handoff` | `handoff.Transport` stores its `handoff.Artifact` and `Receipt` types. |

`github.com/justusschock/go-messaging` appears only indirectly.

**Nested modules** — the provider SDK and nothing else at run time:

| Module | Run-time dependencies | Test-only or fake dependencies |
|---|---|---|
| `backend/s3` | `aws-sdk-go-v2` (`service/s3`, `feature/s3/manager` for the legacy `New` constructor), `smithy-go` for error classification | `johannesboyne/gofakes3` (in-process S3), `aws-sdk-go-v2/config` and `credentials` |
| `backend/gcs` | `cloud.google.com/go/storage`, `google.golang.org/api`, `google.golang.org/grpc` (status codes for error classification) | `fsouza/fake-gcs-server`, imported by the public `gcstest` package |
| `backend/azure` | `azure-sdk-for-go/sdk/azcore`, `sdk/storage/azblob` | none: `azuretest` is a hand-written Blob REST fake |
| `backend/sftp` | `github.com/pkg/sftp`, `golang.org/x/crypto/ssh` | none: `sftptest` serves `pkg/sftp` in process |
| `backend/http` | none (standard library) | none |

Constructors take caller-owned SDK clients, so credentials, endpoints,
transports and retry policies remain application concerns and the library
pulls in no credential provider of its own.

## Consequences

### Positive
- `go get` of the root module adds only sibling libraries.
- Fakes run in process; `go test ./...` needs no network beyond loopback.

### Negative
- `gcstest` makes `fake-gcs-server` a dependency of `backend/gcs` for every
  consumer of that module, not only for tests.
- The S3 module keeps `feature/s3/manager`, which AWS has deprecated, for
  the `New` compatibility constructor.

### Follow-ups
- Drop `feature/s3/manager` when `s3adapter.New` is removed in a future
  minor release (with its own ADR).

## Links

- ADR-0003 for the module split.
