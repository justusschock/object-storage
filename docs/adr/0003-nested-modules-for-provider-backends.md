# ADR 0003: Nested modules for provider backends, release order and tags

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0004 |

## Context

Recorded retrospectively for the first public release. The provider SDKs
are large: the AWS SDK v2, `cloud.google.com/go/storage` (with gRPC,
OpenTelemetry and protobuf), the Azure SDK and `golang.org/x/crypto/ssh`
with `github.com/pkg/sftp` together add well over a hundred modules to a
build graph. Most consumers need one provider, some need none (they use the
memory or filesystem store and the storage layers). Go resolves the
requirements of every package in a module, so one module containing all
backends would impose every SDK on every consumer.

## Decision drivers

- D1: A consumer of the contract or of one backend acquires only the
  dependencies that backend needs.
- D2: Backends and the contract must stay consistent across releases.
- D3: Releases must be reproducible from tags with standard Go tooling: no
  committed `go.work`, no `replace` directives.

## Considered options

1. **Root module with the contract, the stdlib-only backends and the
   storage layers; one nested module per provider backend**
   - Pro: provider SDKs stay out of the root graph; each backend versions
     independently; consumers pick backends with `go get`.
   - Con: several modules to tag; a backend needs a root release before it
     can use new root API.
2. **Single module with every backend**
   - Pro: one version, one tag, no ordering.
   - Con: every consumer downloads and verifies every SDK.
3. **Separate repositories per backend**
   - Pro: full isolation.
   - Con: contract changes span repositories; the shared conformance suite
     and test servers are harder to keep in lockstep.
4. **Build tags to exclude backends**
   - Pro: one module.
   - Con: build tags do not remove requirements from `go.mod`; the module
     graph is the same.

## Decision

We will keep one repository with these modules:

| Module | Path | Contents |
|---|---|---|
| root | `github.com/justusschock/object-storage` | `objectstore`, `backend/memory`, `backend/fs`, `cas`, `pack`, `multipart`, `lease`, `handoff`, `storetest` |
| `backend/s3` | `…/backend/s3` | AWS SDK v2 adapter |
| `backend/gcs` | `…/backend/gcs` | Cloud Storage adapter and `gcstest` |
| `backend/azure` | `…/backend/azure` | Blob Storage adapter and `azuretest` |
| `backend/sftp` | `…/backend/sftp` | SFTP adapter and `sftptest` |
| `backend/http` | `…/backend/http` | read-only HTTP(S) store |

Each nested module requires a released root version and contains no
`replace` directive; the repository contains no committed `go.work`. CI
enforces both and checks that the root module's dependency graph contains no
provider or SSH SDK.

**Tags.** The root module is tagged `vX.Y.Z`; a nested module is tagged with
its directory as prefix, `backend/<name>/vX.Y.Z` (for example
`backend/s3/v0.1.0`), as the Go module system requires. Nested modules version
independently of the root and of each other.

**Release order.**

1. Tag the root module `vX.Y.Z` on `main`.
2. In a pull request, raise every nested module that needs the new root to
   `require github.com/justusschock/object-storage vX.Y.Z`, run
   `go mod tidy` in it, and merge.
3. Tag each changed nested module `backend/<name>/vA.B.C` on the merge commit.

CI tests pull requests with a temporary workspace over all six modules, so
backends are tested against the checked-out root. On release tags CI tests
every module without a workspace, exactly as consumers resolve it.

The first public release tagged the root and every nested module `v0.1.0` on
one commit. The nested modules require root `v0.1.0`; their `go.sum` entries
for it were computed from that commit before it was pushed, which is valid
because a module zip never contains nested modules.

## Consequences

### Positive
- Root consumers have four direct dependencies, all sibling libraries.
- A backend fix ships without a root release.

### Negative
- A change that spans the root and a backend needs two releases in order.
- Between steps 1 and 2 a backend may lag the root; until then it can only
  use API of the root version it requires.

### Follow-ups
- None.

## Links

- [Go modules: multi-module repositories](https://go.dev/doc/modules/managing-source#multiple-module-source)
- `.github/workflows/ci.yml`
