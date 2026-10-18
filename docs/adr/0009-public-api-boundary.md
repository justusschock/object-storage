# ADR 0009: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0003, ADR-0008, ADR-0011 |

## Context

Recorded retrospectively for the first public release. Before the release
every package and exported identifier was reviewed and classified as public
contract or implementation detail. Constraints:

- Downstream projects import the root package, `backend/memory`,
  `backend/fs`, `cas`, `lease`, every nested backend and the `azuretest`,
  `gcstest` and `sftptest` fakes. None of that may move or disappear.
- The nested modules are themselves consumers of the root module: they
  require root `v0.3.0` and use `ValidateBucket`, `ValidateKey`,
  `PutOptions.ValidateSize`, `Object.Digest`, the `Clone` methods and the
  internal conformance suite of that release.
- The conformance suite was `internal/storetest`, so implementers outside
  this repository could not run it; at least one downstream project kept a
  vendored copy and a test that compared it with the upstream file.

## Decision drivers

- D1: The public surface is exactly what users and implementers need.
- D2: No consumer breaks, including the nested modules built against the
  released root.
- D3: Every public identifier is documented and tested.

## Considered options

1. **Keep the existing surface, publish the conformance suite, keep a
   forwarding `internal/storetest` until the nested modules move on**
   - Pro: implementers get the suite; nothing breaks; the shim is tiny.
   - Con: two import paths for the suite during the transition.
2. **Keep the suite internal**
   - Pro: no new API to keep stable.
   - Con: third-party backends cannot prove conformance; downstream copies
     drift.
3. **Move the suite and update the nested modules now**
   - Pro: one import path.
   - Con: the nested modules require root `v0.3.0`, which has no public
     `storetest`; their standalone builds would break until a root release
     and a version bump, which is outside this change.
4. **Unexport helpers the backends use (`ValidateBucket`, `ValidateKey`,
   `PutOptions.ValidateSize`, `Object.Digest`)**
   - Pro: smaller root surface.
   - Con: they are part of the contract third-party backends implement
     against, and the released nested modules call them.

## Decision

We will use option 1. The public API is every exported identifier outside
`internal/`:

| Package | Public because |
|---|---|
| `objectstore` (root) | the contract: interfaces, value types, options, sentinel errors, validation and progress helpers |
| `backend/memory`, `backend/fs` | reference and filesystem stores |
| `cas`, `pack`, `multipart`, `lease`, `handoff` | storage layers on any Store |
| `storetest` | conformance suite for implementers (newly public) |
| `backend/s3`, `backend/gcs`, `backend/azure`, `backend/sftp`, `backend/http` | provider adapters (nested modules) |
| `backend/gcs/gcstest`, `backend/azure/azuretest`, `backend/sftp/sftptest` | in-process fakes used by downstream tests (ADR-0008) |

Internal:

| Package | Why |
|---|---|
| `internal/storetest` | forwards to `storetest` for the nested modules until they require a root release that contains it; then deleted |

Changes made for the release:

- Moved `internal/storetest` to the public `storetest` (`Run`, `Factory`,
  `StreamingSize`) and added the forwarding `internal/storetest`.
- No exported identifier was unexported or removed: the review found no
  exported implementation detail. Encoders, lock protocols, error mapping,
  staging and the S3 test handler were already unexported or in test files.
- Removed unexported test-only clock hooks (the `now` fields of the
  filesystem and SFTP stores, the clock parameter of the Azure fake's
  service); tests use `testing/synctest` instead (ADR-0011).
- `lease.Options.Clock` stays: it is the `distributed-coordination`
  `Clock` abstraction, and that library's conformance suite drives time
  through it.

## Consequences

### Positive
- Third-party backends can run `storetest.Run`; downstream projects can drop
  vendored copies.
- No consumer needs a code change.

### Negative
- Two import paths for the suite until the nested modules require the next
  root release.

### Follow-ups
- After the next root release, raise the nested modules' root requirement,
  switch their conformance tests to `storetest`, delete
  `internal/storetest` (ADR-0003 release order).

## Links

- `storetest/`, `internal/storetest/`
- CHANGELOG `[Unreleased]`
