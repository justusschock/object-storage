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
- The nested modules are themselves consumers of the root module: they use
  `ValidateBucket`, `ValidateKey`, `PutOptions.ValidateSize`,
  `Object.Digest`, the `Clone` methods and the conformance suite.
- The conformance suite was `internal/storetest`, so implementers outside
  this repository could not run it; at least one downstream project kept a
  vendored copy and a test that compared it with the upstream file.

## Decision drivers

- D1: The public surface is exactly what users and implementers need.
- D2: No consumer breaks, including the nested modules built against the
  released root.
- D3: Every public identifier is documented and tested.

## Considered options

1. **Keep the existing surface and publish the conformance suite; the nested
   modules import it from the root release that contains it**
   - Pro: implementers get the suite; nothing breaks; one import path.
   - Con: the suite becomes public API that must stay stable.
2. **Keep the suite internal**
   - Pro: no new API to keep stable.
   - Con: third-party backends cannot prove conformance; downstream copies
     drift.
3. **Publish the suite and keep a forwarding `internal/storetest`**
   - Pro: nested modules could keep requiring an older root release.
   - Con: two import paths for the suite; a shim to delete later.
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

Internal: none. There is no `internal/` package; implementation details are
unexported.

Changes made for the release:

- Moved `internal/storetest` to the public `storetest` (`Run`, `Factory`,
  `StreamingSize`); the nested modules' conformance tests import it.
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
- `storetest` is public API: changing it follows the same compatibility
  rules as the contract.

### Follow-ups
- None.

## Links

- `storetest/`
- CHANGELOG `[0.1.0]`
