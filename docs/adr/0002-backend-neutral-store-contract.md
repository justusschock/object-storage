# ADR 0002: Backend-neutral store contract and package shape

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0003, ADR-0005, ADR-0006 |

## Context

Recorded retrospectively for the first public release. Applications store
artifacts on local disks, S3-compatible services, Google Cloud Storage, Azure
Blob Storage, SFTP servers and static HTTP origins. Each provider SDK has its
own types, conditional-write mechanism, version identifiers and error
values. The storage layers in this repository (content addressing, packs,
leases, lifecycle handoff, multipart orchestration) and their users must be
written once against a contract every provider can honour exactly.

## Decision drivers

- D1: One contract that every supported provider implements without
  emulation that weakens its guarantees.
- D2: Compare-and-swap must be expressible portably, because leases and
  content addressing depend on it.
- D3: Providers differ in what they can do beyond the core (server-side copy,
  presigning, batch calls); callers must be able to use those features
  without the core growing provider-specific methods.
- D4: Values crossing the API must not alias caller or backend storage.
- D5: Small, stable surface for a `v0` library.

## Considered options

1. **Small interfaces (`Store`, `MultipartStore`) over plain value types, with
   optional capability interfaces discovered by type assertion**
   - Pro: the core is the intersection all providers support; extra features
     are opt-in and testable per backend; adding a capability does not break
     existing implementations.
   - Con: callers must type-assert and handle absence; capabilities are not
     visible in the `Store` type.
2. **One large interface with every operation, returning `ErrNotSupported`
   where unavailable**
   - Pro: one type to learn.
   - Con: every implementation must stub every method; adding an operation
     breaks all third-party implementations; support is discovered at run
     time through errors.
3. **Generic store over a provider-specific handle (`Store[C any]`)**
   - Pro: typed access to provider features.
   - Con: leaks provider types into callers, which defeats backend
     neutrality; no generic code in the repository needs the parameter.
4. **Expose provider clients directly and share only helpers**
   - Pro: no abstraction cost.
   - Con: every consumer re-implements conditional writes, digests and
     error mapping per provider.

## Decision

We will define the contract in the root package `objectstore` as:

- `Store` with `Put`, `Get`, `Stat`, `Delete` and `List`, and
  `MultipartStore`, which adds `BeginMultipart`, `PutPart`,
  `CompleteMultipart` and `AbortMultipart`. Every method takes a
  `context.Context` first.
- Plain value types: `Bucket` and `Key` (string types validated by
  `ValidateBucket` and `ValidateKey`), `Object`, `PutOptions`, `GetOptions`,
  `ListOptions`, `Page`, `ByteRange`, `Upload`, `CompletedPart`,
  `CopyOptions`, `StatResult` and `PresignedRequest`. Configuration is an
  options struct whose zero value is the default, not functional options.
  Every type with reference fields has a `Clone` method, and backends return
  values the caller owns.
- Optional capabilities `BatchStater`, `BatchDeleter`, `Copier` and
  `Presigner`, discovered with type assertions and keeping the core error
  vocabulary.
- Objects are immutable versions. `PutOptions.IfAbsent` and
  `PutOptions.IfMatchVersion` are the portable compare-and-swap; `ByteRange`
  is half-open and served exactly or rejected; listings are key-ordered and
  paged with `After` and `NextAfter`.
- A declared `PutOptions.SHA256` and positive `PutOptions.Size` are verified
  before publication. `Object.SHA256Known` reports whether a digest is
  known; backends never download an object to compute one.
- Storage layers are separate packages that depend only on the contract:
  `cas`, `pack`, `multipart`, `lease` and `handoff`. Backends live under
  `backend/` and expose a concrete `*Store` with a `New` constructor and,
  where needed, an `Options` struct.
- No generics in the contract. Generics appear only where a sibling library
  defines them (`cas.New` accepts a `go-cache` `Cache[string, Object]`).

## Consequences

### Positive
- Every backend passes one conformance suite (ADR-0008).
- New capabilities can be added without breaking implementations.
- Storage layers work unchanged on every backend, including test doubles.

### Negative
- Provider features outside the contract (storage classes, tags, lifecycle
  rules, bucket provisioning) are not reachable through it; applications use
  the provider client they passed in.
- Versions are opaque strings with provider-specific meaning (S3 entity
  tags repeat for identical bytes), which callers using compare-and-swap must
  account for.

### Follow-ups
- None.

## Links

- `store.go`, `object.go`, `capabilities.go`, `progress.go`
- PRD-0001 FR-OBJ-01 to FR-OBJ-10
