# ADR 0005: Error semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002 |

## Context

Recorded retrospectively for the first public release. Callers of a
backend-neutral store must react to the same failure the same way on every
provider: retry a lost compare-and-swap, treat a missing object as absent,
reject a bad range. Each provider reports these conditions differently
(S3 error codes, GCS `googleapi.Error` and gRPC status codes, Azure
`bloberror` codes, SFTP status codes, HTTP statuses).

## Decision drivers

- D1: One portable vocabulary, testable with `errors.Is`.
- D2: The original provider error stays visible for diagnosis.
- D3: An error must not match two classifications at once; a reclassified
  error (for example a precondition failure during `PutPart`, reported as
  `ErrInvalidPart`) must not also match the original class.
- D4: Cancellation stays recognisable as the context's error.

## Considered options

1. **Sentinel errors in the root package; backends map provider errors onto
   exactly one sentinel and keep the provider error as text**
   - Pro: portable; `errors.Is` works everywhere; no double matches.
   - Con: `errors.As` cannot reach the provider error of a classified
     failure.
2. **Sentinels wrapping the provider error as well (`%w: %w`)**
   - Pro: `errors.As` reaches provider types.
   - Con: a reclassified error matches both sentinels; provider error types
     become de-facto API.
3. **A typed error struct with a `Kind` field**
   - Pro: one type to inspect.
   - Con: needs `errors.As` boilerplate; sentinels compose better with
     wrapping by callers.

## Decision

We will use option 1:

- The root package defines `ErrNotFound`, `ErrPrecondition`,
  `ErrInvalidBucket`, `ErrInvalidKey`, `ErrInvalidRange` and
  `ErrInvalidPart`. Every Store, MultipartStore and capability method
  reports these conditions with them, wrapped with `%w` and detail.
- The SDK backends (S3, GCS, Azure) classify provider errors in one
  `mapError` function each; the SFTP and HTTP backends map file-system and
  HTTP statuses where they occur (the HTTP store maps 404 and 410 to
  `ErrNotFound`, 412 to `ErrPrecondition` and 416 to `ErrInvalidRange`).
  A classified error is `fmt.Errorf("%w: %v", sentinel, cause)`: exactly one
  sentinel, the cause as text. These sites carry a
  `//nolint:errorlint` comment that cites this ADR.
- Provider errors that match no class are returned unchanged.
- `context.Canceled` and `context.DeadlineExceeded` are returned as they
  are, never classified.
- Packages and backends add sentinels only for conditions outside the core
  vocabulary: `cas.ErrInvalidDigest`, `cas.ErrDigestMismatch`,
  `pack.ErrInvalidPack`, `pack.ErrDuplicateDigest`,
  `handoff.ErrDigestMismatch`, `handoff.ErrInvalidReceipt`,
  `azure.ErrInvalidMetadata`, `azure.ErrPresignUnavailable`,
  `http.ErrNotSupported` and `http.ErrReadOnly` (which wraps
  `ErrNotSupported`), plus the typed `*http.StatusError` for unexpected HTTP
  statuses.
- `lease` reports through the `distributed-coordination` vocabulary
  (`ErrInvalidRequest`, `ErrNotAcquired`, `ErrLeaseLost`).
- Batch capabilities report per-key failures in their result slices and
  reserve the error return for whole-call failures.

## Consequences

### Positive
- Portable error handling with `errors.Is` across all backends.
- Provider detail remains in the message.

### Negative
- Callers that need provider error types for a classified failure cannot
  get them through `errors.As`.

### Follow-ups
- If callers need the provider cause, a new ADR can add it (for example
  through an `Unwrap() []error` that excludes sentinels).

## Links

- `errors.go`; `mapError` in `backend/s3`, `backend/gcs`, `backend/azure`
- Tests: `TestMapsS3Errors`, `TestMapError` (gcs, azure)
