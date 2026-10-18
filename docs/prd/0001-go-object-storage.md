# PRD 0001: go-object-storage

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0001 to ADR-0011 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the library
as implemented.

## 1. Problem and context

Go services that keep artifacts, caches, checkpoints or coordination state
in object storage face six different APIs (local disk, S3-compatible
services, Google Cloud Storage, Azure Blob Storage, SFTP servers, static HTTP
origins), each with its own conditional-write mechanism, version
identifiers, digest support and errors. Code that needs exact semantics
(create-only writes, compare-and-swap, exact byte ranges, verified content)
is rewritten per provider, and tests need either cloud accounts or slow
emulators.

go-object-storage defines one backend-neutral contract with exact semantics,
implements it for those six media, and builds reusable layers on top:
content-addressed storage, immutable packs, multipart orchestration, fenced
leases and lifecycle artifact handoff.

## 2. Users and use cases

- As an application developer, I want to write objects with create-only or
  compare-and-swap conditions and read exact ranges, so that my code behaves
  the same on disk, S3, GCS, Azure and SFTP.
- As an application developer, I want to store content once per SHA-256
  digest even when many processes upload it concurrently, so that
  deduplication needs no database.
- As an application developer, I want many small blobs in one object with
  one ranged read per blob, so that small-object overhead disappears.
- As a distributed-systems developer, I want fenced leases stored in the
  same bucket as my data, so that I need no separate lock service.
- As a backend author, I want a conformance suite, so that I can prove my
  implementation honours the contract.
- As a test author, I want in-process GCS, Azure and SFTP servers, so that
  integration tests run without Docker or credentials.

## 3. Goals and non-goals

### Goals
- G1: One contract (`Store`, `MultipartStore`, optional capabilities) with
  identical semantics on every backend.
- G2: Atomic conditional writes usable as a compare-and-swap primitive.
- G3: Verified integrity: declared digests and sizes are checked before
  publication; digests are reported when known cheaply.
- G4: Bounded memory for arbitrarily large bodies.
- G5: Provider SDKs only in the modules that need them.
- G6: Hermetic, deterministic tests and reusable test infrastructure.

### Non-goals
- NG1: Bucket provisioning, IAM, lifecycle rules, storage classes, object
  tags, versioning APIs — application concerns handled with the provider
  client.
- NG2: Credential discovery — callers pass configured clients.
- NG3: Write support over plain HTTP — HTTP has no portable conditional
  write or listing.
- NG4: Strong cross-process guarantees on media without atomic primitives
  (see the SFTP and filesystem limitations in ADR-0006).

## 4. Functional requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-OBJ-01 | `Store` provides `Put`, `Get`, `Stat`, `Delete` and `List` over immutable object versions addressed by `Bucket` and `Key`; `ValidateBucket` and `ValidateKey` reject empty, NUL-containing, leading-slash, backslash and parent-segment names with `ErrInvalidBucket`/`ErrInvalidKey`. | MUST |
| FR-OBJ-02 | `GetOptions.Range` is the half-open interval `[Offset, Offset+Length)`; stores return exactly those bytes or `ErrInvalidRange`. | MUST |
| FR-OBJ-03 | `List` returns key-ordered pages filtered by `Prefix`, continued with `After`/`NextAfter` and bounded by `Limit`; `IncludeMetadata` makes listed objects carry the fields `Stat` returns. | MUST |
| FR-OBJ-04 | `PutOptions.IfAbsent` and `PutOptions.IfMatchVersion` are evaluated atomically with publication; a lost condition returns `ErrPrecondition`. `Delete` accepts a version precondition. `GetOptions.IfMatchVersion` conditions reads. | MUST |
| FR-OBJ-05 | A non-nil `PutOptions.SHA256` and a positive `PutOptions.Size` are verified before the object becomes visible (`ErrPrecondition` on mismatch). `Object.SHA256Known` reports whether the digest is known; stores never read object bytes to compute it. | MUST |
| FR-OBJ-06 | `MultipartStore` provides `BeginMultipart`, `PutPart`, `CompleteMultipart` and `AbortMultipart`; parts are verified by size and digest, completion requires ascending part numbers (`ErrInvalidPart`), and abort removes staged data. | MUST |
| FR-OBJ-07 | Optional capabilities are discovered by type assertion: `BatchStater.StatMany` and `BatchDeleter.DeleteMany` return one result per key in input order; `Copier.Copy` copies server-side with source-version and destination conditions; `Presigner` issues time-limited `PresignedRequest`s for get, put and part upload. | SHOULD |
| FR-OBJ-08 | Returned objects, options, pages, uploads and presigned requests own their mutable fields; `Clone` methods copy them and preserve nil. | MUST |
| FR-OBJ-09 | `WithProgress` and `WithProgressReadCloser` report cumulative bytes to a `ProgressFunc`. | SHOULD |
| FR-OBJ-10 | Every blocking operation honours its context and returns the context's error; a cancelled write does not publish. | MUST |
| FR-OBJ-11 | `backend/memory` is a race-safe in-memory reference implementation of `MultipartStore` and the batch and copy capabilities. | MUST |
| FR-OBJ-12 | `backend/fs` stores objects under one directory, safe for many goroutines and processes; publication is one atomic rename so readers never see partial objects; staging files are removed; copies hard-link immutable data. | MUST |
| FR-OBJ-13 | `backend/s3` adapts a caller-owned AWS SDK v2 client: streaming writes with bounded memory, digest-verified single and multipart uploads that abort on mismatch, in-place idempotent multipart completion, `DeleteObjects` batching, `CopyObject`/`UploadPartCopy`, SigV4 presigning, and `Options.NoHeadAfterWrite`. | MUST |
| FR-OBJ-14 | `backend/gcs` adapts a caller-owned `*storage.Client`: generation versions and preconditions, verified streaming uploads, compose-based multipart (hierarchical beyond 32 sources) under a reserved prefix, rewrite copies, parallel batch calls and V4 signed URLs. | MUST |
| FR-OBJ-15 | `backend/azure` adapts a caller-owned `*azblob.Client`: ETag conditions, staged-block streaming and multipart, Copy Blob, batch calls, service SAS presigning, and validated metadata names (`ErrInvalidMetadata`). | MUST |
| FR-OBJ-16 | `backend/sftp` stores objects through a caller-owned `*sftp.Client` with the filesystem layout, atomic publication via `posix-rename@openssh.com` with fallbacks, conditional writes under a heartbeated lock file with stale-lock breaking, multipart and hard-link copies. | MUST |
| FR-OBJ-17 | `backend/http` reads objects over HTTP(S) with exact ranges (also from servers that ignore `Range`), HEAD-based `Stat`/`StatMany`, digests from metadata headers or `Repr-Digest`, and fails writes with `ErrReadOnly` and listing with `ErrNotSupported`. | SHOULD |
| FR-OBJ-18 | `gcstest`, `azuretest` and `sftptest` provide in-process GCS, Azure Blob and SFTP servers that enforce the provider behaviour the adapters rely on, for use in downstream tests. | SHOULD |
| FR-OBJ-19 | `storetest.Run` verifies the complete `Store`/`MultipartStore` contract and every optional capability a store implements; every read/write backend passes it. | MUST |
| FR-OBJ-20 | `multipart.Uploader` uploads an `io.ReaderAt` in fixed parts with bounded parallelism, completes parts in order, declares a verified whole-object digest at `BeginMultipart`, and aborts exactly once on failure. | MUST |
| FR-OBJ-21 | `cas.Store` stores content once per SHA-256 digest under a fenced lease, verifies every byte of full reads (`ErrDigestMismatch`), serves exact ranged reads, and fails when the lease is lost. | MUST |
| FR-OBJ-22 | `pack.Writer` commits blobs into a deterministic data object and index; `pack.Reader` validates the index and serves each blob with one exact ranged read, detecting truncation and tampering (`ErrInvalidPack`). | MUST |
| FR-OBJ-23 | `lease.Backend` implements `go-distributed-coordination` fenced leases on any store: one holder at a time, strictly increasing fencing tokens across release and expiry, expiry at the TTL, waiting acquisition that polls and wakes at expiry, and cancellation. | MUST |
| FR-OBJ-24 | `handoff.Transport` stores `go-lifecycle` artifacts as canonical, digest-bound envelopes; sends are idempotent; tampered or misbound envelopes fail with `ErrDigestMismatch`. | MUST |
| FR-OBJ-25 | Backends classify provider errors onto the root sentinel errors (one class per error) and return unclassified errors unchanged. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-OBJ-01 | The root module depends on no provider or SSH SDK. | CI `boundaries` job: `go list -deps ./...` matches none of the SDK paths |
| NFR-OBJ-02 | Every exported store and layer is safe for concurrent use. | `go test -race -count=3 ./...` passes in every module |
| NFR-OBJ-03 | Bodies stream with memory bounded by configured part, block or chunk sizes. | `TestPutStreamingPathsAvoidSpoolingWhenPossible`, `TestPutBodyPaths` |
| NFR-OBJ-04 | Tests are hermetic: no Docker, cloud account or external network. | `go test ./...` passes offline (loopback only); the S3 integration test is opt-in |
| NFR-OBJ-05 | The filesystem store builds for linux, darwin, windows, freebsd, solaris and js/wasm; the SFTP and HTTP stores for linux, darwin, windows and freebsd. | CI cross-compilation step |
| NFR-OBJ-06 | Time-dependent tests run on virtual time and are deterministic. | `testing/synctest` tests listed in ADR-0011 |
| NFR-OBJ-07 | Minimum Go version 1.25; tested on 1.25.x, oldstable and stable. | `go.mod` files; CI matrix |
| NFR-OBJ-08 | The library never logs or writes to stdout/stderr. | Code review; no `log` or `fmt.Print` in non-test code |

## 6. API sketch

```go
package objectstore

type Store interface {
	Put(context.Context, Bucket, Key, io.Reader, PutOptions) (Object, error)
	Get(context.Context, Bucket, Key, GetOptions) (io.ReadCloser, Object, error)
	Stat(context.Context, Bucket, Key) (Object, error)
	Delete(context.Context, Bucket, Key, string) error
	List(context.Context, Bucket, ListOptions) (Page, error)
}

type MultipartStore interface {
	Store
	BeginMultipart(context.Context, Bucket, Key, PutOptions) (Upload, error)
	PutPart(context.Context, Upload, int, io.Reader, int64, [32]byte) (CompletedPart, error)
	CompleteMultipart(context.Context, Upload, []CompletedPart) (Object, error)
	AbortMultipart(context.Context, Upload) error
}

// Optional: BatchStater, BatchDeleter, Copier, Presigner.

// Backends
memory.New() *memory.Store
fs.New(directory string) (*fs.Store, error)
s3adapter.NewWithOptions(client *awss3.Client, options s3adapter.Options) (*s3adapter.Store, error)
gcs.New(client *storage.Client, options gcs.Options) (*gcs.Store, error)
azure.New(client *azblob.Client, options azure.Options) (*azure.Store, error)
sftp.New(client *pkgsftp.Client, root string, options sftp.Options) (*sftp.Store, error)
http.New(baseURL string, options http.Options) (*http.Store, error)

// Layers
cas.New(objects objectstore.Store, bucket objectstore.Bucket, prefix string,
	metadata cache.Cache[string, objectstore.Object], backend coordination.Backend, owner string) (*cas.Store, error)
pack.NewWriter(tempDir string) (*pack.Writer, error)
pack.OpenReader(ctx context.Context, store objectstore.Store, metadata pack.Metadata) (*pack.Reader, error)
multipart.NewUploader(store objectstore.MultipartStore, options multipart.Options) (*multipart.Uploader, error)
lease.New(store objectstore.Store, bucket objectstore.Bucket, prefix string, options lease.Options) (*lease.Backend, error)
handoff.NewTransport(store objectstore.Store, bucket objectstore.Bucket, prefix string) (*handoff.Transport, error)

// Conformance
storetest.Run(t *testing.T, factory storetest.Factory)
```

## 7. Acceptance criteria

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| Bucket and key validation | FR-OBJ-01 | `TestValidateBucket`, `TestValidateKey` |
| Range validation and exact ranges | FR-OBJ-02 | `TestByteRangeValidate`, `TestMemoryConformance` |
| Full contract on the reference store | FR-OBJ-01–07, FR-OBJ-10, FR-OBJ-19 | `TestMemoryConformance`, `TestStoreContract` |
| Defensive copies | FR-OBJ-08 | `TestContractClonesOwnMutableFields`, `TestContractClonesPreserveNilFields` |
| Progress and digest helpers | FR-OBJ-05, FR-OBJ-09 | `TestWithProgressReportsCumulativeBytes`, `TestObjectDigestAndOptionHelpers` |
| Cancellation | FR-OBJ-10 | `TestPutCanceledWhileWaitingForLockDoesNotMutate`, `TestPutCancellationDuringStreaming`, `TestGetBodyIsBoundToContext`, `TestWaitHonorsCancellation` |
| Memory store | FR-OBJ-11 | `TestStoreContract`, `TestPutCanceledWhileWaitingForLockDoesNotMutate` |
| Filesystem store | FR-OBJ-03, FR-OBJ-04, FR-OBJ-07, FR-OBJ-12 | `TestConformance` (fs), `TestListOrdersNestedAndEscapedKeys`, `TestCrossProcessConditionalWrites`, `TestReaderSurvivesConcurrentReplacement`, `TestStagingFilesAreRemoved`, `TestCopyHardLinksImmutableData` |
| S3 adapter | FR-OBJ-05, FR-OBJ-06, FR-OBJ-07, FR-OBJ-13 | `TestConformance` (s3), `TestPutStreamingDigestMismatchAbortsWithoutPublishing`, `TestForeignObjectsReportUnknownDigestWithoutDownloading`, `TestMultipartCompletesInPlaceAndIdempotently`, `TestDeleteManyMapsPerKeyErrors`, `TestNoHeadAfterWriteReturnsTheAcknowledgedObject`, `TestPutStreamingPathsAvoidSpoolingWhenPossible` |
| GCS adapter | FR-OBJ-06, FR-OBJ-07, FR-OBJ-14 | `TestConformance` (gcs), `TestVersionIsGeneration`, `TestMultipartHierarchicalCompose`, `TestPresignShape`, `TestBatchOperations`, `TestPutBodyPaths` |
| Azure adapter | FR-OBJ-07, FR-OBJ-15 | `TestConformance` (azure), `TestStreamingPutLosesToConcurrentWrite`, `TestMetadataValidation`, `TestPresign`, `TestPutCancellationDuringStreaming` |
| SFTP adapter | FR-OBJ-04, FR-OBJ-16 | `TestConformance` (sftp), `TestConformanceWithoutExtensions`, `TestCompareAndSwapAcrossStores`, `TestLockContentionAcrossProcesses`, `TestStaleLockIsBroken`, `TestGetBodyIsBoundToContext` |
| HTTP store | FR-OBJ-17 | `TestReadContract`, `TestReadOnly`, `TestServerIgnoringRange` |
| Test servers | FR-OBJ-18 | `TestNewServerBucketsAndCleanup`, `TestUploadPreconditions` (gcstest), `TestRoundTripWithMetadataAndProperties`, `TestSharedAccessSignatures` (azuretest), `TestPasswordAuthentication`, `TestExtensions` (sftptest) |
| Conformance suite | FR-OBJ-19 | `TestMemoryConformance`, `TestConformance` (fs, s3, gcs, azure, sftp) |
| Multipart uploader | FR-OBJ-20 | `TestUploaderOrdersPartsAndBoundsParallelism`, `TestUploaderAbortsOnceOnPartFailure`, `TestUploaderDeclaresVerifiedWholeObjectDigest` |
| Content addressing | FR-OBJ-21 | `TestPutDeduplicatesConcurrentWriters`, `TestOpenFullReadDetectsCorruptionAtEOF`, `TestOpenRangeUsesExactBytes`, `TestPutReportsLeaseLoss` |
| Packs | FR-OBJ-22 | `TestPackCommitIsDeterministicAndReadsOneRange`, `TestReaderDetectsSameSizeDataTampering`, `TestOpenReaderRejectsTruncatedAndOverflowingIndexes` |
| Leases | FR-OBJ-23 | `TestConformanceMemoryStore`, `TestExpiryClosesDoneAtTTL`, `TestWaitPollsUntilRelease`, `TestWaitHonorsCancellation`, `TestConcurrentAcquireGrantsOneHolder` |
| Lifecycle handoff | FR-OBJ-24 | `TestTransportRoundTripsDefensiveArtifactCopies`, `TestTransportRejectsTamperedArtifact`, `TestTransportSendIsIdempotent` |
| Error classification | FR-OBJ-25 | `TestMapsS3Errors`, `TestMapError` (gcs, azure) |

## 8. Source references

None.

## 9. Open questions

- Q1: Should classified errors also expose the provider cause to
  `errors.As` (ADR-0005 follow-up)?
- Q2: Should GCS and Azure gain opt-in integration tests against real
  services, as S3 has (ADR-0008 follow-up)?
