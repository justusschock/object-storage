# ADR 0011: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0008, ADR-0010 |

## Context

Recorded retrospectively for the first public release. The library's
guarantees are concurrency guarantees (atomic conditions, lease exclusion,
duplicate suppression) and time guarantees (lease TTLs, waits, stale-lock
detection, presign expiry). Tests of such properties are easily flaky when
they depend on the scheduler or the wall clock, and slow when they sleep.
The previous repository also carried tests that asserted the text of the CI
workflow, the README and private dependency versions; they encoded release
mechanics rather than behaviour.

## Decision drivers

- D1: Tests are deterministic and pass with `go test -race -count=3 ./...`.
- D2: Tests are hermetic and fast (ADR-0008).
- D3: Every functional requirement has a test that cites it.

## Considered options

1. **Unit tests per package, the shared conformance suite per backend,
   `testing/synctest` for time and goroutine coordination, the race detector
   everywhere, examples as documentation tests**
   - Pro: deterministic; virtual time makes long TTLs instant.
   - Con: code in a synctest bubble must not block on real I/O, so
     network-backed tests stay on real time.
2. **Injected fake clocks**
   - Pro: works with real I/O.
   - Con: an extra API in production code only for tests; every timer must
     go through it.
3. **Real time with generous sleeps**
   - Pro: simple.
   - Con: slow and flaky.

## Decision

We will test with:

- **Unit tests** in every package, internal (`package x`) where they need
  unexported state, external (`package x_test`) otherwise.
- **Conformance**: `storetest.Run` for every read/write backend;
  `go-distributed-coordination`'s `conformance.Run` for `lease`.
- **`testing/synctest`** for TTLs, waits, polling, heartbeats, stale-lock
  breaking, presign expiry and bounded parallelism: the lease tests, the
  SFTP lock tests (over an in-memory lock file system), the multipart
  uploader tests, the Azure fake's SAS-expiry and date-skew tests and the
  GCS presign tests.
- **Real time, deliberately**, where the code under test does real network
  or file I/O inside the test: the end-to-end Azure presign-expiry test
  (whole-second SAS expiry over loopback HTTP), the Azure streaming-race
  poll for staged blocks, the GCS concurrent-replacement test, and the SFTP
  stale-lock test over SSH. Their logic is also covered by synctest tests
  of the components (`TestSASExpiry`, `TestStaleLockIsBroken`).
- **Fake clock, deliberately**, in the lease conformance tests:
  `conformance.Run` creates subtests with `t.Run`, which is not allowed in
  a synctest bubble, and advances time through its factory's callback.
- **Race detector** on every module in CI; locally `-count=3`.
- **Examples** (`example_test.go`) for the root contract, the filesystem
  store, each nested backend constructor and each storage layer; the README
  quick start is the root `Example`.
- **Traceability**: tests named in PRD-0001's acceptance table carry
  `// FR-OBJ-NN` comments.
- **Integration**: the S3 integration test behind `-tags=integration`.
- Tests that assert workflow or README text are not kept; CI and lint
  enforce policy directly.

## Consequences

### Positive
- Time-based tests run in microseconds and do not flake.
- A conformance failure points to the exact contract case.

### Negative
- A few network-backed tests still take real time (about one second for
  the Azure presign expiry).

### Follow-ups
- None.

## Links

- [testing/synctest](https://pkg.go.dev/testing/synctest)
