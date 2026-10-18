# AGENTS.md

Rules for anyone, human or agent, changing object-storage. They are
binding. If a change has to break one, say so in the pull request and, where
it is a design decision, record it in an ADR.

object-storage is a set of backend-neutral object storage contracts with
exact semantics, their implementations for memory, filesystems, S3, GCS,
Azure Blob, SFTP and HTTP, and storage layers built on them. Its requirements
and design decisions are recorded in [docs/](docs/README.md).

## Docs first

No behaviour is added or changed before the documents that govern it say so.

- **PRDs** (`docs/prd/NNNN-title.md`, from [the template](docs/prd/TEMPLATE.md))
  state *what* the library does: problem, use cases, goals and non-goals,
  functional requirements `FR-OBJ-NN`, non-functional requirements
  `NFR-OBJ-NN`, API sketch, acceptance criteria mapped to named tests, open
  questions. This library's area code is `OBJ`.
- **ADRs** (`docs/adr/NNNN-title.md`, MADR style, from
  [the template](docs/adr/TEMPLATE.md)) state *how and why*: context, decision
  drivers, considered options with pros and cons, decision, consequences.
- **Lifecycle.** New PRDs and ADRs start as `Proposed` and are reviewed in a
  pull request; merged, they become `Accepted`. Implementation follows
  acceptance — for small changes in the same pull request, as separate
  commits, documents first.
- A decision made while implementing gets a new ADR **in the same pull
  request as the code**. Changing public API, adding a dependency, changing a
  wire or persistence format, or changing error, concurrency or cancellation
  semantics always needs one.
- **Accepted ADRs are never edited**, except for their status line. To change
  a decision, add a new ADR with `Supersedes: ADR-NNNN` and set the old one to
  `Superseded by ADR-MMMM`.
- PRDs are kept current: a pull request that changes behaviour updates the
  requirements it touches. Requirement IDs are never reused or renumbered.
- [docs/README.md](docs/README.md) indexes every PRD and ADR with its status;
  update it whenever a document is added or changes status.
- **Traceability.** Tests cite the requirement they verify in a comment
  (`// FR-OBJ-03`). Every functional requirement has at least one test.

## Public API

- The public API is every exported identifier outside `internal/`. It is
  documented, covered by tests and examples, and changed only through an ADR.
- Implementation details live under `internal/` or stay unexported. Do not
  export something only to test it; test from inside the package instead.
- Conformance suites for implementers of the library's interfaces are public;
  test helpers used only by this repository are internal.
- Versioning follows semver. While the major version is `0`, a minor release
  may break the API; every break is listed in `CHANGELOG.md` and backed by an
  ADR.

## Layout

The repository holds six Go modules (ADR-0003). The root module:

- `.` (package `objectstore`): the contract — `Store`, `MultipartStore`,
  optional capabilities, value types, sentinel errors, progress helpers.
- `backend/memory/`: in-memory reference implementation.
- `backend/fs/`: filesystem store, safe across processes.
- `storetest/`: conformance suite every implementation must pass.
- `cas/`, `pack/`, `multipart/`, `lease/`, `handoff/`: storage layers that
  depend only on the contract.
- `internal/storetest/`: forwards to `storetest` for the nested modules until
  they require a root release that contains it (ADR-0009); then deleted.

Nested modules, each with its own `go.mod` requiring a released root
version:

- `backend/s3/`: AWS SDK v2 adapter.
- `backend/gcs/` and `backend/gcs/gcstest/`: Cloud Storage adapter and
  in-process GCS server.
- `backend/azure/` and `backend/azure/azuretest/`: Blob Storage adapter and
  in-process Blob service.
- `backend/sftp/` and `backend/sftp/sftptest/`: SFTP adapter and in-process
  SSH/SFTP server.
- `backend/http/`: read-only HTTP(S) store.

The `*test` packages are public: downstream projects use them in their own
tests. The root module must not import provider or SSH SDKs.

## Engineering rules

1. **Small, explicit contracts.** Every exported type and function documents
   its behaviour, errors, concurrency safety and cancellation.
2. **Dependencies.** Standard library first. The root module may use only
   the sibling libraries listed in ADR-0004; provider SDKs and their fakes
   live only in the nested module that needs them. A new dependency in any
   module needs an ADR.
3. **Errors.** Sentinel errors and typed errors are part of the API; wrap
   with `%w` and test with `errors.Is`/`errors.As`. Backends classify a
   provider error onto exactly one root sentinel and keep the cause as text
   (`fmt.Errorf("%w: %v", …)`, marked `//nolint:errorlint`); context errors
   pass through unchanged (ADR-0005).
4. **Context.** Operations that block or do I/O take `ctx context.Context`
   first and honour cancellation.
5. **No logging in the library.** Report through return values or observer
   hooks; never write to stdout/stderr or a global logger.
6. **Concurrency.** Every exported type states whether it is safe for
   concurrent use; tests run with the race detector.

## Testing

- Unit tests for every package; examples (`example_test.go`) for every
  public entry point, kept in sync with the README.
- **`testing/synctest`** for anything involving time or concurrency
  (timeouts, TTLs, retries, tickers, leases, goroutine coordination). No real
  `time.Sleep` waits in tests, except the few end-to-end tests over real
  network I/O that ADR-0011 lists.
- Implementations of the library's interfaces pass the public conformance
  suite (`storetest.Run`; `lease` also passes the `distributed-coordination`
  conformance suite).
- Tests are deterministic and pass with `go test -race -count=3 ./...`.

## Workflow

- Branch from `main`, open a pull request, merge only with green CI. Nothing
  is pushed to `main` directly.
- Commit subjects use conventional-commit prefixes (`feat:`, `fix:`,
  `refactor:`, `docs:`, `test:`, `ci:`, `chore:`; `!` for breaking changes)
  and say what changed; the body says why.
- Before opening a pull request:

  ```sh
  gofmt -l .                     # prints nothing
  go vet ./...
  go test -race -count=1 ./...
  golangci-lint run ./...
  go mod tidy && git diff --exit-code go.mod go.sum
  ```

  Run them in every module directory: `.`, `backend/azure`, `backend/gcs`,
  `backend/http`, `backend/s3`, `backend/sftp`. To test the nested modules
  against your root changes, create a workspace as CI does
  (`go work init . ./backend/azure ./backend/gcs ./backend/http ./backend/s3 ./backend/sftp`;
  `go.work` is git-ignored) and run `go mod tidy` with `GOWORK=off`. Never
  commit `go.work` or `replace` directives. Tests of the nested modules bind
  loopback ports.
- CI workflows pin every action to a full commit SHA, use only `push` and
  `pull_request` triggers (never `pull_request_target`) and read-only
  permissions. Dependabot keeps Go modules and actions current.
- Releases are tags `vX.Y.Z` (root) and `backend/<name>/vX.Y.Z` (nested
  modules) on `main`, with the changelog section as release notes. Order
  (ADR-0003): tag the root; raise the root requirement of each nested module
  that needs it in a pull request (`go mod tidy`); after merging, tag those
  nested modules.
