# Changelog

All notable changes to this repository are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
modules follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html):
while the major version is 0, a minor release may break the API.

The root module is tagged `vX.Y.Z`; each nested module is tagged
`backend/<name>/vX.Y.Z` and versions independently (see
[ADR-0003](docs/adr/0003-nested-modules-for-provider-backends.md)).

## [Unreleased]

Preparation for the first public release. The root module's code is
unchanged since v0.3.0 apart from the items below; the nested modules on
`main` are the released `backend/s3` v0.4.0 and `backend/{gcs,azure,sftp,http}`
v0.3.0 plus the items below.

### Added
- `storetest`: the conformance suite (`Run`, `Factory`, `StreamingSize`) is
  public, so third-party `Store` implementations can run it
  ([ADR-0009](docs/adr/0009-public-api-boundary.md)). `internal/storetest`
  forwards to it until the nested modules require the next root release.
- Package documentation for `objectstore`, `cas` and `pack`, concurrency
  statements on every exported type, and runnable examples for `fs`, `cas`,
  `pack`, `multipart`, `lease` and `handoff`.
- `NOTICE`, `AGENTS.md`, and `docs/` with PRD-0001 and ADR-0001 to ADR-0011,
  recorded retrospectively.
- golangci-lint configuration and Dependabot for all six modules and the
  GitHub Actions.

### Changed
- CI tests every module on Go 1.25.x, oldstable and stable with the race
  detector, lints every module, checks `go mod tidy`, the root dependency
  boundary and cross-compilation, and on release tags tests every module
  without a workspace. It no longer needs private-module credentials.
- The multipart uploader tests run in `testing/synctest` bubbles.
- Minimum Go version stays 1.25 ([ADR-0010](docs/adr/0010-supported-go-versions.md)).

### Removed
- Tests that asserted the CI workflow text, the README text and private
  dependency versions, and the release-candidate scripts under `scripts/`.
- Unexported test-only clock hooks in the filesystem and SFTP stores and in
  the Azure fake.

## backend/s3 [0.4.0] - 2026-10-16

### Added
- `Options.NoHeadAfterWrite` builds the result of single-request writes from
  the write acknowledgement instead of a HEAD request.

## backend/gcs, backend/azure, backend/sftp, backend/http [0.3.0] - 2026-10-15

### Added
- `backend/gcs` with `gcstest`, `backend/azure` with `azuretest`,
  `backend/sftp` with `sftptest`, and the read-only `backend/http`, each
  requiring root v0.3.0 and passing the conformance suite (HTTP: the read
  subset).

## [0.3.0] - 2026-10-15

Root module and `backend/s3` v0.3.0.

### Added
- `Object.SHA256Known` and `Object.Digest` for objects whose digest is not
  known cheaply.
- Optional capabilities `BatchStater`, `BatchDeleter`, `Copier` and
  `Presigner`; `WithProgress` and `WithProgressReadCloser`.
- `backend/fs`, a filesystem store safe across processes.
- `lease`, fenced coordination leases stored as objects.
- `backend/s3`: streaming writes with bounded memory, in-place multipart
  completion, `NewWithOptions`.

### Changed
- Minimum Go version 1.25.

## [0.2.0] - 2026-07-20

Root module and `backend/s3` v0.2.0: the store contract, the memory backend,
`cas`, `pack`, `multipart`, `handoff`, and the isolated S3 adapter.

[Unreleased]: https://github.com/justusschock/go-object-storage/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/justusschock/go-object-storage/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/justusschock/go-object-storage/releases/tag/v0.2.0
