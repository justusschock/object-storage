# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- One `Store` / `MultipartStore` contract with identical semantics on every
  backend, plus optional `BatchStater`, `BatchDeleter`, `Copier` and
  `Presigner` capabilities discovered by type assertion.
- Atomic create-only writes (`IfAbsent`) and compare-and-swap
  (`IfMatchVersion`) on every read/write backend.
- Declared digests and sizes verified before publication, SHA-256 digests
  reported without downloading (`Object.SHA256Known`), streaming writes
  with bounded memory and exact half-open ranged reads.
- Root-module backends: `backend/memory` (in-memory reference) and
  `backend/fs` (filesystem, safe across processes).
- Nested provider modules, each tagged `backend/<name>/v0.1.0`:
  `backend/s3` (S3-compatible), `backend/gcs`, `backend/azure`,
  `backend/sftp` and the read-only `backend/http`.
- Packages `cas` (content-addressed storage with fenced duplicate
  suppression), `pack` (immutable pack files, one ranged read per blob) and
  `multipart` (bounded-parallel multipart uploads).
- Packages `lease` (fenced `distributed-coordination` leases stored as
  objects) and `handoff` (`lifecycle` artifact envelopes stored as objects).
- Package `storetest`: the `Run` conformance suite for `Store`
  implementations, and in-process GCS, Azure Blob and SFTP servers
  (`gcstest`, `azuretest`, `sftptest`) for hermetic tests.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/object-storage/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/object-storage/releases/tag/v0.1.0
