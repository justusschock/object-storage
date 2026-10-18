# ADR 0008: Conformance suite and in-process test servers

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0009, ADR-0011 |

## Context

Recorded retrospectively for the first public release. The value of a
backend-neutral contract depends on every backend implementing it exactly:
the same conditional semantics, range handling, pagination, digest checks and
error classes. Provider emulators (Docker images such as MinIO, Azurite or
fake-gcs-server) are slow to start, need a container runtime, and do not
implement every conditional header. Downstream projects that use the
backends also need realistic stores in their own tests.

## Decision drivers

- D1: One executable definition of the contract that every backend runs.
- D2: Tests run in process, without Docker, network access or credentials.
- D3: Fakes must enforce the provider behaviour the adapters rely on
  (preconditions, signatures, block lists), not merely store bytes.
- D4: Downstream projects can reuse both the suite and the fakes.

## Considered options

1. **A conformance suite package plus in-process provider fakes shipped as
   importable packages**
   - Pro: every backend and third-party implementation is held to the same
     tests; fast, hermetic tests here and downstream.
   - Con: the fakes are code to maintain and can drift from the providers.
2. **Per-backend tests only**
   - Pro: no shared harness.
   - Con: the contract is restated, and drifts, per backend.
3. **Container-based emulators**
   - Pro: closer to real services.
   - Con: slow; need Docker; several lack the conditional semantics the
     adapters depend on.

## Decision

We will maintain:

- `storetest.Run(t, factory)`, the conformance suite for `Store` and
  `MultipartStore` plus every optional capability the store implements. The
  memory, filesystem, S3, GCS, Azure and SFTP backends run it (SFTP also
  without server extensions). The read-only HTTP store runs the read subset
  in its own tests.
- In-process fakes as public packages of the nested modules:
  - `backend/gcs/gcstest`: `fake-gcs-server` behind a middleware that adds
    generation preconditions, compose semantics, signed XML uploads and
    strictly increasing generations.
  - `backend/azure/azuretest`: a Blob REST fake that verifies Shared Key and
    service SAS signatures and implements conditional headers, ranges,
    paging, block lists and copies.
  - `backend/sftp/sftptest`: an SSH server serving a directory over SFTP,
    with options that disable OpenSSH extensions or enforce strict SFTP v3
    rename.
  - S3 tests use `gofakes3` with a test-only handler for conditional
    requests and `UploadPartCopy`; this handler stays in test files.
- An opt-in S3 integration test (`-tags=integration`) that runs the suite
  against a real endpoint configured by `S3_ENDPOINT`, `S3_REGION` and
  `S3_BUCKET` and skips when they or credentials are missing.

## Consequences

### Positive
- A behaviour change shows up as a suite failure on every affected backend.
- Third-party implementations and downstream tests get the same tools.

### Negative
- The fakes are part of the public API and follow its stability rules.
- A fake can accept behaviour the real service rejects; the S3 integration
  test is the only check against a real provider.

### Follow-ups
- Integration tests against real GCS and Azure would close the same gap for
  those backends.

## Links

- `storetest/`, `backend/*/…test/`
