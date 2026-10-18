# ADR 0007: Persistence and wire formats

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0006 |

## Context

Recorded retrospectively for the first public release. Data written by one
release is read by later releases and, for some formats, by other tools
(the filesystem and SFTP layouts are shared; objects written by S3, GCS and
Azure carry a digest that the HTTP store reads back through a CDN). These
formats outlive any single process and are therefore part of the contract,
even where no Go type exposes them.

## Decision drivers

- D1: Formats are self-describing or versioned so they can evolve.
- D2: Integrity is checkable: every format records a SHA-256 digest.
- D3: Formats are simple enough to inspect with standard tools.
- D4: Writes publish with one atomic step (ADR-0006).

## Considered options

1. **Small, explicit formats per concern: JSON records for metadata,
   provider metadata keys for digests, a fixed-width binary index for packs,
   a length-prefixed frame for handoff envelopes**
   - Pro: each format is minimal and documented next to its code; JSON is
     inspectable; the binary index allows one ranged read per entry.
   - Con: several formats to keep compatible.
2. **One generic container format (for example protobuf or CBOR) for
   everything**
   - Pro: one schema mechanism.
   - Con: adds a dependency to the root module; less inspectable; the pack
     index needs fixed-width records for direct access anyway.

## Decision

We will keep these formats stable. Changing any of them needs a new ADR, a
version or format marker change, and a reader for the previous format.

| Format | Where | Shape |
|---|---|---|
| Filesystem layout | `backend/fs`, `backend/sftp` | per bucket `b-<hex bucket>/`: `lock` (fs) or `sftp.lock` (sftp), `tmp/`, `objects/<key segments>/@object.json`, `objects/<key segments>/@data-<version>`, `uploads/<upload id>/`; key segments starting with `@` get one extra `@` |
| Object record | `@object.json` | JSON `{version, sha256, size, modified_at, content_type, metadata}`; its rename is the commit point and it names the immutable data file |
| Digest metadata | S3, GCS | user metadata `go-object-storage-sha256` (hex) |
| Digest metadata | Azure | metadata name `goobjectstoragesha256` (hex) |
| Multipart staging | GCS | part objects under `gcs.DefaultMultipartPrefix` (`.go-object-storage/multipart/`), hidden from listings and rejected as user keys |
| Multipart staging | Azure | uncommitted blocks on the final blob with IDs derived from upload ID and part number, plus a marker blob under `.go-object-storage/multipart/` |
| Lease record | `lease` | JSON `{format: 1, key, owner, token, acquired_at, expires_at, released}` at `prefix/<key>`, at most 64 KiB |
| CAS object | `cas` | content at `prefix/<lowercase hex SHA-256>`, metadata `object-storage-fencing-token` with the writer's fencing token; digests print as `sha256:<hex>` |
| Pack | `pack` | data at `prefix/<index SHA-256>.pack`; index at `prefix/<index SHA-256>.idx`: magic `GOPACK01`, big-endian `uint64` count, then per blob 32-byte digest, `uint64` offset, `uint64` length, sorted by digest |
| Handoff envelope | `handoff` | at `prefix/<artifact id>/<artifact id>`: big-endian `uint64` header length, canonical JSON header (`format: "go-object-storage/handoff/v1"`, artifact ID, media type, version, digests, data length, metadata; unknown fields and non-canonical encodings rejected), then the artifact bytes |

The HTTP store reads digests from the S3, GCS and Azure metadata headers
above or from an RFC 9530 `Repr-Digest` header.

## Consequences

### Positive
- Objects written by the filesystem store can be served by the SFTP store
  and vice versa (not concurrently: they lock differently).
- Corruption is detected on read in `cas`, `pack` and `handoff`.

### Negative
- The reserved metadata names and the `.go-object-storage/` prefix are
  unavailable to applications on the affected backends.
- Objects written by other tools carry no digest metadata and report
  `SHA256Known` false.

### Follow-ups
- None.

## Links

- Package comments of `backend/fs`, `backend/sftp`, `backend/s3`,
  `backend/gcs`, `backend/azure`, `pack`; `lease/backend.go`,
  `handoff/transport.go`, `cas/store.go`
