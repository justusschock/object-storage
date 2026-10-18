# ADR 0006: Conditional writes, concurrency and cancellation

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0005 |

## Context

Recorded retrospectively for the first public release. The storage layers
build coordination on object storage itself: `lease` implements fenced
leases with compare-and-swap, `cas` suppresses duplicate uploads, and
applications use `IfMatchVersion` for optimistic updates. These are only
correct if every backend evaluates conditions atomically with publication,
including across processes, and if readers never observe partial objects.
Stores are shared by many goroutines, and long transfers must stop when the
caller gives up.

## Decision drivers

- D1: `IfAbsent` and `IfMatchVersion` are atomic on every read/write backend,
  across goroutines and, where the medium allows, across processes.
- D2: Readers see either the old or the new version, never a mix or a
  partial body; crashes leave one of the two.
- D3: Bounded memory for arbitrarily large bodies.
- D4: Every blocking operation honours its context.

## Considered options

1. **Native conditional primitives where the provider has them; a lock plus
   atomic rename where it does not**
   - Pro: provider-enforced atomicity on cloud stores; exact semantics on
     local disks.
   - Con: file-based backends need a lock protocol and its failure modes.
2. **Client-side read-check-write everywhere**
   - Pro: uniform code.
   - Con: racy; useless for leases.
3. **External coordinator (database or lock service) for conditions**
   - Pro: strong atomicity on any medium.
   - Con: a second system for every deployment; contradicts the goal of
     coordinating through object storage.

## Decision

We will use option 1:

| Backend | Atomic condition | Publication |
|---|---|---|
| memory | one `sync.RWMutex` | map update under the lock |
| fs | exclusive OS file lock per bucket (`flock`, `LockFileEx`) | rename of `@object.json`, which names an immutable data file |
| s3 | `If-None-Match: *` / `If-Match` on `PutObject`, `CompleteMultipartUpload`, `CopyObject`, `DeleteObject` | provider |
| gcs | `DoesNotExist` / `GenerationMatch` preconditions on uploads, compose, rewrite and delete | provider |
| azure | `If-None-Match: *` / `If-Match` on Put Blob, Put Block List, Copy Blob, Delete Blob | provider |
| sftp | per-bucket lock file created with `O_EXCL`, heartbeated, broken as stale after `Options.StaleLockAge` of unchanged contents | `posix-rename@openssh.com`, with documented fallbacks |
| http | read-only | — |

- Every store type is safe for concurrent use and says so in its doc
  comment. Bodies stream through bounded buffers; bodies with an unknown
  size and digest are hashed in a first pass when seekable or spooled once
  to a temporary file.
- Every operation that blocks or does I/O takes `context.Context` first and
  returns the context's error when it ends. Bodies returned by `Get` are
  bound to that context. SFTP requests are not context-aware, so the SFTP
  store checks the context between requests and on every streamed read.
- Multipart uploads are explicit custody: `CompleteMultipart` is idempotent
  across lost responses on S3; an abandoned upload is removed with
  `AbortMultipart`. `multipart.Uploader` aborts exactly once on failure.
- Leases (`lease`) are one JSON object per key; acquisition, renewal and
  release are compare-and-swaps, release keeps the object so fencing tokens
  increase strictly, and waiters poll with `Options.PollInterval` and wake
  at the holder's expiry. Expiry uses the participants' clocks, which must
  agree to well within one TTL; protected resources check fencing tokens.
- The stores and storage layers never log. Their goroutines (bounded
  parallel transfers, batch calls, the SFTP lock heartbeat, the `cas`
  keep-alive) end before the call that started them returns; the only
  exception is the lease expiry watcher, which ends with the lease. The
  in-process test servers run until they are closed.

## Consequences

### Positive
- `lease` and `cas` are correct on every read/write backend.
- Large transfers do not require memory proportional to their size.

### Negative
- SFTP conditions are exact among goroutines sharing one store, and across
  processes only if the server implements exclusive create atomically and
  no live lock is broken as stale; otherwise they are best effort.
- Filesystem cross-process safety requires working advisory locks; on
  platforms without them the fs store is safe within one process only.
- S3 entity tags repeat for identical bytes; compare-and-swap users encode a
  changing value in the object.

### Follow-ups
- None.

## Links

- Package comments of `backend/fs`, `backend/sftp`, `backend/s3`,
  `backend/gcs`, `backend/azure`, `lease`
