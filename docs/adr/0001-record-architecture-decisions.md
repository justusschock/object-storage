# ADR 0001: Record architecture decisions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. go-object-storage
grew from a root module into a root module plus five nested backend modules,
with storage layers (content addressing, packs, leases, lifecycle handoff)
built on one contract. The reasons behind the contract, the module split,
the error vocabulary and the persistence formats lived in commit messages
and package comments. Contributors and downstream users need one place that
says what the library guarantees and why, and a process that keeps it true.

## Decision drivers

- D1: New contributors must be able to learn why the code is shaped as it is
  without reading the history.
- D2: Changes to public API, formats and semantics must be reviewed as
  decisions, not only as diffs.
- D3: The record must be cheap to keep and live next to the code.

## Considered options

1. **Markdown ADRs (MADR style) and PRDs in `docs/`, docs first**
   - Pro: versioned and reviewed with the code; plain text; widely known
     format.
   - Con: needs discipline to write the document before the code.
2. **Design notes in a wiki or issue tracker**
   - Pro: easy to edit.
   - Con: drifts from the code; not part of review; lost when the hosting
     changes.
3. **Package comments and commit messages only**
   - Pro: no extra files.
   - Con: no place for rejected alternatives; scattered; hard to supersede.

## Decision

We will record requirements as PRDs in `docs/prd/` and decisions as
MADR-style ADRs in `docs/adr/`, both from the templates in those directories,
and index all of them with their status in `docs/README.md`.

- Documents come first: behaviour is added or changed only after the PRD or
  ADR that governs it says so, in the same pull request for small changes.
- New documents start as `Proposed` and become `Accepted` when merged.
- Accepted ADRs are immutable except for their status line. A changed
  decision is a new ADR with `Supersedes: ADR-NNNN`; the old one becomes
  `Superseded by ADR-MMMM`.
- PRDs are kept current; requirement IDs (`FR-OBJ-NN`, `NFR-OBJ-NN`) are never
  reused or renumbered, and tests cite the IDs they verify.

## Consequences

### Positive
- Decisions and their trade-offs are reviewable and discoverable.
- Requirement IDs connect the PRD, the code and the tests.

### Negative
- Every significant change carries a documentation step.

### Follow-ups
- `AGENTS.md` states these rules for every contributor.

## Links

- [MADR](https://adr.github.io/madr/)
- [docs/README.md](../README.md)
