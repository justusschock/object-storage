# ADR 0010: Supported Go versions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0011 |

## Context

Recorded retrospectively for the first public release. Every module's
`go.mod` declares `go 1.25.0` without a `toolchain` line. The tests use
`testing/synctest`, whose stable API (`synctest.Test`, `synctest.Wait`)
first shipped in Go 1.25. The Go project supports the two most recent major
releases; at the time of writing those are Go 1.26 and Go 1.27.

## Decision drivers

- D1: Consumers on any supported Go release can build the library.
- D2: Tests use virtual time through `testing/synctest`.
- D3: CI tests every version the modules claim to support.

## Considered options

1. **Floor at Go 1.25; support the floor and the two current releases**
   - Pro: synctest is available; consumers one release behind the
     supported window still build.
   - Con: one more CI matrix entry than upstream support requires.
2. **Floor at the oldest upstream-supported release (Go 1.26)**
   - Pro: smallest matrix.
   - Con: drops Go 1.25 users with no technical need.
3. **Lower floor (Go 1.22–1.24) with `GOEXPERIMENT=synctest` or no
   synctest**
   - Pro: wider reach.
   - Con: the experimental API differs; time-based tests fall back to real
     sleeps.

## Decision

We will declare `go 1.25.0` in every module and test on Go `1.25.x`,
`oldstable` and `stable` in CI. Raising the floor needs a new ADR and a
changelog entry; the floor follows the oldest Go release that provides every
standard-library API the modules use, and never drops below what
`testing/synctest` requires.

## Consequences

### Positive
- The supported range is explicit and tested.

### Negative
- Language and library features newer than Go 1.25 cannot be used until the
  floor is raised.

### Follow-ups
- Revisit the floor when Go 1.25 leaves the matrix's useful range.

## Links

- [Go release policy](https://go.dev/doc/devel/release#policy)
- [testing/synctest](https://pkg.go.dev/testing/synctest)
