# Documentation

Requirements (PRDs) and architecture decisions (ADRs) for object-storage.
The process is described in [ADR-0001](adr/0001-record-architecture-decisions.md)
and [AGENTS.md](../AGENTS.md). New documents start from the templates:
[PRD template](prd/TEMPLATE.md), [ADR template](adr/TEMPLATE.md).

## Product requirements

| PRD | Title | Status | Implementation |
|---|---|---|---|
| [0001](prd/0001-object-storage.md) | object-storage | Accepted | Complete |

## Architecture decisions

| ADR | Title | Status |
|---|---|---|
| [0001](adr/0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](adr/0002-backend-neutral-store-contract.md) | Backend-neutral store contract and package shape | Accepted |
| [0003](adr/0003-nested-modules-for-provider-backends.md) | Nested modules for provider backends, release order and tags | Accepted |
| [0004](adr/0004-dependency-policy.md) | Dependency policy | Accepted |
| [0005](adr/0005-error-semantics.md) | Error semantics | Accepted |
| [0006](adr/0006-conditional-writes-concurrency-and-cancellation.md) | Conditional writes, concurrency and cancellation | Accepted |
| [0007](adr/0007-persistence-and-wire-formats.md) | Persistence and wire formats | Accepted |
| [0008](adr/0008-conformance-suite-and-in-process-test-servers.md) | Conformance suite and in-process test servers | Accepted |
| [0009](adr/0009-public-api-boundary.md) | Public API boundary | Accepted |
| [0010](adr/0010-supported-go-versions.md) | Supported Go versions | Accepted |
| [0011](adr/0011-testing-strategy.md) | Testing strategy | Accepted |
