# PRD NNNN: <Title>

| Field | Value |
|---|---|
| Status | Draft \| Proposed \| Accepted \| Superseded by PRD-NNNN |
| Implementation | Not started \| In progress \| Complete |
| Owners | <names> |
| Related ADRs | ADR-NNNN, … |
| Last updated | YYYY-MM-DD |

## 1. Problem and context

What problem does this solve, for whom, and why now? Link prior art.

## 2. Users and use cases

Concrete use cases written as
"As a <persona>, I want <capability> so that <outcome>".

## 3. Goals and non-goals

### Goals
- G1 …

### Non-goals
- NG1 … (explicitly out of scope, with the reason)

## 4. Functional requirements

Each requirement gets a stable ID `FR-<AREA>-NN`. IDs are never reused or
renumbered. Tests reference them (for example `// FR-TRF-03`).

| ID | Requirement | Priority (MUST/SHOULD/MAY) |
|---|---|---|
| FR-XXX-01 | … | MUST |

## 5. Non-functional requirements

Accuracy and tolerances, performance budgets, portability and build tags,
determinism, security.

| ID | Requirement | Measure |
|---|---|---|
| NFR-XXX-01 | … | … |

## 6. API sketch

Go signatures. These are illustrative; the
code is authoritative once merged.

```go
// sketch
```

## 7. Acceptance criteria

Each criterion maps to named tests (unit, property, integration, golden, fuzz).

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| … | FR-XXX-01 | `TestXxx`, `TestPropXxx` |

## 8. Source references

Prior art and references, and what is taken from each.

## 9. Open questions

- Q1 …
