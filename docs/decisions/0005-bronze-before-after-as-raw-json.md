# ADR 0005: Bronze `before`/`after` columns are raw JSON strings, not typed structs

## Status
Accepted

## Context
Debezium's CDC envelope carries the row's old/new state as nested JSON
objects (`before`, `after`). Bronze (spec section 9) must be immutable,
append-only, and must not require re-reading PostgreSQL to reprocess
downstream failures. Spec section 18 requires a later schema-evolution
demonstration (Phase 4) that must not break Bronze.

## Decision
`before` and `after` are stored as raw `STRING` columns (the JSON text,
extracted verbatim via `get_json_object`/plain JSON parsing), not as typed
`STRUCT` columns matching each table's current shape.

## Consequences
- A Postgres schema change (add/rename/retype a column) never forces an
  Iceberg schema-evolution on Bronze itself -- Bronze's schema is stable
  regardless of source churn.
- Unknown/new fields are preserved verbatim rather than silently dropped
  (which `from_json` with a typed sub-schema would do on drift).
- Silver (Phase 3) is responsible for parsing `before`/`after` into typed,
  per-table columns -- that's where schema-awareness belongs, since Silver
  is what's allowed to evolve its schema when the source does.
