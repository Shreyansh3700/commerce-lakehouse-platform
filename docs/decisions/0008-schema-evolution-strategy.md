# ADR 0008: Schema-evolution compatibility rules

## Status
Accepted

## Context
Spec section 18 (Scenario 7) requires demonstrating that a source schema
change propagates safely through the platform, and specifically calls out
renames/type changes as needing "special handling" distinct from additive
changes. ADR 0002 deferred picking a concrete demonstration to Phase 4, once
a live CDC pipeline existed to observe the change propagate; that
demonstration is now implemented (Part C of the Phase 4 plan):
`orders.discount_amount` (additive) and `shipments.carrier` ->
`carrier_name` (rename). This ADR generalizes what that demonstration
established into a rule of thumb for schema changes made to this project's
source tables (`commerce.*`) going forward, independent of which specific
column changes next.

Two facts specific to this pipeline's design make the difference between
"safe in place" and "needs special handling" concrete rather than abstract:
- Bronze stores `before`/`after` as raw JSON strings, not typed structs (ADR
  0005) -- so Bronze itself never breaks on any source schema change, additive
  or not. All compatibility concerns below are about Silver, which is the
  first layer that imposes a typed schema (`from_json` with a fixed
  `StructType`, see `streaming/pyspark/silver_schemas.py`).
- `from_json` silently drops JSON keys it doesn't have a StructField for, and
  silently nulls a StructField that the JSON payload doesn't contain. Neither
  case raises. That silence is what makes additive changes safe by default
  and what makes renames dangerous by default -- both behaviors come from the
  exact same mechanism.

## Decision
Three categories of source schema change, and the handling each requires:

1. **Additive nullable/defaulted columns are safe in both directions.**
   Adding a column (e.g. `orders.discount_amount NOT NULL DEFAULT 0`) is
   safe to apply directly against the running source with no coordination:
   old Silver code (not yet updated) keeps working unmodified, because
   `from_json` simply ignores the new key it doesn't know about yet -- no
   crash, no restart required. New Silver code, once deployed, picks up the
   new key from that point forward. The Iceberg-side change (an actual
   `ALTER TABLE ... ADD COLUMN`) is applied via the idempotent
   `ensure_column()` helper in `streaming/pyspark/schema_evolution.py`,
   registered in its `SCHEMA_EVOLUTIONS` list and run once at Silver writer
   startup -- safe to call on every restart, the same way
   `CREATE TABLE IF NOT EXISTS` already is for the rest of `silver_ddl.sql`.
   Known limitation: rows never touched by a CDC event after the column
   started being captured keep showing NULL/default in Silver until a
   Bronze -> Silver replay reprocesses their history (see the Phase 4 plan's
   backfill note) -- this is a property of Silver being built from an
   incremental change stream, not a flaw in the ALTER itself.

2. **Renames and narrowing type changes are NOT safe in place and require
   expand-contract.** A rename (`shipments.carrier` -> `carrier_name`) is
   the concrete case this phase demonstrates: the instant it's applied at
   the source, Debezium starts emitting the new key name in every
   subsequent envelope, and old Silver code parsing for the old key gets a
   **silent NULL**, not an error -- `from_json` has no way to tell "key
   renamed" apart from "key legitimately absent". A narrowing type change
   (e.g. a column shrinking precision, or a nullable column becoming
   effectively incompatible with its old cast) has the same silent-failure
   shape: `from_json`/`CAST` produce NULL or a truncated value rather than
   raising. Neither is safe to handle with a single in-place `ALTER TABLE`
   on the Silver/Iceberg side. Both require the standard expand-contract
   sequence instead: add the new field alongside the old one (dual-write at
   the source is out of this project's control since Debezium mirrors
   Postgres directly, so here "dual-read" is what's actually implemented --
   parsing both keys), resolve the single logical value via
   `COALESCE(new, old)` so both pre- and post-change events parse correctly,
   let consumers migrate, and only then retire the old field. This
   project's shipments case implements exactly the dual-read half
   (`silver_schemas.py`'s `EXTRA_PARSE_FIELDS`/`FIELD_COALESCE`,
   consumed by `silver_transform.py`'s `validate()`); dropping the old
   `carrier` key from the parse schema entirely is documented as a later
   cleanup step once no historical/replay concern remains, not implemented
   now -- keeping the demo bounded, per the Phase 4 plan.

3. **Type-widening is safe and reuses the additive-column mechanism.** A
   widening change (e.g. `INTEGER` -> `BIGINT`, or `DECIMAL(10,2)` ->
   `DECIMAL(12,2)`) never loses information for existing values, so it's
   handled the same way as case 1: an idempotent `ALTER TABLE ... ALTER
   COLUMN` (or, for a genuinely new field, `ADD COLUMN`) via the same
   `ensure_column()`-style check-before-alter pattern in
   `schema_evolution.py`, not expand-contract. No entry of this kind exists
   in `SCHEMA_EVOLUTIONS` yet, since this phase's demonstration is
   deliberately scoped to one additive column and one rename (ADR 0002), but
   the mechanism is the same one that would apply.

## Consequences
- Additive changes and type-widening can be rolled out to `commerce.*`
  directly against a live system, with Silver code updated on its own
  schedule afterward -- no coordinated cutover required, at the cost of a
  documented backfill gap for rows untouched since the change.
- Renames and narrowing type changes require planning an expand-contract
  window (both keys parsed, one logical column resolved via COALESCE) before
  the old field can ever be dropped -- they cannot be treated as a same-day,
  single-`ALTER` change the way case 1/3 can.
- The next schema evolution of either kind has a concrete pattern to follow
  in this codebase: `schema_evolution.py`'s `SCHEMA_EVOLUTIONS` list for
  case 1/3, `silver_schemas.py`'s `EXTRA_PARSE_FIELDS`/`FIELD_COALESCE` for
  case 2 -- rather than being designed from scratch.
