# Data Quality (Phase 3 -- not yet implemented)

This document will cover the Great Expectations checks from spec section 24
(completeness, validity, referential integrity, business rules) once the
Silver/Gold layers exist to validate.

## What Phase 1 already guarantees at the source

- `shipments.delivered_at >= shipments.shipped_at` is enforced by a DB
  `CHECK` constraint -- see [data_model.md](data_model.md#invariants-enforced-at-the-source).
- Monetary/quantity columns are non-negative; status columns are constrained
  to known enums.

Because the source is clean by construction, Phase 3's "bad data" test
scenarios should be injected at the Kafka/Bronze layer (a deliberately
malformed event) rather than by weakening these Postgres constraints.
