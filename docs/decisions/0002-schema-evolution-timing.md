# ADR 0002: Defer schema evolution demonstration to Phase 4

## Status
Accepted

## Context
The spec (section 18) requires demonstrating a source schema change
propagating safely through the platform, including an additive column and
a rename/type-change example.

## Decision
`orders.discount_amount` (additive) and a `shipments.carrier` ->
`carrier_name` rename (the "requires special handling" case) are **not**
part of the Phase 1 schema. They will be introduced as explicit `ALTER
TABLE` statements in Phase 4, once a live CDC pipeline (Postgres ->
Debezium -> Kafka -> Bronze -> Silver) exists to observe the change
propagate end-to-end.

## Consequences
Adding these columns in Phase 1 would just be inert DDL with nothing to
observe -- there's no CDC pipeline yet. Phase 4's schema-evolution work
(spec section 18, Scenario 7) will alter `commerce.orders` and
`commerce.shipments` directly, and Debezium/Bronze/Silver handling of the
change is what gets demonstrated and documented at that point.
