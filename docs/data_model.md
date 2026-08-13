# Data Model (Phase 1)

All tables live in the `commerce` schema (not `public`), so Debezium's
`schema.include.list` and per-role GRANTs stay scoped cleanly.

## Entities

| Table | Purpose |
|---|---|
| `warehouses` | Fixed reference data (~15 rows), FK target for `inventory.warehouse_id`. Not part of the spec's required entity list, added for a real FK instead of a bare int. |
| `customers` | Registered customers. |
| `products` | Product catalog. |
| `orders` | Customer orders; `order_status` state machine below. |
| `order_items` | Line items per order; price-at-purchase-time snapshot (jittered vs. catalog price). |
| `payments` | One or more payment attempts per order. |
| `inventory` | Per (product, warehouse) stock levels; `UNIQUE (product_id, warehouse_id)`. |
| `shipments` | At most one shipment per order, only for orders that reached `PAID` or later. |

## State machines

```
orders.order_status:  PLACED -> PAID -> SHIPPED -> DELIVERED
                        |         |
                        +--> CANCELLED (only from PLACED or PAID)

payments.payment_status:  PENDING -> SUCCESS | FAILED
                           SUCCESS -> REFUNDED (if order cancelled after payment)

shipments.shipment_status: PENDING -> SHIPPED -> IN_TRANSIT -> DELIVERED
                                                            -> RETURNED
```

A shipment row only exists once an order reaches `PAID` (simulates the
warehouse "pick" step); `PLACED` and `CANCELLED` orders never get one.

## Delete strategy (documented per entity, per spec section 13)

| Entity | Strategy | Rationale |
|---|---|---|
| `customers`, `orders`, `payments`, `shipments` | Never hard-deleted in Phase 1 | Business lifecycle is represented via status transitions (`CANCELLED`, `REFUNDED`) rather than deletion. Avoids cascade complexity across FK-referencing tables. |
| `order_items` | Hard delete | Simulates a line item removed before payment. Leaf table -- no downstream FK references it. |
| `inventory` | Hard delete (rare) | Simulates a housekeeping correction (e.g., discontinued product/warehouse pairing). Leaf table. |
| `products` | Never hard-deleted | Out-of-stock is represented via `stock_quantity = 0`, not deletion, since `order_items`/`inventory` reference it historically. |

## Invariants enforced at the source

- `shipments.delivered_at >= shipments.shipped_at` (DB `CHECK` constraint) --
  the source is clean by construction, so Phase 3's Great Expectations rule
  for this is validating a genuinely correct source; bad-data scenarios for
  data-quality testing should be injected at the Kafka/Bronze layer instead.
- All monetary and quantity columns are non-negative (`CHECK` constraints).
- `order_status`, `payment_status`, `shipment_status`, `payment_method` are
  constrained to known enums via `CHECK`.

## CDC readiness

See [ADR 0001](decisions/0001-logical-replication-in-phase1.md) for why
`REPLICA IDENTITY FULL`, the `commerce_cdc` publication, and the `debezium`
role already exist even though Debezium itself isn't part of Phase 1.
