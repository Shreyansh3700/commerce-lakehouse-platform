# Streaming analytics, late-arriving events, and watermarks (Phase 4)

Implements spec sections 16-17: real-time metrics computed directly from
Kafka via PySpark Structured Streaming, and late-arriving-event handling
with watermarks and allowed lateness. See
[architecture.md](architecture.md#phase-4-streaming-analytics--late-arriving-events--schema-evolution)
for how this fits the overall pipeline, and
[decisions/0008-schema-evolution-strategy.md](decisions/0008-schema-evolution-strategy.md)
for the schema-evolution half of Phase 4 (a separate concern, covered there).

## Why Kafka, not Bronze/Silver

`streaming/pyspark/silver_writer.py` reads Bronze *Iceberg* tables
incrementally. `streaming/pyspark/analytics_writer.py` reads the `cdc.*`
*Kafka* topics directly instead -- a deliberate difference, not an
inconsistency: the spec explicitly asks for "real-time metrics from Kafka
using PySpark Structured Streaming" (section 16), to demonstrate genuine
Kafka-native stream processing rather than routing everything through the
lakehouse first. It runs as its own independent Kafka consumer group per
sink, coexisting with Bronze's own independent consumption of the same
topics -- Kafka supports arbitrarily many independent consumer groups per
topic with no conflict.

## Metrics

| Metric | Table | Window | Watermark | Notes |
|---|---|---|---|---|
| Orders / revenue per minute | `nessie.analytics.orders_per_minute` | 1 min tumbling | 5 min | Restricted to insert/snapshot events (`operation IN ('c','r')`) -- see below |
| 5-minute revenue | `nessie.analytics.revenue_5min` | 5 min tumbling | 5 min | Same restriction; mirrors the spec's own diagram example, demonstrates a second window granularity off the same code path |
| Payment failure rate | `nessie.analytics.payment_failure_rate` | 5 min tumbling | 5 min | Windowed on `updated_at` (resolution time), not `created_at`; `failure_rate = failed_count / resolved_count` computed at query time, same idiom as `gold.payment_metrics` |
| Product activity ("top products") | `nessie.analytics.product_activity` | 5 min tumbling, x `product_id` | 5 min | "Top N" is a plain `ORDER BY ... LIMIT N` (`make top-products`), not hand-rolled streaming top-N state |
| Inventory alerts | `nessie.analytics.inventory_alerts` | none (stateless) | none | Filter+append, `available_quantity <= LOW_STOCK_THRESHOLD` (20) |

All windowed queries are config-driven (`streaming/pyspark/analytics_config.py`'s
`WindowedMetricConfig`/`WINDOWED_METRIC_CONFIGS`) through one generic
implementation (`analytics_transform.py`'s `run_windowed_query`) -- adding a
new windowed metric is a new config entry, not new query code.

### Why `orders_per_minute`/`revenue_5min` filter to creation events only

An order's `created_at` never changes, but `cdc.orders` also carries every
later status-change event for that same order (PAID/SHIPPED/DELIVERED/
CANCELLED, and total_amount recalculation on item deletion) -- all windowed
on the same unchanging `created_at`, landing in the same window bucket as
the original creation event. Without a filter, a naive `count(*)`/
`sum(total_amount)` would count each of those later UPDATEs again,
inflating both the order count and revenue for that minute every time an
order's status changes within the watermark's allowed lateness of its own
creation. `WindowedMetricConfig.operation_filter=("c", "r")` on these two
metrics excludes everything but the original insert/snapshot event, so the
metric genuinely means "orders created in this window" / "revenue from
orders created in this window" -- matching the metric names, and avoiding
the double-count. (`payment_failure_rate`/`product_activity` don't need this
guard: their aggregations are conditional `CASE WHEN`/sum expressions that
naturally contribute nothing from an event that doesn't match, and
`order_items` in this project are only ever inserted or hard-deleted, never
updated.)

## Late-arriving events (spec section 17)

### Why the demonstration is scoped to `orders`

Only `orders.created_at` is both a genuine business event-time column *and*
explicitly settable by the client on INSERT with nothing overwriting it --
`commerce.set_updated_at()` (see `infrastructure/postgres/init/03_triggers_and_cdc.sql`)
only fires `BEFORE UPDATE`, never `BEFORE INSERT`. Payment/shipment status
*changes* go through `UPDATE`, so their `updated_at` is always the true
DB-commit time and can't be legitimately backdated -- which is also exactly
why `payment_failure_rate` windows on `updated_at` rather than trying to
simulate lateness there. So late-arriving-event handling is demonstrated on
order creation specifically, matching the spec's own worked example ("10:01
Order Created ... delivered late").

### The simulator's late-order event

`generator/streaming/simulator.py`'s `event_late_order` creates an order
(reusing the same `_create_order()` path as the normal `event_new_order`)
that's committed to Postgres *right now* -- so `source_lsn`, Kafka delivery,
and processing time are all "now" -- but whose `created_at` claims to have
happened 1-15 minutes in the past (`Settings.sim_late_orders_per_min`,
default 2/min). This is the platform's concrete source of late-arriving
events: an order whose business event-time is behind its actual arrival
time, the standard definition of "late data" in stream processing.

### The chosen threshold: a 5-minute watermark

`orders_per_minute`/`revenue_5min` use a 5-minute watermark. The
simulator's 1-15 minute backdate range deliberately straddles it:

- **Backdates under 5 minutes** land within Spark's native allowed lateness
  and correctly update an already-materialized but still-open window (the
  window's `order_count`/`revenue` gets revised upward on a later MERGE).
- **Backdates over 5 minutes** exceed it. Spark's engine would silently
  drop these from the windowed aggregation's state -- which is exactly what
  the late-event detector below exists to catch instead of losing them.

5 minutes was chosen as roughly the same order of magnitude as the window
sizes themselves (1-5 min) -- long enough to genuinely absorb ordinary
Kafka/consumer jitter and demonstrate a late row correctly revising an
already-written window, short enough that unbounded streaming state doesn't
grow indefinitely, and short enough relative to the simulator's 1-15 minute
range that a meaningful fraction of late orders land on *both* sides of the
threshold -- so both code paths (native watermark absorption, and the
explicit late-events capture) are actually exercised during a normal demo
run, not just one of them.

### Catching what Spark's watermark silently drops

Spark's built-in watermark mechanism never exposes *which* rows it dropped
-- it just excludes them from the stateful aggregation. So
`analytics_transform.py`'s `run_late_event_query` is a small, separate,
hand-rolled check (not a Spark watermark feature): a tiny
`nessie.analytics.watermark_state` table holds one row per opted-in query,
recording the watermark bound as of the end of the last completed batch.
Each micro-batch:

1. Read the stored bound (*before* this batch touches it -- mirrors Spark's
   own semantics of only advancing a watermark after a batch completes).
2. Any row with `event_time < stored_bound - lateness_threshold` is
   late-beyond-threshold -> appended to `nessie.analytics.late_events`
   (the "dedicated reconciliation/late-events dataset" spec section 17
   requires) instead of being silently lost.
3. Raise the stored bound to `GREATEST(stored_bound, this batch's own max
   event_time)`.

This runs as its own separate streaming query against `cdc.orders` (own
consumer group, own checkpoint) rather than inside `orders_per_minute`'s
`foreachBatch`, because a windowed aggregation's `foreachBatch` only ever
sees the aggregated `(window, count, sum)` rows, never the raw per-event
rows a lateness check needs.

## Recovery

Every query above uses Structured Streaming's standard
`checkpointLocation` (`s3a://lakehouse/checkpoints/analytics/<sink>/`), the
same mechanism Bronze/Silver already rely on -- a killed
`spark-analytics-writer` resumes from its last committed offset per query
on restart, with no duplicated or lost window rows (Iceberg's MERGE-by-key
write is itself idempotent per micro-batch, the same property Silver's
MERGE guard relies on).
