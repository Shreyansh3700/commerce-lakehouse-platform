from __future__ import annotations

from dataclasses import dataclass

# The watermark analytics_transform.py's run_windowed_query() applies to
# orders_per_minute, and the lateness threshold run_late_event_query() checks
# stored-watermark-relative arrival for the very same cdc.orders topic, MUST
# be identical: a backdated order that Spark's own watermark would still
# accept into an open window (< 5 min late) should NOT also be flagged
# late-beyond-threshold by the hand-rolled check below -- otherwise the same
# row would both correctly update an open window AND wrongly show up in
# nessie.analytics.late_events. See the Phase 4 plan's "run_late_event_query
# mechanics" section. Defined once here so both WindowedMetricConfig.watermark
# (the orders_per_minute entry below) and LateEventConfig.lateness_threshold
# reference the same literal instead of two independently-maintained
# "5 minutes" strings that could drift apart.
ORDERS_WATERMARK = "5 minutes"

# Used by the other three windowed metrics, whose event-time columns
# (payments.updated_at, order_items.created_at) are never deliberately
# backdated by the simulator (see Part B's "Why orders specifically") -- this
# is just a generous-enough allowed-lateness for ordinary Kafka/consumer
# jitter, not tied to any specific demo scenario like ORDERS_WATERMARK is.
_DEFAULT_WATERMARK = "5 minutes"


@dataclass(frozen=True)
class WindowedMetricConfig:
    """Declarative definition of one windowed streaming aggregation, driving
    analytics_transform.py's generic run_windowed_query() -- mirrors
    silver_tables.py's TableConfig/TABLE_CONFIGS split (data lives here,
    behavior lives in analytics_transform.py). `agg_exprs` are plain Spark
    SQL expression strings (interpreted via pyspark.sql.functions.expr at
    query-build time), each aliased to one of `value_cols` -- keeping this
    module free of any pyspark import, same as TableConfig."""

    entity: str  # key into silver_schemas.ENTITY_SCHEMAS
    topic: str  # source Kafka topic, e.g. "cdc.orders"
    event_time_col: str
    window_duration: str  # e.g. "1 minute"
    watermark: str  # e.g. "5 minutes"
    extra_group_cols: tuple[str, ...]  # beyond the window itself, e.g. ("product_id",)
    agg_exprs: tuple[str, ...]  # SQL expressions, each "... AS <alias>"
    value_cols: tuple[str, ...]  # the aliases above -- the MERGE's non-key columns
    sink_table: str  # nessie.analytics.<table>
    # Debezium `operation` codes to include ('c'=insert, 'r'=snapshot,
    # 'u'=update) -- empty means no filter (every non-delete event counts).
    # orders_per_minute/revenue_5min MUST restrict to ("c", "r"): an order's
    # created_at never changes, so its later status-change/total_amount
    # UPDATE events (PAID/SHIPPED/DELIVERED/CANCELLED, item-deletion total
    # recalculation) would otherwise land in the SAME window as its original
    # creation event and be double/triple-counted by count(*)/sum(total_amount)
    # -- this field exists specifically to exclude those, so the metric means
    # "orders created" / "revenue from orders created", not "order events".
    operation_filter: tuple[str, ...] = ()


WINDOWED_METRIC_CONFIGS: list[WindowedMetricConfig] = [
    # Satisfies both "orders per minute" and "revenue per minute" off one
    # 1-minute tumbling window over cdc.orders.
    WindowedMetricConfig(
        entity="orders",
        topic="cdc.orders",
        event_time_col="created_at",
        window_duration="1 minute",
        watermark=ORDERS_WATERMARK,
        extra_group_cols=(),
        agg_exprs=(
            "CAST(count(*) AS BIGINT) AS order_count",
            "CAST(sum(total_amount) AS DECIMAL(18,2)) AS revenue",
        ),
        value_cols=("order_count", "revenue"),
        sink_table="nessie.analytics.orders_per_minute",
        operation_filter=("c", "r"),
    ),
    # Same source, a 5-minute window -- mirrors the spec's own diagram
    # example ("5-minute revenue") and demonstrates a second window
    # granularity off the same generic run_windowed_query() code path.
    WindowedMetricConfig(
        entity="orders",
        topic="cdc.orders",
        event_time_col="created_at",
        window_duration="5 minutes",
        watermark=_DEFAULT_WATERMARK,
        extra_group_cols=(),
        agg_exprs=("CAST(sum(total_amount) AS DECIMAL(18,2)) AS revenue",),
        value_cols=("revenue",),
        sink_table="nessie.analytics.revenue_5min",
        operation_filter=("c", "r"),
    ),
    # updated_at (not created_at) -- the genuine DB-committed resolution
    # time for a payment's terminal status, trigger-maintained and not
    # spoofable, unlike orders.created_at (see the Phase 4 plan's Part B
    # "Why orders specifically"). failure_rate itself is computed at query
    # time (make query-analytics), same idiom as gold.payment_metrics --
    # not stored here.
    WindowedMetricConfig(
        entity="payments",
        topic="cdc.payments",
        event_time_col="updated_at",
        window_duration="5 minutes",
        watermark=_DEFAULT_WATERMARK,
        extra_group_cols=(),
        agg_exprs=(
            "CAST(sum(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS BIGINT) AS failed_count",
            "CAST(sum(CASE WHEN payment_status IN ('SUCCESS','FAILED') THEN 1 ELSE 0 END) AS BIGINT)"
            " AS resolved_count",
        ),
        value_cols=("failed_count", "resolved_count"),
        sink_table="nessie.analytics.payment_failure_rate",
    ),
    # Grouped by product_id in addition to the window -- "top products" is
    # then a plain ORDER BY ... LIMIT N over this table (make top-products),
    # not hand-rolled streaming top-N state.
    WindowedMetricConfig(
        entity="order_items",
        topic="cdc.order_items",
        event_time_col="created_at",
        window_duration="5 minutes",
        watermark=_DEFAULT_WATERMARK,
        extra_group_cols=("product_id",),
        agg_exprs=(
            "CAST(sum(quantity) AS BIGINT) AS quantity",
            "CAST(sum(quantity * unit_price) AS DECIMAL(18,2)) AS revenue",
        ),
        value_cols=("quantity", "revenue"),
        sink_table="nessie.analytics.product_activity",
    ),
]


@dataclass(frozen=True)
class AlertConfig:
    """Declarative definition of one stateless filter+append alert query,
    driving analytics_transform.py's generic run_alert_query()."""

    entity: str
    topic: str
    filter_expr: str  # SQL boolean expression over the parsed `after` columns
    select_cols: tuple[str, ...]  # entity columns to carry into the sink, in order
    sink_table: str


# Conservative default: low enough that it doesn't fire on ordinary demand
# fluctuation, high enough to give a warehouse operator real lead time before
# a stockout -- same "one-line rationale next to the constant" style as
# gold_builder.py's FULFILLMENT_SLA_HOURS.
LOW_STOCK_THRESHOLD = 20

ALERT_CONFIGS: list[AlertConfig] = [
    AlertConfig(
        entity="inventory",
        topic="cdc.inventory",
        filter_expr=f"available_quantity <= {LOW_STOCK_THRESHOLD}",
        select_cols=("product_id", "warehouse_id", "available_quantity", "reserved_quantity"),
        sink_table="nessie.analytics.inventory_alerts",
    ),
]


@dataclass(frozen=True)
class LateEventConfig:
    """Declarative definition of the hand-rolled late-arriving-event check,
    driving analytics_transform.py's generic run_late_event_query() -- see
    the Phase 4 plan's "run_late_event_query mechanics" section for why this
    can't just be Spark's own watermark (which never exposes which rows it
    silently dropped)."""

    entity: str
    topic: str
    event_time_col: str
    lateness_threshold: str  # MUST match the paired WindowedMetricConfig's watermark -- see ORDERS_WATERMARK above
    query_name: str  # key into nessie.analytics.watermark_state
    late_events_table: str
    watermark_state_table: str


LATE_EVENT_CONFIGS: list[LateEventConfig] = [
    LateEventConfig(
        entity="orders",
        topic="cdc.orders",
        event_time_col="created_at",
        lateness_threshold=ORDERS_WATERMARK,
        query_name="orders_late_event_detector",
        late_events_table="nessie.analytics.late_events",
        watermark_state_table="nessie.analytics.watermark_state",
    ),
]
