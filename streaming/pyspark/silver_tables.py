from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    """Declarative per-entity config driving silver_transform.py's generic
    validate/dedupe/order/MERGE pipeline -- mirrors bronze_writer.py's
    (kafka_topic, iceberg_table) TABLES list, extended with what Silver needs
    beyond a plain append: the primary key to MERGE on, the delete strategy
    (see docs/data_model.md's per-entity delete documentation), and whether
    this entity also feeds the SCD2 dim_customer_history job (scd2.py)."""

    entity: str
    bronze_table: str
    silver_table: str
    pk_cols: tuple[str, ...]
    delete_strategy: str  # "hard" | "soft"
    has_scd2: bool = False


# order_items/inventory are genuinely hard-deleted at source; everything else
# never is (lifecycle is via status columns instead) and gets a defensive
# soft-delete shape in Silver -- see docs/data_model.md and the Phase 3 plan.
TABLE_CONFIGS: list[TableConfig] = [
    TableConfig(
        entity="customers",
        bronze_table="nessie.bronze.customers_cdc",
        silver_table="nessie.silver.customers",
        pk_cols=("customer_id",),
        delete_strategy="soft",
        has_scd2=True,
    ),
    TableConfig(
        entity="products",
        bronze_table="nessie.bronze.products_cdc",
        silver_table="nessie.silver.products",
        pk_cols=("product_id",),
        delete_strategy="soft",
    ),
    TableConfig(
        entity="orders",
        bronze_table="nessie.bronze.orders_cdc",
        silver_table="nessie.silver.orders",
        pk_cols=("order_id",),
        delete_strategy="soft",
    ),
    TableConfig(
        entity="order_items",
        bronze_table="nessie.bronze.order_items_cdc",
        silver_table="nessie.silver.order_items",
        pk_cols=("order_item_id",),
        delete_strategy="hard",
    ),
    TableConfig(
        entity="payments",
        bronze_table="nessie.bronze.payments_cdc",
        silver_table="nessie.silver.payments",
        pk_cols=("payment_id",),
        delete_strategy="soft",
    ),
    TableConfig(
        entity="inventory",
        bronze_table="nessie.bronze.inventory_cdc",
        silver_table="nessie.silver.inventory",
        pk_cols=("inventory_id",),
        delete_strategy="hard",
    ),
    TableConfig(
        entity="shipments",
        bronze_table="nessie.bronze.shipments_cdc",
        silver_table="nessie.silver.shipments",
        pk_cols=("shipment_id",),
        delete_strategy="soft",
    ),
]
