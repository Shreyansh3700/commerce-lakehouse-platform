from __future__ import annotations

import argparse
import logging
import random
from datetime import datetime, timezone

import psycopg
from faker import Faker
from tqdm import tqdm

from generator.config import get_settings
from generator.db import copy_rows, get_connection, resync_sequence, truncate_all
from generator.entities.customers import CUSTOMER_COLUMNS, generate_customer
from generator.entities.inventory import INVENTORY_COLUMNS, generate_inventory_row
from generator.entities.orders import (
    ORDER_COLUMNS,
    ORDER_ITEM_COLUMNS,
    generate_order_items,
    generate_order_timestamps,
)
from generator.entities.payments import PAYMENT_COLUMNS, generate_payment, generate_retry_payment
from generator.entities.products import PRODUCT_COLUMNS, generate_product
from generator.entities.shipments import SHIPMENT_COLUMNS, SHIPMENT_ELIGIBLE_ORDER_STATUSES, generate_shipment
from generator.streaming.transitions import seed_order_status
from generator.utils.ids import IdCounter

logger = logging.getLogger(__name__)

# Fraction of eligible (PAID/SHIPPED/DELIVERED) orders that get an extra
# FAILED retry payment before the real one -- pushes total payment volume
# comfortably past a strict 1:1 ratio with orders.
RETRY_PAYMENT_PROBABILITY = 0.08


def load_customers(conn: psycopg.Connection, fake: Faker, n: int, batch_size: int, now: datetime) -> None:
    logger.info("Seeding %d customers", n)
    ids = IdCounter(1)
    written = 0
    with tqdm(total=n, desc="customers") as bar:
        while written < n:
            batch_n = min(batch_size, n - written)
            rows = [generate_customer(cid, fake, now) for cid in ids.take(batch_n)]
            copy_rows(conn, "customers", CUSTOMER_COLUMNS, rows)
            written += batch_n
            bar.update(batch_n)
    resync_sequence(conn, "customers", "customer_id")


def load_products(conn: psycopg.Connection, fake: Faker, rng: random.Random, n: int, batch_size: int, now: datetime) -> None:
    logger.info("Seeding %d products", n)
    ids = IdCounter(1)
    written = 0
    with tqdm(total=n, desc="products") as bar:
        while written < n:
            batch_n = min(batch_size, n - written)
            rows = [generate_product(pid, fake, rng, now) for pid in ids.take(batch_n)]
            copy_rows(conn, "products", PRODUCT_COLUMNS, rows)
            written += batch_n
            bar.update(batch_n)
    resync_sequence(conn, "products", "product_id")


def load_orders_and_dependents(
    conn: psycopg.Connection,
    rng: random.Random,
    n_orders: int,
    n_customers: int,
    max_items_per_order: int,
    batch_size: int,
    now: datetime,
) -> None:
    """Generates orders + order_items + payments + shipments together, batch
    by batch: total_amount depends on that order's own items, and payment /
    shipment existence depends on the status chosen for that order, so all
    four tables are produced from the same in-memory batch before COPY-ing."""
    logger.info("Seeding %d orders (+ order_items, payments, shipments)", n_orders)

    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price FROM commerce.products ORDER BY product_id")
        product_pool = cur.fetchall()

    order_ids = IdCounter(1)
    item_ids = IdCounter(1)
    payment_ids = IdCounter(1)
    shipment_ids = IdCounter(1)

    written = 0
    with tqdm(total=n_orders, desc="orders") as bar:
        while written < n_orders:
            batch_n = min(batch_size, n_orders - written)

            order_rows: list[tuple] = []
            item_rows: list[tuple] = []
            payment_rows: list[tuple] = []
            shipment_rows: list[tuple] = []

            for order_id in order_ids.take(batch_n):
                customer_id = rng.randint(1, n_customers)
                status = seed_order_status(rng)
                created_at, updated_at = generate_order_timestamps(rng, status, now)

                items, total_amount = generate_order_items(
                    order_id, item_ids, product_pool, rng, created_at, max_items_per_order
                )
                item_rows.extend(items)
                order_rows.append((order_id, customer_id, status, total_amount, created_at, updated_at))

                if status in SHIPMENT_ELIGIBLE_ORDER_STATUSES and rng.random() < RETRY_PAYMENT_PROBABILITY:
                    payment_rows.append(
                        generate_retry_payment(payment_ids.take_one(), order_id, total_amount, rng, created_at)
                    )
                payment_rows.append(
                    generate_payment(payment_ids.take_one(), order_id, status, total_amount, rng, created_at)
                )

                if status in SHIPMENT_ELIGIBLE_ORDER_STATUSES:
                    shipment_rows.append(
                        generate_shipment(shipment_ids.take_one(), order_id, status, rng, updated_at)
                    )

            copy_rows(conn, "orders", ORDER_COLUMNS, order_rows)
            copy_rows(conn, "order_items", ORDER_ITEM_COLUMNS, item_rows)
            copy_rows(conn, "payments", PAYMENT_COLUMNS, payment_rows)
            if shipment_rows:
                copy_rows(conn, "shipments", SHIPMENT_COLUMNS, shipment_rows)

            written += batch_n
            bar.update(batch_n)

    resync_sequence(conn, "orders", "order_id")
    resync_sequence(conn, "order_items", "order_item_id")
    resync_sequence(conn, "payments", "payment_id")
    resync_sequence(conn, "shipments", "shipment_id")


def load_inventory(conn: psycopg.Connection, rng: random.Random, n_products: int, now: datetime) -> None:
    logger.info("Seeding inventory across all products x warehouses")
    with conn.cursor() as cur:
        cur.execute("SELECT warehouse_id FROM commerce.warehouses ORDER BY warehouse_id")
        warehouse_ids = [row[0] for row in cur.fetchall()]
    if not warehouse_ids:
        raise RuntimeError("No warehouses found -- did infrastructure/postgres/init/04_reference_data.sql run?")

    ids = IdCounter(1)
    rows: list[tuple] = []
    total = n_products * len(warehouse_ids)
    with tqdm(total=total, desc="inventory") as bar:
        for product_id in range(1, n_products + 1):
            for warehouse_id in warehouse_ids:
                rows.append(generate_inventory_row(ids.take_one(), product_id, warehouse_id, rng, now))
                bar.update(1)
            if len(rows) >= 25_000:
                copy_rows(conn, "inventory", INVENTORY_COLUMNS, rows)
                rows = []
        if rows:
            copy_rows(conn, "inventory", INVENTORY_COLUMNS, rows)
    resync_sequence(conn, "inventory", "inventory_id")


def run(reset: bool = False) -> None:
    settings = get_settings()
    rng = random.Random(settings.seed_random_seed)
    fake = Faker()
    fake.seed_instance(settings.seed_random_seed)
    now = datetime.now(timezone.utc)

    with get_connection() as conn:
        if reset:
            logger.info("Resetting existing transactional data (TRUNCATE ... CASCADE)")
            truncate_all(conn)

        load_customers(conn, fake, settings.seed_customers, settings.seed_batch_size, now)
        load_products(conn, fake, rng, settings.seed_products, settings.seed_batch_size, now)
        load_orders_and_dependents(
            conn,
            rng,
            settings.seed_orders,
            settings.seed_customers,
            settings.seed_order_items_max_per_order,
            settings.seed_batch_size,
            now,
        )
        load_inventory(conn, rng, settings.seed_products, now)

    logger.info("Seeding complete.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Bulk-seed the commerce OLTP database")
    parser.add_argument("--reset", action="store_true", help="Truncate existing transactional data before reseeding")
    args = parser.parse_args()
    run(reset=args.reset)
