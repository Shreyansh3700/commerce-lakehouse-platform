from __future__ import annotations

import random
from datetime import datetime, timedelta

from generator.utils.ids import IdCounter

ORDER_COLUMNS = ("order_id", "customer_id", "order_status", "total_amount", "created_at", "updated_at")
ORDER_ITEM_COLUMNS = ("order_item_id", "order_id", "product_id", "quantity", "unit_price", "created_at", "updated_at")

# How far past created_at an order's updated_at can drift, per settled status --
# more advanced states imply more elapsed time.
_PROGRESS_DAYS = {
    "PLACED": 0,
    "PAID": 1,
    "SHIPPED": 3,
    "DELIVERED": 6,
    "CANCELLED": 1,
}


def generate_order_items(
    order_id: int,
    item_id_counter: IdCounter,
    product_pool: list[tuple[int, float]],
    rng: random.Random,
    created_at: datetime,
    max_items: int,
) -> tuple[list[tuple], float]:
    """Returns (item_rows, total_amount) for one order. product_pool is a list
    of (product_id, catalog_price) to sample from without a DB round-trip."""
    num_items = rng.randint(1, max_items)
    chosen = rng.sample(product_pool, k=min(num_items, len(product_pool)))

    rows = []
    total = 0.0
    for product_id, catalog_price in chosen:
        item_id = item_id_counter.take_one()
        quantity = rng.randint(1, 5)
        # price-at-purchase-time snapshot, jittered vs. current catalog price
        unit_price = round(float(catalog_price) * rng.uniform(0.9, 1.1), 2)
        rows.append((item_id, order_id, product_id, quantity, unit_price, created_at, created_at))
        total += unit_price * quantity

    return rows, round(total, 2)


def generate_order_timestamps(rng: random.Random, status: str, now: datetime) -> tuple[datetime, datetime]:
    """Historical created_at within the last ~180 days; updated_at offset by
    how far the state machine has progressed for a settled historical order."""
    created_at = now - timedelta(days=rng.uniform(0, 180), hours=rng.uniform(0, 24))
    progress_days = _PROGRESS_DAYS[status]
    updated_at = created_at + timedelta(days=rng.uniform(0, progress_days + 1))
    return created_at, updated_at
