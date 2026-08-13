from __future__ import annotations

import random
from datetime import datetime

INVENTORY_COLUMNS = ("inventory_id", "product_id", "warehouse_id", "available_quantity", "reserved_quantity", "updated_at")


def generate_inventory_row(
    inventory_id: int, product_id: int, warehouse_id: int, rng: random.Random, now: datetime
) -> tuple:
    available = rng.randint(0, 500)
    reserved = rng.randint(0, min(50, available)) if available else 0
    return (inventory_id, product_id, warehouse_id, available, reserved, now)
