from __future__ import annotations

import random
from datetime import datetime, timedelta

SHIPMENT_COLUMNS = ("shipment_id", "order_id", "shipment_status", "carrier", "shipped_at", "delivered_at", "updated_at")

CARRIERS = ("BlueDart", "Delhivery", "DTDC", "India Post", "Ekart", "FedEx")

# Shipments only exist for orders that reached at least PAID (the warehouse
# "pick" step) -- PLACED/CANCELLED orders never get a shipment row.
SHIPMENT_ELIGIBLE_ORDER_STATUSES = ("PAID", "SHIPPED", "DELIVERED")


def shipment_status_for_order(order_status: str, rng: random.Random) -> str:
    if order_status == "SHIPPED":
        return rng.choice(["SHIPPED", "IN_TRANSIT"])
    if order_status == "DELIVERED":
        return rng.choices(["DELIVERED", "RETURNED"], weights=[0.95, 0.05], k=1)[0]
    return "PENDING"  # order_status == "PAID"


def generate_shipment(
    shipment_id: int, order_id: int, order_status: str, rng: random.Random, order_updated_at: datetime
) -> tuple:
    status = shipment_status_for_order(order_status, rng)
    carrier = rng.choice(CARRIERS)

    shipped_at = None
    delivered_at = None
    if status in ("SHIPPED", "IN_TRANSIT", "DELIVERED", "RETURNED"):
        shipped_at = order_updated_at - timedelta(hours=rng.uniform(0, 12))
    if status in ("DELIVERED", "RETURNED"):
        delivered_at = shipped_at + timedelta(days=rng.uniform(0.5, 5))

    updated_at = delivered_at or shipped_at or order_updated_at
    return (shipment_id, order_id, status, carrier, shipped_at, delivered_at, updated_at)
