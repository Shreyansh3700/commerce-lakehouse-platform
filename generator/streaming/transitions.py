from __future__ import annotations

import random

# Pure state-machine definitions shared by both the bulk seeder (to pick a
# plausible historical status for a past order) and the streaming simulator
# (to advance one step at a time). No DB access here.

ORDER_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PLACED": ("PAID", "CANCELLED"),
    "PAID": ("SHIPPED", "CANCELLED"),
    "SHIPPED": ("DELIVERED",),
    "DELIVERED": (),
    "CANCELLED": (),
}

# Distribution used only when seeding historical orders in bulk -- most
# already-existing orders should look like they settled long ago.
ORDER_STATUS_SEED_WEIGHTS: dict[str, float] = {
    "PLACED": 0.05,
    "PAID": 0.08,
    "SHIPPED": 0.12,
    "DELIVERED": 0.65,
    "CANCELLED": 0.10,
}

PAYMENT_STATUSES = ("PENDING", "SUCCESS", "FAILED", "REFUNDED")
SHIPMENT_STATUSES = ("PENDING", "SHIPPED", "IN_TRANSIT", "DELIVERED", "RETURNED")
PAYMENT_METHODS = ("CREDIT_CARD", "DEBIT_CARD", "UPI", "NET_BANKING", "WALLET", "COD")


def is_order_terminal(status: str) -> bool:
    return not ORDER_TRANSITIONS[status]


def next_order_status(status: str, rng: random.Random) -> str | None:
    """Returns a valid next status for `status`, or None if it's terminal."""
    options = ORDER_TRANSITIONS[status]
    return rng.choice(options) if options else None


def seed_order_status(rng: random.Random) -> str:
    statuses = list(ORDER_STATUS_SEED_WEIGHTS)
    weights = list(ORDER_STATUS_SEED_WEIGHTS.values())
    return rng.choices(statuses, weights=weights, k=1)[0]
