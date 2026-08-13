from __future__ import annotations

import random
from datetime import datetime, timedelta

from generator.streaming.transitions import PAYMENT_METHODS

PAYMENT_COLUMNS = ("payment_id", "order_id", "payment_status", "payment_method", "amount", "created_at", "updated_at")


def payment_status_for_order(order_status: str, rng: random.Random) -> str:
    if order_status in ("PAID", "SHIPPED", "DELIVERED"):
        return "SUCCESS"
    if order_status == "CANCELLED":
        return rng.choice(["REFUNDED", "FAILED"])
    return rng.choice(["PENDING", "FAILED"])


def generate_payment(
    payment_id: int,
    order_id: int,
    order_status: str,
    amount: float,
    rng: random.Random,
    order_created_at: datetime,
) -> tuple:
    status = payment_status_for_order(order_status, rng)
    method = rng.choice(PAYMENT_METHODS)
    created_at = order_created_at + timedelta(minutes=rng.uniform(1, 120))
    return (payment_id, order_id, status, method, amount, created_at, created_at)


def generate_retry_payment(
    payment_id: int, order_id: int, amount: float, rng: random.Random, order_created_at: datetime
) -> tuple:
    """A FAILED attempt preceding the real payment -- adds realistic volume
    above a strict 1:1 ratio with orders."""
    method = rng.choice(PAYMENT_METHODS)
    created_at = order_created_at + timedelta(minutes=rng.uniform(0.5, 30))
    return (payment_id, order_id, "FAILED", method, amount, created_at, created_at)
