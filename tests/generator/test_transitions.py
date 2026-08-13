import random

import pytest

from generator.streaming.transitions import (
    ORDER_STATUS_SEED_WEIGHTS,
    ORDER_TRANSITIONS,
    is_order_terminal,
    next_order_status,
    seed_order_status,
)


def test_terminal_statuses_have_no_transitions():
    assert is_order_terminal("DELIVERED")
    assert is_order_terminal("CANCELLED")
    assert not is_order_terminal("PLACED")
    assert not is_order_terminal("PAID")
    assert not is_order_terminal("SHIPPED")


def test_next_order_status_terminal_returns_none():
    rng = random.Random(0)
    assert next_order_status("DELIVERED", rng) is None
    assert next_order_status("CANCELLED", rng) is None


def test_next_order_status_only_returns_valid_transitions():
    rng = random.Random(1)
    for status, options in ORDER_TRANSITIONS.items():
        if not options:
            continue
        for _ in range(50):
            result = next_order_status(status, rng)
            assert result in options


def test_cancellation_only_reachable_from_placed_or_paid():
    for status, options in ORDER_TRANSITIONS.items():
        if "CANCELLED" in options:
            assert status in ("PLACED", "PAID")
    assert "CANCELLED" not in ORDER_TRANSITIONS["SHIPPED"]
    assert "CANCELLED" not in ORDER_TRANSITIONS["DELIVERED"]


def test_seed_weights_sum_to_one():
    assert ORDER_STATUS_SEED_WEIGHTS["PLACED"] == pytest.approx(0.05)
    assert sum(ORDER_STATUS_SEED_WEIGHTS.values()) == pytest.approx(1.0)


def test_seed_order_status_only_returns_known_statuses():
    rng = random.Random(2)
    valid = set(ORDER_TRANSITIONS)
    for _ in range(200):
        assert seed_order_status(rng) in valid
