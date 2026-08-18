from __future__ import annotations

import logging
import random
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg
from faker import Faker

from generator.config import Settings, get_settings
from generator.db import get_connection, max_id
from generator.entities.customers import generate_customer
from generator.entities.shipments import CARRIERS
from generator.streaming.transitions import PAYMENT_METHODS
from generator.utils.logging import configure_logging, log_mutation

logger = logging.getLogger(__name__)

# Cancellation is only ever chosen for orders still in PLACED/PAID -- SHIPPED
# and DELIVERED are excluded here at the call site, matching ORDER_TRANSITIONS.
CANCELLABLE_STATUSES = ("PLACED", "PAID")


@dataclass
class SimulatorState:
    conn: psycopg.Connection
    rng: random.Random
    fake: Faker
    settings: Settings

    max_customer_id: int
    max_product_id: int
    warehouse_ids: list[int]

    # Orders created or touched during this run that haven't reached a
    # terminal status yet -- {order_id: order_status}. Seeded at startup with
    # a sample of open historical orders so status-change events have a
    # backlog to work with immediately.
    open_orders: dict[int, str] = field(default_factory=dict)

    next_customer_id: int = 0
    next_order_id: int = 0
    next_item_id: int = 0
    next_payment_id: int = 0
    next_shipment_id: int = 0


def _load_initial_state(conn: psycopg.Connection, rng: random.Random, fake: Faker, settings: Settings) -> SimulatorState:
    with conn.cursor() as cur:
        cur.execute("SELECT warehouse_id FROM commerce.warehouses ORDER BY warehouse_id")
        warehouse_ids = [row[0] for row in cur.fetchall()]
        cur.execute(
            "SELECT order_id, order_status FROM commerce.orders "
            "WHERE order_status NOT IN ('DELIVERED', 'CANCELLED') "
            "ORDER BY random() LIMIT 5000"
        )
        open_orders = dict(cur.fetchall())

    state = SimulatorState(
        conn=conn,
        rng=rng,
        fake=fake,
        settings=settings,
        max_customer_id=max_id(conn, "customers", "customer_id"),
        max_product_id=max_id(conn, "products", "product_id"),
        warehouse_ids=warehouse_ids,
        open_orders=open_orders,
    )
    state.next_customer_id = state.max_customer_id + 1
    state.next_order_id = max_id(conn, "orders", "order_id") + 1
    state.next_item_id = max_id(conn, "order_items", "order_item_id") + 1
    state.next_payment_id = max_id(conn, "payments", "payment_id") + 1
    state.next_shipment_id = max_id(conn, "shipments", "shipment_id") + 1
    return state


# --- individual mutation events -------------------------------------------------
# Each of these performs one committed unit of work (conn is autocommit=True),
# so every mutation produces its own distinct WAL record / CDC event.

def event_new_customer(state: SimulatorState) -> None:
    now = datetime.now(timezone.utc)
    customer_id = state.next_customer_id
    row = generate_customer(customer_id, state.fake, now)
    state.conn.execute(
        "INSERT INTO commerce.customers (customer_id, name, email, city, country, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        row,
    )
    state.next_customer_id += 1
    state.max_customer_id = customer_id
    log_mutation("new_customer", customer_id=customer_id)


def event_new_order(state: SimulatorState) -> None:
    if state.max_customer_id < 1 or state.max_product_id < 1:
        return
    rng = state.rng
    order_id = state.next_order_id
    customer_id = rng.randint(1, state.max_customer_id)
    now = datetime.now(timezone.utc)

    num_items = rng.randint(1, 4)
    product_ids = [rng.randint(1, state.max_product_id) for _ in range(num_items)]
    total_amount = 0.0
    item_rows = []
    for product_id in product_ids:
        with state.conn.cursor() as cur:
            cur.execute("SELECT price FROM commerce.products WHERE product_id = %s", (product_id,))
            result = cur.fetchone()
        if result is None:
            continue
        price = float(result[0])
        quantity = rng.randint(1, 5)
        unit_price = round(price * rng.uniform(0.9, 1.1), 2)
        item_id = state.next_item_id
        state.next_item_id += 1
        item_rows.append((item_id, order_id, product_id, quantity, unit_price, now, now))
        total_amount += unit_price * quantity

    if not item_rows:
        return

    state.conn.execute(
        "INSERT INTO commerce.orders (order_id, customer_id, order_status, total_amount, created_at, updated_at) "
        "VALUES (%s, %s, 'PLACED', %s, %s, %s)",
        (order_id, customer_id, round(total_amount, 2), now, now),
    )
    for item in item_rows:
        state.conn.execute(
            "INSERT INTO commerce.order_items "
            "(order_item_id, order_id, product_id, quantity, unit_price, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            item,
        )
    payment_id = state.next_payment_id
    state.next_payment_id += 1
    method = rng.choice(PAYMENT_METHODS)
    state.conn.execute(
        "INSERT INTO commerce.payments "
        "(payment_id, order_id, payment_status, payment_method, amount, created_at, updated_at) "
        "VALUES (%s, %s, 'PENDING', %s, %s, %s, %s)",
        (payment_id, order_id, method, round(total_amount, 2), now, now),
    )

    for item_id, _oid, product_id, quantity, *_ in item_rows:
        warehouse_id = rng.choice(state.warehouse_ids)
        state.conn.execute(
            "UPDATE commerce.inventory "
            "SET available_quantity = available_quantity - %s, reserved_quantity = reserved_quantity + %s "
            "WHERE product_id = %s AND warehouse_id = %s AND available_quantity >= %s",
            (quantity, quantity, product_id, warehouse_id, quantity),
        )

    state.next_order_id += 1
    state.open_orders[order_id] = "PLACED"
    log_mutation("new_order", order_id=order_id, customer_id=customer_id, items=len(item_rows))


def event_payment_change(state: SimulatorState) -> None:
    """Resolves a PENDING payment to SUCCESS/FAILED; a SUCCESS advances the parent order to PAID."""
    if not state.open_orders:
        return
    rng = state.rng
    order_id = rng.choice(list(state.open_orders))
    if state.open_orders.get(order_id) != "PLACED":
        return

    with state.conn.cursor() as cur:
        cur.execute(
            "SELECT payment_id FROM commerce.payments "
            "WHERE order_id = %s AND payment_status = 'PENDING' LIMIT 1",
            (order_id,),
        )
        result = cur.fetchone()
    if result is None:
        return
    (payment_id,) = result

    new_status = rng.choices(["SUCCESS", "FAILED"], weights=[0.85, 0.15], k=1)[0]
    state.conn.execute(
        "UPDATE commerce.payments SET payment_status = %s WHERE payment_id = %s",
        (new_status, payment_id),
    )
    if new_status == "SUCCESS":
        state.conn.execute(
            "UPDATE commerce.orders SET order_status = 'PAID' WHERE order_id = %s AND order_status = 'PLACED'",
            (order_id,),
        )
        state.open_orders[order_id] = "PAID"
    log_mutation("payment_change", order_id=order_id, payment_id=payment_id, status=new_status)


def event_shipment_change(state: SimulatorState) -> None:
    """Advances a PAID order's shipment lifecycle: create PENDING shipment,
    then PENDING -> SHIPPED -> DELIVERED (or RETURNED)."""
    if not state.open_orders:
        return
    rng = state.rng
    order_id = rng.choice(list(state.open_orders))
    status = state.open_orders.get(order_id)
    if status not in ("PAID", "SHIPPED"):
        return
    now = datetime.now(timezone.utc)

    with state.conn.cursor() as cur:
        cur.execute(
            "SELECT shipment_id, shipment_status FROM commerce.shipments WHERE order_id = %s", (order_id,)
        )
        existing = cur.fetchone()

    if existing is None:
        shipment_id = state.next_shipment_id
        state.next_shipment_id += 1
        carrier = rng.choice(CARRIERS)
        state.conn.execute(
            "INSERT INTO commerce.shipments "
            "(shipment_id, order_id, shipment_status, carrier, updated_at) VALUES (%s, %s, 'PENDING', %s, %s)",
            (shipment_id, order_id, carrier, now),
        )
        log_mutation("shipment_created", order_id=order_id, shipment_id=shipment_id)
        return

    shipment_id, shipment_status = existing
    if shipment_status == "PENDING":
        state.conn.execute(
            "UPDATE commerce.shipments SET shipment_status = 'SHIPPED', shipped_at = %s WHERE shipment_id = %s",
            (now, shipment_id),
        )
        state.conn.execute(
            "UPDATE commerce.orders SET order_status = 'SHIPPED' WHERE order_id = %s", (order_id,)
        )
        state.open_orders[order_id] = "SHIPPED"
        log_mutation("shipment_dispatched", order_id=order_id, shipment_id=shipment_id)
    elif shipment_status == "SHIPPED":
        final_status = rng.choices(["DELIVERED", "RETURNED"], weights=[0.95, 0.05], k=1)[0]
        state.conn.execute(
            "UPDATE commerce.shipments SET shipment_status = %s, delivered_at = %s WHERE shipment_id = %s",
            (final_status, now, shipment_id),
        )
        if final_status == "DELIVERED":
            state.conn.execute(
                "UPDATE commerce.orders SET order_status = 'DELIVERED' WHERE order_id = %s", (order_id,)
            )
            state.open_orders.pop(order_id, None)
        log_mutation("shipment_settled", order_id=order_id, shipment_id=shipment_id, status=final_status)


def event_order_cancellation(state: SimulatorState) -> None:
    """The only source of CANCELLED transitions -- releases any inventory
    reservation for the order's items."""
    candidates = [oid for oid, status in state.open_orders.items() if status in CANCELLABLE_STATUSES]
    if not candidates:
        return
    rng = state.rng
    order_id = rng.choice(candidates)

    state.conn.execute(
        "UPDATE commerce.orders SET order_status = 'CANCELLED' WHERE order_id = %s", (order_id,)
    )
    with state.conn.cursor() as cur:
        cur.execute(
            "SELECT product_id, quantity FROM commerce.order_items WHERE order_id = %s", (order_id,)
        )
        items = cur.fetchall()
    for product_id, quantity in items:
        warehouse_id = rng.choice(state.warehouse_ids)
        state.conn.execute(
            "UPDATE commerce.inventory "
            "SET available_quantity = available_quantity + %s, "
            "    reserved_quantity = GREATEST(reserved_quantity - %s, 0) "
            "WHERE product_id = %s AND warehouse_id = %s",
            (quantity, quantity, product_id, warehouse_id),
        )
    state.open_orders.pop(order_id, None)
    log_mutation("order_cancelled", order_id=order_id)


def event_delete_order_item(state: SimulatorState) -> None:
    """Deletes one line item from a still-PLACED order (removed pre-payment).
    Hard delete on a leaf table -- see docs/data_model.md's per-entity
    delete-strategy table. Never deletes an order's last remaining item."""
    candidates = [oid for oid, status in state.open_orders.items() if status == "PLACED"]
    if not candidates:
        return
    rng = state.rng
    order_id = rng.choice(candidates)

    with state.conn.cursor() as cur:
        cur.execute(
            "SELECT order_item_id, quantity, unit_price FROM commerce.order_items WHERE order_id = %s",
            (order_id,),
        )
        items = cur.fetchall()
    if len(items) <= 1:
        return

    item_id, quantity, unit_price = rng.choice(items)
    state.conn.execute("DELETE FROM commerce.order_items WHERE order_item_id = %s", (item_id,))
    removed_amount = float(unit_price) * quantity
    state.conn.execute(
        "UPDATE commerce.orders SET total_amount = GREATEST(total_amount - %s, 0) WHERE order_id = %s",
        (removed_amount, order_id),
    )
    log_mutation("order_item_deleted", order_id=order_id, order_item_id=item_id)


def event_inventory_restock(state: SimulatorState) -> None:
    if state.max_product_id < 1 or not state.warehouse_ids:
        return
    rng = state.rng
    product_id = rng.randint(1, state.max_product_id)
    warehouse_id = rng.choice(state.warehouse_ids)
    restock_amount = rng.randint(10, 200)
    state.conn.execute(
        "UPDATE commerce.inventory SET available_quantity = available_quantity + %s "
        "WHERE product_id = %s AND warehouse_id = %s",
        (restock_amount, product_id, warehouse_id),
    )
    log_mutation("inventory_restock", product_id=product_id, warehouse_id=warehouse_id, amount=restock_amount)


def event_profile_update(state: SimulatorState) -> None:
    if state.max_customer_id < 1:
        return
    rng = state.rng
    customer_id = rng.randint(1, state.max_customer_id)
    new_city = state.fake.city()
    state.conn.execute(
        "UPDATE commerce.customers SET city = %s WHERE customer_id = %s", (new_city, customer_id)
    )
    log_mutation("profile_update", customer_id=customer_id, city=new_city)


def event_price_stock_change(state: SimulatorState) -> None:
    if state.max_product_id < 1:
        return
    rng = state.rng
    product_id = rng.randint(1, state.max_product_id)
    if rng.random() < 0.5:
        delta_pct = rng.uniform(-0.1, 0.1)
        state.conn.execute(
            "UPDATE commerce.products SET price = GREATEST(price * (1 + %s), 1) WHERE product_id = %s",
            (delta_pct, product_id),
        )
        log_mutation("price_change", product_id=product_id, delta_pct=round(delta_pct, 3))
    else:
        delta = rng.randint(-20, 50)
        state.conn.execute(
            "UPDATE commerce.products SET stock_quantity = GREATEST(stock_quantity + %s, 0) WHERE product_id = %s",
            (delta, product_id),
        )
        log_mutation("stock_change", product_id=product_id, delta=delta)


# --- rate-driven scheduler -------------------------------------------------

@dataclass
class RatedEvent:
    name: str
    per_minute: float
    fn: object
    credit: float = 0.0


def _build_schedule(settings: Settings) -> list[RatedEvent]:
    return [
        RatedEvent("new_customer", settings.sim_new_customers_per_min, event_new_customer),
        RatedEvent("new_order", settings.sim_new_orders_per_min, event_new_order),
        RatedEvent("payment_change", settings.sim_payment_changes_per_min, event_payment_change),
        RatedEvent("shipment_change", settings.sim_shipment_changes_per_min, event_shipment_change),
        RatedEvent(
            "order_cancellation",
            settings.sim_order_status_changes_per_min,
            event_order_cancellation,
        ),
        RatedEvent("inventory_restock", settings.sim_inventory_changes_per_min, event_inventory_restock),
        RatedEvent("profile_update", settings.sim_profile_updates_per_min, event_profile_update),
        RatedEvent("price_stock_change", settings.sim_price_stock_changes_per_min, event_price_stock_change),
        RatedEvent("delete_order_item", settings.sim_order_item_deletions_per_min, event_delete_order_item),
    ]


def run(tick_seconds: float = 1.0) -> None:
    configure_logging()
    settings = get_settings()
    rng = random.Random()
    fake = Faker()

    stop = False

    def _handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    with get_connection(autocommit=True) as conn:
        state = _load_initial_state(conn, rng, fake, settings)
        schedule = _build_schedule(settings)
        log_mutation(
            "simulator_started",
            open_orders=len(state.open_orders),
            max_customer_id=state.max_customer_id,
            max_product_id=state.max_product_id,
        )

        while not stop:
            for event in schedule:
                event.credit += event.per_minute / 60.0 * tick_seconds
                while event.credit >= 1.0 and not stop:
                    try:
                        event.fn(state)
                    except Exception:  # noqa: BLE001 -- one bad event must not kill the simulator
                        logger.exception("event %s failed", event.name)
                    event.credit -= 1.0
            time.sleep(tick_seconds)

    log_mutation("simulator_stopped")


def main() -> None:
    run()
