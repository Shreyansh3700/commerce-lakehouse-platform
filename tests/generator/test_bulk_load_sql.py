import random
from datetime import datetime, timezone

from faker import Faker

from generator.entities.customers import CUSTOMER_COLUMNS, generate_customer
from generator.entities.inventory import INVENTORY_COLUMNS, generate_inventory_row
from generator.entities.orders import ORDER_ITEM_COLUMNS, generate_order_items, generate_order_timestamps
from generator.entities.payments import PAYMENT_COLUMNS, generate_payment, payment_status_for_order
from generator.entities.products import PRODUCT_COLUMNS, generate_product
from generator.entities.shipments import (
    SHIPMENT_COLUMNS,
    SHIPMENT_ELIGIBLE_ORDER_STATUSES,
    generate_shipment,
    shipment_status_for_order,
)
from generator.utils.ids import IdCounter

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_id_counter_hands_out_ascending_non_colliding_ids():
    counter = IdCounter(1)
    assert counter.take_one() == 1
    assert list(counter.take(3)) == [2, 3, 4]
    assert counter.take_one() == 5


def test_customer_row_matches_declared_columns_and_has_unique_email():
    fake = Faker()
    fake.seed_instance(42)
    row_a = generate_customer(1, fake, NOW)
    row_b = generate_customer(2, fake, NOW)
    assert len(row_a) == len(CUSTOMER_COLUMNS)
    assert row_a[CUSTOMER_COLUMNS.index("email")] != row_b[CUSTOMER_COLUMNS.index("email")]


def test_product_row_matches_declared_columns_and_price_non_negative():
    fake = Faker()
    fake.seed_instance(1)
    rng = random.Random(1)
    row = generate_product(1, fake, rng, NOW)
    assert len(row) == len(PRODUCT_COLUMNS)
    price = row[PRODUCT_COLUMNS.index("price")]
    stock = row[PRODUCT_COLUMNS.index("stock_quantity")]
    assert price >= 0
    assert stock >= 0


def test_order_items_total_matches_sum_of_line_totals():
    rng = random.Random(7)
    item_ids = IdCounter(1)
    product_pool = [(pid, 10.0 * pid) for pid in range(1, 6)]
    items, total = generate_order_items(order_id=100, item_id_counter=item_ids, product_pool=product_pool, rng=rng, created_at=NOW, max_items=4)

    assert 1 <= len(items) <= 4
    for item in items:
        assert len(item) == len(ORDER_ITEM_COLUMNS)
        quantity = item[ORDER_ITEM_COLUMNS.index("quantity")]
        unit_price = item[ORDER_ITEM_COLUMNS.index("unit_price")]
        assert quantity > 0
        assert unit_price >= 0

    expected_total = round(
        sum(item[ORDER_ITEM_COLUMNS.index("unit_price")] * item[ORDER_ITEM_COLUMNS.index("quantity")] for item in items),
        2,
    )
    assert total == expected_total


def test_order_timestamps_updated_at_never_before_created_at():
    rng = random.Random(3)
    for status in ("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"):
        created_at, updated_at = generate_order_timestamps(rng, status, NOW)
        assert updated_at >= created_at


def test_payment_status_matches_order_status_business_rules():
    rng = random.Random(4)
    assert payment_status_for_order("PAID", rng) == "SUCCESS"
    assert payment_status_for_order("SHIPPED", rng) == "SUCCESS"
    assert payment_status_for_order("DELIVERED", rng) == "SUCCESS"
    assert payment_status_for_order("CANCELLED", rng) in ("REFUNDED", "FAILED")
    assert payment_status_for_order("PLACED", rng) in ("PENDING", "FAILED")


def test_payment_row_matches_declared_columns():
    rng = random.Random(5)
    row = generate_payment(1, order_id=10, order_status="DELIVERED", amount=99.99, rng=rng, order_created_at=NOW)
    assert len(row) == len(PAYMENT_COLUMNS)
    assert row[PAYMENT_COLUMNS.index("payment_status")] == "SUCCESS"


def test_shipments_only_generated_for_eligible_order_statuses():
    assert SHIPMENT_ELIGIBLE_ORDER_STATUSES == ("PAID", "SHIPPED", "DELIVERED")
    rng = random.Random(6)
    assert shipment_status_for_order("PAID", rng) == "PENDING"
    assert shipment_status_for_order("SHIPPED", rng) in ("SHIPPED", "IN_TRANSIT")
    assert shipment_status_for_order("DELIVERED", rng) in ("DELIVERED", "RETURNED")


def test_shipment_delivered_at_never_before_shipped_at():
    rng = random.Random(8)
    for _ in range(50):
        row = generate_shipment(1, order_id=1, order_status="DELIVERED", rng=rng, order_updated_at=NOW)
        assert len(row) == len(SHIPMENT_COLUMNS)
        shipped_at = row[SHIPMENT_COLUMNS.index("shipped_at")]
        delivered_at = row[SHIPMENT_COLUMNS.index("delivered_at")]
        if shipped_at is not None and delivered_at is not None:
            assert delivered_at >= shipped_at


def test_inventory_reserved_never_exceeds_available():
    rng = random.Random(9)
    for _ in range(50):
        row = generate_inventory_row(1, product_id=1, warehouse_id=1, rng=rng, now=NOW)
        assert len(row) == len(INVENTORY_COLUMNS)
        available = row[INVENTORY_COLUMNS.index("available_quantity")]
        reserved = row[INVENTORY_COLUMNS.index("reserved_quantity")]
        assert reserved <= available
        assert available >= 0
        assert reserved >= 0
