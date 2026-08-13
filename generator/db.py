from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from contextlib import contextmanager

import psycopg

from generator.config import get_settings

logger = logging.getLogger(__name__)

# Reference data (warehouses) is seeded once via infrastructure/postgres/init/
# and is NOT part of a reset -- only the transactional tables are truncated.
_TRANSACTIONAL_TABLES = (
    "shipments",
    "inventory",
    "payments",
    "order_items",
    "orders",
    "products",
    "customers",
)


@contextmanager
def get_connection(autocommit: bool = False):
    settings = get_settings()
    conn = psycopg.connect(settings.dsn, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def resync_sequence(conn: psycopg.Connection, table: str, id_column: str) -> None:
    """Advance table's identity sequence past any explicit IDs COPY'd in during bulk load."""
    qualified = f"commerce.{table}"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence(%s, %s), "
            f"COALESCE((SELECT MAX({id_column}) FROM {qualified}), 1))",
            (qualified, id_column),
        )
    conn.commit()


def copy_rows(conn: psycopg.Connection, table: str, columns: Sequence[str], rows: Iterable[Sequence]) -> int:
    """Bulk-load rows via COPY FROM STDIN. Commits at the end. Returns row count written."""
    qualified = f"commerce.{table}"
    col_list = ", ".join(columns)
    count = 0
    with conn.cursor() as cur, cur.copy(f"COPY {qualified} ({col_list}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    conn.commit()
    return count


def truncate_all(conn: psycopg.Connection) -> None:
    tables = ", ".join(f"commerce.{t}" for t in _TRANSACTIONAL_TABLES)
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    conn.commit()


def max_id(conn: psycopg.Connection, table: str, id_column: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX({id_column}), 0) FROM commerce.{table}")
        (value,) = cur.fetchone()
    return value
