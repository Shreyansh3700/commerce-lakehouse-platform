from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

# Pure Python, no pyspark import -- this module is unit-testable without a
# cluster (see tests/streaming/test_debezium_envelope.py). bronze_writer.py
# wraps extract_fields() in a Spark UDF for the actual streaming pipeline.


class EnvelopeParseError(Exception):
    """Raised when a Kafka message's value isn't a well-formed Debezium envelope."""


_REQUIRED_KEYS = ("op", "source")


def extract_fields(raw_value: str | None) -> dict[str, Any]:
    """Parses one Debezium JSON envelope (schemas.enable=false, so the value
    is the flat {op, before, after, source, ts_ms, transaction} object, not
    the {schema, payload} wrapper) into Bronze's column shape.

    Raises EnvelopeParseError on anything that isn't a parseable, well-formed
    envelope -- the Bronze writer routes those rows to the DLQ instead of
    failing the whole micro-batch.
    """
    if raw_value is None:
        raise EnvelopeParseError("value is null")
    try:
        envelope = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError) as exc:
        raise EnvelopeParseError(f"invalid JSON: {exc}") from exc

    if not isinstance(envelope, dict):
        raise EnvelopeParseError("envelope is not a JSON object")
    for key in _REQUIRED_KEYS:
        if key not in envelope:
            raise EnvelopeParseError(f"missing required field '{key}'")

    source = envelope.get("source") or {}
    transaction = envelope.get("transaction") or {}

    ts_ms = source.get("ts_ms")
    source_timestamp = (
        datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms is not None else None
    )

    before = envelope.get("before")
    after = envelope.get("after")

    return {
        "operation": envelope.get("op"),
        "before": json.dumps(before) if before is not None else None,
        "after": json.dumps(after) if after is not None else None,
        "source_timestamp": source_timestamp,
        "source_lsn": source.get("lsn"),
        "transaction_id": transaction.get("id") if transaction else None,
    }


def compute_event_id(topic: str, partition: int, offset: int) -> str:
    """Deterministic Bronze dedup key: the one identifying triple present on
    every Kafka message, including snapshot-phase rows where transaction_id
    is legitimately NULL (Debezium's "transaction" block only accompanies
    streaming CDC events, not one-time snapshot reads -- source_lsn, by
    contrast, is still populated during snapshot as the read's consistency
    LSN). Does NOT make Bronze writes idempotent by itself (see docs/cdc.md)
    -- it exists so Phase 3's Silver MERGE has a stable key to dedup on."""
    key = f"{topic}|{partition}|{offset}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_dlq_record(
    raw_value: str | None,
    error_type: str,
    error_message: str,
    source_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    failed_at: datetime,
) -> dict[str, Any]:
    return {
        "original_event": raw_value,
        "error_message": error_message,
        "error_type": error_type,
        "source_topic": source_topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "failed_at": failed_at,
    }
