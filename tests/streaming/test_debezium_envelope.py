import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# streaming/pyspark isn't an importable package (it's copied flat into the
# Spark image), so add it to sys.path directly rather than restructuring the
# Docker build around a proper package layout for one test module.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "streaming" / "pyspark"))

from debezium_envelope import (  # noqa: E402
    EnvelopeParseError,
    build_dlq_record,
    compute_event_id,
    extract_fields,
)


def _envelope(op="c", before=None, after=None, ts_ms=1750000000000, lsn=12345, tx_id="987"):
    return json.dumps(
        {
            "op": op,
            "before": before,
            "after": after,
            "source": {"ts_ms": ts_ms, "lsn": lsn},
            "transaction": {"id": tx_id} if tx_id else None,
        }
    )


def test_extract_fields_insert_has_after_but_no_before():
    raw = _envelope(op="c", after={"order_id": 1, "order_status": "PLACED"})
    fields = extract_fields(raw)
    assert fields["operation"] == "c"
    assert fields["before"] is None
    assert json.loads(fields["after"]) == {"order_id": 1, "order_status": "PLACED"}
    assert fields["source_lsn"] == 12345
    assert fields["transaction_id"] == "987"
    assert fields["source_timestamp"] == datetime.fromtimestamp(1750000000000 / 1000.0, tz=timezone.utc)


def test_extract_fields_update_has_both_before_and_after():
    raw = _envelope(
        op="u",
        before={"order_id": 1, "order_status": "PLACED"},
        after={"order_id": 1, "order_status": "PAID"},
    )
    fields = extract_fields(raw)
    assert fields["operation"] == "u"
    before = json.loads(fields["before"])
    after = json.loads(fields["after"])
    assert before["order_status"] == "PLACED"
    assert after["order_status"] == "PAID"


def test_extract_fields_delete_has_before_but_no_after():
    raw = _envelope(op="d", before={"order_id": 1, "order_status": "DELIVERED"}, after=None)
    fields = extract_fields(raw)
    assert fields["operation"] == "d"
    assert fields["after"] is None
    assert json.loads(fields["before"])["order_status"] == "DELIVERED"


def test_extract_fields_snapshot_row_has_null_transaction_id():
    # Confirmed against a real Debezium 2.7 snapshot message: source.lsn is
    # still populated (the snapshot read's consistency LSN) even for op="r",
    # but the top-level "transaction" block is null -- it only accompanies
    # streaming CDC events, not one-time snapshot reads.
    raw = json.dumps(
        {
            "op": "r",
            "before": None,
            "after": {"order_id": 1},
            "source": {"ts_ms": 1750000000000, "lsn": 29481192},
            "transaction": None,
        }
    )
    fields = extract_fields(raw)
    assert fields["operation"] == "r"
    assert fields["source_lsn"] == 29481192
    assert fields["transaction_id"] is None


def test_extract_fields_raises_on_invalid_json():
    with pytest.raises(EnvelopeParseError):
        extract_fields("{not valid json")


def test_extract_fields_raises_on_missing_required_field():
    with pytest.raises(EnvelopeParseError):
        extract_fields(json.dumps({"before": None, "after": {}}))  # missing "op" and "source"


def test_extract_fields_raises_on_non_object_json():
    with pytest.raises(EnvelopeParseError):
        extract_fields(json.dumps([1, 2, 3]))


def test_extract_fields_raises_on_null_value():
    with pytest.raises(EnvelopeParseError):
        extract_fields(None)


def test_compute_event_id_is_deterministic_and_unique_per_offset():
    id_a = compute_event_id("cdc.orders", 0, 42)
    id_b = compute_event_id("cdc.orders", 0, 42)
    id_c = compute_event_id("cdc.orders", 0, 43)
    assert id_a == id_b
    assert id_a != id_c
    assert len(id_a) == 64  # sha256 hex digest


def test_compute_event_id_distinguishes_snapshot_rows_with_null_transaction_id():
    # Snapshot rows all share a NULL transaction_id, so topic+partition+offset
    # must be what makes each event_id unique -- exactly what compute_event_id uses.
    id_a = compute_event_id("cdc.customers", 1, 100)
    id_b = compute_event_id("cdc.customers", 1, 101)
    assert id_a != id_b


def test_build_dlq_record_captures_error_context():
    failed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    record = build_dlq_record(
        "garbage", "EnvelopeParseError", "invalid JSON: boom", "cdc.orders", 2, 55, failed_at
    )
    assert record["original_event"] == "garbage"
    assert record["error_type"] == "EnvelopeParseError"
    assert "invalid JSON" in record["error_message"]
    assert record["source_topic"] == "cdc.orders"
    assert record["kafka_partition"] == 2
    assert record["kafka_offset"] == 55
    assert record["failed_at"] == failed_at
