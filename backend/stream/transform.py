"""Pure boundary logic used by the Spark normalization query."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from backend.contracts.validation import classify_payload, validate_event

RAW_TOPIC = "support.raw.v1"
ENRICHED_TOPIC = "support.enriched.v1"
MAX_PAYLOAD_BYTES = 64_000
MAX_EVENT_DELAY_SECONDS = 120
PROCESSING_VERSION = "normalize-v1"
EMAIL = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
WHITESPACE = re.compile(r"\s+")


def utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset().total_seconds() != 0:
        raise ValueError("timestamp must be UTC")
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def normalized_text(value: str) -> str:
    return WHITESPACE.sub(" ", EMAIL.sub("[EMAIL]", value)).strip()


def normalize_record(
    key: bytes | None, payload: bytes | None, processed_at: datetime
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate a raw Kafka record and return a safe enriched event or reason."""
    if payload is None:
        return None, "invalid_json"
    if len(payload) > MAX_PAYLOAD_BYTES:
        return None, "oversize"
    raw, reason = classify_payload(RAW_TOPIC, payload)
    if reason:
        return None, reason
    assert raw is not None
    if raw.get("metadata", {}).get("synthetic") is not True:
        return None, "non_synthetic"
    expected_key = f"{raw['tenant_id']}:{raw['customer_id']}".encode("utf-8")
    if key != expected_key:
        return None, "key_mismatch"
    event_time = datetime.fromisoformat(raw["event_time"].replace("Z", "+00:00"))
    ingest_time = datetime.fromisoformat(raw["ingest_time"].replace("Z", "+00:00"))
    if (ingest_time - event_time).total_seconds() > MAX_EVENT_DELAY_SECONDS:
        return None, "late_event"
    clean_text = normalized_text(raw["text"])
    if not clean_text:
        return None, "schema_violation"
    enriched = {
        "schema_version": "1.0.0",
        "event_id": raw["event_id"],
        "tenant_id": raw["tenant_id"],
        "trace_id": raw["trace_id"],
        "customer_id": raw["customer_id"],
        "event_time": raw["event_time"],
        "processed_time": utc(processed_at),
        "channel": raw["channel"],
        "product": raw["product"],
        "normalized_text": clean_text,
        "error_code": raw.get("error_code"),
        "language": "en",
        "redaction_status": "synthetic",
        "processing_version": PROCESSING_VERSION,
    }
    validate_event(ENRICHED_TOPIC, enriched)
    return enriched, None


def stream_dlq_event(
    *,
    partition: int,
    offset: int,
    key: bytes | None,
    payload: bytes | None,
    reason: str,
    failed_at: datetime,
) -> dict[str, Any]:
    """Create a replayable local DLQ record without trusting raw payload fields."""
    value = payload or b""
    digest = hashlib.sha256(value).hexdigest()
    event = {
        "schema_version": "1.0.0",
        "event_id": f"stream-dlq-{partition}-{offset}",
        "source_topic": RAW_TOPIC,
        "source_partition": partition,
        "source_offset": offset,
        "record_key_sha256": hashlib.sha256(key or b"").hexdigest(),
        "reason_code": reason,
        "payload_sha256": digest,
        "failed_at": utc(failed_at),
        "validation_stage": "stream",
    }
    validate_event("stream.dlq.v1", event)
    return event


def compact_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def parse_enriched_record(key: bytes | None, payload: bytes | None) -> dict[str, Any]:
    """Fail closed if another producer writes an invalid enriched event."""
    if payload is None:
        raise ValueError("Missing enriched payload")
    try:
        event = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid enriched JSON") from exc
    if not isinstance(event, dict):
        raise ValueError("Enriched payload must be an object")
    validate_event(ENRICHED_TOPIC, event)
    expected_key = f"{event['tenant_id']}:{event['customer_id']}".encode("utf-8")
    if key != expected_key:
        raise ValueError("Enriched Kafka key does not match tenant and customer")
    return event
