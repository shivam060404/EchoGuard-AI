"""Deterministic Phase 3 correlation keys and validated storage records."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from backend.contracts.validation import validate_event


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("Event time must be UTC")
    return parsed.astimezone(timezone.utc)


def _id(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]


def case_record(event: dict) -> dict:
    validate_event("support.enriched.v1", event)
    if event["redaction_status"] not in {"redacted", "synthetic"}:
        raise ValueError("Unredacted case cannot be stored")
    return {
        "_id": f"case-{_id(event['tenant_id'], event['event_id'])}",
        "tenant_id": event["tenant_id"],
        "event_id": event["event_id"],
        "trace_id": event["trace_id"],
        "customer_id": event["customer_id"],
        "event_time": _utc(event["event_time"]),
        "product": event["product"],
        "error_code": event.get("error_code"),
        "channel": event["channel"],
        "normalized_text": event["normalized_text"],
        "redaction_status": event["redaction_status"],
        "processing_version": event["processing_version"],
    }


def signal_record(event: dict) -> dict:
    validate_event("incident.signals.v1", event)
    if event["event_id"] != event["signal_id"]:
        raise ValueError("Signal event ID must equal signal ID")
    if event["distinct_customers"] > event["current_count"]:
        raise ValueError("Distinct customers cannot exceed event count")
    start = _utc(event["window_start"])
    end = _utc(event["window_end"])
    if end <= start:
        raise ValueError("Signal window must have positive duration")
    # The deterministic 30-minute bucket is a bounded first-stage correlation rule.
    # A human-reviewed semantic merge is a later workflow, never an implicit cross-tenant join.
    bucket = int(start.timestamp()) // 1800
    cluster_id = f"cluster-{_id(event['tenant_id'], event['product'], event.get('error_code') or '', str(bucket))}"
    return {
        "_id": f"signal-{_id(event['tenant_id'], event['signal_id'])}",
        "signal_id": event["signal_id"],
        "tenant_id": event["tenant_id"],
        "cluster_id": cluster_id,
        "signal_key": event["signal_key"],
        "product": event["product"],
        "error_code": event.get("error_code"),
        "window_start": start,
        "window_end": end,
        "current_count": event["current_count"],
        "distinct_customers": event["distinct_customers"],
        "sample_event_ids": event["sample_event_ids"],
        "rule_version": event["rule_version"],
    }
