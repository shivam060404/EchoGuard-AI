"""Versioned deterministic incident candidate rule."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from backend.contracts.validation import validate_event
from .transform import utc

SIGNAL_TOPIC = "incident.signals.v1"
RULE_VERSION = "spike-v1"
MIN_EVENTS = 20
MIN_CUSTOMERS = 10
BASELINE_COUNT = 2.0
MIN_RATIO = 5.0


def candidate_signal(
    *,
    tenant_id: str,
    product: str,
    error_code: str | None,
    window_start: datetime,
    window_end: datetime,
    event_ids: list[str],
    customer_ids: list[str],
    detected_at: datetime,
) -> dict[str, Any] | None:
    """Build one logical candidate per tenant, group, and event-time window."""
    unique_events = sorted(set(event_ids))
    unique_customers = set(customer_ids)
    count = len(unique_events)
    ratio = count / BASELINE_COUNT
    if count < MIN_EVENTS or len(unique_customers) < MIN_CUSTOMERS or ratio < MIN_RATIO:
        return None
    start = utc(window_start)
    end = utc(window_end)
    material = f"{tenant_id}|{product}|{error_code or ''}|{start}|{end}|{RULE_VERSION}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    signal_id = f"sig-{digest[:32]}"
    group_digest = hashlib.sha256(f"{product}|{error_code or ''}".encode("utf-8")).hexdigest()[:16]
    signal_key = f"{product[:80]}:{(error_code or 'general')[:80]}:{group_digest}"
    signal = {
        "schema_version": "1.0.0",
        "event_id": signal_id,
        "signal_id": signal_id,
        "tenant_id": tenant_id,
        "trace_id": f"trace-{signal_id}",
        "signal_key": signal_key,
        "product": product,
        "error_code": error_code,
        "window_start": start,
        "window_end": end,
        "detected_time": utc(detected_at),
        "current_count": count,
        "baseline_count": BASELINE_COUNT,
        "ratio": ratio,
        "distinct_customers": len(unique_customers),
        "sample_event_ids": unique_events[:100],
        "rule_version": RULE_VERSION,
    }
    validate_event(SIGNAL_TOPIC, signal)
    return signal
