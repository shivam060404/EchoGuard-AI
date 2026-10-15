"""Validate and publish a scenario with delivery acknowledgments."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from backend.contracts.validation import classify_payload, validate_event
from backend.ingest.scenario import DLQ_TOPIC, RAW_TOPIC, ScenarioRecord


MAX_PAYLOAD_BYTES = 64_000


class ProducerLike(Protocol):
    def produce(self, topic: str, *, value: bytes, key: bytes,
                headers: list[tuple[str, bytes]], on_delivery: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class PreparedMessage:
    topic: str
    key: bytes
    value: bytes
    headers: list[tuple[str, bytes]]
    record_id: str
    logical_event_id: str


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset().total_seconds() != 0:
        raise ValueError("failed_at must be UTC")
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def prepare_message(record: ScenarioRecord, tenant_id: str, run_id: str,
                    failed_at: datetime) -> PreparedMessage:
    payload = record.payload.encode("utf-8")
    parsed, reason = classify_payload(RAW_TOPIC, payload)
    if len(payload) > MAX_PAYLOAD_BYTES:
        reason = "oversize"
    elif parsed is not None:
        if parsed["tenant_id"] != tenant_id or record.record_key != f"{tenant_id}:{parsed['customer_id']}":
            reason = "key_mismatch"
    else:
        try:
            maybe = json.loads(payload)
            if isinstance(maybe, dict) and isinstance(maybe.get("schema_version"), str):
                if not maybe["schema_version"].startswith("1."):
                    reason = "unknown_schema"
        except json.JSONDecodeError:
            pass

    headers = [("x-delivery-run-id", run_id.encode("ascii")),
               ("x-record-id", record.record_id.encode("ascii"))]
    if reason is None:
        assert parsed is not None
        return PreparedMessage(RAW_TOPIC, record.record_key.encode("utf-8"), payload,
                               headers, record.record_id, parsed["event_id"])

    digest = hashlib.sha256(payload).hexdigest()
    event_id = f"dlq-{hashlib.sha256(record.record_id.encode('utf-8')).hexdigest()[:24]}"
    trace_id = None
    try:
        maybe = json.loads(payload)
        if isinstance(maybe, dict) and isinstance(maybe.get("trace_id"), str):
            trace_id = maybe["trace_id"][:128]
    except json.JSONDecodeError:
        pass
    dlq = {
        "schema_version": "1.0.0",
        "event_id": event_id,
        "tenant_id": tenant_id,
        "trace_id": trace_id or f"trace-{event_id}",
        "source_topic": RAW_TOPIC,
        "source_record_id": record.record_id,
        "record_key": record.record_key,
        "reason_code": reason,
        "payload_sha256": digest,
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "failed_at": _iso_utc(failed_at),
        "validation_stage": "ingest",
    }
    validate_event(DLQ_TOPIC, dlq)
    return PreparedMessage(DLQ_TOPIC, f"{tenant_id}:{record.record_id}".encode("utf-8"),
                           _json_bytes(dlq), headers, record.record_id, event_id)


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": 120_000,
        "linger.ms": 5,
        "compression.type": "gzip",
    }


def publish_messages(messages: list[PreparedMessage], producer: ProducerLike,
                     timeout_seconds: float = 180.0,
                     rate_per_second: float | None = None) -> dict[str, int]:
    """A receipt is valid only after every delivery callback succeeds."""
    if not messages:
        raise ValueError("Cannot publish an empty scenario")
    if rate_per_second is not None and rate_per_second <= 0:
        raise ValueError("rate_per_second must be positive")
    deadline = time.monotonic() + timeout_seconds
    started = time.monotonic()
    delivered: Counter[str] = Counter()
    failures: list[str] = []

    def on_delivery(error: object, message: object) -> None:
        if error is not None:
            failures.append(str(error))
        else:
            delivered[message.topic()] += 1  # type: ignore[attr-defined]

    for index, item in enumerate(messages):
        if rate_per_second is not None:
            scheduled = started + index / rate_per_second
            while time.monotonic() < scheduled:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Publish pacing exceeded timeout")
                producer.poll(min(0.1, scheduled - time.monotonic()))
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("Producer queue did not accept all messages")
            try:
                producer.produce(item.topic, value=item.value, key=item.key,
                                 headers=item.headers, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)

    remaining = producer.flush(max(0.0, deadline - time.monotonic()))
    if remaining:
        raise TimeoutError(f"{remaining} Kafka messages still queued after flush")
    if failures:
        raise RuntimeError(f"{len(failures)} Kafka delivery failures; first: {failures[0]}")
    if sum(delivered.values()) != len(messages):
        raise RuntimeError(f"Only {sum(delivered.values())}/{len(messages)} delivery callbacks completed")
    return dict(sorted(delivered.items()))

