"""Verify Phase 2 signals and late-event routing against the seeded scenario."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid

from backend.contracts.validation import validate_event


def verify(bootstrap: str, timeout: float) -> dict[str, object]:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"echoguard-stream-verify-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    expected = {"PAY-502": 150, "AUTH-401": 40}
    signals: dict[str, dict] = {}
    late_offsets: set[tuple[int, int]] = set()
    deadline = time.monotonic() + timeout
    try:
        consumer.subscribe(["incident.signals.v1", "stream.dlq.v1"])
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            event = json.loads(message.value())
            validate_event(message.topic(), event)
            if message.topic() == "incident.signals.v1":
                expected_key = f"{event['tenant_id']}:{event['signal_key']}".encode("utf-8")
                if message.key() != expected_key:
                    raise ValueError("Signal Kafka key does not match its contract")
                if event["tenant_id"] == "demo-tenant" and event.get("error_code") in expected:
                    code = event["error_code"]
                    if event["current_count"] > signals.get(code, {}).get("current_count", 0):
                        signals[code] = event
            elif event["reason_code"] == "late_event":
                expected_key = (
                    f"stream-dlq:{event['source_partition']}:{event['source_offset']}"
                ).encode("utf-8")
                if message.key() != expected_key:
                    raise ValueError("Stream DLQ Kafka key does not match its source offset")
                late_offsets.add((event["source_partition"], event["source_offset"]))
            if (
                all(signals.get(code, {}).get("current_count", 0) >= count
                    for code, count in expected.items())
                and len(late_offsets) >= 5
            ):
                return {
                    "status": "verified",
                    "signals": {code: signals[code]["signal_id"] for code in sorted(expected)},
                    "late_events": len(late_offsets),
                }
    finally:
        consumer.close()
    counts = {code: signals.get(code, {}).get("current_count", 0) for code in expected}
    raise TimeoutError(f"Phase 2 output incomplete: counts={counts}, late_events={len(late_offsets)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--timeout", type=float, default=300.0)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    print(json.dumps(verify(args.bootstrap, args.timeout), sort_keys=True))


if __name__ == "__main__":
    main()
