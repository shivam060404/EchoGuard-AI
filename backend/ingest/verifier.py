"""Read back one physical publish attempt from Kafka."""

from __future__ import annotations

import time
from collections import Counter
from typing import Any, Protocol

from backend.ingest.publisher import PreparedMessage
from backend.ingest.scenario import DLQ_TOPIC, RAW_TOPIC


class ConsumerLike(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, timeout: float) -> Any: ...
    def close(self) -> None: ...


def consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "allow.auto.create.topics": False,
    }


def verify_delivery(consumer: ConsumerLike, expected: list[PreparedMessage],
                    run_id: str, timeout_seconds: float = 90.0) -> dict[str, int]:
    if not expected:
        raise ValueError("Cannot verify an empty publish attempt")
    by_record_id = {item.record_id: item for item in expected}
    if len(by_record_id) != len(expected):
        raise ValueError("Physical record IDs must be unique")
    received: set[str] = set()
    counts: Counter[str] = Counter()
    deadline = time.monotonic() + timeout_seconds
    consumer.subscribe([RAW_TOPIC, DLQ_TOPIC])
    try:
        while len(received) < len(expected):
            if time.monotonic() >= deadline:
                missing = sorted(set(by_record_id) - received)
                raise TimeoutError(f"Only {len(received)}/{len(expected)} records verified; missing: {missing[:5]}")
            message = consumer.poll(min(1.0, max(0.0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(f"Kafka read error: {message.error()}")
            headers = dict(message.headers() or [])
            if headers.get("x-delivery-run-id") != run_id.encode("ascii"):
                continue
            record_id_bytes = headers.get("x-record-id")
            if record_id_bytes is None:
                raise ValueError("Run message lacks x-record-id")
            record_id = record_id_bytes.decode("ascii")
            if record_id in received:
                raise ValueError(f"Duplicate physical record in run: {record_id}")
            item = by_record_id.get(record_id)
            if item is None:
                raise ValueError(f"Unexpected physical record in run: {record_id}")
            if (message.topic(), message.key(), message.value()) != (item.topic, item.key, item.value):
                raise ValueError(f"Kafka payload, key, or topic differs for {record_id}")
            received.add(record_id)
            counts[item.topic] += 1
    finally:
        consumer.close()
    return dict(sorted(counts.items()))

