import base64
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone

import pytest

from backend.contracts.validation import validate_event
from backend.ingest.publisher import prepare_message, publish_messages, producer_config
from backend.ingest.scenario import (
    DLQ_TOPIC,
    RAW_TOPIC,
    ScenarioConfig,
    generate_records,
    read_scenario,
    write_scenario,
)
from backend.ingest.verifier import verify_delivery


def small_config(seed=42):
    return ScenarioConfig(seed=seed, normal_count=5, payment_count=10, login_count=3,
                          duplicate_count=2, late_count=1)


def test_scenario_is_deterministic_and_labeled(tmp_path):
    dataset = tmp_path / "scenario.jsonl"
    manifest_path = tmp_path / "scenario.manifest.json"
    manifest = write_scenario(small_config(), dataset, manifest_path)
    original = dataset.read_bytes()
    assert manifest["record_count"] == 23
    assert manifest["expected_topics"] == {DLQ_TOPIC: 2, RAW_TOPIC: 21}
    assert manifest["duplicate_deliveries"] == 2
    assert manifest["unique_valid_event_ids"] == 19
    assert manifest["late_event_count"] == 1
    assert manifest["reason_codes"] == {"invalid_json": 1, "schema_violation": 1}
    records, loaded = read_scenario(dataset, manifest_path)
    assert loaded == manifest
    assert len({record.record_id for record in records}) == len(records)
    assert [record.payload for record in records if record.kind == "duplicate"] == [
        record.payload for record in records if record.kind == "payment"
    ][:2]
    late = [record for record in records if record.kind == "late"][0]
    late_time = json.loads(late.payload)["event_time"]
    payment_time = json.loads(next(record.payload for record in records if record.kind == "payment"))["event_time"]
    assert late_time < payment_time
    write_scenario(small_config(), dataset, manifest_path)
    assert dataset.read_bytes() == original
    with pytest.raises(FileExistsError):
        write_scenario(small_config(seed=43), dataset, manifest_path)
    assert dataset.read_bytes() == original
    assert generate_records(small_config(seed=43)) != records


def test_manifest_detects_tampering(tmp_path):
    dataset = tmp_path / "scenario.jsonl"
    manifest_path = tmp_path / "scenario.manifest.json"
    write_scenario(small_config(), dataset, manifest_path)
    dataset.write_bytes(dataset.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="digest"):
        read_scenario(dataset, manifest_path)


def test_prepare_routes_invalid_payload_to_redriveable_dlq():
    records = generate_records(small_config())
    failed_at = datetime(2026, 10, 15, 10, 12, tzinfo=timezone.utc)
    prepared = [prepare_message(record, "demo-tenant", "physical-run-1", failed_at) for record in records]
    assert Counter(item.topic for item in prepared) == {RAW_TOPIC: 21, DLQ_TOPIC: 2}
    for item in prepared:
        assert dict(item.headers)["x-record-id"] == item.record_id.encode("ascii")
        if item.topic == DLQ_TOPIC:
            event = json.loads(item.value)
            validate_event(DLQ_TOPIC, event)
            payload = base64.b64decode(event["payload_base64"])
            assert hashlib.sha256(payload).hexdigest() == event["payload_sha256"]
            assert event["reason_code"] in {"invalid_json", "schema_violation"}


class FakeMessage:
    def __init__(self, item=None, *, run_id=None, error=None):
        self.item = item
        self.run_id = run_id
        self._error = error

    def topic(self):
        return self.item.topic

    def key(self):
        return self.item.key

    def value(self):
        return self.item.value

    def headers(self):
        if self.run_id is None:
            return self.item.headers
        return [("x-delivery-run-id", self.run_id.encode("ascii")),
                ("x-record-id", self.item.record_id.encode("ascii"))]

    def error(self):
        return self._error


class FakeProducer:
    def __init__(self, *, fail=False, queue_full_once=False, leave_queued=False):
        self.fail = fail
        self.queue_full_once = queue_full_once
        self.leave_queued = leave_queued
        self.pending = []
        self.attempts = 0

    def produce(self, topic, *, value, key, headers, on_delivery):
        self.attempts += 1
        if self.queue_full_once:
            self.queue_full_once = False
            raise BufferError("queue full")
        self.pending.append((topic, on_delivery))

    def poll(self, timeout):
        if not self.pending or self.leave_queued:
            return 0
        topic, callback = self.pending.pop(0)
        callback("delivery failed" if self.fail else None, type("Delivered", (), {"topic": lambda self: topic})())
        return 1

    def flush(self, timeout):
        while self.pending and not self.leave_queued:
            self.poll(0)
        return len(self.pending)


def test_publisher_requires_every_ack_and_handles_queue_pressure():
    record = generate_records(small_config())[0]
    item = prepare_message(record, "demo-tenant", "physical-run-1", datetime.now(timezone.utc))
    producer = FakeProducer(queue_full_once=True)
    assert publish_messages([item], producer) == {RAW_TOPIC: 1}
    assert producer.attempts == 2
    with pytest.raises(RuntimeError, match="delivery failures"):
        publish_messages([item], FakeProducer(fail=True))
    with pytest.raises(TimeoutError, match="still queued"):
        publish_messages([item], FakeProducer(leave_queued=True), timeout_seconds=0.01)
    assert producer_config("localhost:9092")["enable.idempotence"] is True


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.closed = False
        self.subscribed = []

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def close(self):
        self.closed = True


def test_verifier_filters_other_runs_and_checks_record_bytes():
    records = generate_records(small_config())[:3]
    at = datetime(2026, 10, 15, 10, 12, tzinfo=timezone.utc)
    prepared = [prepare_message(record, "demo-tenant", "run-new", at) for record in records]
    consumer = FakeConsumer([FakeMessage(prepared[0], run_id="run-old")] +
                            [FakeMessage(item) for item in reversed(prepared)])
    assert verify_delivery(consumer, prepared, "run-new") == {RAW_TOPIC: 3}
    assert consumer.closed
    bad = FakeMessage(prepared[0])
    bad.item = type("Wrong", (), {"topic": RAW_TOPIC, "key": b"wrong", "value": prepared[0].value,
                                  "headers": prepared[0].headers, "record_id": prepared[0].record_id})()
    consumer = FakeConsumer([bad])
    with pytest.raises(ValueError, match="differs"):
        verify_delivery(consumer, prepared[:1], "run-new")
    assert consumer.closed

