import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from backend.contracts.validation import validate_event
from backend.ingest.scenario import ScenarioConfig, generate_records
from backend.stream.rules import candidate_signal
from backend.stream.transform import normalize_record, parse_enriched_record, stream_dlq_event


NOW = datetime(2026, 10, 15, 10, 12, tzinfo=timezone.utc)


def _raw(kind: str) -> tuple[bytes, bytes]:
    record = next(item for item in generate_records(ScenarioConfig()) if item.kind == kind)
    return record.record_key.encode(), record.payload.encode()


def test_normalization_redacts_and_preserves_identity() -> None:
    key, value = _raw("payment")
    raw = json.loads(value)
    raw["text"] = "Payment  failed.\n Write to help@example.com. PAY-502"
    enriched, reason = normalize_record(key, json.dumps(raw).encode(), NOW)
    assert reason is None
    assert enriched is not None
    assert enriched["event_id"] == raw["event_id"]
    assert enriched["normalized_text"] == "Payment failed. Write to [EMAIL]. PAY-502"
    assert enriched["error_code"] == "PAY-502"
    validate_event("support.enriched.v1", enriched)
    assert parse_enriched_record(key, json.dumps(enriched).encode()) == enriched


def test_bad_key_real_data_late_and_invalid_payloads_are_blocked() -> None:
    key, value = _raw("payment")
    assert normalize_record(b"wrong", value, NOW)[1] == "key_mismatch"
    assert normalize_record(key, b"{", NOW)[1] == "invalid_json"
    assert normalize_record(key, b"x" * 64_001, NOW)[1] == "oversize"
    raw = json.loads(value)
    raw["metadata"]["synthetic"] = False
    assert normalize_record(key, json.dumps(raw).encode(), NOW)[1] == "non_synthetic"
    late_key, late_value = _raw("late")
    assert normalize_record(late_key, late_value, NOW)[1] == "late_event"


def test_stream_dlq_keeps_provenance_and_safe_reason() -> None:
    event = stream_dlq_event(
        partition=1, offset=42, key=b"tenant:customer", payload=b"{",
        reason="invalid_json", failed_at=NOW,
    )
    assert event["event_id"] == "stream-dlq-1-42"
    assert event["payload_sha256"] == hashlib.sha256(b"{").hexdigest()
    assert "payload_base64" not in event
    validate_event("stream.dlq.v1", event)


def test_candidate_uses_unique_events_customers_and_stable_id() -> None:
    start = datetime(2026, 10, 15, 10, 5, tzinfo=timezone.utc)
    ids = [f"e-{index}" for index in range(25)]
    customers = [f"c-{index}" for index in range(25)]
    kwargs = dict(
        tenant_id="tenant-a", product="payments", error_code="PAY-502",
        window_start=start, window_end=start + timedelta(minutes=1),
        event_ids=ids + ids[:5], customer_ids=customers + customers[:5],
        detected_at=NOW,
    )
    signal = candidate_signal(**kwargs)
    assert signal is not None
    assert signal["current_count"] == 25
    assert signal["distinct_customers"] == 25
    assert signal["signal_id"] == candidate_signal(**kwargs)["signal_id"]
    assert candidate_signal(**{**kwargs, "tenant_id": "tenant-b"})["signal_id"] != signal["signal_id"]
    assert candidate_signal(**{**kwargs, "event_ids": ids[:19], "customer_ids": customers[:19]}) is None
    validate_event("incident.signals.v1", signal)


def test_seeded_scenario_produces_payment_and_login_candidates() -> None:
    grouped = defaultdict(lambda: {"events": [], "customers": []})
    seen = set()
    reasons = defaultdict(int)
    for record in generate_records(ScenarioConfig()):
        enriched, reason = normalize_record(record.record_key.encode(), record.payload.encode(), NOW)
        if reason:
            reasons[reason] += 1
            continue
        assert enriched is not None
        identity = (enriched["tenant_id"], enriched["event_id"])
        if identity in seen:
            continue
        seen.add(identity)
        when = datetime.fromisoformat(enriched["event_time"].replace("Z", "+00:00"))
        start = when.replace(second=0, microsecond=0)
        group = (enriched["tenant_id"], enriched["product"], enriched["error_code"], start)
        grouped[group]["events"].append(enriched["event_id"])
        grouped[group]["customers"].append(enriched["customer_id"])
    found = {}
    for (tenant, product, code, start), members in grouped.items():
        signal = candidate_signal(
            tenant_id=tenant, product=product, error_code=code,
            window_start=start, window_end=start + timedelta(minutes=1),
            event_ids=members["events"], customer_ids=members["customers"],
            detected_at=NOW,
        )
        if signal:
            found[code] = signal["current_count"]
    assert found == {"PAY-502": 150, "AUTH-401": 40}
    assert reasons == {"late_event": 5, "invalid_json": 1, "schema_violation": 1}
    assert len(seen) == 250
