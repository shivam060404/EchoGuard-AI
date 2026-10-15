"""Deterministic, labeled support traffic for replayable incident scenarios."""

from __future__ import annotations

import hashlib
import json
import os
import random
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.contracts.validation import classify_payload


RAW_TOPIC = "support.raw.v1"
DLQ_TOPIC = "ingest.dlq.v1"
DEFAULT_START = datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)
CHANNELS = ("web_chat", "email", "ticket")
NORMAL_TEXTS = (
    "How can I update my account profile?",
    "Where can I download last month's invoice?",
    "I need help changing notification settings.",
    "The dashboard is loading slowly today.",
    "Can I add another team member to my workspace?",
)
PAYMENT_TEXTS = (
    "Payment failed but the amount was deducted. Error PAY-502.",
    "Checkout says PAY-502 and my card was charged.",
    "I was charged but my subscription renewal failed with PAY-502.",
)
LOGIN_TEXTS = (
    "Sign in failed with AUTH-401 after entering a valid password.",
    "I cannot access my account; the login page shows AUTH-401.",
    "My session was rejected with AUTH-401.",
)


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("Scenario times must be UTC")
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _digest(*parts: object) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()[:20]


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class ScenarioConfig:
    seed: int = 42
    tenant_id: str = "demo-tenant"
    start_time: datetime = DEFAULT_START
    normal_count: int = 60
    payment_count: int = 150
    login_count: int = 40
    duplicate_count: int = 8
    late_count: int = 5

    def __post_init__(self) -> None:
        if not self.tenant_id or len(self.tenant_id) > 128:
            raise ValueError("tenant_id must contain 1–128 characters")
        _utc(self.start_time)
        for name in ("normal_count", "payment_count", "login_count", "duplicate_count", "late_count"):
            value = getattr(self, name)
            if value < 0 or value > 10_000:
                raise ValueError(f"{name} must be between 0 and 10,000")
        if self.duplicate_count > self.payment_count:
            raise ValueError("duplicate_count cannot exceed payment_count")


@dataclass(frozen=True)
class ScenarioRecord:
    record_id: str
    record_key: str
    payload: str
    kind: str
    label: str

    def as_json(self) -> str:
        return _json(asdict(self))


def _event(
    config: ScenarioConfig,
    kind: str,
    index: int,
    event_time: datetime,
    ingest_time: datetime,
    product: str,
    text: str,
    error_code: str | None,
    rng: random.Random,
) -> ScenarioRecord:
    token = _digest(config.seed, config.tenant_id, kind, index)
    customer_id = f"customer-{kind}-{index:04d}"
    channel = CHANNELS[rng.randrange(len(CHANNELS))]
    event = {
        "schema_version": "1.0.0",
        "event_id": f"evt-{token}",
        "tenant_id": config.tenant_id,
        "trace_id": f"trace-{token}",
        "source": "simulator",
        "source_message_id": f"msg-{token}",
        "channel": channel,
        "customer_id": customer_id,
        "event_time": _utc(event_time),
        "ingest_time": _utc(ingest_time),
        "product": product,
        "text": text,
        "error_code": error_code,
        "metadata": {"synthetic": True, "scenario": kind},
    }
    return ScenarioRecord(
        record_id=f"rec-{kind}-{index:04d}",
        record_key=f"{config.tenant_id}:{customer_id}",
        payload=_json(event),
        kind=kind,
        label={"payment": "payment_incident", "login": "login_incident"}.get(kind, "normal"),
    )


def generate_records(config: ScenarioConfig) -> list[ScenarioRecord]:
    rng = random.Random(config.seed)
    start = config.start_time
    records: list[ScenarioRecord] = []

    for index in range(config.normal_count):
        event_time = start + timedelta(seconds=index * 4)
        records.append(_event(config, "normal", index, event_time, event_time + timedelta(seconds=2),
                              "account", rng.choice(NORMAL_TEXTS), None, rng))

    payments: list[ScenarioRecord] = []
    for index in range(config.payment_count):
        event_time = start + timedelta(minutes=5, seconds=index * 60 // max(config.payment_count, 1))
        payments.append(_event(config, "payment", index, event_time, event_time + timedelta(seconds=2),
                               "payments", rng.choice(PAYMENT_TEXTS), "PAY-502", rng))
    records.extend(payments)

    for index in range(config.login_count):
        event_time = start + timedelta(minutes=7, seconds=index * 60 // max(config.login_count, 1))
        records.append(_event(config, "login", index, event_time, event_time + timedelta(seconds=2),
                              "identity", rng.choice(LOGIN_TEXTS), "AUTH-401", rng))

    for index, original in enumerate(payments[:config.duplicate_count]):
        records.append(ScenarioRecord(f"retry-payment-{index:04d}", original.record_key,
                                      original.payload, "duplicate", "payment_incident"))

    for index in range(config.late_count):
        event_time = start + timedelta(minutes=2, seconds=index)
        ingest_time = start + timedelta(minutes=9, seconds=index)
        records.append(_event(config, "late", index, event_time, ingest_time,
                              "account", rng.choice(NORMAL_TEXTS), None, rng))

    invalid_json = '{"schema_version":"1.0.0","event_id":'
    records.append(ScenarioRecord("malformed-json-0000", f"{config.tenant_id}:malformed-json-0000",
                                  invalid_json, "malformed", "invalid_json"))
    schema_invalid = json.loads(_event(config, "malformed", 1, start + timedelta(minutes=10),
                                       start + timedelta(minutes=10, seconds=1), "payments",
                                       PAYMENT_TEXTS[0], "PAY-502", rng).payload)
    del schema_invalid["event_id"]
    records.append(ScenarioRecord("malformed-schema-0001", f"{config.tenant_id}:malformed-schema-0001",
                                  _json(schema_invalid), "malformed", "schema_violation"))
    return records


def _manifest(config: ScenarioConfig, records: list[ScenarioRecord], content: bytes) -> dict[str, Any]:
    routes = Counter()
    event_ids: list[str] = []
    reason_codes = Counter()
    for record in records:
        parsed, reason = classify_payload(RAW_TOPIC, record.payload.encode("utf-8"))
        if reason:
            routes[DLQ_TOPIC] += 1
            reason_codes[reason] += 1
        else:
            routes[RAW_TOPIC] += 1
            event_ids.append(parsed["event_id"])
    return {
        "manifest_version": "1.0.0",
        "scenario": "payment_and_login_spikes",
        "config": {**asdict(config), "start_time": _utc(config.start_time)},
        "dataset_sha256": hashlib.sha256(content).hexdigest(),
        "record_count": len(records),
        "expected_topics": dict(sorted(routes.items())),
        "unique_valid_event_ids": len(set(event_ids)),
        "duplicate_deliveries": len(event_ids) - len(set(event_ids)),
        "late_event_count": sum(record.kind == "late" for record in records),
        "reason_codes": dict(sorted(reason_codes.items())),
        "labels": dict(sorted(Counter(record.label for record in records).items())),
        "expected_incident_labels": [label for label, count in
                                     (("payment_incident", config.payment_count),
                                      ("login_incident", config.login_count)) if count > 0],
    }


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    os.replace(temporary, path)


def write_scenario(config: ScenarioConfig, dataset_path: Path, manifest_path: Path, force: bool = False) -> dict[str, Any]:
    if dataset_path.resolve() == manifest_path.resolve():
        raise ValueError("Dataset and manifest paths must differ")
    records = generate_records(config)
    content = ("\n".join(record.as_json() for record in records) + "\n").encode("utf-8")
    manifest = _manifest(config, records, content)
    manifest_bytes = (_json(manifest) + "\n").encode("utf-8")
    for path, expected in ((dataset_path, content), (manifest_path, manifest_bytes)):
        if path.exists() and path.read_bytes() != expected and not force:
            raise FileExistsError(f"{path} differs; use --force to replace it")
    _atomic_write(dataset_path, content)
    _atomic_write(manifest_path, manifest_bytes)
    return manifest


def read_scenario(dataset_path: Path, manifest_path: Path) -> tuple[list[ScenarioRecord], dict[str, Any]]:
    content = dataset_path.read_bytes()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if hashlib.sha256(content).hexdigest() != manifest["dataset_sha256"]:
        raise ValueError("Dataset digest does not match manifest")
    records = [ScenarioRecord(**json.loads(line)) for line in content.decode("utf-8").splitlines()]
    config_data = dict(manifest["config"])
    config_data["start_time"] = datetime.fromisoformat(config_data["start_time"].replace("Z", "+00:00"))
    config = ScenarioConfig(**config_data)
    expected = _manifest(config, records, content)
    if manifest != expected:
        raise ValueError("Manifest assertions do not match dataset")
    if len({record.record_id for record in records}) != len(records):
        raise ValueError("record_id must be unique per physical record")
    if records != generate_records(config):
        raise ValueError("Dataset is not the deterministic output of its config")
    for record in records:
        if not record.record_key.startswith(f"{config_data['tenant_id']}:"):
            raise ValueError("Record key has a different tenant")
        if len(record.payload.encode("utf-8")) > 64_000:
            raise ValueError("Record payload exceeds 64 KB")
    return records, manifest

