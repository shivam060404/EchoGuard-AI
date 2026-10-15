"""Correlation invariants before Kafka/Mongo integration."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.worker.correlation import case_record, signal_record


FIXTURES = Path(__file__).parents[1] / "contracts" / "fixtures"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_case_identity_is_stable_and_tenant_scoped() -> None:
    event = fixture("support.enriched.v1.valid.json")
    first = case_record(event)
    assert first == case_record(copy.deepcopy(event))
    assert first["_id"] != case_record({**event, "tenant_id": "other-tenant"})["_id"]
    assert first["normalized_text"] == event["normalized_text"]


def test_signal_correlates_nearby_windows_but_isolates_tenants_and_codes() -> None:
    event = fixture("incident.signals.v1.valid.json")
    first = signal_record(event)
    second = signal_record({**event, "window_start": "2026-10-15T10:06:00Z",
                            "window_end": "2026-10-15T10:07:00Z"})
    assert first["cluster_id"] == second["cluster_id"]
    assert first["cluster_id"] != signal_record({**event, "tenant_id": "other-tenant"})["cluster_id"]
    assert first["_id"] != signal_record({**event, "tenant_id": "other-tenant"})["_id"]
    assert first["cluster_id"] != signal_record({**event, "error_code": "PAY-503"})["cluster_id"]


def test_invalid_or_unredacted_case_is_rejected() -> None:
    event = fixture("support.enriched.v1.valid.json")
    with pytest.raises(Exception):
        case_record({**event, "redaction_status": "raw"})


def test_invalid_signal_window_is_rejected() -> None:
    event = fixture("incident.signals.v1.valid.json")
    with pytest.raises(ValueError, match="positive duration"):
        signal_record({**event, "window_end": event["window_start"]})
