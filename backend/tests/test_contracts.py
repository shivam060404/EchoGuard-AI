from copy import deepcopy
import json

import pytest
from jsonschema import Draft202012Validator, ValidationError

from backend.contracts.check_contracts import CONTRACTS, FORMAT_CHECKER, validate_contracts


def _fixture(name: str) -> dict:
    return json.loads((CONTRACTS / "fixtures" / name).read_text())


def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((CONTRACTS / "schemas" / name).read_text())
    return Draft202012Validator(schema, format_checker=FORMAT_CHECKER)


def test_all_contracts_and_fixtures_are_valid() -> None:
    assert len(validate_contracts()) == 4


@pytest.mark.parametrize("mutation", [
    lambda event: event.pop("event_id"),
    lambda event: event.update({"unexpected": "new field"}),
    lambda event: event.update({"event_time": "not-a-date"}),
    lambda event: event.update({"event_time": "2026-10-15T15:35:00+05:30"}),
])
def test_raw_contract_rejects_bad_events(mutation) -> None:
    event = deepcopy(_fixture("support.raw.v1.valid.json"))
    mutation(event)
    with pytest.raises(ValidationError):
        _validator("support.raw.v1.schema.json").validate(event)


def test_signal_rejects_negative_count() -> None:
    event = _fixture("incident.signals.v1.valid.json")
    event["current_count"] = -1
    with pytest.raises(ValidationError):
        _validator("incident.signals.v1.schema.json").validate(event)

