"""Validate versioned schemas, fixtures, and event invariants."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator

from .validation import FORMAT_CHECKER


CONTRACTS = Path(__file__).resolve().parent
SCHEMAS = CONTRACTS / "schemas"
FIXTURES = CONTRACTS / "fixtures"
NAME = re.compile(r"^(?P<topic>.+)\.v(?P<major>\d+)\.schema\.json$")


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_utc(value: str, field: str, path: Path) -> None:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"{path.name}: {field} must be UTC")


def validate_contracts() -> list[str]:
    results: list[str] = []
    schema_paths = sorted(SCHEMAS.glob("*.schema.json"))
    if not schema_paths:
        raise ValueError("No event schemas found")

    for schema_path in schema_paths:
        match = NAME.fullmatch(schema_path.name)
        if not match:
            raise ValueError(f"Unexpected schema filename: {schema_path.name}")
        schema = _load(schema_path)
        Draft202012Validator.check_schema(schema)
        if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            raise ValueError(f"{schema_path.name}: unsupported JSON Schema draft")

        topic = match.group("topic")
        major = match.group("major")
        fixture_paths = sorted(FIXTURES.glob(f"{topic}.v{major}*.valid.json"))
        if not fixture_paths:
            raise ValueError(f"{schema_path.name}: missing positive fixture")
        validator = Draft202012Validator(schema, format_checker=FORMAT_CHECKER)
        for fixture_path in fixture_paths:
            event = _load(fixture_path)
            validator.validate(event)
            if event["schema_version"].split(".")[0] != major:
                raise ValueError(f"{fixture_path.name}: major version mismatch")
            for field in ("event_time", "ingest_time", "processed_time", "window_start", "window_end", "detected_time"):
                if field in event:
                    _require_utc(event[field], field, fixture_path)
            if "window_start" in event and event["window_start"] >= event["window_end"]:
                raise ValueError(f"{fixture_path.name}: invalid event-time window")
            if "signal_id" in event and event["event_id"] != event["signal_id"]:
                raise ValueError(f"{fixture_path.name}: signal event ID must be stable signal ID")
            results.append(f"{schema_path.name} accepts {fixture_path.name}")
    return results


if __name__ == "__main__":
    for result in validate_contracts():
        print(result)

