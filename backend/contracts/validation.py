"""Schema validation at the ingestion boundary."""

from __future__ import annotations

import json
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker, ValidationError


SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
FORMAT_CHECKER = FormatChecker()


@FORMAT_CHECKER.checks("date-time")
def is_datetime(value: object) -> bool:
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


@lru_cache(maxsize=16)
def validator_for(topic: str) -> Draft202012Validator:
    if not re.fullmatch(r"[a-z][a-z0-9.]+\.v[1-9][0-9]*", topic):
        raise ValueError(f"Invalid topic name: {topic!r}")
    path = SCHEMA_DIR / f"{topic}.schema.json"
    schema = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FORMAT_CHECKER)


def validate_event(topic: str, event: dict[str, Any]) -> None:
    validator_for(topic).validate(event)


def classify_payload(topic: str, payload: bytes) -> tuple[dict[str, Any] | None, str | None]:
    """Return parsed event or a safe reason code; never include payload in errors."""
    try:
        parsed = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, "invalid_json"
    if not isinstance(parsed, dict):
        return None, "schema_violation"
    try:
        validate_event(topic, parsed)
    except (ValidationError, ValueError):
        return None, "schema_violation"
    return parsed, None

