"""Validated configuration for the API process."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _host(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} must be a nonempty host")
    return value


def _port(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer port") from exc
    if not 1 <= value <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535")
    return value


@dataclass(frozen=True)
class ApiSettings:
    kafka_host: str
    kafka_port: int
    mongo_host: str
    mongo_port: int

    @classmethod
    def from_env(cls) -> ApiSettings:
        return cls(
            kafka_host=_host("ECHOGUARD_KAFKA_HOST", "kafka"),
            kafka_port=_port("ECHOGUARD_KAFKA_PORT", 9092),
            mongo_host=_host("ECHOGUARD_MONGO_HOST", "mongo"),
            mongo_port=_port("ECHOGUARD_MONGO_PORT", 27017),
        )
