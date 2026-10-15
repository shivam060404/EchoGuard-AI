"""Strict boundaries between retrieved evidence, model proposals and decisions."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

GRAPH_VERSION = "incident-review-1.0.0"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Incident(StrictModel):
    incident_id: str = Field(min_length=1, max_length=128)
    tenant_id: str = Field(min_length=1, max_length=128)
    product: str = Field(min_length=1, max_length=128)
    error_code: str | None
    version: int = Field(ge=1)
    status: str
    peak_count: int = Field(ge=0)
    last_seen: str
    signal_ids: list[str] = Field(max_length=1000)


class Evidence(StrictModel):
    event_id: str
    tenant_id: str
    product: str
    normalized_text: str = Field(min_length=1, max_length=20000)
    redaction_status: Literal["synthetic", "redacted"]


class Step(StrictModel):
    citation: str = Field(min_length=1, max_length=1024)
    # Extractive recommendations: free-form executable instructions are not accepted.
    quote: str = Field(min_length=20, max_length=3000)


class Proposal(StrictModel):
    hypothesis: str = Field(min_length=1, max_length=1000)
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    evidence_ids: list[str] = Field(min_length=1, max_length=20)
    steps: list[Step] = Field(min_length=1, max_length=5)
    action: Literal["create_ticket", "post_internal_notification"]


class Usage(StrictModel):
    provider: str
    model: str
    prompt_version: str
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cost_usd: float = Field(ge=0, allow_inf_nan=False)
    estimated_tokens: bool = False


class Decision(StrictModel):
    decision: Literal["approve", "reject"]
    proposal_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    actor_id: str = Field(min_length=1, max_length=128)
    tenant_id: str = Field(min_length=1, max_length=128)
    role: Literal["reviewer"]


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def proposal_hash(state: dict) -> str:
    return digest({"proposal": state["proposal"], "incident": state["incident"],
                   "graph_version": state["graph_version"], "expires_at": state["expires_at"],
                   "run_id": state["run_id"],
                   **({"revision": state["revision"]} if state.get("revision") else {})})
