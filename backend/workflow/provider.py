"""Provider-independent planning boundary; default provider makes no network calls."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from .models import Usage


@dataclass(frozen=True)
class ModelResponse:
    payload: dict
    usage: Usage


class PlanningProvider(Protocol):
    # Live adapters must implement a transport deadline and bounded output size.
    def plan(self, context: dict, *, timeout_seconds: float) -> ModelResponse: ...


class MockPlanningProvider:
    def plan(self, context: dict, *, timeout_seconds: float) -> ModelResponse:
        incident = context["incident"]
        payload = {
            "hypothesis": f"A shared {incident['product']} failure is suspected; root cause is unconfirmed.",
            "confidence": 0.8,
            "evidence_ids": [item["event_id"] for item in context["evidence"][:5]],
            "steps": [{"citation": item["citation"], "quote": item["text"]}
                      for item in context["knowledge"][:3]],
            "action": "create_ticket",
        }
        return ModelResponse(payload, Usage(
            provider="mock", model="extractive-demo-v1", prompt_version="incident-plan-v1",
            input_tokens=max(1, len(json.dumps(context)) // 4),
            output_tokens=max(1, len(json.dumps(payload)) // 4), cost_usd=0.0,
            estimated_tokens=True,
        ))
