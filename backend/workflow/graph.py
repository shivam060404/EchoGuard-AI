"""Bounded LangGraph stages; no execution tools exist in this graph."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from pydantic import ValidationError

from .models import Decision, Evidence, Incident, Proposal, proposal_hash, now_utc


class WorkflowState(TypedDict, total=False):
    run_id: str
    graph_version: str
    incident: dict
    evidence: list[dict]
    created_at: str
    expires_at: str
    status: str
    reason: str
    triage: dict
    knowledge: list[dict]
    proposal: dict
    proposal_hash: str
    usage: dict
    attempts: int
    retrieval_calls: int
    decision: dict
    revision: dict


def blocked(reason: str) -> dict:
    return {"status": "needs_review", "reason": reason, "proposal": {}, "proposal_hash": ""}


def build_graph(provider, retriever, checkpointer, clock=now_utc):
    def triage(state):
        incident = Incident.model_validate(state["incident"])
        return {"triage": {"severity": "high" if incident.peak_count >= 100 else "medium",
                           "rationale": f"Candidate peak: {incident.peak_count} reports; impact unconfirmed."}}

    def investigate(state):
        try:
            evidence = [Evidence.model_validate(item) for item in state["evidence"]]
        except ValidationError:
            return blocked("invalid_evidence")
        incident = state["incident"]
        if not evidence:
            return blocked("missing_evidence")
        if any(item.tenant_id != incident["tenant_id"] or item.product != incident["product"]
               for item in evidence):
            return blocked("evidence_scope_mismatch")
        return {}

    def retrieve(state):
        incident = state["incident"]
        try:
            hits = retriever.retrieve(tenant_id=incident["tenant_id"], product=incident["product"],
                                      query=f"{incident['product']} {incident['error_code'] or ''}",
                                      as_of=clock(), top_k=5)
            knowledge = [asdict(hit) for hit in hits]
            if not knowledge:
                return {**blocked("missing_knowledge"), "retrieval_calls": 1}
            if any(not hit["citation"].startswith(f"knowledge://{incident['tenant_id']}/")
                   for hit in knowledge):
                return blocked("knowledge_scope_mismatch")
            if incident["error_code"] and not any(
                incident["error_code"].casefold() in (hit["title"] + " " + hit["text"]).casefold()
                for hit in knowledge
            ):
                return blocked("knowledge_not_relevant")
            return {"knowledge": knowledge, "retrieval_calls": 1}
        except Exception:
            # Dependency diagnostics belong in restricted telemetry, not checkpointed model output.
            return {**blocked("retrieval_unavailable"), "retrieval_calls": 1}

    def plan(state):
        context = {key: state[key] for key in ("incident", "evidence", "knowledge")}
        for attempt in (1, 2):
            try:
                response = provider.plan(context, timeout_seconds=15.0)
                return {"proposal": response.payload, "usage": response.usage.model_dump(),
                        "attempts": attempt}
            except (TimeoutError, ConnectionError):
                if attempt == 2:
                    return {**blocked("model_unavailable"), "attempts": attempt}
            except Exception:
                return {**blocked("model_invalid_response"), "attempts": attempt}
        raise AssertionError("Unreachable")

    def validate(state):
        try:
            proposal = Proposal.model_validate(state["proposal"])
            if proposal.confidence < 0.7:
                return blocked("low_confidence")
            evidence_ids = {item["event_id"] for item in state["evidence"]}
            if not set(proposal.evidence_ids).issubset(evidence_ids):
                return blocked("unsupported_evidence")
            knowledge = {item["citation"]: item for item in state["knowledge"]}
            for step in proposal.steps:
                if step.citation not in knowledge or step.quote != knowledge[step.citation]["text"]:
                    return blocked("unsupported_citation_or_step")
            cleaned = proposal.model_dump()
            return {"proposal": cleaned,
                    "proposal_hash": proposal_hash({**state, "proposal": cleaned}),
                    "status": "awaiting_approval"}
        except (ValidationError, ValueError, TypeError):
            return blocked("model_invalid_response")

    def review(state):
        # Keep interrupt outside exception handlers; the runtime uses an interrupt exception.
        raw = interrupt({"run_id": state["run_id"], "proposal_hash": state["proposal_hash"],
                         "proposal": state["proposal"], "expires_at": state["expires_at"]})
        decision = Decision.model_validate(raw)
        if (decision.tenant_id != state["incident"]["tenant_id"]
                or decision.proposal_hash != proposal_hash(state)
                or decision.proposal_hash != state["proposal_hash"]):
            raise ValueError("Decision does not match this proposal")
        if decision.decision == "approve" and clock() >= datetime.fromisoformat(state["expires_at"]):
            raise ValueError("Approval expired")
        return {"decision": decision.model_dump(),
                "status": "approved" if decision.decision == "approve" else "rejected"}

    builder = StateGraph(WorkflowState)
    for name, node in (("triage", triage), ("investigate", investigate), ("retrieve", retrieve),
                       ("plan", plan), ("validate", validate), ("review", review)):
        builder.add_node(name, node)
    builder.add_edge(START, "triage")
    builder.add_edge("triage", "investigate")
    for current, following in (("investigate", "retrieve"), ("retrieve", "plan"),
                               ("plan", "validate"), ("validate", "review")):
        builder.add_conditional_edges(current, lambda s, target=following:
                                      END if s["status"] == "needs_review" else target)
    builder.add_edge("review", END)
    return builder.compile(checkpointer=checkpointer)
