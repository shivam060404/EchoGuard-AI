"""Trusted application boundary for tenant access and human review decisions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from langgraph.types import Command

from .models import Decision, GRAPH_VERSION, digest, now_utc, proposal_hash
from .store import Conflict


@dataclass(frozen=True)
class Principal:
    # Construct from trusted authentication/CLI configuration, never model output.
    tenant_id: str
    actor_id: str
    role: str


class WorkflowService:
    def __init__(self, graph, store, retriever, clock=now_utc):
        self.graph, self.store, self.retriever, self.clock = graph, store, retriever, clock

    @staticmethod
    def config(run_id):
        return {"configurable": {"thread_id": run_id}, "recursion_limit": 20}

    def _principal(self, principal):
        if not principal.tenant_id or not principal.actor_id:
            raise PermissionError("Authenticated principal required")
        if principal.role not in {"operator", "reviewer"}:
            raise PermissionError("Operator or reviewer role required")

    def _record(self, principal, run_id):
        self._principal(principal)
        record = self.store.get(principal.tenant_id, run_id)
        if record is None:
            raise LookupError("Workflow not found in tenant")
        if record["initial"]["graph_version"] != GRAPH_VERSION:
            raise Conflict("Workflow requires its original graph version")
        return record

    def _project(self, principal, run_id, owner):
        state = dict(self.graph.get_state(self.config(run_id)).values)
        if state:
            self.store.save(principal.tenant_id, run_id, owner, state)
        return state

    def start(self, principal: Principal, incident_id: str, request_key: str):
        self._principal(principal)
        if not 1 <= len(request_key) <= 128:
            raise ValueError("Request key must contain 1–128 characters")
        run_id = "wf-" + digest([principal.tenant_id, request_key])[:32]
        existing = self.store.get(principal.tenant_id, run_id)
        if existing is None:
            incident, evidence = self.store.context(principal.tenant_id, incident_id)
            if incident["status"] not in {"candidate", "reopened", "investigating"}:
                raise Conflict("Incident is not eligible for investigation")
            now = self.clock()
            initial = {"run_id": run_id, "graph_version": GRAPH_VERSION,
                       "incident": incident, "evidence": evidence,
                       "created_at": now.isoformat(), "expires_at": (now + timedelta(hours=24)).isoformat(),
                       "status": "running", "reason": "", "attempts": 0, "retrieval_calls": 0}
            existing = self.store.create({"_id": run_id, "tenant_id": principal.tenant_id,
                                          "incident_id": incident_id, "created_at": now.isoformat(),
                                          "created_by": principal.actor_id, "initial": initial,
                                          "owner": None, "audit": []})
        if existing["incident_id"] != incident_id:
            raise Conflict("Request key already belongs to a different incident")
        self._record(principal, run_id)
        with self.store.locked(principal.tenant_id, run_id) as owner:
            snapshot = self.graph.get_state(self.config(run_id))
            if not snapshot.values:
                self.graph.invoke(existing["initial"], self.config(run_id), durability="sync")
            elif snapshot.next and not any(task.interrupts for task in snapshot.tasks):
                self.graph.invoke(None, self.config(run_id), durability="sync")
            return self._project(principal, run_id, owner)

    def status(self, principal: Principal, run_id: str):
        self._record(principal, run_id)
        with self.store.locked(principal.tenant_id, run_id) as owner:
            return self._project(principal, run_id, owner)

    def decide(self, principal: Principal, run_id: str, decision: str, expected_hash: str):
        self._record(principal, run_id)
        if principal.role != "reviewer":
            raise PermissionError("Reviewer role required")
        submitted = Decision(decision=decision, proposal_hash=expected_hash,
                             actor_id=principal.actor_id, tenant_id=principal.tenant_id, role="reviewer")
        with self.store.locked(principal.tenant_id, run_id) as owner:
            state = self._project(principal, run_id, owner)
            if state.get("status") in {"approved", "rejected"}:
                if state["decision"] != submitted.model_dump():
                    raise Conflict("Workflow already has a different decision")
                return state
            if state.get("status") != "awaiting_approval":
                raise Conflict("Workflow has no approvable proposal")
            if expected_hash != state["proposal_hash"] or expected_hash != proposal_hash(state):
                raise Conflict("Proposal hash changed")
            if decision == "approve":
                if self.clock() >= datetime.fromisoformat(state["expires_at"]):
                    raise Conflict("Proposal expired; start a new workflow")
                current, _ = self.store.context(principal.tenant_id, state["incident"]["incident_id"])
                if digest(current) != digest(state["incident"]):
                    raise Conflict("Incident changed; start a new workflow")
                # Recheck approval/expiry at decision time, including exact quoted content.
                hits = self.retriever.retrieve(
                    tenant_id=principal.tenant_id, product=current["product"],
                    query=f"{current['product']} {current['error_code'] or ''}",
                    as_of=self.clock(), top_k=5,
                )
                approved = {hit.citation: hit.text for hit in hits}
                if any(approved.get(step["citation"]) != step["quote"] for step in state["proposal"]["steps"]):
                    raise Conflict("Knowledge changed or was revoked; start a new workflow")
            self.graph.invoke(Command(resume=submitted.model_dump()), self.config(run_id), durability="sync")
            return self._project(principal, run_id, owner)

    def revise(self, principal, run_id, expected_hash, action, step_indices, request_key):
        self._record(principal, run_id)
        if principal.role != "reviewer":
            raise PermissionError("Reviewer role required")
        if action not in {"create_ticket", "post_internal_notification"}:
            raise Conflict("Action is not permitted")
        with self.store.locked(principal.tenant_id, run_id) as owner:
            state = self._project(principal, run_id, owner)
            revision = {"actor": principal.actor_id, "request_key": request_key,
                        "previous_hash": expected_hash, "action": action, "steps": step_indices}
            if state.get("revision") == revision:
                snapshot = self.graph.get_state(self.config(run_id))
                if snapshot.next and not any(task.interrupts for task in snapshot.tasks):
                    self.graph.invoke(None, self.config(run_id), durability="sync")
                return self._project(principal, run_id, owner)
            if state.get("status") != "awaiting_approval" or state.get("proposal_hash") != expected_hash:
                raise Conflict("Only the current unapproved proposal can be edited")
            if self.clock() >= datetime.fromisoformat(state["expires_at"]):
                raise Conflict("Proposal expired")
            steps = state["proposal"]["steps"]
            if (not step_indices or len(set(step_indices)) != len(step_indices)
                    or any(type(i) is not int or i < 0 or i >= len(steps) for i in step_indices)):
                raise Conflict("Select distinct existing cited steps")
            proposal = {**state["proposal"], "action": action, "steps": [steps[i] for i in step_indices]}
            # Re-run validation and create a fresh interrupt; approval hashes cannot survive edits.
            self.graph.update_state(self.config(run_id),
                                    {"proposal": proposal, "revision": revision}, as_node="plan")
            self.graph.invoke(None, self.config(run_id), durability="sync")
            return self._project(principal, run_id, owner)
