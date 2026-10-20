from datetime import datetime, timedelta, timezone

from backend.workflow.models import digest, now_utc, proposal_hash
from backend.workflow.store import Conflict


class ActionService:
    def __init__(self, workflow, repository, adapter, clock=now_utc):
        self.workflow, self.repo, self.adapter, self.clock = workflow, repository, adapter, clock

    def execute(self, actor, run_id, expected_hash, expected_version, request_key):
        if actor.role != "reviewer":
            raise PermissionError("Reviewer role required")
        self.workflow._record(actor, run_id)
        with self.workflow.store.locked(actor.tenant_id, run_id) as owner:
            state = self.workflow._project(actor, run_id, owner)
            if (state.get("status") != "approved" or state.get("proposal_hash") != expected_hash
                    or expected_hash != proposal_hash(state)
                    or state["incident"]["version"] != expected_version):
                raise Conflict("Action requires the exact approved proposal and incident version")
            existing = self.repo.get(actor.tenant_id, run_id)
            if existing and existing["status"] == "succeeded":
                return existing
            # Reconciliation reads an existing effect; it never authorizes a new one.
            # Keep this before freshness checks so expiry/revocation cannot strand an
            # effect that was already committed by the adapter.
            if existing:
                effect = self.adapter.lookup(actor.tenant_id, existing)
                if effect is not None:
                    completed = {**existing, "status": "succeeded", **effect}
                    self.repo.complete(actor.tenant_id, run_id, owner, completed)
                    return completed
            if self.clock() >= datetime.fromisoformat(state["expires_at"]):
                raise Conflict("Approval expired")
            current, _ = self.workflow.store.context(actor.tenant_id, state["incident"]["incident_id"])
            if digest(current) != digest(state["incident"]):
                raise Conflict("Incident changed after approval")
            hits = self.workflow.retriever.retrieve(
                tenant_id=actor.tenant_id, product=current["product"],
                query=f"{current['product']} {current['error_code'] or ''}", as_of=self.clock(), top_k=5)
            knowledge = {hit.citation: hit.text for hit in hits}
            if any(knowledge.get(step["citation"]) != step["quote"] for step in state["proposal"]["steps"]):
                raise Conflict("Approved knowledge changed")
            kind = state["proposal"]["action"]
            if kind not in {"create_ticket", "post_internal_notification"}:
                raise Conflict("Action is not permitted")
            action = existing or {
                "action_id": "act-" + digest([actor.tenant_id, current["incident_id"], expected_hash, kind])[:32],
                "status": "pending", "kind": kind, "proposal_hash": expected_hash,
                "requested_by": actor.actor_id, "approved_by": state["decision"]["actor_id"],
                "request_key": request_key, "requested_at": self.clock().isoformat(),
            }
            self.repo.pending(actor.tenant_id, run_id, owner, action)
            # This adapter is a local database simulation, never a remote HTTP integration.
            effect = self.adapter.perform(actor.tenant_id, action, state)
            completed = {**action, "status": "succeeded", **effect}
            self.repo.complete(actor.tenant_id, run_id, owner, completed)
            return completed


class MongoActionRepository:
    def __init__(self, database):
        self.db = database

    def get(self, tenant, run_id):
        run = self.db.workflow_runs.find_one({"_id": run_id, "tenant_id": tenant})
        return run.get("action") if run else None

    def _write(self, tenant, run_id, owner, action):
        result = self.db.workflow_runs.update_one(
            {"_id": run_id, "tenant_id": tenant, "owner": owner},
            {"$set": {"action": action}, "$addToSet": {"audit": {
                "status": "action_" + action["status"], "action_id": action["action_id"],
                "actor": action["requested_by"], "proposal_hash": action["proposal_hash"],
            }}},
        )
        if not result.matched_count:
            raise Conflict("Action writer lost ownership")

    pending = _write
    complete = _write


class SimulatedAdapter:
    def __init__(self, database):
        self.db = database

    def lookup(self, tenant, action):
        record = self.db.simulated_actions.find_one({"_id": action["action_id"], "tenant_id": tenant})
        if record is None:
            return None
        if record.get("proposal_hash") != action["proposal_hash"] or record.get("kind") != action["kind"]:
            raise Conflict("Stored effect does not match authorized action")
        if "completed_at" not in record:
            # Older effects have no trustworthy effect timestamp. Do not invent one
            # or execute again; require explicit operator reconciliation.
            raise Conflict("Legacy simulated effect requires timestamp reconciliation")
        return {"result": record["result"], "completed_at": record["completed_at"]}

    def perform(self, tenant, action, state):
        action_id = action["action_id"]
        result = {"reference": "SIM-" + action_id.removeprefix("act-")[:12], "simulated": True}
        # Repeated delivery after a crash returns the same simulated external object.
        self.db.simulated_actions.update_one(
            {"_id": action_id}, {"$setOnInsert": {
                "_id": action_id, "tenant_id": tenant, "kind": action["kind"],
                "incident_id": state["incident"]["incident_id"],
                "proposal_hash": action["proposal_hash"], "result": result,
                "completed_at": now_utc().isoformat(),
            }}, upsert=True,
        )
        return self.lookup(tenant, action)


def outcome(database, tenant, incident, action, now=None):
    now = now or now_utc()
    if not action or action["status"] != "succeeded":
        return {"status": "insufficient_data", "reason": "No completed action", "window_minutes": 5}
    started = datetime.fromisoformat(action["completed_at"])
    end = started + timedelta(minutes=5)
    base = {"tenant_id": tenant, "product": incident["product"], "error_code": incident.get("error_code")}
    before = database.cases.count_documents({**base, "event_time": {"$gte": started-timedelta(minutes=5), "$lt": started}})
    after = database.cases.count_documents({**base, "event_time": {"$gte": started, "$lt": end}})
    # No complaints is not proof of a healthy pipeline: require later evidence of stream progress.
    progressed = database.cases.find_one({"tenant_id": tenant, "event_time": {"$gte": end}}, {"_id": 1})
    if now < end or before == 0 or not progressed:
        status = "insufficient_data"
    else:
        ratio = after / before
        status = "improving" if ratio < .75 else "worsening" if ratio > 1.25 else "unchanged"
    return {"status": status, "before_count": before, "after_count": after,
            "window_minutes": 5, "window_end": end.isoformat(),
            "reason": "Observed complaint counts; not proof that the simulated action caused a change"}
