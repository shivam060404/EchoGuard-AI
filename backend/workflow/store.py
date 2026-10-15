"""MongoDB run registry and exclusive writers; graph checkpoints are separate."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import uuid4

from .models import Incident


class Conflict(ValueError):
    pass


class Busy(Conflict):
    pass


def incident_snapshot(document: dict) -> dict:
    seen = document["last_seen"]
    if isinstance(seen, datetime):
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=timezone.utc)  # MongoDB BSON dates are UTC.
        seen = seen.astimezone(timezone.utc).isoformat()
    return Incident(
        incident_id=document["_id"], tenant_id=document["tenant_id"],
        product=document["product"], error_code=document.get("error_code"),
        version=document["version"], status=document["status"], peak_count=document["peak_count"],
        last_seen=seen, signal_ids=sorted(document["signal_ids"]),
    ).model_dump()


class MongoWorkflowStore:
    def __init__(self, database):
        self.db = database
        self.runs = database.workflow_runs
        self.runs.create_index([("tenant_id", 1), ("incident_id", 1), ("created_at", -1)])

    def context(self, tenant: str, incident_id: str) -> tuple[dict, list[dict]]:
        document = self.db.incidents.find_one({"_id": incident_id, "tenant_id": tenant})
        if document is None:
            raise LookupError("Incident not found in tenant")
        incident = incident_snapshot(document)
        signals = list(self.db.signals.find({"_id": {"$in": incident["signal_ids"]},
                                            "tenant_id": tenant, "product": incident["product"]}))
        event_ids = sorted({event for signal in signals for event in signal["sample_event_ids"]})
        fields = ("event_id", "tenant_id", "product", "normalized_text", "redaction_status")
        cases = list(self.db.cases.find({"tenant_id": tenant, "product": incident["product"],
                                        "event_id": {"$in": event_ids}}).sort("event_id", 1).limit(20))
        return incident, [{field: case[field] for field in fields} for case in cases]

    def get(self, tenant: str, run_id: str):
        return self.runs.find_one({"_id": run_id, "tenant_id": tenant})

    def create(self, record: dict):
        from pymongo.errors import DuplicateKeyError
        try:
            self.runs.update_one({"_id": record["_id"]}, {"$setOnInsert": record}, upsert=True)
        except DuplicateKeyError:
            pass  # A concurrent creator won; the service checks the stored identity.
        return self.get(record["tenant_id"], record["_id"])

    @contextmanager
    def locked(self, tenant: str, run_id: str):
        owner = uuid4().hex
        result = self.runs.update_one({"_id": run_id, "tenant_id": tenant, "owner": None},
                                      {"$set": {"owner": owner}})
        if result.modified_count != 1:
            record = self.get(tenant, run_id)
            if record is None:
                raise LookupError("Workflow not found in tenant")
            raise Busy(f"Workflow has an active or abandoned writer: {record.get('owner')}")
        try:
            yield owner
        finally:
            self.runs.update_one({"_id": run_id, "tenant_id": tenant, "owner": owner},
                                 {"$set": {"owner": None}})

    def save(self, tenant: str, run_id: str, owner: str, state: dict):
        event = {"status": state["status"], "proposal_hash": state.get("proposal_hash", ""),
                 "decision": state.get("decision"), "reason": state.get("reason"), "revision": state.get("revision")}
        result = self.runs.update_one(
            {"_id": run_id, "tenant_id": tenant, "owner": owner},
            {"$set": {"view": state}, "$addToSet": {"audit": event}},
        )
        if not result.matched_count:
            raise Busy("Workflow writer ownership lost")

    def recover_lock(self, tenant: str, run_id: str, expected_owner: str, actor: str):
        # Maintenance only: caller must stop all writers before releasing a crash lock.
        result = self.runs.update_one(
            {"_id": run_id, "tenant_id": tenant, "owner": expected_owner},
            {"$set": {"owner": None}, "$push": {"audit": {
                "status": "lock_recovered", "actor": actor, "previous_owner": expected_owner,
                "at": datetime.now(timezone.utc).isoformat(),
            }}},
        )
        if result.modified_count != 1:
            raise Conflict("Lock owner changed or workflow not found")
