"""Authenticated incident review, workflow and simulated action APIs."""
import asyncio
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from backend.actions.service import ActionService, MongoActionRepository, SimulatedAdapter, outcome
from backend.workflow.models import digest
from backend.workflow.store import Conflict
from ..auth import principal
from ..dependencies import console

router = APIRouter(tags=["incident review"])


class StartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_version: int = Field(ge=1)


class ReviewRequest(StartRequest):
    proposal_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    decision: Literal["approve", "reject"]


class ExecuteRequest(StartRequest):
    proposal_hash: str = Field(pattern=r"^[a-f0-9]{64}$")


class RevisionRequest(ExecuteRequest):
    action: Literal["create_ticket", "post_internal_notification"]
    step_indices: list[int] = Field(min_length=1, max_length=5)


class ResolveRequest(StartRequest):
    incident_hash: str = Field(pattern=r"^[a-f0-9]{64}$")


def request_key(idempotency_key: str = Header(min_length=1, max_length=128)):
    return idempotency_key


@router.get("/me")
def me(actor=Depends(principal)):
    return actor


@router.get("/incidents")
def incidents(actor=Depends(principal), resources=Depends(console),
              limit: int = Query(50, ge=1, le=100), after: str = Query("", max_length=128)):
    _, db = resources
    scope = {"tenant_id": actor.tenant_id}
    if after:
        scope["_id"] = {"$gt": after}
    items = list(db.incidents.find(scope).sort("_id", 1).limit(limit + 1))
    return {"items": items[:limit], "next_cursor": items[limit-1]["_id"] if len(items) > limit else None}


@router.get("/incidents/{incident_id}")
def detail(incident_id: str, actor=Depends(principal), resources=Depends(console)):
    service, db = resources
    incident, evidence = service.store.context(actor.tenant_id, incident_id)
    latest = next(iter(db.workflow_runs.find({"tenant_id": actor.tenant_id, "incident_id": incident_id},
                                            {"_id": 1, "view": 1, "action": 1, "audit": 1})
                       .sort("created_at", -1).limit(1)), None)
    if latest:
        latest["view"] = service.status(actor, latest["_id"])
    return {"incident": incident, "incident_hash": digest(incident), "evidence": evidence, "workflow": latest}


@router.post("/incidents/{incident_id}/workflows")
def start(incident_id: str, body: StartRequest, key=Depends(request_key),
          actor=Depends(principal), resources=Depends(console)):
    service, _ = resources
    current, _ = service.store.context(actor.tenant_id, incident_id)
    if current["version"] != body.expected_version:
        raise Conflict("Incident version changed")
    return service.start(actor, incident_id, key)


@router.get("/workflows/{run_id}")
def workflow(run_id: str, actor=Depends(principal), resources=Depends(console)):
    service, _ = resources
    state = service.status(actor, run_id)
    record = service.store.get(actor.tenant_id, run_id)
    return {"state": state, "audit": record.get("audit", []), "action": record.get("action")}


@router.post("/workflows/{run_id}/decision")
def review(run_id: str, body: ReviewRequest, key=Depends(request_key),
           actor=Depends(principal), resources=Depends(console)):
    service, _ = resources
    state = service.status(actor, run_id)
    if state["incident"]["version"] != body.expected_version:
        raise Conflict("Proposal incident version changed")
    return service.decide(actor, run_id, body.decision, body.proposal_hash)


@router.post("/workflows/{run_id}/action")
def execute(run_id: str, body: ExecuteRequest, key=Depends(request_key),
            actor=Depends(principal), resources=Depends(console)):
    service, db = resources
    return ActionService(service, MongoActionRepository(db), SimulatedAdapter(db)).execute(
        actor, run_id, body.proposal_hash, body.expected_version, key)


@router.get("/workflows/{run_id}/outcome")
def monitor(run_id: str, actor=Depends(principal), resources=Depends(console)):
    service, db = resources
    record = service._record(actor, run_id)
    return outcome(db, actor.tenant_id, record["initial"]["incident"], record.get("action"))


@router.post("/incidents/{incident_id}/resolve")
def resolve(incident_id: str, body: ResolveRequest, key=Depends(request_key),
            actor=Depends(principal), resources=Depends(console)):
    if actor.role != "reviewer":
        raise PermissionError("Reviewer role required")
    service, db = resources
    raw = db.incidents.find_one({"_id": incident_id, "tenant_id": actor.tenant_id})
    if raw and raw.get("resolution_key") == key and raw["status"] == "resolved":
        return {"status": "resolved", "incident_id": incident_id}
    current, _ = service.store.context(actor.tenant_id, incident_id)
    if current["version"] != body.expected_version or digest(current) != body.incident_hash:
        raise Conflict("Incident changed")
    action = db.workflow_runs.find_one({"tenant_id": actor.tenant_id, "incident_id": incident_id,
                                       "action.status": "succeeded",
                                       "initial.incident.version": current["version"]})
    if not action:
        raise Conflict("A completed simulated action is required before manual closure")
    if current["status"] == "resolved":
        raise Conflict("Incident already resolved")
    # The same atomic document update applies closure, version and the human audit entry.
    result = db.incidents.update_one({"_id": incident_id, "tenant_id": actor.tenant_id,
                                      "version": body.expected_version, "status": current["status"],
                                      "peak_count": current["peak_count"], "last_seen": raw["last_seen"],
                                      "signal_ids": raw["signal_ids"]},
        {"$set": {"status": "resolved", "resolved_at": datetime.now(timezone.utc), "resolution_key": key},
         "$inc": {"version": 1}, "$push": {"review_audit": {
             "actor": actor.actor_id, "decision": "resolved", "request_key": key,
             "at": datetime.now(timezone.utc).isoformat()}}})
    if not result.modified_count:
        raise Conflict("Incident changed during closure")
    return {"status": "resolved", "incident_id": incident_id}


@router.get("/events")
async def events(request: Request, actor=Depends(principal)):
    async def updates():
        # Invalidation stream, not an audit log. Reconnect + GET recovers missed notifications.
        for _ in range(6):
            if await request.is_disconnected():
                break
            yield "event: refresh\ndata: {}\n\n"
            await asyncio.sleep(5)
    return StreamingResponse(updates(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


@router.post("/workflows/{run_id}/proposal")
def revise(run_id: str, body: RevisionRequest, key=Depends(request_key),
           actor=Depends(principal), resources=Depends(console)):
    service, _ = resources
    state = service.status(actor, run_id)
    if state["incident"]["version"] != body.expected_version:
        raise Conflict("Proposal incident version changed")
    return service.revise(actor, run_id, body.proposal_hash, body.action, body.step_indices, key)
