"""Authenticated incident review, workflow and simulated action APIs."""
import asyncio
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from backend.actions.service import ActionService, MongoActionRepository, SimulatedAdapter, outcome
from backend.worker.store import CaseStore
from backend.workflow.models import digest
from backend.workflow.store import Busy, Conflict
from .demo import local_demo_enabled, require_demo_recovery, window_seconds
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


def incident_summary(document: dict) -> dict:
    claim = document.get('execution_claim')
    authorized = bool(isinstance(claim, dict)
                      and claim.get('incident_version') == document['version'])
    result = {field: document.get(field) for field in (
        '_id', 'product', 'error_code', 'status', 'version', 'peak_count', 'last_seen')}
    result['action_authorized'] = authorized
    return result


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
    fields = {'_id': 1, 'product': 1, 'error_code': 1, 'status': 1, 'version': 1,
              'peak_count': 1, 'last_seen': 1, 'execution_claim.incident_version': 1}
    items = list(db.incidents.find(scope, fields).sort("_id", 1).limit(limit + 1))
    return {"items": [incident_summary(item) for item in items[:limit]],
            "next_cursor": items[limit-1]["_id"] if len(items) > limit else None}


@router.get("/incidents/{incident_id}")
def detail(incident_id: str, actor=Depends(principal), resources=Depends(console)):
    service, db = resources
    incident, evidence = service.store.context(actor.tenant_id, incident_id)
    raw = db.incidents.find_one({'_id': incident_id, 'tenant_id': actor.tenant_id},
                                {'execution_claim': 1})
    claim = raw.get('execution_claim') if raw else None
    authorized_run_id = claim.get('run_id') if isinstance(claim, dict) else None
    action_authorized = bool(isinstance(claim, dict)
                             and claim.get('incident_version') == incident['version'])
    event_ids = [item['event_id'] for item in evidence]
    cases = {item['event_id']: item for item in db.cases.find({
        'tenant_id': actor.tenant_id, 'product': incident['product'],
        'event_id': {'$in': event_ids}}, {'event_id': 1, 'channel': 1, 'event_time': 1})}
    evidence = [{**item, 'channel': cases.get(item['event_id'], {}).get('channel'),
                 'event_time': cases.get(item['event_id'], {}).get('event_time')}
                for item in evidence]
    signals = list(db.signals.find({'_id': {'$in': incident['signal_ids']},
                                    'tenant_id': actor.tenant_id},
                                   {'signal_id': 1, 'window_start': 1, 'window_end': 1,
                                    'current_count': 1, 'distinct_customers': 1})
                   .sort('window_start', 1))
    lifecycle = list(db.audit_events.find({'tenant_id': actor.tenant_id,
                                           'subject_id': incident_id},
                                          {'kind': 1, 'event_time': 1, 'actor': 1})
                     .sort('event_time', 1))
    workflow_scope = {'tenant_id': actor.tenant_id, 'incident_id': incident_id}
    workflow_fields = {'_id': 1, 'view': 1, 'action': 1, 'audit': 1}
    # A resolved incident displays the response that closed it. A reopened
    # revision must display only investigations for that new revision.
    if incident['status'] != 'resolved':
        workflow_scope['initial.incident.version'] = incident['version']
    latest = (db.workflow_runs.find_one({**workflow_scope, '_id': authorized_run_id}, workflow_fields)
              if authorized_run_id and (action_authorized or incident['status'] == 'resolved') else None)
    if latest is None:
        latest = next(iter(db.workflow_runs.find(workflow_scope, workflow_fields)
                           .sort('created_at', -1).limit(1)), None)
    if latest:
        try:
            latest["view"] = service.status(actor, latest["_id"])
        except Busy:
            # A concurrent action owns the graph writer. The last stored view
            # remains readable until the next live refresh.
            if not latest.get('view'):
                raise
    return {"incident": incident, "incident_hash": digest(incident), "evidence": evidence,
            "signals": signals, "lifecycle": lifecycle, "workflow": latest,
            "action_authorized": action_authorized}


@router.post("/incidents/{incident_id}/workflows")
def start(incident_id: str, body: StartRequest, key=Depends(request_key),
          actor=Depends(principal), resources=Depends(console)):
    service, db = resources
    if db is not None:
        raw = db.incidents.find_one({'_id': incident_id, 'tenant_id': actor.tenant_id},
                                    {'version': 1, 'execution_claim': 1})
        if raw:
            claim = raw.get('execution_claim')
            requested_run_id = 'wf-' + digest([actor.tenant_id, key])[:32]
            if (isinstance(claim, dict) and claim.get('incident_version') == raw['version']
                    and claim.get('run_id') != requested_run_id):
                raise Conflict('An action is already authorized for this incident revision')
    return service.start(actor, incident_id, key, expected_version=body.expected_version)


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
    return outcome(db, actor.tenant_id, record["initial"]["incident"], record.get("action"),
                   window_seconds=window_seconds())


@router.post("/incidents/{incident_id}/resolve")
def resolve(incident_id: str, body: ResolveRequest, key=Depends(request_key),
            actor=Depends(principal), resources=Depends(console)):
    if actor.role != "reviewer":
        raise PermissionError("Reviewer role required")
    service, db = resources
    case_store = CaseStore(db)
    raw = db.incidents.find_one({"_id": incident_id, "tenant_id": actor.tenant_id})
    if raw and raw.get("resolution_key") == key and raw["status"] == "resolved":
        if (raw.get("resolution_expected_version") != body.expected_version
                or raw.get("resolution_incident_hash") != body.incident_hash):
            raise Conflict("Request key already belongs to a different closure request")
        case_store.project_lifecycle_audit(actor.tenant_id, incident_id)
        return {"status": "resolved", "incident_id": incident_id}
    current, _ = service.store.context(actor.tenant_id, incident_id)
    if current["version"] != body.expected_version or digest(current) != body.incident_hash:
        raise Conflict("Incident changed")
    action = db.workflow_runs.find_one({"tenant_id": actor.tenant_id, "incident_id": incident_id,
                                       "action.status": "succeeded",
                                       "initial.incident.version": current["version"]})
    if not action:
        raise Conflict("A completed simulated action is required before manual closure")
    if local_demo_enabled():
        require_demo_recovery(db, actor.tenant_id, current, action)
    if current["status"] == "resolved":
        raise Conflict("Incident already resolved")
    if current["status"] not in {"candidate", "reopened"}:
        raise Conflict("Incident is not eligible for closure")
    closed_at = datetime.now(timezone.utc)
    lifecycle_event = {"_id": f"audit-{incident_id}-resolved-v{body.expected_version}",
                       "tenant_id": actor.tenant_id, "subject_id": incident_id,
                       "kind": "incident_resolved", "event_time": closed_at,
                       "actor": actor.actor_id, "request_key": key}
    # Status, version, human audit, and the repairable ledger journal are one
    # incident-document update. A retry repairs the separate ledger projection.
    result = db.incidents.update_one({"_id": incident_id, "tenant_id": actor.tenant_id,
                                      "version": body.expected_version, "status": current["status"],
                                      "peak_count": current["peak_count"], "last_seen": raw["last_seen"],
                                      "signal_ids": raw["signal_ids"]},
        {"$set": {"status": "resolved", "resolved_at": closed_at, "resolution_key": key,
                  "resolution_expected_version": body.expected_version,
                  "resolution_incident_hash": body.incident_hash},
         "$inc": {"version": 1}, "$push": {"lifecycle_audit": lifecycle_event, "review_audit": {
             "actor": actor.actor_id, "decision": "resolved", "request_key": key,
             "at": closed_at.isoformat()}}})
    if not result.modified_count:
        raise Conflict("Incident changed during closure")
    case_store.project_lifecycle_audit(actor.tenant_id, incident_id)
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
