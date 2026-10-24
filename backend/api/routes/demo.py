"""Disabled-by-default traffic controls for an isolated local workspace."""
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from backend.demo.traffic import build_events, publish_events, traffic_record_id
from backend.actions.service import outcome
from backend.workflow.models import digest
from backend.workflow.store import Conflict
from ..auth import principal
from ..dependencies import console

router = APIRouter(prefix='/demo', tags=['local demonstration'])


def window_seconds() -> int:
    value = int(os.getenv('ECHOGUARD_OUTCOME_WINDOW_SECONDS', '300'))
    if not 30 <= value <= 3600:
        raise ValueError('Outcome window must be between 30 and 3600 seconds')
    return value


def local_demo_enabled() -> bool:
    return os.getenv('ECHOGUARD_LOCAL_DEMO', 'false').lower() == 'true'


def require_local_demo() -> None:
    if not local_demo_enabled():
        raise HTTPException(404, 'Local demo controls are disabled')


def require_demo_recovery(database, tenant_id: str, incident: dict, action_record: dict) -> None:
    if digest(action_record['initial']['incident']) != digest(incident):
        raise Conflict('Incident changed since the demo action; investigate again')
    observation = outcome(database, tenant_id, incident, action_record['action'],
                          window_seconds=window_seconds())
    if observation['status'] != 'improving':
        raise Conflict('Wait for an improving observed trend before closing this demo incident')


class TrafficRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    incident_id: str
    kind: Literal['baseline', 'recovery']


@router.get('/status')
def status(actor=Depends(principal), resources=Depends(console)):
    require_local_demo()
    _, database = resources
    scope = {'tenant_id': actor.tenant_id}
    return {'mode': 'local', 'tenant_id': actor.tenant_id,
            'outcome_window_seconds': window_seconds(),
            'cases': database.cases.count_documents(scope),
            'incidents': database.incidents.count_documents(scope),
            'knowledge_documents': database.knowledge_documents.count_documents(scope)}


@router.get('/traffic')
def traffic_state(incident_id: str = Query(min_length=1, max_length=128),
                  actor=Depends(principal), resources=Depends(console)):
    require_local_demo()
    _, database = resources
    if database.incidents.find_one({'_id': incident_id, 'tenant_id': actor.tenant_id}) is None:
        raise LookupError('Incident not found in tenant')
    result = {}
    for kind in ('baseline', 'recovery'):
        record = database.demo_traffic.find_one({'tenant_id': actor.tenant_id,
                                                 'incident_id': incident_id, 'kind': kind},
                                                sort=[('started_at', -1)])
        if record is None:
            continue
        event_ids = [json.loads(value)['event_id'] for _, value in build_events(record)]
        count = database.cases.count_documents({'tenant_id': actor.tenant_id,
                                                'event_id': {'$in': event_ids}})
        result[kind] = {'kind': kind, 'status': 'ready' if count == len(event_ids) else 'processing',
                        'projected_cases': count, 'expected_cases': len(event_ids),
                        'started_at': record['started_at']}
    return result


@router.post('/traffic')
def traffic(body: TrafficRequest, idempotency_key: str = Header(min_length=1, max_length=128),
            actor=Depends(principal), resources=Depends(console)):
    require_local_demo()
    if actor.role != 'reviewer':
        raise PermissionError('Reviewer role required')
    _, database = resources
    incident = database.incidents.find_one({'_id': body.incident_id, 'tenant_id': actor.tenant_id})
    if incident is None:
        raise LookupError('Incident not found in tenant')
    identifier = traffic_record_id(actor.tenant_id, idempotency_key)
    existing = database.demo_traffic.find_one({'_id': identifier, 'tenant_id': actor.tenant_id})
    if existing and (existing['incident_id'] != body.incident_id or existing['kind'] != body.kind):
        raise Conflict('Request key already belongs to different demo traffic')
    if body.kind == 'recovery' and existing is None:
        action = database.workflow_runs.find_one({
            'tenant_id': actor.tenant_id, 'incident_id': body.incident_id,
            'action.status': 'succeeded'}, {'action': 1}, sort=[('created_at', -1)])
        if action is None:
            raise Conflict('Execute an approved simulated action before recovery traffic')
        eligible = datetime.fromisoformat(action['action']['completed_at']) + timedelta(seconds=window_seconds())
        if datetime.now(timezone.utc) < eligible:
            raise Conflict('Wait for the outcome observation window before recovery traffic')
    product = incident['product'] if body.kind == 'baseline' else 'account'
    error_code = incident.get('error_code') if body.kind == 'baseline' else None
    database.demo_traffic.update_one({'_id': identifier}, {'$setOnInsert': {
        '_id': identifier, 'tenant_id': actor.tenant_id, 'incident_id': body.incident_id,
        'kind': body.kind, 'product': product, 'error_code': error_code,
        'started_at': datetime.now(timezone.utc), 'status': 'prepared',
    }}, upsert=True)
    record = database.demo_traffic.find_one({'_id': identifier, 'tenant_id': actor.tenant_id})
    if (record is None or record['incident_id'] != body.incident_id
            or record['kind'] != body.kind):
        raise Conflict('Request key already belongs to different demo traffic')
    events = build_events(record)
    if record['status'] != 'published':
        bootstrap = f"{os.getenv('ECHOGUARD_KAFKA_HOST', 'kafka')}:{os.getenv('ECHOGUARD_KAFKA_PORT', '19092')}"
        try:
            publish_events(bootstrap, events)
        except Exception as exc:
            raise HTTPException(503, 'Demo traffic publication failed; retry with the same key') from exc
        database.demo_traffic.update_one({'_id': identifier, 'tenant_id': actor.tenant_id},
                                         {'$set': {'status': 'published'}})
    event_ids = [json.loads(value)['event_id'] for _, value in events]
    deadline = time.monotonic() + 18
    count = 0
    while time.monotonic() < deadline:
        count = database.cases.count_documents({'tenant_id': actor.tenant_id,
                                                 'event_id': {'$in': event_ids}})
        if count == len(event_ids):
            break
        time.sleep(1)
    return {'status': 'ready' if count == len(event_ids) else 'processing',
            'kind': body.kind, 'projected_cases': count, 'expected_cases': len(event_ids),
            'started_at': record['started_at']}
