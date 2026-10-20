"""Failure-boundary tests for atomic journals and idempotent ledger projection."""
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import pytest

from backend.tests.test_workflow import NOW
from backend.worker.store import CaseStore


class IncidentCollection:
    def __init__(self, status):
        self.document = {'_id': 'incident-a', 'tenant_id': 'tenant-a', 'status': status,
                         'version': 1, 'resolved_at': NOW - timedelta(minutes=1)}

    def find_one(self, query):
        if query['tenant_id'] != self.document['tenant_id']:
            return None
        return deepcopy(self.document)

    def update_one(self, query, update):
        for key, expected in query.items():
            actual = self.document.get(key)
            if isinstance(expected, dict):
                matches = actual in expected['$in'] if '$in' in expected else actual < expected['$lt']
            else:
                matches = actual == expected
            if not matches:
                return SimpleNamespace(modified_count=0)
        # Both lifecycle state and journal must be in this same atomic operation.
        assert '$push' in update and 'lifecycle_audit' in update['$push']
        self.document.update(update['$set'])
        self.document['version'] += update['$inc']['version']
        self.document.setdefault('lifecycle_audit', []).append(deepcopy(update['$push']['lifecycle_audit']))
        return SimpleNamespace(modified_count=1)


class Ledger:
    def __init__(self):
        self.events = {}
        self.fail = True

    def update_one(self, query, update, upsert):
        if self.fail:
            raise ConnectionError('ledger unavailable after incident commit')
        self.events.setdefault(query['_id'], deepcopy(update['$setOnInsert']))


@pytest.mark.parametrize('transition', ['reopen', 'resolve'])
def test_replay_repairs_ledger_after_state_commit(transition):
    incidents = IncidentCollection('resolved' if transition == 'reopen' else 'candidate')
    ledger = Ledger()
    store = CaseStore(SimpleNamespace(incidents=incidents, audit_events=ledger))
    def apply():
        if transition == 'reopen':
            return store.reopen_incident('tenant-a', 'incident-a', 'signal-a', NOW)
        return store.resolve_incident('tenant-a', 'incident-a', 1, NOW)
    with pytest.raises(ConnectionError):
        apply()
    assert incidents.document['version'] == 2
    assert len(incidents.document['lifecycle_audit']) == 1
    ledger.fail = False
    for _ in range(2):
        assert apply() is False  # transition is already committed, ledger still repaired
    assert incidents.document['version'] == 2
    assert len(ledger.events) == 1
    assert next(iter(ledger.events.values())) == incidents.document['lifecycle_audit'][0]


def test_projection_does_not_read_another_tenant():
    incidents, ledger = IncidentCollection('candidate'), Ledger()
    ledger.fail = False
    incidents.document['lifecycle_audit'] = [{'_id': 'private'}]
    CaseStore(SimpleNamespace(incidents=incidents, audit_events=ledger)).project_lifecycle_audit('tenant-b', 'incident-a')
    assert not ledger.events
