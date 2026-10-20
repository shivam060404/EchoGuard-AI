import copy
from datetime import timedelta
from types import SimpleNamespace

import pytest
from backend.actions.service import ActionService, outcome
from backend.workflow.store import Conflict
from backend.tests.test_workflow import setup, NOW, OPERATOR, REVIEWER, INCIDENT


class Repository:
    action = None
    fail_complete = False

    def get(self, *args):
        return copy.deepcopy(self.action)

    def pending(self, *args):
        self.action = copy.deepcopy(args[-1])

    def complete(self, *args):
        if self.fail_complete:
            raise ConnectionError('interrupted after effect')
        self.pending(*args)


class Adapter:
    def __init__(self):
        self.effects = {}

    def perform(self, tenant, action, state):
        self.effects.setdefault(action['action_id'], {
            'result': {'reference': action['action_id'], 'simulated': True},
            'completed_at': NOW.isoformat()})
        return self.lookup(tenant, action)

    def lookup(self, tenant, action):
        return self.effects.get(action['action_id'])


def prepared(approve=True):
    workflow, store, retriever, _ = setup()
    state = workflow.start(OPERATOR, 'incident-a', 'action-test')
    if approve:
        state = workflow.decide(REVIEWER, state['run_id'], 'approve', state['proposal_hash'])
    repo, adapter = Repository(), Adapter()
    service = ActionService(workflow, repo, adapter, lambda: NOW)
    return service, state, store, retriever, repo, adapter


def execute(service, state, actor=REVIEWER):
    return service.execute(actor, state['run_id'], state['proposal_hash'], 1, 'execute-key')


def test_crash_after_effect_retries_without_duplicate():
    service, state, _, _, repo, adapter = prepared()
    repo.fail_complete = True
    with pytest.raises(ConnectionError):
        execute(service, state)
    assert repo.action['status'] == 'pending'
    repo.fail_complete = False
    result = execute(service, state)
    assert result['status'] == 'succeeded'
    assert execute(service, state) == result
    assert len(adapter.effects) == 1


@pytest.mark.parametrize('fault', ['unapproved', 'role', 'hash', 'version', 'stale', 'expired', 'revoked'])
def test_action_boundaries(fault):
    service, state, store, retriever, repo, adapter = prepared(fault != 'unapproved')
    if fault == 'hash': state['proposal_hash'] = '0' * 64
    if fault == 'stale': store.incident['peak_count'] += 1
    if fault == 'expired': service.clock = lambda: NOW + timedelta(days=2)
    if fault == 'revoked': retriever.hits = []
    with pytest.raises((Conflict, PermissionError)):
        service.execute(OPERATOR if fault == 'role' else REVIEWER, state['run_id'],
                        state['proposal_hash'], 2 if fault == 'version' else 1, 'key')
    assert not adapter.effects and repo.action is None


@pytest.mark.parametrize('progress,before,after,expected', [
    (False, 10, 0, 'insufficient_data'), (True, 0, 0, 'insufficient_data'),
    (True, 10, 2, 'improving'), (True, 10, 10, 'unchanged'), (True, 10, 20, 'worsening'),
])
def test_outcome_requires_baseline_and_stream_progress(progress, before, after, expected):
    counts = iter([before, after])
    class Cases:
        def count_documents(self, query):
            assert query['tenant_id'] == 'tenant-a'
            assert query['error_code'] == 'PAY-502'
            return next(counts)
        def find_one(self, query, projection):
            assert query['tenant_id'] == 'tenant-a'
            return {'_id': 'later'} if progress else None
    result = outcome(SimpleNamespace(cases=Cases()), 'tenant-a', INCIDENT,
                     {'status': 'succeeded', 'completed_at': NOW.isoformat()}, NOW + timedelta(minutes=6))
    assert result['status'] == expected


@pytest.mark.parametrize('fault', ['expired', 'stale', 'revoked'])
def test_existing_effect_reconciles_without_reauthorization(fault):
    service, state, store, retriever, repo, adapter = prepared()
    repo.fail_complete = True
    with pytest.raises(ConnectionError):
        execute(service, state)
    repo.fail_complete = False
    if fault == 'expired': service.clock = lambda: NOW + timedelta(days=2)
    if fault == 'stale': store.incident['peak_count'] += 1
    if fault == 'revoked': retriever.hits = []
    result = execute(service, state)
    assert result['status'] == 'succeeded'
    assert result['completed_at'] == NOW.isoformat()
    assert len(adapter.effects) == 1


def test_pending_without_effect_does_not_bypass_expiry():
    service, state, _, _, repo, adapter = prepared()
    repo.action = {'action_id': 'missing', 'status': 'pending'}
    service.clock = lambda: NOW + timedelta(days=2)
    with pytest.raises(Conflict, match='expired'):
        execute(service, state)
    assert not adapter.effects
