import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest
from backend.api.main import create_app
from backend.api.dependencies import console
from backend.api.routes.console import ResolveRequest, StartRequest, detail, incident_summary, resolve, start
from backend.tests.test_workflow import setup
from backend.workflow.service import Principal
from backend.workflow.store import Busy, Conflict


def test_incident_list_projection_exposes_only_review_fields():
    row = {'_id': 'incident-a', 'product': 'payments', 'error_code': 'PAY-502',
           'status': 'candidate', 'version': 1, 'peak_count': 150,
           'execution_claim': {'run_id': 'private', 'incident_version': 1},
           'authorization_audit': [{'actor': 'private'}]}
    summary = incident_summary(row)
    assert summary['action_authorized'] is True
    assert 'execution_claim' not in summary and 'authorization_audit' not in summary


def test_detail_displays_the_workflow_that_owns_the_authorized_action():
    class Cursor(list):
        def sort(self, *_args):
            return self
        def limit(self, count):
            return Cursor(self[:count])

    claim = {'run_id': 'original', 'incident_version': 1}
    class Incidents:
        def find_one(self, _query, _projection=None):
            return {'version': 1, 'execution_claim': claim}

    class Runs:
        def find_one(self, query, _projection):
            return {'_id': 'original', 'action': {'status': 'succeeded'}, 'audit': []} if query.get('_id') == 'original' else None
        def find(self, _query, _projection):
            return Cursor([{'_id': 'newer'}])

    incident = {'incident_id': 'incident-a', 'tenant_id': 'tenant-a',
                'product': 'payments', 'signal_ids': [], 'version': 1, 'status': 'candidate'}
    service = SimpleNamespace(store=SimpleNamespace(context=lambda *_args: (incident, [])),
                              status=lambda _actor, run_id: {'run_id': run_id, 'status': 'approved'})
    empty = SimpleNamespace(find=lambda *_args, **_kwargs: Cursor([]))
    database = SimpleNamespace(incidents=Incidents(), workflow_runs=Runs(), cases=empty,
                               signals=empty, audit_events=empty)
    result = detail('incident-a', actor=Principal('tenant-a', 'reviewer', 'reviewer'),
                    resources=(service, database))
    assert result['workflow']['_id'] == 'original'
    assert result['workflow']['action']['status'] == 'succeeded'
    assert result['action_authorized'] is True


def test_detail_uses_saved_view_while_action_writer_is_active():
    class Cursor(list):
        def sort(self, *_args):
            return self
        def limit(self, count):
            return Cursor(self[:count])

    incident = {'incident_id': 'incident-a', 'tenant_id': 'tenant-a',
                'product': 'payments', 'signal_ids': [], 'version': 1, 'status': 'candidate'}
    empty = SimpleNamespace(find=lambda *_args, **_kwargs: Cursor([]))
    database = SimpleNamespace(
        incidents=SimpleNamespace(find_one=lambda *_args, **_kwargs: {}),
        cases=empty, signals=empty, audit_events=empty,
        workflow_runs=SimpleNamespace(find=lambda *_args, **_kwargs: Cursor([
            {'_id': 'active', 'view': {'run_id': 'active', 'status': 'approved'}}])))
    def busy_status(*_args):
        raise Busy('writer active')
    service = SimpleNamespace(store=SimpleNamespace(context=lambda *_args: (incident, [])),
                              status=busy_status)
    result = detail('incident-a', actor=Principal('tenant-a', 'reviewer', 'reviewer'),
                    resources=(service, database))
    assert result['workflow']['view']['status'] == 'approved'


def test_reopened_revision_does_not_display_prior_action_as_current_work():
    class Cursor(list):
        def sort(self, *_args):
            return self
        def limit(self, count):
            return Cursor(self[:count])

    incident = {'incident_id': 'incident-a', 'tenant_id': 'tenant-a', 'product': 'payments',
                'signal_ids': [], 'version': 2, 'status': 'reopened'}
    empty = SimpleNamespace(find=lambda *_args, **_kwargs: Cursor([]))
    searches = []
    def workflows(query, _fields):
        searches.append(query)
        return Cursor([])
    database = SimpleNamespace(
        incidents=SimpleNamespace(find_one=lambda *_args, **_kwargs: {
            'execution_claim': {'run_id': 'old-run', 'incident_version': 1}}),
        cases=empty, signals=empty, audit_events=empty,
        workflow_runs=SimpleNamespace(find=workflows, find_one=lambda *_args: None))
    service = SimpleNamespace(store=SimpleNamespace(context=lambda *_args: (incident, [])))
    result = detail('incident-a', actor=Principal('tenant-a', 'reviewer', 'reviewer'),
                    resources=(service, database))
    assert result['action_authorized'] is False and result['workflow'] is None
    assert searches[0]['initial.incident.version'] == 2


def test_new_workflow_is_blocked_after_action_claim_but_same_key_can_retry():
    from backend.workflow.models import digest
    key = 'original-key'
    claim = {'run_id': 'wf-' + digest(['tenant-a', key])[:32], 'incident_version': 1}
    database = SimpleNamespace(incidents=SimpleNamespace(
        find_one=lambda *_args, **_kwargs: {'version': 1, 'execution_claim': claim}))
    calls = []
    service = SimpleNamespace(start=lambda *_args, **_kwargs: calls.append(True) or {'status': 'approved'})
    actor = Principal('tenant-a', 'reviewer', 'reviewer')
    with pytest.raises(Conflict, match='already authorized'):
        start('incident-a', StartRequest(expected_version=1), key='new-key',
              actor=actor, resources=(service, database))
    assert not calls
    assert start('incident-a', StartRequest(expected_version=1), key=key,
                 actor=actor, resources=(service, database))['status'] == 'approved'


def client(monkeypatch):
    registry = {}
    for token, tenant, role in [('operator-token', 'tenant-a', 'operator'),
                                 ('reviewer-token', 'tenant-a', 'reviewer'),
                                 ('foreign-token', 'tenant-b', 'reviewer')]:
        registry[hashlib.sha256(token.encode()).hexdigest()] = {
            'tenant_id': tenant, 'actor_id': role, 'role': role,
            'expires_at': (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()}
    monkeypatch.setenv('ECHOGUARD_AUTH_JSON', json.dumps(registry))
    service, *_ = setup()
    app = create_app()
    app.dependency_overrides[console] = lambda: (service, None)
    return TestClient(app)


def headers(token='operator-token'):
    return {'Authorization': 'Bearer ' + token, 'Idempotency-Key': 'request-1'}


def test_authenticated_review_and_tenant_boundaries(monkeypatch):
    with client(monkeypatch) as http:
        assert http.get('/me').status_code == 401
        assert http.get('/me', headers=headers('bad')).status_code == 401
        assert http.get('/me', headers={**headers(), 'X-Tenant-ID': 'tenant-b'}).json()['tenant_id'] == 'tenant-a'
        path = '/incidents/incident-a/workflows'
        assert http.post(path, headers=headers(), json={'expected_version': 2}).status_code == 409
        assert http.post(path, headers=headers(), json={'expected_version': 1, 'tenant_id': 'tenant-b'}).status_code == 422
        result = http.post(path, headers=headers(), json={'expected_version': 1})
        assert result.status_code == 200
        state = result.json()
        run = '/workflows/' + state['run_id']
        assert http.get(run, headers=headers('foreign-token')).status_code == 404
        body = {'expected_version': 1, 'proposal_hash': state['proposal_hash'], 'decision': 'approve'}
        assert http.post(run + '/decision', headers=headers(), json=body).status_code == 403
        body['proposal_hash'] = '0' * 64
        assert http.post(run + '/decision', headers=headers('reviewer-token'), json=body).status_code == 409
        body['proposal_hash'] = state['proposal_hash']
        for _ in range(2):
            response = http.post(run + '/decision', headers=headers('reviewer-token'), json=body)
            assert response.status_code == 200
            assert response.json()['status'] == 'approved'


def test_workflow_start_retry_keeps_original_incident_version(monkeypatch):
    with client(monkeypatch) as http:
        path = '/incidents/incident-a/workflows'
        first = http.post(path, headers=headers(), json={'expected_version': 1})
        assert first.status_code == 200
        service, _ = http.app.dependency_overrides[console]()
        service.store.incident['version'] = 2
        retry = http.post(path, headers=headers(), json={'expected_version': 1})
        assert retry.status_code == 200
        assert retry.json()['run_id'] == first.json()['run_id']
        assert http.post(path, headers=headers(), json={'expected_version': 2}).status_code == 409
        assert http.post(path, headers={**headers(), 'Idempotency-Key': 'new-request'},
                         json={'expected_version': 1}).status_code == 409


def test_closure_key_cannot_be_reused_with_changed_hash():
    class Incidents:
        def find_one(self, _query):
            return {'status': 'resolved', 'resolution_key': 'closure-1',
                    'resolution_expected_version': 1, 'resolution_incident_hash': 'a' * 64}

    database = SimpleNamespace(incidents=Incidents())
    reviewer = Principal('tenant-a', 'reviewer', 'reviewer')
    with pytest.raises(Conflict, match='different closure request'):
        resolve('incident-a', ResolveRequest(expected_version=1, incident_hash='b' * 64),
                key='closure-1', actor=reviewer, resources=(None, database))


def test_bad_registry_fails_closed(monkeypatch):
    http = client(monkeypatch)
    monkeypatch.setenv('ECHOGUARD_AUTH_JSON', 'invalid-json')
    assert http.get('/me', headers=headers()).status_code == 503


def test_expired_credentials_are_rejected(monkeypatch):
    http = client(monkeypatch)
    monkeypatch.setenv('ECHOGUARD_AUTH_JSON', json.dumps({
        hashlib.sha256(b'operator-token').hexdigest(): {
            'tenant_id': 'tenant-a', 'actor_id': 'operator', 'role': 'operator',
            'expires_at': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}}))
    assert http.get('/me', headers=headers()).status_code == 401
