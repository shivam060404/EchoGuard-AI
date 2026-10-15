import hashlib
import json
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from backend.api.main import create_app
from backend.api.dependencies import console
from backend.tests.test_workflow import setup


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
