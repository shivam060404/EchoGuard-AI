"""The local demo controls remain synthetic, scoped and replay-safe."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.api.routes import demo
from backend.demo.traffic import build_events, traffic_record_id
from backend.demo import local
from backend.demo.verify import select_incident
from backend.knowledge.seed import load_sources, sources_for_tenant
from backend.workflow.service import Principal
from backend.workflow.store import Conflict


def test_acceptance_uses_remaining_seeded_incident_and_reports_exhaustion():
    items = [
        {'_id': 'payment', 'error_code': 'PAY-502', 'status': 'resolved'},
        {'_id': 'login', 'error_code': 'AUTH-401', 'status': 'candidate'},
    ]
    assert select_incident(items)['_id'] == 'login'
    items[1]['execution_claim'] = {'incident_version': 1, 'run_id': 'prior-run'}
    items[1]['version'] = 1
    with pytest.raises(RuntimeError, match='Start a fresh isolated workspace'):
        select_incident(items)
    items[1]['version'] = 2
    assert select_incident(items)['_id'] == 'login'
    items[1]['action_authorized'] = True
    with pytest.raises(RuntimeError, match='Start a fresh isolated workspace'):
        select_incident(items)


def test_demo_events_are_valid_and_stable():
    record = {'_id': 'traffic-a', 'tenant_id': 'faculty-a', 'kind': 'baseline',
              'product': 'payments', 'error_code': 'PAY-502',
              'started_at': datetime(2026, 10, 23, tzinfo=timezone.utc)}
    events = build_events(record)
    assert len(events) == 12
    assert build_events(record) == events
    assert len({json.loads(value)['event_id'] for _, value in events}) == 12
    assert all(key.startswith(b'faculty-a:') for key, _ in events)
    record['kind'] = 'recovery'
    record['product'] = 'account'
    record['error_code'] = None
    assert len(build_events(record)) == 3


def test_faculty_knowledge_seed_is_isolated_to_new_tenant():
    original = load_sources()
    cloned = sources_for_tenant('faculty-a', original)
    assert sum(item['tenant_id'] == 'faculty-a' for item in cloned) == 5
    assert sum(item['tenant_id'] == 'another-tenant' for item in cloned) == 1
    assert sum(item['tenant_id'] == 'demo-tenant' for item in original) == 5


def test_launcher_wires_isolated_project_and_waits_for_projection(monkeypatch, tmp_path):
    monkeypatch.setattr(local, 'ROOT', tmp_path)
    monkeypatch.setattr(local.subprocess, 'run', lambda *_args, **_kwargs: None)
    ports = iter((31001, 8101, 9101, 27101))
    monkeypatch.setattr(local, 'available_port', lambda _reserved: next(ports))
    calls = []
    monkeypatch.setattr(local, 'compose', lambda project, environment, *args:
                        calls.append((project, environment, args)))
    monkeypatch.setattr(local, 'incident_list', lambda *_args: [
        {'_id': 'payment', 'error_code': 'PAY-502', 'peak_count': 150},
        {'_id': 'login', 'error_code': 'AUTH-401', 'peak_count': 40}])
    monkeypatch.setattr(local, 'demo_status', lambda *_args: {
        'cases': 250, 'knowledge_documents': 5})
    monkeypatch.setattr(local, 'require_authentication', lambda *_args: {'status': 'verified'})
    local.launch()
    project = calls[0][0]
    assert project.startswith('echoguard-local-')
    assert all(item[0] == project for item in calls)
    assert calls[0][2][:4] == ('up', '-d', '--build', '--wait')
    assert calls[1][2] == ('build', 'knowledge-tool', 'producer')
    assert calls[2][2][-2:] == ('--tenant', f'local-{project.rsplit("-", 1)[-1]}')
    assert calls[3][2][2] == 'producer' and calls[3][2][3] == 'publish'
    assert calls[4][2][3] == 'verify'
    environment = calls[0][1]
    assert environment['ECHOGUARD_LOCAL_DEMO'] == 'true'
    assert environment['ECHOGUARD_OUTCOME_WINDOW_SECONDS'] == '60'
    assert environment['CONSOLE_ORIGIN'] == 'http://localhost:31001'
    assert environment['ECHOGUARD_KAFKA_PORT'] == '9101'
    report = json.loads((tmp_path / 'data/local_demo' / project / 'run.json').read_text())
    assert report['incidents'] == {'PAY-502': 'payment', 'AUTH-401': 'login'}


def test_saved_demo_registry_and_repair_ports_are_stable(tmp_path, monkeypatch):
    monkeypatch.setattr(local, 'ROOT', tmp_path)
    project = 'echoguard-faculty-0123456789'
    tenant = 'faculty-0123456789'
    directory = tmp_path / 'data/local_demo' / project
    tokens, registry = local.credentials(tenant, directory)
    run = {'project': project, 'tenant_id': tenant,
           'web_url': 'http://localhost:32001', 'api_url': 'http://localhost:8201',
           'kafka_port': 9201, 'mongo_port': 27201}
    (directory / 'run.json').write_text(json.dumps(run))
    loaded, saved = local.saved_project(project)
    assert loaded == run
    assert saved['tokens'] == tokens
    assert local.registry_for_credentials(tenant, saved) == registry
    environment = local.project_environment(loaded, registry)
    assert environment['ECHOGUARD_WEB_PORT'] == '32001'
    assert environment['ECHOGUARD_API_PORT'] == '8201'
    assert environment['ECHOGUARD_KAFKA_PORT'] == '9201'
    assert environment['ECHOGUARD_MONGO_PORT'] == '27201'
    with pytest.raises(ValueError, match='different demo tenant'):
        local.registry_for_credentials('wrong-tenant', saved)


def test_refresh_rebuilds_only_api_and_web_with_saved_credentials(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(local, 'ROOT', tmp_path)
    project = 'echoguard-local-0123456789'
    tenant = 'local-0123456789'
    directory = tmp_path / 'data/local_demo' / project
    local.credentials(tenant, directory)
    (directory / 'run.json').write_text(json.dumps({
        'project': project, 'tenant_id': tenant,
        'web_url': 'http://localhost:32001', 'api_url': 'http://localhost:8201',
        'kafka_port': 9201, 'mongo_port': 27201}))
    calls = []
    monkeypatch.setattr(local, 'compose', lambda name, env, *args: calls.append((name, env, args)))
    monkeypatch.setattr(local, 'authentication_check', lambda *_args: {
        'api': {'status': 200, 'tenant_match': True},
        'web_session': {'status': 200, 'tenant_match': True}})
    monkeypatch.setattr('sys.argv', ['local', 'refresh', project])
    local.main()
    assert [call[2][-1] for call in calls] == ['api', 'web']
    assert all(call[1]['ECHOGUARD_LOCAL_DEMO'] == 'true' for call in calls)
    assert json.loads(capsys.readouterr().out)['updated_services'] == ['api', 'web']


def test_new_local_credentials_use_neutral_actor_ids(tmp_path):
    tenant = 'local-0123456789'
    directory = tmp_path / 'workspace'
    _, registry = local.credentials(tenant, directory)
    actors = {record['actor_id'] for record in json.loads(registry).values()}
    assert actors == {'local-operator', 'local-reviewer'}
    saved = json.loads((directory / 'credentials.json').read_text())
    assert local.registry_for_credentials(tenant, saved) == registry


def test_demo_controls_are_disabled_by_default(monkeypatch):
    monkeypatch.delenv('ECHOGUARD_LOCAL_DEMO', raising=False)
    with pytest.raises(HTTPException) as error:
        demo.status(Principal('faculty-a', 'reviewer', 'reviewer'), resources=(None, None))
    assert error.value.status_code == 404


def test_demo_traffic_is_tenant_scoped_and_idempotent(monkeypatch):
    monkeypatch.setenv('ECHOGUARD_LOCAL_DEMO', 'true')
    monkeypatch.setenv('ECHOGUARD_OUTCOME_WINDOW_SECONDS', '60')
    record = {}
    published = []

    class TrafficCollection:
        def update_one(self, query, update, upsert=False):
            if not record and upsert:
                record.update(update['$setOnInsert'])
            elif '$set' in update:
                record.update(update['$set'])

        def find_one(self, query):
            return dict(record) if record and query['_id'] == record['_id'] else None

    class Incidents:
        def find_one(self, query):
            if query['tenant_id'] == 'faculty-a' and query['_id'] == 'incident-a':
                return {'_id': 'incident-a', 'product': 'payments', 'error_code': 'PAY-502'}
            return None

    class Cases:
        def count_documents(self, query):
            return len(query['event_id']['$in']) if published else 0

    database = SimpleNamespace(incidents=Incidents(), demo_traffic=TrafficCollection(),
                               cases=Cases())
    monkeypatch.setattr(demo, 'publish_events', lambda _bootstrap, events: published.append(events))
    reviewer = Principal('faculty-a', 'reviewer', 'reviewer')
    body = demo.TrafficRequest(incident_id='incident-a', kind='baseline')
    result = demo.traffic(body, idempotency_key='request-a', actor=reviewer,
                          resources=(None, database))
    assert result['status'] == 'ready' and result['projected_cases'] == 12
    assert record['_id'] == traffic_record_id('faculty-a', 'request-a')
    assert demo.traffic(body, idempotency_key='request-a', actor=reviewer,
                        resources=(None, database)) == result
    assert len(published) == 1
    with pytest.raises(Conflict, match='different demo traffic'):
        demo.traffic(demo.TrafficRequest(incident_id='incident-a', kind='recovery'),
                     idempotency_key='request-a', actor=reviewer, resources=(None, database))
    with pytest.raises(PermissionError):
        demo.traffic(body, idempotency_key='request-b',
                     actor=Principal('faculty-a', 'operator', 'operator'),
                     resources=(None, database))
    with pytest.raises(LookupError):
        demo.traffic(body, idempotency_key='request-b',
                     actor=Principal('other', 'reviewer', 'reviewer'),
                     resources=(None, database))


def test_demo_closure_requires_same_snapshot_and_improving_observation(monkeypatch):
    monkeypatch.setenv('ECHOGUARD_OUTCOME_WINDOW_SECONDS', '60')
    now = datetime.now(timezone.utc)
    incident = {'incident_id': 'incident-a', 'tenant_id': 'faculty-a',
                'product': 'payments', 'error_code': 'PAY-502', 'version': 1,
                'status': 'candidate', 'peak_count': 150,
                'last_seen': now.isoformat(), 'signal_ids': ['signal-a']}
    action = {'initial': {'incident': incident}, 'action': {
        'status': 'succeeded', 'completed_at': (now - timedelta(seconds=65)).isoformat()}}

    class Cases:
        def __init__(self, before, progressed):
            self.before, self.progressed = before, progressed
            self.calls = 0

        def count_documents(self, _query):
            self.calls += 1
            return self.before if self.calls % 2 else 0

        def find_one(self, _query, _projection):
            return {'_id': 'progress'} if self.progressed else None

    demo.require_demo_recovery(SimpleNamespace(cases=Cases(12, True)), 'faculty-a', incident, action)
    with pytest.raises(Conflict, match='improving observed trend'):
        demo.require_demo_recovery(SimpleNamespace(cases=Cases(0, False)), 'faculty-a', incident, action)
    with pytest.raises(Conflict, match='changed since the demo action'):
        demo.require_demo_recovery(SimpleNamespace(cases=Cases(12, True)), 'faculty-a',
                                   {**incident, 'peak_count': 151}, action)
