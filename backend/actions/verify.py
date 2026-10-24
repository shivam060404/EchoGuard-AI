"""Real MongoDB effect reconciliation and lifecycle audit repair gate."""
from datetime import timedelta
import json
from uuid import uuid4

from backend.knowledge.seed import seed, load_sources
from backend.workflow.runtime import runtime
from backend.workflow.service import Principal
from backend.workflow.models import now_utc
from backend.worker.store import CaseStore
from backend.workflow.models import digest
from backend.workflow.store import Conflict
from backend.api.routes.console import ResolveRequest, resolve
from .service import ActionService, MongoActionRepository, SimulatedAdapter


def main():
    with runtime() as (workflow, database):
        seed(database, load_sources())
        incident = database.incidents.find_one({'tenant_id': 'demo-tenant', 'error_code': 'PAY-502', 'status': 'candidate'})
        if incident is None:
            raise AssertionError('Fresh payment incident required')
        reviewer = Principal('demo-tenant', 'recovery-gate', 'reviewer')
        state = workflow.start(reviewer, incident['_id'], 'recovery-' + uuid4().hex)
        workflow.decide(reviewer, state['run_id'], 'approve', state['proposal_hash'])
        class InterruptedRepository(MongoActionRepository):
            def complete(self, *args):
                raise ConnectionError('injected loss of completion write')
        action = ActionService(workflow, InterruptedRepository(database), SimulatedAdapter(database))
        args = (reviewer, state['run_id'], state['proposal_hash'], incident['version'], 'recovery-action')
        try:
            action.execute(*args)
        except ConnectionError:
            pass
        else:
            raise AssertionError('Failure was not injected')
        pending = database.workflow_runs.find_one({'_id': state['run_id']})['action']
        effect = database.simulated_actions.find_one({'_id': pending['action_id']})
        assert pending['status'] == 'pending' and effect is not None
        completed = ActionService(workflow, MongoActionRepository(database), SimulatedAdapter(database),
                                  clock=lambda: now_utc() + timedelta(days=2)).execute(*args)
        assert completed['completed_at'] == effect['completed_at']
        assert database.simulated_actions.count_documents({'_id': pending['action_id']}) == 1
        competing = workflow.start(reviewer, incident['_id'], 'competing-' + uuid4().hex)
        workflow.decide(reviewer, competing['run_id'], 'approve', competing['proposal_hash'])
        competing_args = (reviewer, competing['run_id'], competing['proposal_hash'],
                          incident['version'], 'competing-action')
        try:
            ActionService(workflow, MongoActionRepository(database), SimulatedAdapter(database)).execute(*competing_args)
        except Conflict as exc:
            assert 'different authorized action' in str(exc)
        else:
            raise AssertionError('Second workflow authorized another action for the same incident revision')
        assert database.simulated_actions.count_documents({'incident_id': incident['_id']}) == 1

        auth_incident = database.incidents.find_one({'tenant_id': 'demo-tenant', 'error_code': 'AUTH-401'})
        assert auth_incident is not None
        auth_state = workflow.start(reviewer, auth_incident['_id'], 'stale-' + uuid4().hex)
        workflow.decide(reviewer, auth_state['run_id'], 'approve', auth_state['proposal_hash'])
        class RacingIncidents:
            def __init__(self, collection):
                self.collection = collection
                self.changed = False

            def find_one(self, *args, **kwargs):
                return self.collection.find_one(*args, **kwargs)

            def update_one(self, query, update, **kwargs):
                if 'execution_claim' in update.get('$set', {}) and not self.changed:
                    self.changed = True
                    self.collection.update_one({'_id': auth_incident['_id']},
                                               {'$inc': {'peak_count': 1}})
                return self.collection.update_one(query, update, **kwargs)

        class RacingDatabase:
            incidents = RacingIncidents(database.incidents)

            def __getattr__(self, name):
                return getattr(database, name)

        stale_args = (reviewer, auth_state['run_id'], auth_state['proposal_hash'],
                      auth_incident['version'], 'stale-action')
        try:
            ActionService(workflow, MongoActionRepository(RacingDatabase()),
                          SimulatedAdapter(database)).execute(*stale_args)
        except Conflict as exc:
            assert 'changed during action authorization' in str(exc)
        else:
            raise AssertionError('Worker update did not invalidate the action claim')
        assert database.simulated_actions.count_documents({'incident_id': auth_incident['_id']}) == 0

        # API closure commits its journal before the queryable ledger write.
        current, _ = workflow.store.context('demo-tenant', incident['_id'])
        body = ResolveRequest(expected_version=current['version'], incident_hash=digest(current))
        class BrokenLedger:
            def update_one(self, *args, **kwargs):
                raise ConnectionError('injected API ledger outage')
        class DatabaseWithBrokenApiLedger:
            audit_events = BrokenLedger()

            def __getattr__(self, name):
                return getattr(database, name)

        closure_key = 'closure-' + uuid4().hex
        try:
            resolve(incident['_id'], body, key=closure_key, actor=reviewer,
                    resources=(workflow, DatabaseWithBrokenApiLedger()))
        except ConnectionError:
            pass
        else:
            raise AssertionError('API ledger failure was not injected')
        assert database.incidents.find_one({'_id': incident['_id']})['status'] == 'resolved'
        assert resolve(incident['_id'], body, key=closure_key, actor=reviewer,
                       resources=(workflow, database))['status'] == 'resolved'
        try:
            resolve(incident['_id'], ResolveRequest(expected_version=body.expected_version,
                    incident_hash='0' * 64), key=closure_key, actor=reviewer,
                    resources=(workflow, database))
        except Conflict as exc:
            assert 'different closure request' in str(exc)
        else:
            raise AssertionError('Closure key accepted changed request content')
        assert database.audit_events.count_documents({'_id': f"audit-{incident['_id']}-resolved-v{current['version']}"}) == 1
        # Additional test-only incident for worker lifecycle repair.
        test_id = 'incident-recovery-' + uuid4().hex
        database.incidents.insert_one({'_id': test_id, 'tenant_id': 'demo-tenant', 'status': 'candidate', 'version': 1})
        class BrokenLedger:
            def update_one(self, *args, **kwargs):
                raise ConnectionError('injected ledger outage')
        class DatabaseWithBrokenLedger:
            incidents = database.incidents
            audit_events = BrokenLedger()
        at = now_utc()
        try:
            CaseStore(DatabaseWithBrokenLedger()).resolve_incident('demo-tenant', test_id, 1, at)
        except ConnectionError:
            pass
        assert database.incidents.find_one({'_id': test_id})['status'] == 'resolved'
        store = CaseStore(database)
        assert not store.resolve_incident('demo-tenant', test_id, 1, at)
        assert database.audit_events.count_documents({'subject_id': test_id}) == 1
        assert store.reopen_incident('demo-tenant', test_id, 'recovery-signal', at + timedelta(seconds=1))
        assert not store.reopen_incident('demo-tenant', test_id, 'recovery-signal', at + timedelta(seconds=1))
        assert database.audit_events.count_documents({'subject_id': test_id}) == 2
        print(json.dumps({'status': 'verified', 'effect_after_expiry': 'reconciled',
                          'effect_time': 'preserved', 'lifecycle_audit': 'repaired',
                          'competing_action': 'blocked', 'concurrent_worker_update': 'blocked',
                          'api_closure_audit': 'repaired', 'closure_key_reuse': 'blocked'}))


if __name__ == '__main__':
    main()
