"""Real MongoDB effect reconciliation and lifecycle audit repair gate."""
from datetime import timedelta
import json
from uuid import uuid4

from backend.knowledge.seed import seed, load_sources
from backend.workflow.runtime import runtime
from backend.workflow.service import Principal
from backend.workflow.models import now_utc
from backend.worker.store import CaseStore
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
        # Separate test-only incident; never resolve the seeded scenario incident.
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
                          'effect_time': 'preserved', 'lifecycle_audit': 'repaired'}))


if __name__ == '__main__':
    main()
