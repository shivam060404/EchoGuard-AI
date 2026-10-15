"""Two-process MongoDB checkpoint gate using an isolated synthetic demo database."""
import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend.knowledge.seed import load_sources, seed
from backend.knowledge.evaluate import evaluate
from backend.worker.store import CaseStore
from .models import digest, now_utc
from .runtime import runtime
from .service import Principal
from .store import Busy

DEMO_DATABASE = "echoguard_workflow_demo"
FIXTURES = Path(__file__).parents[1] / "contracts" / "fixtures"
OPERATOR = Principal("demo-tenant", "demo-operator", "operator")
REVIEWER = Principal("demo-tenant", "demo-reviewer", "reviewer")


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["prepare", "resume"])
    parser.add_argument("--request-key", required=True)
    args = parser.parse_args()
    with runtime(DEMO_DATABASE) as (service, database):
        if args.stage == "prepare":
            seed(database, load_sources())
            report = evaluate(database, now_utc())
            writer = CaseStore(database)
            writer.ensure_indexes()
            event = json.loads((FIXTURES / "support.enriched.v1.valid.json").read_text())
            signal = json.loads((FIXTURES / "incident.signals.v1.valid.json").read_text())
            writer.write_case(event)
            cluster = writer.write_signal(signal)
            incident_id = "incident-" + cluster.removeprefix("cluster-")
            state = service.start(OPERATOR, incident_id, args.request_key)
            check(state["status"] == "awaiting_approval", "Workflow did not pause for review")
            check("decision" not in state, "Decision exists before review")
            print(json.dumps({"status": "awaiting_approval", "run_id": state["run_id"],
                              "proposal_hash": state["proposal_hash"],
                              "retrieval_recall_at_5": report["recall_at_5"]}, sort_keys=True))
        else:
            run_id = "wf-" + digest([OPERATOR.tenant_id, args.request_key])[:32]
            state = service.status(REVIEWER, run_id)
            check(state["status"] == "awaiting_approval", "Checkpoint not restored at human review")
            def concurrent_review():
                with runtime(DEMO_DATABASE) as (other_service, _):
                    try:
                        return other_service.decide(REVIEWER, run_id, "approve", state["proposal_hash"])["status"]
                    except Busy:
                        return "busy"
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(lambda _: concurrent_review(), range(2)))
            check("approved" in outcomes and set(outcomes) <= {"approved", "busy"},
                  "Concurrent review failed")
            approved = service.status(REVIEWER, run_id)
            duplicate = service.decide(REVIEWER, run_id, "approve", state["proposal_hash"])
            check(approved == duplicate, "Duplicate approval changed completed workflow")
            record = service.store.get(REVIEWER.tenant_id, run_id)
            check(sum(event["status"] == "approved" for event in record["audit"]) == 1,
                  "Approval audit duplicated")
            check(database.actions.count_documents({}) == 0, "Workflow executed an external action")
            print(json.dumps({"status": "verified", "workflow_status": approved["status"],
                              "checkpoint_restart": "passed", "duplicate_approval": "idempotent",
                              "concurrent_review": "passed",
                              "run_id": run_id, "usage": approved["usage"]}, sort_keys=True))


if __name__ == "__main__":
    main()
