"""Trusted local operator CLI. Public authentication belongs to the API phase."""
import argparse
import json
import os

from .runtime import runtime
from .service import Principal


def principal():
    actor = os.getenv("WORKFLOW_ACTOR", "").strip()
    tenant = os.getenv("WORKFLOW_TENANT", "").strip()
    role = os.getenv("WORKFLOW_ROLE", "operator").strip()
    if not actor or not tenant or role not in {"operator", "reviewer"}:
        raise ValueError("Set WORKFLOW_ACTOR, WORKFLOW_TENANT and WORKFLOW_ROLE (operator/reviewer)")
    return Principal(tenant, actor, role)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--incident-id", required=True)
    start.add_argument("--request-key", required=True)
    status = commands.add_parser("status")
    status.add_argument("--run-id", required=True)
    review = commands.add_parser("review")
    review.add_argument("--run-id", required=True)
    review.add_argument("--decision", choices=["approve", "reject"], required=True)
    review.add_argument("--proposal-hash", required=True)
    recover = commands.add_parser("recover-lock")
    recover.add_argument("--run-id", required=True)
    recover.add_argument("--expected-owner", required=True)
    recover.add_argument("--workers-stopped", action="store_true", required=True)
    args = parser.parse_args()
    actor = principal()
    with runtime() as (service, _):
        if args.command == "start":
            result = service.start(actor, args.incident_id, args.request_key)
        elif args.command == "status":
            result = service.status(actor, args.run_id)
        elif args.command == "review":
            result = service.decide(actor, args.run_id, args.decision, args.proposal_hash)
        else:
            service.store.recover_lock(actor.tenant_id, args.run_id, args.expected_owner, actor.actor_id)
            result = {"status": "lock_recovered", "run_id": args.run_id}
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
