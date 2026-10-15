# Phase 6 — review console and action loop

## Implemented

- Central FastAPI router registration; authenticated tenant-scoped list/detail/workflow APIs with strict request schemas, pagination, and safe HTTP error responses.
- Next.js console with HTTP-only sessions, origin-checked write proxy, live refresh/reconnect, evidence, citations, human edits, approval/rejection, simulation execution, audit history, and outcome display.
- Review edits invalidate the prior proposal hash; approval and execution recheck current incident and approved knowledge.
- Durable pending action state with stable effect identity, idempotent simulated adapters, crash-after-effect retry, and human audited closure.
- Local credential bootstrap, repeatable API/proxy integration verifier, and CI gate.

## Verification

69 backend tests pass locally, including action crash/retry, stale/expired/revoked approval boundaries, tenant/role isolation, edit/reapproval, and monitoring evidence sufficiency. Next.js typecheck and production build pass. Python compilation, shell syntax, and Compose configuration pass.

Docker API access from the agent workspace is denied. The live Phase 6 gate must run in the Docker-enabled terminal:

```sh
bash backend/api/demo.sh
```

Expect `status: verified`, `session_proxy: passed`, `approval_edit: passed`, and `duplicate_action: idempotent`. The gate tests HTTP behavior through Next.js; a manual browser review and automated browser tests remain separate checks. No live Phase 6 success is claimed yet.

## Scope and remaining release work

Actions and the planning provider are simulated. Outcome tracking reports complaint counts and does not prove remediation. Edits are constrained to allowed action types and existing cited steps (action editing is exposed in the UI). Production identity/ingress, background outbox retries, telemetry/customer-impact comparisons, measured latency/load, and the Phase 7 fault/evaluation suite remain open. This is not a production deployment certification.
