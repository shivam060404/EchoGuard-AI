# Incident review API and console

Phase 6 connects FastAPI, the Next.js console, durable workflow review, and local simulated actions. All routers are registered in `backend/api/main.py`. Dependencies remain in `backend/requirements.txt`.

## Run

For a fresh isolated local scenario with guided traffic and a one-minute
observation window, use `python -m backend.demo.local start`. See
[`backend/demo/README.md`](../demo/README.md). The procedure below remains the
seeded regression demo for the regular Compose stack.

From the repository root, with Docker running and the backend virtual environment active:

```sh
bash backend/api/demo.sh
```

This creates credentials if absent, runs the streaming/case and retrieval demos, builds API and web, and verifies the API through the Next.js session proxy. It leaves services running. Open http://localhost:3000 and copy the `reviewer` token from the ignored `data/local-auth.json` file into the sign-in form. Never commit or share this file. An `operator` can investigate and inspect; a `reviewer` can also edit, approve, reject, execute, and close incidents.

For an already populated stack:

```sh
python3 -m backend.api.setup_auth  # first setup only
# --rotate replaces tokens; recreate API after rotation
docker compose up -d --build --wait api web
python3 -m backend.api.verify
```

Tokens expire after seven days. The one-hour browser cookie is HTTP-only and SameSite Strict. Only token hashes and trusted actor/tenant/role mappings are supplied to the API. The browser proxy allows specific paths and rejects writes from a different Origin. For custom ports, set `CONSOLE_ORIGIN` to the exact browser origin in `.env` and pass `--api http://localhost:PORT --web http://localhost:PORT` to the verifier. TLS origins enable Secure cookies.

## Review flow

1. Select an incident and inspect its evidence.
2. Start an investigation; the mock provider produces cited extractive recommendations.
3. A reviewer can change the action type. This creates a new proposal hash and requires fresh approval. The API also supports selecting/reordering existing cited steps; arbitrary instructions are not accepted.
4. Approve or reject. Approval checks the incident snapshot, knowledge validity, role, hash, and expiry.
5. Execute the approved simulation. A durable pending record is saved inside the workflow document, then an idempotent local adapter creates a simulated ticket or notification. Retry returns the same result, including after a failure between effect and completion recording.
6. Compare complaint counts over the configured observation window (five minutes normally, one minute in the isolated local scenario). Insufficient baseline, incomplete monitoring, or no later tenant stream evidence yields `insufficient_data`. Counts are observational, not causal proof or a pipeline-health SLO.
7. A reviewer can mark the incident resolved after inspecting evidence. Closure is version checked and audited; no automatic closure occurs.

SSE sends refresh notifications every five seconds. It reconnects after thirty seconds to reauthenticate, and the console reloads authoritative records. It is a polling invalidation channel, not a durable event log.

Incident detail selects the workflow that owns an authorized action, even if
a newer investigation exists. A second investigation on the same incident
revision is rejected at the API boundary. Retrying with the original request
key remains idempotent.

## Endpoints

All except health require `Authorization: Bearer TOKEN`. Mutations require `Idempotency-Key` and an expected incident version; review/action/edit also require the proposal hash.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/me` | Authenticated identity |
| GET | `/incidents?limit=50&after=ID` | Tenant-scoped summary pagination with action authorization flag |
| GET | `/incidents/{id}` | Snapshot, evidence, authorized workflow when present, and audit |
| POST | `/incidents/{id}/workflows` | Start/retry an investigation |
| GET | `/workflows/{id}` | Reconcile durable checkpoint and return status |
| POST | `/workflows/{id}/proposal` | Edit allowed action/cited step selection |
| POST | `/workflows/{id}/decision` | Approve/reject exact proposal |
| POST | `/workflows/{id}/action` | Execute/retry simulated action |
| GET | `/workflows/{id}/outcome` | Complaint trend and evidence sufficiency |
| POST | `/incidents/{id}/resolve` | Human closure with snapshot hash |
| GET | `/events` | Authenticated SSE refresh notifications |
| GET | `/demo/status` | Local-demo pipeline status and observation window (disabled by default) |
| GET | `/demo/traffic?incident_id=ID` | Local-demo projected traffic status (disabled by default) |
| POST | `/demo/traffic` | Reviewer-only synthetic baseline/recovery traffic (disabled by default) |

OpenAPI request schemas are available at http://localhost:8000/docs. Wrong role returns 403; hidden tenant resources 404; stale or concurrent state 409; invalid schemas 422; missing storage 503.

## Operations and limits

- If a process dies holding a workflow lock, stop all writers before using the maintenance procedure in `backend/workflow/README.md`. Locks are not automatically stolen.
- Simulated adapter effects are stored in `simulated_actions`; pending/completed state and audit entries are embedded atomically in `workflow_runs`. There is no remote Slack/Jira call or background retry worker. Retry is explicit and bounded to one adapter invocation per request.
- Credentials are local demonstration authentication. Production needs managed identity, HTTPS ingress, rate limits, secret management, revocation operations, retention, and observability.
- Outcome monitoring currently uses unique complaint counts and coarse tenant progress. Distinct-customer/telemetry comparisons, browser automation, load and fault drills remain release-hardening work.
- The isolated local launcher enables `/demo/*` only inside its own Compose project. Its 60-second window is an accelerated synthetic demonstration; the ordinary stack retains five-minute windows. Demo closure requires an improving observation and an unchanged incident snapshot after the simulated action.
