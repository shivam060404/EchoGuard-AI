# EchoGuard AI

EchoGuard AI is a project for detecting support incidents from live customer messages, investigating them with evidence, and sending proposed actions through human approval.

**Current state:** Kafka, Spark, MongoDB correlation, approved retrieval, and the LangGraph checkpoint/review workflow have passed local live gates. The authenticated Next.js review console and simulated action HTTP gate have also passed. Full outcome monitoring, browser tests, recovery hardening and release benchmarks remain open; see the [implementation audit](docs/IMPLEMENTATION_AUDIT.md). See the [implementation plan](docs/IMPLEMENTATION_PLAN.md) and [console guide](backend/api/README.md).

## What works today

- A Next.js console displays incident evidence, cited plans, review decisions, simulated actions, and complaint trends.
- A FastAPI service exposes liveness and readiness endpoints.
- A seeded generator creates synthetic support messages, including payment and login spikes, duplicates, late events, and malformed records.
- A Kafka producer validates and publishes records. A verifier reads them back and checks each delivered record.
- Versioned JSON schemas and backend tests check the event contracts.
- An opt-in case writer stores unique cases and correlates candidate signals into MongoDB clusters and incidents.
- An isolated knowledge tool loads approved synthetic runbooks and evaluates tenant-scoped hybrid retrieval.
- A LangGraph workflow validates cited proposals, persists checkpoints, and waits for a reviewer decision with stale-proposal and tenant checks.

The default stack starts an idle Spark runtime. The streaming job runs through an opt-in Compose profile.

## Prerequisites

- Docker Engine or Docker Desktop with Compose v2. Allow about 8 GB of memory and 8 GB of free disk.
- Python 3.12 or newer for the demo generator and backend checks.
- Node.js 20.9 or newer only if you want to run the frontend outside Docker.

Run the commands below from the repository root.

## Run the review console

With the backend virtual environment installed as below and Docker running:

```sh
bash backend/api/demo.sh
```

This runs the data pipeline, seeds knowledge, starts API/web, and verifies the session/approval/action loop. Open http://localhost:3000 and sign in using the reviewer token in the ignored `data/local-auth.json` file. Actions are simulated. See the [console guide](backend/api/README.md) for credential rotation, custom ports, endpoints, and recovery.

## Run the end-to-end ingestion demo

1. Create the local environment and install Python dependencies:

   ~~~sh
   cp .env.example .env
   python3 -m venv .venv
   . .venv/bin/activate
   python -m pip install -r backend/requirements.txt
   ~~~

2. Build and start the local services:

   ~~~sh
   docker compose up --build -d
   infra/smoke.sh
   ~~~

3. Generate, publish, and verify the seeded traffic:

   ~~~sh
   backend/ingest/demo.sh
   ~~~

The demo writes its dataset, manifest, and publish receipt under data/scenarios/. With the default seed, it generates 265 records: 263 valid messages for support.raw.v1 and 2 invalid messages for ingest.dlq.v1. The verifier confirms the records for that publish run. This is an ingestion demonstration; it does not produce an incident alert yet.

To run the Phase 2 Spark job and verify candidate signals, use this separate demo from a terminal with Docker socket access:

~~~sh
backend/stream/demo.sh
~~~

It starts the stream-job profile, reruns the seeded ingestion demo, and waits for PAY-502 and AUTH-401 signals plus five late records. The Phase 2 verifier runs inside the stream-job container, so it uses the Kafka client installed in that image. This live gate passed on 15 October 2026. See the [streaming guide](backend/stream/README.md) and [Phase 2 status](docs/PHASE_2_STATUS.md).

To run the next phase and verify cases and incidents in MongoDB:

~~~sh
backend/worker/demo.sh
~~~

See the [case writer guide](backend/worker/README.md) and [Phase 3 status](docs/PHASE_3_STATUS.md). The worker runs in a separate opt-in Compose profile.

The Phase 3 local demo passed with 250 unique cases and two incidents. To seed and evaluate the next phase's approved knowledge base:

~~~sh
backend/knowledge/demo.sh
~~~

See the [knowledge guide](backend/knowledge/README.md) and [Phase 4 status](docs/PHASE_4_STATUS.md). Its vector is a small offline domain-feature baseline, so the evaluation applies to the seeded synthetic queries.

To verify Phase 5 checkpoint persistence and human review:

~~~sh
backend/workflow/demo.sh
~~~

This also runs the Phase 4 retrieval gate in an isolated synthetic demo database. It pauses in one container process, resumes in another, and checks concurrent and repeated approvals. The provider is a deterministic mock; no external action is executed. See the [workflow guide](backend/workflow/README.md) for reviewing actual stored incidents through the CLI.

Open the dashboard at http://localhost:3000. The API is at http://localhost:8000, with these endpoints:

| Endpoint | Meaning |
| --- | --- |
| GET /health/live | The API process is running. |
| GET /health/ready | The API can reach Kafka and MongoDB; returns 503 if either is unavailable. |

To inspect the generated dataset without publishing it again:

~~~sh
python -m backend.ingest.cli inspect
~~~

## Run checks

After activating the Python environment above:

~~~sh
python -m backend.contracts.check_contracts
python -m pytest -c backend/pyproject.toml -q
docker compose --profile demo config --quiet
docker compose --profile stream config --quiet
docker compose --profile worker config --quiet
docker compose --profile knowledge config --quiet
docker compose --profile workflow config --quiet
~~~

For frontend checks:

~~~sh
cd frontend
npm ci
npm run typecheck
npm run build
~~~

To run the frontend locally, use npm run dev from frontend/. It calls the API at http://localhost:8000 by default. Set INTERNAL_API_URL if the API uses a different address.

## Configuration and operations

The root .env file controls the host ports for the API, dashboard, Kafka, and MongoDB. See [.env.example](.env.example). Compose passes the internal service addresses to the API and frontend.

~~~sh
docker compose ps
docker compose logs api web kafka mongo
docker compose down
~~~

If readiness returns 503, inspect Kafka and MongoDB with docker compose ps and docker compose logs. If the demo reports that an existing dataset differs from the chosen seed, inspect or archive the files in data/scenarios/ before deliberately regenerating them.

To delete local Kafka and MongoDB volumes, run docker compose down -v. This removes local service data; generated files in data/scenarios/ remain on the host.

## Project layout

| Path | Responsibility |
| --- | --- |
| [backend/api](backend/api) | FastAPI app, validated settings, and health routes. Every router is registered in main.py. |
| [backend/contracts](backend/contracts) | Versioned event schemas, fixtures, and validation. |
| [backend/ingest](backend/ingest) | Synthetic data, Kafka publishing, verification, and demo script. |
| [backend/stream](backend/stream) | Spark normalization and candidate detection. |
| [backend/worker](backend/worker) | Kafka case writer, MongoDB projection, and deterministic correlation. |
| [backend/knowledge](backend/knowledge) | Approved runbooks, document versions, hybrid retrieval, and evaluation. |
| [backend/workflow](backend/workflow) | LangGraph stages, provider interface, review service, MongoDB checkpoints, and CLI. |
| [backend/tests](backend/tests) | Backend API, contract, and ingestion tests. |
| [frontend](frontend) | Isolated Next.js dashboard. |
| [infra](infra) | Kafka setup, smoke check, architecture decisions, and threat model. |
| [docs](docs) | Implementation plan, module ownership, and phase status. |
| [evals](evals) | Planned evaluation scenarios and benchmarks. |

Python dependencies are pinned in [backend/requirements.txt](backend/requirements.txt). Frontend dependencies and lockfile stay in frontend/. See the [module ownership guide](docs/STRUCTURE.md) for boundaries between components.

## Release status

This Compose setup uses synthetic data, a single local Kafka broker, and MongoDB without authentication. Do not use it with real customer data or expose it as a production service. Case persistence has passed its local demo. Retrieval and the controlled workflow await live gates. Public authentication, the review console, action execution, observability, and outcome verification remain planned.

The [Phase 5 status](docs/PHASE_5_STATUS.md) records the current integration gates; earlier phases are documented in the same directory.
