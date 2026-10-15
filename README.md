# EchoGuard AI

EchoGuard AI is a project for detecting support incidents from live customer messages, investigating them with evidence, and sending proposed actions through human approval.

**Current state:** the repository has a working foundation and a deterministic Kafka ingestion demo. It does not yet detect incidents or run AI workflows. The [implementation plan](docs/IMPLEMENTATION_PLAN.md) describes those later phases.

## What works today

- A Next.js dashboard displays API, Kafka, and MongoDB health.
- A FastAPI service exposes liveness and readiness endpoints.
- A seeded generator creates synthetic support messages, including payment and login spikes, duplicates, late events, and malformed records.
- A Kafka producer validates and publishes records. A verifier reads them back and checks each delivered record.
- Versioned JSON schemas and backend tests check the event contracts.

The local stack also starts Spark, but no streaming job runs yet.

## Prerequisites

- Docker Engine or Docker Desktop with Compose v2. Allow about 8 GB of memory and 8 GB of free disk.
- Python 3.12 or newer for the demo generator and backend checks.
- Node.js 20.9 or newer only if you want to run the frontend outside Docker.

Run the commands below from the repository root.

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
| [backend/stream](backend/stream) | Planned Spark streaming jobs. |
| [backend/worker](backend/worker) | Planned incident, retrieval, and agent workflows. |
| [backend/tests](backend/tests) | Backend API, contract, and ingestion tests. |
| [frontend](frontend) | Isolated Next.js dashboard. |
| [infra](infra) | Kafka setup, smoke check, architecture decisions, and threat model. |
| [docs](docs) | Implementation plan, module ownership, and phase status. |
| [evals](evals) | Planned evaluation scenarios and benchmarks. |

Python dependencies are pinned in [backend/requirements.txt](backend/requirements.txt). Frontend dependencies and lockfile stay in frontend/. See the [module ownership guide](docs/STRUCTURE.md) for boundaries between components.

## Release status

This Compose setup uses synthetic data, a single local Kafka broker, and MongoDB without authentication. Do not use it with real customer data or expose it as a production service. Streaming detection, case persistence, retrieval, agent workflows, approvals, observability, and outcome verification are still planned.

The [Phase 0 status](docs/PHASE_0_STATUS.md) and [Phase 1 status](docs/PHASE_1_STATUS.md) record completed checks and the live container gates that still need verification.
