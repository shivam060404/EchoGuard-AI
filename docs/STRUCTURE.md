# Project structure and ownership

EchoGuard AI is a monorepo with isolated backend and frontend builds. Root files are limited to repository entry points: `README.md`, `compose.yaml`, `.env.example`, `.github/`, and the module directories.

| Directory | Owns | Dependency boundary |
| --- | --- | --- |
| `backend/api/` | FastAPI application, validated settings, HTTP route modules | All routers are registered in `api/main.py`. Routes call backend services as they are implemented. |
| `backend/contracts/` | Versioned event schemas, validation, fixtures, contract checks | Producers and consumers may import this package; it never imports service modules. |
| `backend/ingest/` | Synthetic scenario generation, Kafka publisher, delivery verifier, demo command | May import contracts; does not import API, stream, or worker. |
| `backend/stream/` | Planned Spark normalization and detection jobs | Consumes raw events and emits enriched events and candidate signals; never runs LLM workflows. |
| `backend/worker/` | Planned correlation, retrieval, agent workflow, action, and outcome workers | Consumes signals and uses explicit service/adaptor interfaces; never exposes HTTP routes. |
| `backend/tests/` | Backend unit and contract tests | Configured by `backend/pyproject.toml`. |
| `frontend/` | Next.js console | Calls the API; never imports backend Python code or reads its database directly. |
| `infra/` | Compose support, Kafka setup, infrastructure smoke checks, ADRs, threat model | No application business logic. |
| `evals/` | Planned labeled scenarios and model/retrieval evaluation | Uses public contracts and API behavior to score runs. |
| `docs/` | Implementation plan and phase status | Records completed gates and known limitations. |

Backend dependencies are pinned in the single `backend/requirements.txt` requested for this project. Both backend images currently install that file. The API and frontend runtime images run as unprivileged users. The ingestion demo writes a receipt to a host bind mount, so its container user and volume permissions require an explicit deployment decision before a production profile is introduced. CI builds the API and frontend images.

## Current release boundary

Only foundation and seeded ingestion are implemented. The [implementation plan](IMPLEMENTATION_PLAN.md) defines the remaining streaming, persistence, retrieval, agent, approval, console, observability, evaluation, and release gates. The local Compose configuration and synthetic data are development assets; they do not constitute a production deployment. Phase 0 and Phase 1 live container gates remain open until they pass on a Docker-enabled machine or in CI.
