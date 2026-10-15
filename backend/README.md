# Backend

This folder owns the FastAPI service, Spark streaming job, workers, and event contracts. Its Docker build context is `backend/`; it does not import or package frontend code.

Phase 0 runs the API health endpoints. Phase 1 adds seeded synthetic ingestion under `ingest/`. Phase 2 adds Spark normalization and candidate signals under `stream/`. Phase 3 adds the Kafka-to-MongoDB case writer and deterministic correlation under `worker/`. Phase 4 adds approved runbook retrieval under `knowledge/`. Phase 5 adds the controlled LangGraph workflow and trusted operator CLI under `workflow/`.

All API routers are registered in `api/main.py`; endpoint implementations live in `api/routes/`. The single `requirements.txt` supplies backend service images, local checks, and CI.

API environment settings are validated in `api/config.py` at startup. Backend tests and pytest configuration live in this folder. Run them from the repository root with `python3 -m pytest -c backend/pyproject.toml -q`.


## Authenticated console and simulated actions

Run `bash backend/api/demo.sh` from the root. See [API operations](api/README.md). Routers are registered in `api/main.py`; simulated execution and outcome logic live in `actions/`. All Python dependencies remain in `backend/requirements.txt`.
