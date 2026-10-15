# Backend

This folder owns the FastAPI service, Spark streaming job, workers, and event contracts. Its Docker build context is `backend/`; it does not import or package frontend code.

Phase 0 runs the API health endpoints. Phase 1 adds seeded synthetic ingestion under `ingest/`. Subsequent phases add streaming, correlation, retrieval, and controlled workflow modules here.

All API routers are registered in `api/main.py`; endpoint implementations live in `api/routes/`. The single `requirements.txt` supplies both backend service images, local checks, and CI.

API environment settings are validated in `api/config.py` at startup. Backend tests and pytest configuration live in this folder. Run them from the repository root with `python3 -m pytest -c backend/pyproject.toml -q`.

