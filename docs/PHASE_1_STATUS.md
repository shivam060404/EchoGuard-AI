# Phase 1 status — 15 October 2026

## Built

- Seeded, deterministic support traffic generator with normal traffic, a payment spike, a separate login spike, stable-ID duplicates, late events, and malformed payloads.
- Canonical JSONL dataset and SHA-256 manifest. A differing existing dataset is protected unless `--force` is supplied. Inspection checks both digest and deterministic regeneration.
- `ingest.dlq.v1` event contract with safe reason code, payload digest, and base64 copy for controlled redrive.
- Kafka topic initializer with explicit partition counts and retention.
- Isolated producer image using a pinned Kafka client, idempotent configuration, acknowledged publish, configurable pacing, unique physical run headers, and a completed/failed receipt.
- Read-back verifier that checks every record's topic, key, bytes, and physical record ID for one publish attempt.
- Compose demo profile, `backend/ingest/demo.sh`, CI broker integration job, and design record.

## Verified here

- Default seed `42` produces **265** records with stable SHA-256 `47a8e4aac365838030fe74e1fc7aa6dc61b0614ed804c720889fe58e8ee09356`.
- Manifest expects **263** records on `support.raw.v1`, **2** on `ingest.dlq.v1`, **8** duplicate logical deliveries, and **5** late events.
- `python3 -m backend.ingest.cli generate` and `inspect` pass.
- `python3 -m pytest -c backend/pyproject.toml -q` — **14 tests passed** at the Phase 1 gate, including deterministic replay, tamper detection, DLQ recovery bytes, producer failure handling, and run-specific verifier behavior.
- Contract validation, Compose static configuration (including the demo profile), and shell syntax checks pass.

## Live gate still open

The workspace denies access to the Docker socket, so Kafka topic creation, producer image build, broker publish, and read-back could not run here. On a Docker-enabled machine, run `backend/ingest/demo.sh`; the CI `phase1-kafka` job runs the same publish and verify sequence. Do not mark Phase 1 complete until one of those live gates passes. Phase 0's container smoke gate also remains open for the same reason.

