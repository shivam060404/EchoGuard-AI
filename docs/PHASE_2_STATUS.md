# Phase 2 status — streaming intelligence

## Built

- Spark Structured Streaming job with separate checkpoints for enrichment, stream DLQ, and candidate signals.
- Independent raw/enriched contract validation, synthetic-only guard, deterministic text normalization, tenant key checks, explicit late-event policy, and watermark deduplication.
- One-minute payment/login candidate rule, stable signal IDs, and a stream DLQ that records Kafka offsets and digests without copying message content.
- Opt-in stream Compose profile, demo script, verifier, tests, and CI live integration job.

## Verified in this workspace

- Pure transformation and rule tests, including the full seeded scenario; 23 backend tests, contract fixtures, Python compilation, shell syntax, and Compose configuration.

## Live gate passed

On 15 October 2026, the user ran `bash backend/stream/demo.sh` with Docker access. The stream-job image built with its Kafka client import check, the producer delivered 263 raw and 2 ingest DLQ records, and read-back verified run `cc423815a4b94beb8835782b8380015e`. The Phase 2 verifier reported `status: verified`, PAY-502 and AUTH-401 signal IDs, and 10 late-event DLQ records. The count includes earlier publishes because the verifier scans the topic from the beginning; five late records per seeded publish were expected. This proves the streaming path emitted the required candidate signals and late-event records. The verifier does not yet attribute its observed output to one publish run. Restart/replay and checkpoint migration drills remain part of the later hardening phase. This workspace process itself still cannot access the Docker socket.
