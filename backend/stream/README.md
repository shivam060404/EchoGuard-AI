# Phase 2: streaming intelligence

Run from the repository root after installing backend requirements and starting Docker:

~~~sh
backend/stream/demo.sh
~~~

The script starts the Spark stream job, runs the seeded ingestion demo, then verifies payment and login candidate signals plus five intentionally late records. The verifier runs inside the stream-job container, using its installed Kafka client and internal broker address; it does not depend on the active host Python environment. The Spark job is an opt-in Compose profile. Inspect it with docker compose --profile stream logs -f stream-job. If verification fails, the script prints the last 100 stream-job log lines.

The job has three independently checkpointed queries:

1. Read support.raw.v1, validate schema and tenant/customer Kafka key, accept synthetic records only, normalize text, reject records more than two minutes late by ingest time, then deduplicate by tenant and event ID within a two-minute watermark. Write support.enriched.v1.
2. Write rejected raw records to stream.dlq.v1 with a safe reason, source partition/offset, and SHA-256 digests. Raw text and keys are not copied into this DLQ; replay can use the source Kafka offset while the raw topic is retained.
3. Read support.enriched.v1, validate it again, and aggregate one-minute event-time windows by tenant, product, and error code. A bootstrap rule emits candidates with at least 20 unique events, 10 unique customers, and a ratio of at least 5 against a baseline count of 2. Payment and login spikes in the seeded scenario qualify.

The signal query emits updates with a stable signal ID. Kafka writes can be repeated after a failure, so downstream consumers must upsert by signal ID and version. The baseline is a deterministic demo default, not a learned production baseline. Query progress logs report input rows, watermark, and watermark drop counts without message text.

Checkpoint state lives in the stream_checkpoints Docker volume. Reusing a checkpoint after changing a query plan, watermark, or state schema is unsafe. Stop the job and use a new checkpoint version, then replay from retained Kafka data and reconcile downstream IDs. Do not delete the volume as a routine restart action.

The local job requires the Spark 4.0.4 Kafka connector package at startup. The explicit ingest-delay check routes the seeded late records; Spark's moving watermark may additionally drop older replayed events. Live Spark/Kafka recovery behavior must pass the [Phase 2 gate](../../docs/PHASE_2_STATUS.md) before this phase is considered complete.
