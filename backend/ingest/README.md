# Phase 1: synthetic ingestion

The generator produces a canonical JSONL dataset and manifest from a seed. The dataset has normal traffic, 150 payment failures, a separate login spike, duplicate deliveries with stable event IDs, delayed events, and two malformed records. Each JSONL line is an envelope with a Kafka key and raw payload string. The manifest contains its SHA-256 digest, expected Kafka topic counts, labels, and anomaly counts.

```sh
python3 -m backend.ingest.cli generate
python3 -m backend.ingest.cli inspect
backend/ingest/demo.sh
```

`generate` never silently replaces a differing dataset; use `--force` deliberately. `publish` validates each payload before sending and defaults to 50 records per second; use `--rate 0` for maximum throughput. Valid events go to `support.raw.v1`; invalid payloads go to `ingest.dlq.v1` with a safe reason code and base64 copy for controlled redrive. It uses an idempotent Kafka producer, waits for every delivery callback, and writes a receipt with a unique physical run ID. `verify` reads Kafka from the start with a fresh consumer group, filters that run ID, and compares every record's topic, key, and bytes with the dataset. Publishing the same dataset again creates a new physical run ID while logical `event_id` values stay stable.

The local broker is single-node and has no replication. Producer idempotence handles retry duplicates within a producer session; replay can append the same logical events again. The Phase 2 stream job deduplicates by tenant and `event_id` within a bounded watermark.

