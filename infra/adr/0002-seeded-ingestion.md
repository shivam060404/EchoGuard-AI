# ADR 0002: Seeded scenario ingestion and replay identity

Status: accepted for Phase 1 · 15 October 2026

## Decision

Use a deterministic JSONL envelope and manifest as the reproducible source dataset. Logical event IDs are derived from the seed, tenant, scenario kind, and index. Each publish attempt gets a separate random run ID in Kafka headers and a receipt written only after all delivery callbacks succeed. This allows a verifier to distinguish one physical publish from prior replays while Phase 2 deduplicates by stable logical event ID.

Use `confluent-kafka` 2.16.0 in an isolated producer image. Enable idempotence and `acks=all`, poll for delivery callbacks, and fail the run if any callback fails or the queue does not flush. Idempotence protects retries within a producer session; it does not deduplicate a later application replay. [Confluent Python producer guide](https://docs.confluent.io/kafka-clients/python/current/overview.html), [Apache Kafka producer configuration](https://kafka.apache.org/43/configuration/producer-configs/).

Create topics explicitly with fixed partition counts and retention. Route malformed input to `ingest.dlq.v1` with a safe reason code, payload digest, and base64 payload for controlled redrive. The local DLQ is synthetic-only. Actual customer data requires access controls, encryption, and a retention policy before use.

## Consequences

The generator and manifest can be validated without Kafka. The broker publish/read-back gate requires Docker access. The single-node local broker cannot demonstrate multi-node durability or failover.

