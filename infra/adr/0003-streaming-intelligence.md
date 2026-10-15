# ADR 0003 — Spark candidate detection

Status: accepted for the local synthetic-data profile.

## Decision

Use Spark 4.0.4 Structured Streaming with a pinned Kafka connector. Run three checkpointed queries: raw-to-enriched with watermark deduplication, raw-to-stream-DLQ, and enriched-to-windowed signals. The signal query reads the enriched topic rather than chaining deduplication and update-mode aggregation in one query.

Use a two-minute event-time watermark and a one-minute tumbling detection window. The raw boundary separately routes events whose declared ingest time is more than two minutes after event time to the stream DLQ. Signal counts use distinct event/customer sets, so repeated enriched Kafka deliveries do not inflate the candidate. Each tenant, group, window, and rule version produces a stable signal ID.

## Consequences

- Spark state is bounded by the watermark and checkpoints. Three queries can progress independently; output across topics is not an atomic transaction.
- Kafka sinks and update-mode window output can publish repeated physical records. Future consumers must use stable event and signal IDs for idempotent writes.
- The initial baseline count of two and thresholds are demo defaults. Calibration and held-out evaluation remain open.
- The explicit ingest delay routes the seeded late records, while Spark can also drop records older than its moving watermark. Watermark drop metrics and a broader reconciliation policy remain hardening work.
- Stream DLQ records contain source offsets and digests but no raw message bytes. Replay depends on source topic retention.

Spark documents the [Kafka connector coordinate and deployment command](https://spark.apache.org/docs/4.0.4/streaming/structured-streaming-kafka-integration.html), [watermark deduplication behavior](https://spark.apache.org/docs/4.0.4/api/java/org/apache/spark/sql/Dataset.html), and [limits on chaining stateful operations in update mode](https://spark.apache.org/docs/4.0.3/streaming/apis-on-dataframes-and-datasets.html).
