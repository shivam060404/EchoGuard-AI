# ADR 0001: Local foundation and reliability boundary

Status: accepted for Phase 0 · 15 October 2026

## Decision

Use a single-node local Compose stack with Apache Kafka 4.3.1, Apache Spark 4.0.4, MongoDB 8.0.32, FastAPI, and a separate Next.js 16.2.12 frontend. The container tags are explicit and support arm64; run a live integration spike before declaring the version set proven on this machine. Keep Spark jobs separate from the API and later agent orchestration. The frontend uses the compatible cached release because npm registry access is unavailable in this workspace; its lockfile, typecheck, and production build are verified locally.

Versioned JSON schemas and stable event IDs define the Kafka boundary. Consumers will use idempotent writes because Spark's Kafka sink may duplicate output after retries. Local Kafka and MongoDB run without authentication and hold synthetic data only.

## Consequences

The local stack is simple to reproduce but provides no broker replication, high availability, or real-data security. Atlas-native hybrid search and live model adapters are later profiles. A missing Docker daemon prevents the Phase 0 live gate; contract, unit, and Compose syntax checks still run locally.

## References

- [Apache Kafka Docker image guide](https://hub.docker.com/r/apache/kafka)
- [Spark 4.0.4 Kafka integration](https://spark.apache.org/docs/4.0.4/streaming/structured-streaming-kafka-integration.html)
- [MongoDB official image](https://hub.docker.com/_/mongo)

