# Phase 3 status — case store and correlation

## Built

- Opt-in case-writer container that consumes enriched events and candidate signals with manual Kafka offset commits.
- MongoDB collections and indexes for cases, signals, clusters, incidents, and audit events.
- Stable tenant-scoped identities, deterministic 30-minute correlation, monotonic signal counts, and idempotent replay projections.
- Version-checked incident resolution and a later-signal reopen rule.
- Docker demo and CI integration gate that expect 250 unique cases, two clusters, two incidents, and 254 creation audit events from the seeded scenario.

## Verified

Pure correlation tests, Python compilation, shell syntax, and Compose configuration passed. On 15 October 2026, the user ran the Docker demo locally. Producer run `7ce833999cc54df3bc709592e0c7857a` delivered and read back 263 raw records and 2 ingest DLQ records. The Phase 3 verifier reported `status: verified`, **250 cases**, **2 clusters**, and **2 incidents**. Cluster IDs were `cluster-c1639565f3b29ed18f229fe3d89ce812` for AUTH-401 and `cluster-39069a8ae387b529fd0e78a92bf7d76a` for PAY-502.

## Remaining hardening

The seeded end-to-end case projection gate passed. The verifier reports aggregate state and does not attribute every MongoDB record to a single producer run. Replayed-topic, concurrent-update, and tenant-isolation drills remain for the hardening phase. This workspace process itself cannot access the Docker socket.
