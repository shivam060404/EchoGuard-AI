# Phase 3: case store and correlation

Run `backend/worker/demo.sh` from the repository root. It starts the Spark stream and case writer, publishes the seeded scenario, and verifies the MongoDB projection inside the case-writer image. The case writer is an opt-in Compose profile.

The worker consumes `support.enriched.v1` and `incident.signals.v1` with manual Kafka commits. It commits an offset only after MongoDB writes return successfully. Deterministic case, signal, cluster, incident, and audit IDs make replay safe. A failed or malformed message remains uncommitted for inspection and redrive; the worker does not silently discard it.

Cases have a unique tenant/event index. Signals with the same tenant, product, error code, and 30-minute event-time bucket join one cluster and incident. Updates only increase peak counts and add signal IDs. The first version uses deterministic correlation; semantic merge and manual split/merge require a later reviewed workflow. A resolved incident can reopen only for a signal with a window after its resolution time. Resolution uses a tenant-scoped version compare-and-set.

MongoDB in the local Compose stack is standalone, so writes to separate collections are not one transaction. The worker writes the source record first, then derived projections and audit events with idempotent upserts. If it stops between writes, Kafka replay repairs the projection. An operator should monitor consumer lag and errors. No real customer data should be used in this local stack.

