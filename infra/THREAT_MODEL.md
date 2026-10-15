# Phase 0 threat model

## Assets and trust boundaries

- Raw support messages and derived embeddings may contain PII. Phase 0 uses synthetic fixtures only.
- Kafka events and retrieved documents are untrusted input. They cannot grant permissions or select tools.
- MongoDB approval/action records and audit events are high-integrity data.
- Model providers and external action adapters are outside the trusted boundary.

## Controls planned and required before real data

| Threat | Control and phase |
| --- | --- |
| Cross-tenant retrieval or clustering | Tenant key in every contract; scoped indexes and negative tests in Phases 3–4. |
| Prompt injection through cases/runbooks | Typed node output, evidence validation, fixed tool allowlist in Phase 5. |
| Duplicate or unauthorized action | Proposal hash, expected version, RBAC, idempotency key, action outbox in Phase 6. |
| PII leakage to model/logs | Redact/minimize before embeddings or model calls; no raw text in telemetry in Phases 2–5. |
| Data loss or replay inconsistency | Kafka retention, Spark checkpoints, stable IDs, DLQ, replay tests in Phases 1–3. |
| Local plaintext ports | Loopback-only host binds; synthetic-only data in Phase 0. TLS/auth required before any shared deployment. |

No production credentials or real customer data should be placed in `.env` or the local stack.

