# Phase 5 status — controlled agent workflow

## Implemented

- Dedicated `backend/workflow` module: strict models, provider interface and deterministic mock, LangGraph nodes, MongoDB checkpoint wiring, run registry, trusted operator CLI, and Docker integration gate.
- Tenant-scoped incident/evidence loading, extractive cited steps, action allowlist, confidence threshold, and explicit abstention reasons.
- Durable human-review interrupt, proposal hashing, expiry, incident snapshot checks, knowledge revalidation, rejection, and idempotent decision replay.
- Serialized run writers, manual crash-lock recovery, atomic run projection/audit updates, and reconciliation from graph checkpoints.
- Provider/model/prompt metadata, mock token estimates, cost and attempt tracking. No external action execution.
- Fixed the Phase 4 naive/aware datetime comparison in citation evaluation; UTC-aware clients and a regression test cover the boundary.

## Verified locally on 16 October 2026

All 52 backend tests pass, including real LangGraph invocation and pause/resume with its memory saver, graph reconstruction, model faults, low confidence, prompt injection against the mock, missing/unrelated knowledge, invalid evidence, tenant/role isolation, expiry, stale snapshots, revocation, duplicate/concurrent approvals, and audit projection recovery. Memory-saver tests do not prove MongoDB process recovery.

## Live integration passed

On 16 October 2026, the user supplied Docker verification for run `wf-c97e877b901986dab7964411e54de01f`: checkpoint restart passed, concurrent review passed, duplicate approval idempotent, workflow approved. Retrieval recall@5 was 1.0 on the five synthetic queries. The provider was `extractive-demo-v1` (mock), with estimated 766 input/277 output tokens and $0 cost.

Phase 6 implementation and its pending live gate are recorded in [PHASE_6_STATUS.md](PHASE_6_STATUS.md). Earlier broader replay, correlation, and production deployment hardening remain open. The repository is not yet production ready.
