# Fresh pipeline and recovery gate

From the repository root, with the backend virtual environment active and Docker running:

```sh
python -m backend.verification.fresh_pipeline
```

This creates a random Compose project with separate Kafka, MongoDB and Spark checkpoint volumes. It generates a temporary dataset and mounts it only into that project's producer. No service ports are exposed. Existing EchoGuard containers, datasets, credentials and volumes are retained.

Kafka waits for `kafka-volume-init` to prepare the mounted data directory for
UID/GID `1000:1000`. Only the initializer runs as root; the broker runs as
`1000:1000`. This handles fresh root-owned volume directories without deleting
metadata, recursively changing log files, or granting world-write permissions.
If initialization fails, Kafka does not start, and its initializer logs are
included in the gate diagnostics.

The gate verifies a fresh ingestion → streaming → case projection, performs a restart/replay smoke check, and injects failures against real MongoDB:

- Adapter effect committed, completion write fails, then reconciliation occurs after approval expiry. Effect count and original timestamp must be preserved.
- Incident transition commits with an embedded journal, ledger projection fails, then replay repairs the ledger without repeating the transition.

Resources are cleaned up in a `finally` block, including on test failure. Successful reports are saved to ignored `data/scenarios/fresh-pipeline-report.json`. CI runs the same command and uploads the report. A killed host/process can interrupt cleanup; use the exact `echoguard-gate-*` project name from its logs to inspect and remove abandoned gate resources.

## Boundaries

The fresh initial-state gate cannot be satisfied by the main stack's historical records. The restart/replay portion still checks aggregate state; it is not proof of complete offset drain or a process kill at every write boundary. The injected Mongo failure is a controlled exception, not a host crash. Full offset-aware replay, concurrency authorization, browser/session verification on fresh inputs, and load/backup/restore drills remain open. The existing Phase 2/3/6 scripts remain useful seeded regression demos, not fresh-run acceptance proofs.

Older simulated effects without an effect timestamp are not automatically assigned a fabricated completion time. Pending legacy effects require explicit operator reconciliation; already completed legacy actions remain readable.

Incident `lifecycle_audit` is authoritative for worker transitions and is committed atomically with the status/version change. The `audit_events` ledger is an idempotent projection repaired on retry. API closure currently retains its separate atomic `review_audit`; consolidating these lifecycle paths and adding retention/size management remains planned work.
