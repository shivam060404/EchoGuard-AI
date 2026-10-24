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

The gate verifies a fresh ingestion → streaming → case projection and injects failures against real MongoDB. After a restart, the second producer run adds 263 physical raw records with the same logical IDs. The gate requires both Spark raw consumers and the Spark signal consumer to commit through their observed Kafka ends, then requires the case writer's committed group offsets to reach the resulting enriched and signal ends. This checks replay processing while the logical case count remains 250.

- Adapter effect committed, completion write fails, then reconciliation occurs after approval expiry. Effect count and original timestamp must be preserved.
- Incident transition commits with an embedded journal, ledger projection fails, then replay repairs the ledger without repeating the transition.
- A second approved workflow cannot create another effect for the same incident revision. A worker update injected immediately before the atomic claim prevents stale authorization.
- API closure commits its journal, then a failed ledger write is repaired by repeating the same request key. Reusing the key with changed closure content is rejected.

Resources are cleaned up in a `finally` block, including on test failure. Successful reports are saved to ignored `data/scenarios/fresh-pipeline-report.json`. CI runs the same command and uploads the report. A killed host/process can interrupt cleanup; use the exact `echoguard-gate-*` project name from its logs to inspect and remove abandoned gate resources.

## Boundaries

The fresh initial-state gate cannot be satisfied by the main stack's historical records. The 23 October run `echoguard-gate-c28cc3378d39` passed the offset-drain, incident-claim, and API closure repair checks. The closure-key mismatch check was added afterward and needs a fresh run. The offset checks prove commits through observed topic ends in an isolated stack; they do not simulate a process kill at every write boundary. The injected Mongo failure is a controlled exception, not a host crash. Browser/session verification on fresh inputs and load/backup/restore drills remain open. The existing Phase 2/3/6 scripts remain useful seeded regression demos, not fresh-run acceptance proofs.

Older simulated effects without an effect timestamp are not automatically assigned a fabricated completion time. Pending legacy effects require explicit operator reconciliation; already completed legacy actions remain readable.

Incident `lifecycle_audit` is authoritative for worker and API closure transitions and is committed atomically with the status/version change. The `audit_events` ledger is an idempotent projection repaired on retry. API closure also retains its human `review_audit`; retention and size management remain planned work.
