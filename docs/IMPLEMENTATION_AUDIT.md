# Implementation audit — 23 October 2026

**Verdict: the approved implementation plan is not complete, and this repository
is not yet production ready.** It is a working synthetic local prototype with
strong replay and approval safeguards for the seeded payment scenario. Passing
that scenario does not establish the plan's broader detection, outcome, security,
observability, evaluation, or release criteria.

## Evidence checked

- Local: 90 backend tests pass; all five contract fixtures validate; Compose
  configuration passes; Next.js typecheck and production build pass.
- User-provided Docker run `echoguard-gate-c28cc3378d39`: a fresh isolated
  pipeline produced 250 unique cases, two clusters and two incidents. A restart
  and duplicate physical replay advanced raw Kafka offsets by 263 records.
  Spark committed through the new raw ends, the case writer reached the
  observed enriched and signal ends, and logical MongoDB counts stayed stable.
  Competing action, concurrent worker update, effect reconciliation after
  expiry, and closure-ledger repair checks passed.
- Earlier user-provided runs passed seeded ingestion, retrieval recall@5 on
  five synthetic queries, MongoDB checkpoint restart/approval, and HTTP
  session/proxy/approval/action checks. The HTTP outcome was
  `insufficient_data`, which is the expected honest response for that run.
- This process cannot access the Docker socket. The closure-key mismatch check
  added after the run above passes locally but has not had a Docker rerun.
  Phase 0's separate `infra/smoke.sh` result was not supplied.
- A new isolated local launcher, guided console traffic controls, and an
  accelerated synthetic monitoring path are implemented and locally checked.
  Their full Docker/browser walkthrough has not been run from this process.

## Phase-by-phase verdict

| Phase | Status against the full plan | Evidence and gap |
| --- | --- | --- |
| 0. Foundation | **Partial** | Modular backend/frontend, contracts, CI, Compose, and smoke script exist. The separate full-stack smoke gate and host capacity check are unrecorded. |
| 1. Ingestion | **Seeded gate passed** | Deterministic 265-record scenario, broker delivery, receipt, and readback passed. A DLQ redrive command is still missing; live source adapters are outside first-release scope. |
| 2. Streaming | **Partial** | Spark normalization, watermark deduplication, checkpointing, and seeded candidate signals work. Baseline is hard-coded at 2; no calibrated product-wide detector, late-data reconciliation, or measured latency/lag metrics. |
| 3. Correlation | **Partial** | Idempotent case/signal projection and same-bucket replay passed. Correlation is exact tenant/product/error/30-minute bucket only; semantic linking, incident continuity across buckets, reviewed merge/split, and complete lifecycle transition enforcement are missing. |
| 4. Knowledge | **Seeded gate passed; broader quality open** | Approved/expired/tenant filters and citation checks work on five labeled synthetic queries. The vector is a hand-built 256-dimensional domain feature hash, not a general embedding model. A meaningful held-out retrieval set is absent; the Atlas adapter is later scope. |
| 5. Workflow | **Controlled mock gate passed; full design partial** | LangGraph checkpoint, bounded proposal validation, approval interrupt, and fail-closed tests work. The runtime correctly uses the mock provider for v1. Measured grounding evaluation and fully typed per-node state/output contracts are missing. |
| 6. Console/action | **HTTP demo passed; outcome loop partial** | Next.js review, RBAC, proposal editing, approval, simulated action, and idempotent effect repair exist. A local local walkthrough now injects baseline and recovery traffic through the pipeline and gates demo closure on an improving trend; its full Docker/browser run is pending. There is no background outbox executor, stored monitoring observations, or telemetry/customer comparison. |
| 7. Hardening/evaluation | **Not complete** | The isolated replay gate is valuable. There is no 20-scenario held-out corpus, precision/recall report, 100 messages/s p95 test, Playwright suite, dashboards/alerts, backup/restore drill, or complete fault matrix. |
| 8. Release package | **Partial** | An isolated local launcher, local walkthrough, and optional HTTP acceptance command now exist. A tagged clean-machine release, browser walkthrough, and live verified-outcome run are still missing. |

## Correctness and operational risks to address first

1. **General closure is not tied to observed recovery.** Outside the isolated
   local scenario, the API requires a successful simulated action but does not
   require a completed monitoring window or an improving outcome. Its action
   lookup checks the incident version, while
   case-writer updates can change peak count and signal membership without
   incrementing that version. A reviewer can therefore close a newly changed
   incident on an older action if they submit its current hash. Bind closure to
   the authorized snapshot or define an explicit override with recorded reason,
   then require a monitored observation for the normal path.
2. **Detection and correlation are demo-specific.** The fixed baseline and
   exact 30-minute key can miss broad, low-volume, code-free, or cross-bucket
   incidents and can produce separate incidents for one continuing outage.
3. **Recovery is incomplete.** The case writer stops on a malformed enriched
   record, and no redrive/quarantine command exists. A pending action requires
   a request retry or manual intervention; there is no background executor or
   alert for stranded intents. The replay gate checks committed offsets after a
   planned restart, not a process kill at each write boundary.
4. **Outcome evidence is narrow.** `outcome()` computes two five-minute case
   counts on request; it does not persist observations, count distinct affected
   customers, use service/error telemetry, or distinguish a quiet pipeline from
   recovery with a positive health signal.
5. **Production controls are absent.** Local MongoDB and Kafka use plaintext
   unauthenticated service traffic; authentication uses a static local token
   registry. No real-data retention/deletion policy, TLS, managed identity,
   backup/restore, or operational alerting is implemented. Synthetic-only local
   use is the supported scope.

## Ordered completion path

1. Rerun the fresh gate after the closure-key change; run and record
   `infra/smoke.sh`. Add a fresh browser-driven review gate.
2. Implement a calibrated baseline, product-wide signals, late-data
   reconciliation, incident continuity, and reviewed semantic merge/split.
3. Complete the monitored outcome path and closure policy; seed post-action
   recovery traffic and prove close/reopen with auditable observations.
4. Add a durable action-intent executor, DLQ inspection/redrive, checkpoint
   migration procedure, and failure alerts.
5. Add observability and the 20-scenario held-out evaluation; report release
   metrics and fault/backup/restore results against the plan's explicit gates.
6. Package and run the Phase 8 clean-machine release demonstration.

The first release remains synthetic and uses simulated external actions.
