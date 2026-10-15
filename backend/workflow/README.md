# Controlled incident workflow

Phase 5 implements `triage → investigate → retrieve → plan → validate → human review` using LangGraph. Each node checkpoints to MongoDB with synchronous durability. The graph stops at a durable interrupt until a trusted reviewer supplies a decision. Approval records authorization for a proposal; simulated action execution and the authenticated review console belong to Phase 6.

## Run the integration gate

From the repository root with Docker running:

```sh
backend/workflow/demo.sh
```

This gate uses the isolated `echoguard_workflow_demo` database. It seeds approved synthetic knowledge and incident evidence, runs the Phase 4 retrieval evaluation, and starts a new workflow with a unique request key. One container exits while the workflow waits for review. A second container restores its checkpoint, submits concurrent synthetic reviewer decisions, repeats approval, and checks that exactly one approval audit exists. Success prints `status: verified`, `checkpoint_restart: passed`, and `concurrent_review: passed`. It does not erase existing databases or contact external services.

## Investigate a stored incident

The CLI operates on the Phase 3 `echoguard` database by default. It reads the incident and only cases referenced by that incident's signals, within its tenant and product. Replace the example incident ID and use a new request key for a new investigation.

```sh
docker compose --profile workflow run --rm \
  -e WORKFLOW_ACTOR=local-operator -e WORKFLOW_TENANT=demo-tenant -e WORKFLOW_ROLE=operator \
  workflow-tool start --incident-id incident-EXAMPLE --request-key investigation-001
```

Inspect the returned `run_id`, `proposal`, `proposal_hash`, `expires_at`, evidence, and usage. An identical start request returns the same run. An existing request key cannot be assigned to another incident.

```sh
docker compose --profile workflow run --rm \
  -e WORKFLOW_ACTOR=local-reviewer -e WORKFLOW_TENANT=demo-tenant -e WORKFLOW_ROLE=reviewer \
  workflow-tool status --run-id wf-EXAMPLE

docker compose --profile workflow run --rm \
  -e WORKFLOW_ACTOR=local-reviewer -e WORKFLOW_TENANT=demo-tenant -e WORKFLOW_ROLE=reviewer \
  workflow-tool review --run-id wf-EXAMPLE --decision approve --proposal-hash HASH_FROM_REVIEW
```

Use `--decision reject` to reject. These environment values are trusted local operator configuration, not public authentication. There are no new HTTP endpoints in this phase. All FastAPI routers continue to be registered in `backend/api/main.py`; the authenticated API boundary is a Phase 6 deliverable.

## Validation and failure behavior

- Strict Pydantic proposal schemas reject extra properties, unknown actions, invalid confidence, and unrecognized evidence. Each recommended step must exactly quote a retrieved approved chunk and cite its ID. Hypotheses are explicitly unconfirmed and require human judgment.
- A coded incident needs relevant knowledge mentioning that code. Missing/invalid evidence, low confidence, unsupported citations, or failed retrieval/model calls end in `needs_review` with no approvable proposal. A new request key starts a fresh investigation after remediation.
- Approval checks tenant and reviewer role, proposal hash, a 24-hour expiry, and the complete incident snapshot. This catches count/status/signal changes even when the incident version was not incremented. Knowledge approval and quoted text are rechecked at approval time. Rejection remains available after expiry or knowledge revocation.
- The graph has no execution tools. Model text cannot supply URLs, shell commands, or API arguments. The current provider is a deterministic extractive mock; its injection tests establish this mock's behavior, not a universal defense for future live models.
- Model timeout/connection failures receive at most two attempts. The provider interface requires adapters to enforce a 15-second transport deadline. Only the mock is shipped; no unbounded network adapter runs. Usage records provider/model/prompt versions, estimated token counts, attempts, and zero mock cost. Failed requests with no provider usage response have unknown cost. Checkpoint replay can repeat an unfinished provider call; accounting is a workflow observation, not a billing ledger.

## Persistence and recovery

`workflow_checkpoints_v1` and `workflow_checkpoint_writes_v1` hold LangGraph state. `workflow_runs` holds request identity, the current projection, writer ownership, and an embedded audit history. One atomic run-document update writes the projection and its audit entry. If a crash occurs after a graph checkpoint but before that update, `status`, an identical `start`, or a retried decision repairs the projection from the checkpoint.

A MongoDB compare-and-set lock serializes writers per run. Locks are deliberately not stolen on a timer: a slow live process must not race another graph writer. A hard process kill may leave an abandoned lock. After stopping all processes using that run, a trusted operator can release the exact reported owner token:

```sh
docker compose --profile workflow run --rm \
  -e WORKFLOW_ACTOR=local-operator -e WORKFLOW_TENANT=demo-tenant -e WORKFLOW_ROLE=operator \
  workflow-tool recover-lock --run-id wf-EXAMPLE --expected-owner OWNER_TOKEN --workers-stopped
```

Then retry the original start request or inspect the run. Recovery is audited. Preserve the graph version for in-flight workflows; incompatible graph/prompt/schema changes need a new version and an explicit migration. The later action executor must revalidate the proposal and incident immediately before execution; Phase 5 approval alone is not an execution guarantee.

The implementation follows LangGraph's [interrupt/resume contract](https://docs.langchain.com/oss/python/langgraph/interrupts) and the official [MongoDB checkpoint saver](https://langchain-mongodb.readthedocs.io/en/latest/langgraph_checkpoint_mongodb/saver/langgraph.checkpoint.mongodb.saver.MongoDBSaver.html).
