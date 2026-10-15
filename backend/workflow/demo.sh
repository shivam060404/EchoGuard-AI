#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
# Each invocation gets a fresh request key; no old approved run can satisfy this gate.
workflow_demo_key="phase5-$(date +%s)-$$"
docker compose --profile workflow build workflow-tool
docker compose --profile workflow run --rm --entrypoint python workflow-tool \
    -m backend.workflow.demo prepare --request-key "$workflow_demo_key"
# New process and client: resume only from the durable MongoDB checkpoint.
docker compose --profile workflow run --rm --entrypoint python workflow-tool \
    -m backend.workflow.demo resume --request-key "$workflow_demo_key"
