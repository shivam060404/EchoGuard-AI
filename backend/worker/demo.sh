#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."

docker compose --profile stream --profile worker up -d --build stream-job case-writer
backend/ingest/demo.sh
if ! docker compose --profile worker exec -T case-writer \
    python -m backend.worker.verify --timeout 300; then
    docker compose --profile worker logs --tail=100 case-writer
    docker compose --profile stream logs --tail=100 stream-job
    exit 1
fi
