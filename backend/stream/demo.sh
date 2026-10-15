#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."

docker compose --profile stream build stream-job
docker compose --profile stream up -d stream-job
backend/ingest/demo.sh
if ! docker compose --profile stream exec -T stream-job \
    python3 -m backend.stream.verify --bootstrap kafka:19092 --timeout 300; then
    docker compose --profile stream logs --tail=100 stream-job
    exit 1
fi
