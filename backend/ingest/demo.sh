#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."

python3 -m backend.ingest.cli generate --dataset data/scenarios/payment_spike.jsonl \
  --manifest data/scenarios/payment_spike.manifest.json
python3 -m backend.ingest.cli inspect --dataset data/scenarios/payment_spike.jsonl \
  --manifest data/scenarios/payment_spike.manifest.json >/dev/null

docker compose up -d kafka topic-init
docker compose --profile demo build producer
docker compose --profile demo run --rm producer publish
docker compose --profile demo run --rm producer verify

