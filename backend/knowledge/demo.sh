#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."

docker compose up -d mongo
docker compose --profile knowledge build knowledge-tool
docker compose --profile knowledge run --rm knowledge-tool python -m backend.knowledge.seed
docker compose --profile knowledge run --rm knowledge-tool python -m backend.knowledge.evaluate
