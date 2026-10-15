#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

docker compose ps
curl --fail --silent --show-error http://localhost:${ECHOGUARD_API_PORT:-8000}/health/live
curl --fail --silent --show-error http://localhost:${ECHOGUARD_API_PORT:-8000}/health/ready
curl --fail --silent --show-error http://localhost:${ECHOGUARD_WEB_PORT:-3000}/ >/dev/null
topics="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list)"
for topic in support.raw.v1 support.enriched.v1 incident.signals.v1 ingest.dlq.v1 stream.dlq.v1; do
  printf '%s\n' "$topics" | grep -Fx "$topic" >/dev/null
done
docker compose exec -T mongo mongosh --quiet --eval "db.adminCommand('ping').ok"
docker compose exec -T spark /opt/spark/bin/spark-submit --version
python3 -m backend.contracts.check_contracts

