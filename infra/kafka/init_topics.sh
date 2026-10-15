#!/bin/sh
set -eu

broker="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
topic_tool=/opt/kafka/bin/kafka-topics.sh

create_topic() {
  topic="$1"
  partitions="$2"
  retention_ms="$3"
  "$topic_tool" --bootstrap-server "$broker" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=$retention_ms"
  description="$("$topic_tool" --bootstrap-server "$broker" --describe --topic "$topic")"
  printf '%s\n' "$description"
  printf '%s\n' "$description" | grep -Eq "PartitionCount: ${partitions}([[:space:]]|$)" || {
    echo "Topic $topic has an unexpected partition count" >&2
    exit 1
  }
}

create_topic support.raw.v1 3 604800000
create_topic support.enriched.v1 3 604800000
create_topic incident.signals.v1 3 604800000
create_topic ingest.dlq.v1 3 1209600000

