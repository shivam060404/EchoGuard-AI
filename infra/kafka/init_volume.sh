#!/bin/sh
set -eu

# Docker can create a fresh named-volume root owned by root. The broker runs
# as 1000:1000; prepare only its mount root before starting the broker.
# Never delete metadata or recursively rewrite an existing Kafka log tree.
kafka_data_dir=/tmp/kraft-combined-logs
mkdir -p "$kafka_data_dir"
chown 1000:1000 "$kafka_data_dir"
chmod u+rwx "$kafka_data_dir"
printf '%s\n' 'Kafka data directory initialized for broker UID/GID 1000:1000.'
