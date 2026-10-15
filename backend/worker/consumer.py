"""At-least-once Kafka consumer with idempotent MongoDB projections."""

from __future__ import annotations

import json
import os
import sys

from backend.worker.store import CaseStore


def _required_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} must be nonempty")
    return value


def run() -> None:
    from confluent_kafka import Consumer
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    bootstrap = _required_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    mongo_uri = _required_env("MONGO_URI", "mongodb://mongo:27017")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    store = CaseStore(client.echoguard)
    store.ensure_indexes()
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "echoguard-case-writer-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    consumer.subscribe(["support.enriched.v1", "incident.signals.v1"])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            # Never commit malformed or unpersisted input. An operator can inspect and
            # redrive the offset after fixing the contract or storage fault.
            event = json.loads(message.value())
            if message.topic() == "support.enriched.v1":
                expected_key = f"{event['tenant_id']}:{event['customer_id']}".encode()
                if message.key() != expected_key:
                    raise ValueError("Case Kafka key does not match tenant/customer")
                subject = store.write_case(event)
            elif message.topic() == "incident.signals.v1":
                expected_key = f"{event['tenant_id']}:{event['signal_key']}".encode()
                if message.key() != expected_key:
                    raise ValueError("Signal Kafka key does not match tenant/signal")
                subject = store.write_signal(event)
            else:
                raise ValueError(f"Unexpected topic: {message.topic()}")
            consumer.store_offsets(message=message)
            consumer.commit(asynchronous=False)
            print(json.dumps({"topic": message.topic(), "partition": message.partition(),
                              "offset": message.offset(), "subject_id": subject}), flush=True)
    except PyMongoError as exc:
        print(f"MongoDB projection failed; Kafka offset remains uncommitted: {exc}", file=sys.stderr)
        raise
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    run()
