"""Replay-safe synthetic traffic used only by the local demonstration API."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256

from backend.contracts.validation import validate_event
from backend.workflow.models import digest


def traffic_record_id(tenant_id: str, request_key: str) -> str:
    return 'traffic-' + digest([tenant_id, request_key])[:32]


def build_events(record: dict) -> list[tuple[bytes, bytes]]:
    # Stay below the candidate threshold so this measurement does not create
    # a second incident while the reviewer is working on the seeded one.
    count = 12 if record['kind'] == 'baseline' else 3
    started = record['started_at']
    if isinstance(started, str):
        started = datetime.fromisoformat(started)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    timestamp = started.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
    items = []
    for index in range(count):
        token = sha256(f"{record['_id']}:{index}".encode()).hexdigest()[:24]
        customer_id = f'demo-{record["kind"]}-{token}'
        event = {
            'schema_version': '1.0.0', 'event_id': f'evt-{token}',
            'tenant_id': record['tenant_id'], 'trace_id': f'trace-{token}',
            'source': 'simulator', 'source_message_id': f'msg-{token}',
            'channel': ('web_chat', 'email', 'ticket')[index % 3],
            'customer_id': customer_id, 'event_time': timestamp, 'ingest_time': timestamp,
            'product': record['product'],
            'error_code': record['error_code'],
            'text': (f"Customers report a shared {record['product']} failure. {record['error_code']}."
                     if record['kind'] == 'baseline' and record['error_code']
                     else 'The account dashboard is working; I have a general question.'),
            'metadata': {'synthetic': True, 'scenario': f'local_{record["kind"]}'},
        }
        validate_event('support.raw.v1', event)
        items.append((f"{record['tenant_id']}:{customer_id}".encode(),
                      json.dumps(event, sort_keys=True, separators=(',', ':')).encode()))
    return items


def publish_events(bootstrap: str, events: list[tuple[bytes, bytes]]) -> None:
    from confluent_kafka import Producer

    failures = []
    producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True,
                         'acks': 'all', 'message.timeout.ms': 30000})
    def delivered(error, _message):
        if error is not None:
            failures.append(str(error))
    for key, value in events:
        producer.produce('support.raw.v1', key=key, value=value, on_delivery=delivered)
        producer.poll(0)
    remaining = producer.flush(35)
    if remaining or failures:
        raise RuntimeError(f'Demo traffic delivery incomplete: pending={remaining}, failures={len(failures)}')
