"""Generate, inspect, publish, and verify a seeded support traffic run."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from backend.ingest.publisher import prepare_message, producer_config, publish_messages
from backend.ingest.scenario import DEFAULT_START, ScenarioConfig, _atomic_write, read_scenario, write_scenario
from backend.ingest.verifier import consumer_config, verify_delivery


DEFAULT_DATASET = Path("data/scenarios/payment_spike.jsonl")
DEFAULT_MANIFEST = Path("data/scenarios/payment_spike.manifest.json")
DEFAULT_RECEIPT = Path("data/scenarios/payment_spike.receipt.json")


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise argparse.ArgumentTypeError("--start must include a UTC offset")
    return parsed


def _json_write(path: Path, value: dict) -> None:
    _atomic_write(path, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8"))


def _args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("generate", "inspect", "publish", "verify"):
        part = sub.add_parser(command)
        part.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
        part.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
        if command in ("publish", "verify"):
            part.add_argument("--receipt", type=Path, default=DEFAULT_RECEIPT)
            part.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    sub.choices["publish"].add_argument("--rate", type=float, default=50.0,
                                         help="Physical records per second; 0 sends as fast as possible")
    sub.choices["publish"].add_argument("--timeout", type=float, default=180.0)
    gen = sub.choices["generate"]
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--tenant", default="demo-tenant")
    gen.add_argument("--start", type=_utc, default=DEFAULT_START)
    gen.add_argument("--normal", type=int, default=60)
    gen.add_argument("--payments", type=int, default=150)
    gen.add_argument("--logins", type=int, default=40)
    gen.add_argument("--duplicates", type=int, default=8)
    gen.add_argument("--late", type=int, default=5)
    gen.add_argument("--force", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _args().parse_args(argv)
    if args.command == "generate":
        config = ScenarioConfig(seed=args.seed, tenant_id=args.tenant, start_time=args.start,
                                normal_count=args.normal, payment_count=args.payments,
                                login_count=args.logins, duplicate_count=args.duplicates,
                                late_count=args.late)
        manifest = write_scenario(config, args.dataset, args.manifest, force=args.force)
        print(json.dumps({"dataset": str(args.dataset), "manifest": str(args.manifest),
                          "record_count": manifest["record_count"], "sha256": manifest["dataset_sha256"]}))
        return 0

    records, manifest = read_scenario(args.dataset, args.manifest)
    if args.command == "inspect":
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    if args.command == "publish":
        from confluent_kafka import Producer

        if args.rate < 0 or args.timeout <= 0:
            raise ValueError("--rate must be nonnegative and --timeout must be positive")

        run_id = uuid.uuid4().hex
        failed_at = datetime.now(timezone.utc)
        messages = [prepare_message(record, manifest["config"]["tenant_id"], run_id, failed_at)
                    for record in records]
        expected_topics = dict(sorted(Counter(item.topic for item in messages).items()))
        if expected_topics != manifest["expected_topics"]:
            raise ValueError("Prepared topics differ from scenario manifest")
        receipt = {"receipt_version": "1.0.0", "status": "started", "run_id": run_id,
                   "dataset_sha256": manifest["dataset_sha256"], "failed_at": failed_at.isoformat(),
                   "expected_topics": expected_topics, "record_count": len(messages)}
        _json_write(args.receipt, receipt)
        try:
            delivered = publish_messages(messages, Producer(producer_config(args.bootstrap)),
                                         timeout_seconds=args.timeout,
                                         rate_per_second=args.rate or None)
        except Exception as exc:
            receipt.update({"status": "failed", "error_type": type(exc).__name__})
            _json_write(args.receipt, receipt)
            raise
        receipt.update({"status": "completed", "delivered_topics": delivered,
                        "published_at": datetime.now(timezone.utc).isoformat()})
        _json_write(args.receipt, receipt)
        print(json.dumps({"receipt": str(args.receipt), "run_id": run_id,
                          "delivered_topics": delivered}, sort_keys=True))
        return 0

    if args.command == "verify":
        from confluent_kafka import Consumer

        receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
        if receipt.get("status") != "completed":
            raise ValueError("Publish receipt is not completed")
        if receipt["dataset_sha256"] != manifest["dataset_sha256"]:
            raise ValueError("Receipt belongs to a different dataset")
        failed_at = datetime.fromisoformat(receipt["failed_at"])
        messages = [prepare_message(record, manifest["config"]["tenant_id"], receipt["run_id"], failed_at)
                    for record in records]
        group_id = f"echoguard-verify-{uuid.uuid4().hex}"
        counts = verify_delivery(Consumer(consumer_config(args.bootstrap, group_id)),
                                 messages, receipt["run_id"])
        if counts != manifest["expected_topics"] or counts != receipt["delivered_topics"]:
            raise ValueError("Kafka topic counts differ from manifest or receipt")
        print(json.dumps({"status": "verified", "run_id": receipt["run_id"],
                          "topics": counts}, sort_keys=True))
        return 0
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (FileNotFoundError, ValueError, TimeoutError, RuntimeError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)

