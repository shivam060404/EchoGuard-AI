"""Prove replay consumption against Kafka end offsets and Spark commit logs."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import time

RAW = 'support.raw.v1'
ENRICHED = 'support.enriched.v1'
WORKER_INPUTS = ('support.enriched.v1', 'incident.signals.v1')


def topic_ends(consumer, topics):
    from confluent_kafka import TopicPartition

    result = {}
    for topic in topics:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        details = metadata.topics.get(topic)
        if details is None or details.error:
            raise RuntimeError(f'Topic unavailable: {topic}')
        result[topic] = {}
        for partition in sorted(details.partitions):
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10,
                                                     cached=False)
            result[topic][str(partition)] = high
    return result


def committed_spark_offsets(checkpoint: Path, topic: str):
    commits = checkpoint / 'commits'
    offsets = checkpoint / 'offsets'
    if not commits.is_dir() or not offsets.is_dir():
        return {}
    for batch in sorted((int(item.name) for item in commits.iterdir() if item.name.isdecimal()), reverse=True):
        path = offsets / str(batch)
        if not path.is_file():
            continue
        lines = path.read_text().splitlines()
        if not lines or lines[0] != 'v1':
            raise ValueError('Unrecognized Spark checkpoint offset format')
        # Spark stores metadata followed by one JSON offset line per source.
        for line in lines[2:]:
            value = json.loads(line)
            if isinstance(value, dict) and topic in value:
                return {str(partition): int(offset) for partition, offset in value[topic].items()}
    return {}


def covers(actual, expected):
    # An empty Kafka partition needs no consumer-group commit and may not be
    # listed in a Spark checkpoint batch.
    return any(target > 0 for target in expected.values()) and all(
        target <= 0 or actual.get(partition, -1) >= target
        for partition, target in expected.items())


def verify_spark(consumer, root: Path, timeout: float):
    target = topic_ends(consumer, (RAW,))[RAW]
    if not any(target.values()):
        raise AssertionError('Raw topic is empty')
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        enriched = committed_spark_offsets(root / 'enriched-v1', RAW)
        dlq = committed_spark_offsets(root / 'dlq-v1', RAW)
        if covers(enriched, target) and covers(dlq, target):
            enriched_end = topic_ends(consumer, (ENRICHED,))[ENRICHED]
            signals = committed_spark_offsets(root / 'signals-v1', ENRICHED)
            if covers(signals, enriched_end):
                return {'status': 'verified', 'layer': 'spark', 'raw_end': target,
                        'enriched_end': enriched_end, 'enriched_committed': enriched,
                        'dlq_committed': dlq, 'signals_committed': signals}
        time.sleep(2)
    raise TimeoutError(f'Spark did not commit raw offsets: target={target}, '
                       f'enriched={enriched}, dlq={dlq}, signals={signals if "signals" in locals() else {}}')


def verify_worker(consumer, timeout: float):
    from confluent_kafka import TopicPartition

    target = topic_ends(consumer, WORKER_INPUTS)
    if not all(any(offsets.values()) for offsets in target.values()):
        raise AssertionError('Worker input topic is empty')
    partitions = [TopicPartition(topic, int(partition))
                  for topic, offsets in target.items() for partition in offsets]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        positions = consumer.committed(partitions, timeout=10)
        committed = {topic: {} for topic in target}
        for item in positions:
            committed[item.topic][str(item.partition)] = item.offset
        if all(covers(committed[topic], offsets) for topic, offsets in target.items()):
            return {'status': 'verified', 'layer': 'worker', 'input_end': target,
                    'committed': committed}
        time.sleep(2)
    raise TimeoutError(f'Case writer did not commit inputs: target={target}, committed={committed}')


def main():
    from confluent_kafka import Consumer

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('snapshot', 'spark', 'worker'))
    parser.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:19092'))
    parser.add_argument('--checkpoint-root', type=Path,
                        default=Path(os.getenv('STREAM_CHECKPOINT_ROOT', '/opt/echoguard/checkpoints')))
    parser.add_argument('--timeout', type=float, default=300)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    group = 'echoguard-case-writer-v1' if args.mode == 'worker' else 'echoguard-offset-observer'
    consumer = Consumer({'bootstrap.servers': args.bootstrap, 'group.id': group,
                         'enable.auto.commit': False, 'allow.auto.create.topics': False})
    try:
        if args.mode == 'snapshot':
            result = {'status': 'observed', 'topic_end': topic_ends(consumer, (RAW, *WORKER_INPUTS))}
        elif args.mode == 'spark':
            result = verify_spark(consumer, args.checkpoint_root, args.timeout)
        else:
            result = verify_worker(consumer, args.timeout)
        print(json.dumps(result, sort_keys=True))
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
