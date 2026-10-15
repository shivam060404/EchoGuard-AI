"""Spark Structured Streaming: raw normalization, DLQ, and candidate signals."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from backend.stream.rules import candidate_signal
from backend.stream.transform import (
    ENRICHED_TOPIC,
    RAW_TOPIC,
    compact_json,
    normalize_record,
    parse_enriched_record,
    stream_dlq_event,
)


def main() -> None:
    # Kept inside main so pure stream rules can be tested without a local Spark installation.
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    checkpoint_root = os.getenv("STREAM_CHECKPOINT_ROOT", "/opt/echoguard/checkpoints").rstrip("/")
    starting_offsets = os.getenv("STREAM_STARTING_OFFSETS", "earliest")
    if not bootstrap or not checkpoint_root or starting_offsets not in {"earliest", "latest"}:
        raise ValueError("Invalid stream configuration")

    spark = (
        SparkSession.builder.appName("EchoGuardStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_result_schema = StructType([
        StructField("event_json", StringType()),
        StructField("reason", StringType()),
        StructField("event_time", StringType()),
        StructField("tenant_id", StringType()),
        StructField("event_id", StringType()),
    ])

    @F.udf(raw_result_schema)
    def parse_raw(key: bytes | None, payload: bytes | None):
        event, reason = normalize_record(key, payload, datetime.now(timezone.utc))
        if event is None:
            return None, reason, None, None, None
        return compact_json(event), None, event["event_time"], event["tenant_id"], event["event_id"]

    @F.udf(StringType())
    def make_dlq(partition: int, offset: int, key: bytes | None, payload: bytes | None, reason: str):
        event = stream_dlq_event(
            partition=partition, offset=offset, key=key, payload=payload,
            reason=reason, failed_at=datetime.now(timezone.utc),
        )
        return compact_json(event)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
        .select("key", "value", "partition", "offset")
    )
    checked = (
        raw.withColumn("parsed", parse_raw(F.col("key"), F.col("value")))
        .select(
            "key", "value", "partition", "offset",
            F.col("parsed.event_json").alias("event_json"),
            F.col("parsed.reason").alias("reason"),
            F.col("parsed.event_time").alias("event_time"),
            F.col("parsed.tenant_id").alias("tenant_id"),
            F.col("parsed.event_id").alias("event_id"),
        )
    )
    dead_letters = (
        checked.filter(F.col("reason").isNotNull())
        .select(
            F.concat(F.lit("stream-dlq:"), F.col("partition").cast("string"),
                     F.lit(":"), F.col("offset").cast("string")).alias("key"),
            make_dlq("partition", "offset", "key", "value", "reason").alias("value"),
        )
    )
    dlq_query = (
        dead_letters.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", "stream.dlq.v1")
        .option("checkpointLocation", f"{checkpoint_root}/dlq-v1")
        .outputMode("append")
        .queryName("echoguard-stream-dlq")
        .start()
    )

    unique = (
        checked.filter(F.col("reason").isNull())
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
        .withWatermark("event_time_ts", "2 minutes")
        .dropDuplicatesWithinWatermark(["tenant_id", "event_id"])
    )
    enriched_query = (
        unique.select(F.col("key").cast("string").alias("key"),
                      F.col("event_json").alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", ENRICHED_TOPIC)
        .option("checkpointLocation", f"{checkpoint_root}/enriched-v1")
        .outputMode("append")
        .queryName("echoguard-enriched")
        .start()
    )

    enriched_fields = StructType([
        StructField("tenant_id", StringType()),
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product", StringType()),
        StructField("error_code", StringType()),
        StructField("event_time", StringType()),
    ])

    @F.udf(enriched_fields)
    def checked_enriched(key: bytes | None, payload: bytes | None):
        event = parse_enriched_record(key, payload)
        return tuple(event.get(name) for name in (
            "tenant_id", "event_id", "customer_id", "product", "error_code", "event_time"
        ))

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", ENRICHED_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
        .withColumn("checked", checked_enriched("key", "value"))
        .select("checked.*")
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
    )
    windows = (
        events.withWatermark("event_time_ts", "2 minutes")
        .groupBy(F.window("event_time_ts", "1 minute"), "tenant_id", "product", "error_code")
        .agg(
            F.collect_set("event_id").alias("event_ids"),
            F.collect_set("customer_id").alias("customer_ids"),
        )
    )

    @F.udf(StringType())
    def make_signal(window, tenant_id, product, error_code, event_ids, customer_ids):
        def aware(value: datetime) -> datetime:
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value

        signal = candidate_signal(
            tenant_id=tenant_id, product=product, error_code=error_code,
            window_start=aware(window.start), window_end=aware(window.end),
            event_ids=event_ids, customer_ids=customer_ids,
            detected_at=datetime.now(timezone.utc),
        )
        return compact_json(signal) if signal else None

    def write_signals(batch, batch_id: int) -> None:
        del batch_id  # Stable signal IDs are the downstream idempotency key.
        out = (
            batch.withColumn(
                "signal_json",
                make_signal("window", "tenant_id", "product", "error_code",
                            "event_ids", "customer_ids"),
            )
            .filter(F.col("signal_json").isNotNull())
            .select(
                F.concat_ws(":", F.col("tenant_id"),
                            F.get_json_object("signal_json", "$.signal_key")).alias("key"),
                F.col("signal_json").alias("value"),
            )
        )
        out.write.format("kafka").option("kafka.bootstrap.servers", bootstrap).option(
            "topic", "incident.signals.v1"
        ).save()

    signal_query = (
        windows.writeStream.foreachBatch(write_signals)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_root}/signals-v1")
        .queryName("echoguard-signals")
        .start()
    )
    queries = (signal_query, enriched_query, dlq_query)
    last_batches: dict[str, int] = {}
    try:
        while all(query.isActive for query in queries):
            for query in queries:
                progress = query.lastProgress
                if not progress:
                    continue
                name = query.name
                batch_id = progress["batchId"]
                if last_batches.get(name) == batch_id:
                    continue
                last_batches[name] = batch_id
                state = progress.get("stateOperators") or []
                print(json.dumps({
                    "query": name,
                    "batch_id": batch_id,
                    "input_rows": progress.get("numInputRows", 0),
                    "watermark": (progress.get("eventTime") or {}).get("watermark"),
                    "watermark_drops": sum(
                        item.get("numRowsDroppedByWatermark", 0) for item in state
                    ),
                }, sort_keys=True), flush=True)
            time.sleep(5)
        for query in queries:
            failure = query.exception()
            if failure is not None:
                raise RuntimeError(f"{query.name} failed: {failure}") from failure
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
