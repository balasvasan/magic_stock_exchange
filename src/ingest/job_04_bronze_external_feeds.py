#!/usr/bin/env python3
"""
JOB-04 — bronze_external_feeds
==============================
Multi-topic Spark Structured Streaming consumer for the external feeds
plus the PRATEEK reference and surveillance-state streams.

PRD reference: §7 (JOB-04).
Sources (per-student namespaced):
    - argus.${SID}.bbo.v1                (12 partitions)  → external_feeds (source='BBO')
    - argus.${SID}.regulator.feed.v1     (3 partitions)   → external_feeds (source='SEBI')
    - argus.${SID}.news.v1               (6 partitions)   → external_feeds (source='NEWS')
    - argus.${SID}.instrument.cdc.v1     (3 partitions, compacted) → instrument_cdc
    - argus.${SID}.surveillance.state.v1 (6 partitions)   → instrument_cdc
Sinks:
    - argus_${SID}_bronze.external_feeds   (BBO + SEBI + news, source-tagged)
    - argus_${SID}_bronze.instrument_cdc   (PRATEEK reference + corp actions + ESM/ASM state)

Two streams in one job because the volumes are modest and the ops
overhead of separate jobs isn't justified — keeps the JOB count at the
PRD's 9.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F

from src.common.naming import (
    topic, fqtn, consumer_group, cde_job, s3_bucket,
)


def stream_external(spark: SparkSession, bucket: str) -> None:
    """BBO + SEBI + news → external_feeds Bronze."""
    subscribe_list = ",".join([
        topic("bbo.v1"),
        topic("regulator.feed.v1"),
        topic("news.v1"),
    ])
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
           .option("subscribe", subscribe_list)
           .option("kafka.group.id", consumer_group("bronze.external_feeds"))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 100_000)
           .load())

    enriched = (raw
                .withColumn("source",
                            F.when(F.col("topic") == topic("bbo.v1"), F.lit("BBO"))
                             .when(F.col("topic") == topic("regulator.feed.v1"), F.lit("SEBI"))
                             .otherwise(F.lit("NEWS")))
                .select(
                    F.col("source"),
                    F.col("key").cast("string").alias("event_id"),
                    F.col("timestamp").alias("event_ts"),
                    F.current_timestamp().alias("ts_ingest"),
                    F.get_json_object(F.col("value").cast("string"),
                                      "$.instrument_code").alias("instrument_code"),
                    F.get_json_object(F.col("value").cast("string"),
                                      "$.venue").alias("venue"),
                    F.col("value").cast("string").alias("payload"),
                    F.col("value").cast("string").alias("raw_message"),
                    F.to_date(F.current_timestamp()).alias("ingest_date"),
                ))

    (enriched.writeStream
     .format("iceberg")
     .outputMode("append")
     .option("path", fqtn("bronze", "external_feeds"))
     .option("checkpointLocation",
             f"s3a://{bucket}/checkpoints/job_04_external/")
     .trigger(processingTime="20 seconds")
     .start())


def stream_instrument(spark: SparkSession, bucket: str) -> None:
    """instrument.cdc + surveillance.state → instrument_cdc Bronze."""
    subscribe_list = ",".join([
        topic("instrument.cdc.v1"),
        topic("surveillance.state.v1"),
    ])
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
           .option("subscribe", subscribe_list)
           .option("kafka.group.id", consumer_group("bronze.instrument_cdc"))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 20_000)
           .load())

    enriched = (raw
                .withColumn("event_kind",
                            F.when(F.col("topic") == topic("surveillance.state.v1"),
                                   F.lit("SURVEILLANCE_STATE"))
                             .otherwise(F.lit("INSTRUMENT")))
                .select(
                    F.lit("UPDATE").alias("cdc_op"),  # compacted topic, treat as upsert
                    F.col("timestamp").alias("cdc_ts"),
                    F.current_timestamp().alias("ts_ingest"),
                    F.col("event_kind"),
                    F.col("key").cast("string").alias("instrument_code"),
                    F.lit(None).cast("string").alias("instrument_type"),
                    F.lit(None).cast("string").alias("underlying_code"),
                    F.lit(None).cast("date").alias("expiry_date"),
                    F.lit(None).cast("decimal(18,4)").alias("strike_price"),
                    F.lit(None).cast("bigint").alias("lot_size"),
                    F.lit(None).cast("decimal(8,4)").alias("tick_size"),
                    F.lit(None).cast("string").alias("corp_action_type"),
                    F.lit(None).cast("string").alias("corp_action_ratio"),
                    F.lit(None).cast("date").alias("corp_action_date"),
                    F.get_json_object(F.col("value").cast("string"),
                                      "$.esm_flag").alias("esm_flag"),
                    F.get_json_object(F.col("value").cast("string"),
                                      "$.asm_flag").alias("asm_flag"),
                    F.get_json_object(F.col("value").cast("string"),
                                      "$.circuit_band_pct").cast("decimal(5,2)").alias("circuit_band_pct"),
                    F.col("value").cast("string").alias("raw_payload"),
                    F.to_date(F.current_timestamp()).alias("ingest_date"),
                ))

    (enriched.writeStream
     .format("iceberg")
     .outputMode("append")
     .option("path", fqtn("bronze", "instrument_cdc"))
     .option("checkpointLocation",
             f"s3a://{bucket}/checkpoints/job_04_instrument/")
     .trigger(processingTime="60 seconds")
     .start())


def main() -> None:
    bucket = s3_bucket()
    spark = (SparkSession.builder
             .appName(cde_job("bronze.external_feeds"))
             .getOrCreate())

    stream_external(spark, bucket)
    stream_instrument(spark, bucket)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
