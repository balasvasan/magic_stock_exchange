#!/usr/bin/env python3
"""
JOB-12 — realtime_alert_persistence (CDE / Spark Structured Streaming)
======================================================================
Lands real-time alerts from JOB-10 (Flink CEP) and JOB-11 (SSB) into
gold.realtime_alert_stream so they're queryable from CDW for analysts
and cross-checkable against the JOB-08 batch alert_candidates.

PRD reference: §7 (JOB-12). Closes the persistence loop for the
streaming detection path.

Source: argus.${STUDENT_ID}.realtime_alerts.v1 (written by JOB-10 + JOB-11)
Sink:   argus_${STUDENT_ID}_gold.realtime_alert_stream (Iceberg COW/Parquet,
        partitioned by fired_date)

Trigger: 10 seconds (matches the existing Bronze ingest cadence for
operational simplicity — same dashboards, same SLAs).

Resource names (topic, consumer group, table, app name) resolved from
src.common.naming using ${STUDENT_ID}.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

from src.common.naming import topic, consumer_group, fqtn, cde_job


def main() -> int:
    spark = (SparkSession.builder
             .appName(cde_job("realtime_alert_persistence"))
             .getOrCreate())

    # Schema of argus.realtime_alerts.v1 (PRD §5 #9)
    alert_schema = StructType([
        StructField("alert_id",             StringType(), False),
        StructField("fired_ts",             LongType(),   False),
        StructField("source_engine",        StringType(), False),  # FLINK | SSB
        StructField("rule_id",              StringType(), False),
        StructField("severity",             StringType(), False),
        StructField("pattern_type",         StringType(), False),
        StructField("member_firm_id",       StringType(), True),
        StructField("trader_id",            StringType(), True),
        StructField("instrument_code",      StringType(), True),
        StructField("underlying_code",      StringType(), True),
        StructField("window_start_ts",      LongType(),   True),
        StructField("window_end_ts",        LongType(),   True),
        StructField("evidence_json",        StringType(), True),
        StructField("detection_latency_ms", LongType(),   True),
    ])

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", spark.conf.get("spark.kafka.bootstrap.servers"))
           .option("subscribe", topic("realtime_alerts.v1"))
           .option("kafka.group.id", consumer_group("realtime_persistence_v1"))
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"), alert_schema).alias("a"))
              .select("a.*")
              .withColumn("fired_date",
                          F.to_date(F.from_unixtime(F.col("fired_ts") / 1_000_000)))
              .withColumn("ingested_at", F.current_timestamp()))

    target = fqtn("gold", "realtime_alert_stream")
    checkpoint_path = (f"s3a://{spark.conf.get('spark.argus.bucket_name')}"
                       f"/checkpoints/JOB-12/")

    query = (parsed.writeStream
             .format("iceberg")
             .outputMode("append")
             .option("checkpointLocation", checkpoint_path)
             .trigger(processingTime="10 seconds")
             .toTable(target))

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
