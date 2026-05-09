#!/usr/bin/env python3
"""
JOB-01 — bronze_orders_ingest
=============================
Spark Structured Streaming consumer for TARANG matching-engine telemetry.

PRD reference: §7 (JOB-01), closes ARG-1.
Source: Kafka topic `argus.${STUDENT_ID}.orders.v1` (48 partitions)
Sink:   Iceberg table `argus_${STUDENT_ID}_bronze.orders_raw` (MOR/ORC, partitioned by ingest_date)
Cadence: continuous streaming, append-only

Schema validation runs inline; malformed records are routed to the DLQ topic
`argus.${STUDENT_ID}.orders.dlq` instead of being dropped silently. The job
carries forward the original Kafka payload as raw_payload so on-the-fly
replay is possible.

All resource names (topic, table, checkpoint path) are resolved from
src.common.naming using ${STUDENT_ID}. STUDENT_ID must be set in the
environment before running.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType,
)

from src.common.naming import (
    topic, fqtn, consumer_group, cde_job, s3_bucket, get_student_id,
)

ORDER_SCHEMA = StructType([
    StructField("event_id",         StringType(),    nullable=False),
    StructField("ts_us",             LongType(),     nullable=False),
    StructField("member_firm_id",    StringType(),   nullable=False),
    StructField("trader_id",         StringType(),   nullable=True),
    StructField("instrument_code",   StringType(),   nullable=False),
    StructField("side",              StringType(),   nullable=False),
    StructField("order_type",        StringType(),   nullable=False),
    StructField("qty",               LongType(),     nullable=False),
    StructField("price",             DecimalType(18, 4), nullable=True),
    StructField("action",            StringType(),   nullable=False),
    StructField("parent_order_id",   StringType(),   nullable=True),
    StructField("planted_case_idx",  LongType(),     nullable=True),
])


def main() -> None:
    sid = get_student_id()
    bucket = s3_bucket()
    spark = (SparkSession.builder
             .appName(cde_job("bronze.orders_ingest"))
             .config("spark.sql.streaming.checkpointLocation",
                     f"s3a://{bucket}/checkpoints/job_01_bronze_orders/")
             .getOrCreate())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
           .option("subscribe", topic("orders.v1"))
           .option("kafka.group.id", consumer_group("bronze.orders"))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 500_000)
           .load())

    # Parse JSON; rows that fail validation get is_valid=false and are split off
    parsed = (raw
              .select(F.col("value").cast("string").alias("raw_payload"),
                      F.col("timestamp").alias("ts_ingest"),
                      F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("evt"))
              .withColumn("is_valid",
                          F.col("evt").isNotNull() &
                          F.col("evt.event_id").isNotNull() &
                          F.col("evt.ts_us").isNotNull()))

    # Valid → orders_raw
    valid = (parsed.filter("is_valid")
             .select(
                 F.col("evt.event_id").alias("event_id"),
                 F.col("evt.ts_us").alias("ts_us"),
                 F.col("ts_ingest"),
                 F.col("evt.member_firm_id").alias("member_firm_id"),
                 F.col("evt.trader_id").alias("trader_id"),
                 F.col("evt.instrument_code").alias("instrument_code"),
                 F.col("evt.side").alias("side"),
                 F.col("evt.order_type").alias("order_type"),
                 F.col("evt.qty").alias("qty"),
                 F.col("evt.price").alias("price"),
                 F.col("evt.action").alias("action"),
                 F.col("evt.parent_order_id").alias("parent_order_id"),
                 F.lit(None).cast("string").alias("book_state_after"),
                 F.col("raw_payload"),
                 F.to_date(F.col("ts_ingest")).alias("ingest_date")))

    (valid.writeStream
     .format("iceberg")
     .outputMode("append")
     .option("path", fqtn("bronze", "orders_raw"))
     .trigger(processingTime="10 seconds")
     .start())

    # Invalid → DLQ
    dlq = (parsed.filter("NOT is_valid")
           .select(F.col("raw_payload").alias("value"),
                   F.lit(topic("orders.dlq")).alias("topic"))
           .selectExpr("CAST(value AS STRING) AS value", "topic"))
    (dlq.writeStream
     .format("kafka")
     .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
     .option("topic", topic("orders.dlq"))
     .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
