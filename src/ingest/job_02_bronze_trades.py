#!/usr/bin/env python3
"""
JOB-02 — bronze_trades_ingest
=============================
Spark Structured Streaming consumer for NIPATAN executed-trade feed.

PRD reference: §7 (JOB-02).
Source: Kafka topic `argus.${STUDENT_ID}.trades.v1` (24 partitions)
Sink:   Iceberg table `argus_${STUDENT_ID}_bronze.trades_raw`
Cadence: continuous streaming, append-only

Lower volume than orders (~280M/day vs 3.5B/day) since only fills produce
a trade event. Trade events arrive 50–200ms behind their source orders.

Resource names (topic, table, checkpoint) resolved from src.common.naming
using ${STUDENT_ID}.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType, BooleanType,
)

from src.common.naming import (
    topic, fqtn, consumer_group, cde_job, s3_bucket,
)

TRADE_SCHEMA = StructType([
    StructField("trade_id",            StringType(),  nullable=False),
    StructField("ts_us",                LongType(),   nullable=False),
    StructField("instrument_code",      StringType(), nullable=False),
    StructField("buy_member_firm_id",   StringType(), nullable=False),
    StructField("sell_member_firm_id",  StringType(), nullable=False),
    StructField("buy_trader_id",        StringType(), nullable=True),
    StructField("sell_trader_id",       StringType(), nullable=True),
    StructField("buy_investor_acct",    StringType(), nullable=True),
    StructField("sell_investor_acct",   StringType(), nullable=True),
    StructField("exec_price",           DecimalType(18, 4), nullable=False),
    StructField("exec_qty",             LongType(),   nullable=False),
    StructField("settlement_date",      StringType(), nullable=True),
    StructField("clearing_member_flag", StringType(), nullable=True),
    StructField("is_self_trade",        BooleanType(), nullable=True),
    StructField("planted_case_idx",     LongType(),   nullable=True),
])


def main() -> None:
    bucket = s3_bucket()
    spark = (SparkSession.builder
             .appName(cde_job("bronze.trades_ingest"))
             .config("spark.sql.streaming.checkpointLocation",
                     f"s3a://{bucket}/checkpoints/job_02_bronze_trades/")
             .getOrCreate())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
           .option("subscribe", topic("trades.v1"))
           .option("kafka.group.id", consumer_group("bronze.trades"))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 200_000)
           .load())

    parsed = (raw
              .select(F.col("value").cast("string").alias("raw_payload"),
                      F.col("timestamp").alias("ts_ingest"),
                      F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("evt"))
              .withColumn("is_valid",
                          F.col("evt").isNotNull() &
                          F.col("evt.trade_id").isNotNull() &
                          F.col("evt.ts_us").isNotNull()))

    valid = (parsed.filter("is_valid")
             .select(
                 F.col("evt.trade_id").alias("trade_id"),
                 F.col("evt.ts_us").alias("ts_us"),
                 F.col("ts_ingest"),
                 F.col("evt.instrument_code").alias("instrument_code"),
                 F.col("evt.buy_member_firm_id").alias("buy_member_firm_id"),
                 F.col("evt.sell_member_firm_id").alias("sell_member_firm_id"),
                 F.col("evt.buy_investor_acct").alias("buy_investor_acct"),
                 F.col("evt.sell_investor_acct").alias("sell_investor_acct"),
                 F.col("evt.buy_trader_id").alias("buy_trader_id"),
                 F.col("evt.sell_trader_id").alias("sell_trader_id"),
                 F.col("evt.exec_price").alias("exec_price"),
                 F.col("evt.exec_qty").alias("exec_qty"),
                 F.to_date(F.col("evt.settlement_date")).alias("settlement_date"),
                 F.col("evt.clearing_member_flag").alias("clearing_member_flag"),
                 F.col("raw_payload"),
                 F.to_date(F.col("ts_ingest")).alias("ingest_date")))

    (valid.writeStream
     .format("iceberg")
     .outputMode("append")
     .option("path", fqtn("bronze", "trades_raw"))
     .trigger(processingTime="10 seconds")
     .start())

    dlq = parsed.filter("NOT is_valid").select(F.col("raw_payload").alias("value"))
    (dlq.writeStream
     .format("kafka")
     .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
     .option("topic", topic("trades.dlq"))
     .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
