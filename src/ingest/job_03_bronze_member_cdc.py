#!/usr/bin/env python3
"""
JOB-03 — bronze_member_cdc
==========================
Spark Structured Streaming consumer for KAVACH KYC + member-master CDC.

PRD reference: §7 (JOB-03).
Source: Kafka topic `argus.${STUDENT_ID}.member.cdc.v1` (compacted, 6 partitions)
Sink:   Iceberg table `argus_${STUDENT_ID}_bronze.member_cdc`

This stream carries PII — investor PAN, names, email, mobile — so it
must land in a table whose columns are Atlas-classified PII_HIGH /
PII_LOW in Module 7. Do not re-route or copy this stream to any other
table without re-applying the classifications.

In production this stream is fed by Debezium against the KAVACH Oracle
DB; the synthetic generator simulates the same event shape.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DecimalType,
)

from src.common.naming import (
    topic, fqtn, consumer_group, cde_job, s3_bucket,
)

CDC_SCHEMA = StructType([
    StructField("cdc_op",             StringType(),  nullable=False),  # INSERT | UPDATE | DELETE
    StructField("cdc_ts",             StringType(),  nullable=False),  # ISO timestamp
    StructField("member_firm_id",     StringType(),  nullable=False),
    StructField("member_firm_name",   StringType(),  nullable=True),
    StructField("sebi_registration",  StringType(),  nullable=True),
    StructField("capital_adequacy",   DecimalType(18, 2), nullable=True),
    StructField("suspension_history", StringType(),  nullable=True),
    StructField("trader_id",          StringType(),  nullable=True),
    StructField("trader_name",        StringType(),  nullable=True),     # PII_HIGH
    StructField("investor_acct",      StringType(),  nullable=True),     # PII_LOW
    StructField("investor_pan",       StringType(),  nullable=True),     # PII_HIGH
    StructField("investor_email",     StringType(),  nullable=True),     # PII_HIGH
    StructField("investor_mobile",    StringType(),  nullable=True),     # PII_HIGH
    StructField("investor_demat",     StringType(),  nullable=True),     # PII_LOW
    StructField("investor_kyc_tier",  IntegerType(), nullable=True),
])


def main() -> None:
    bucket = s3_bucket()
    spark = (SparkSession.builder
             .appName(cde_job("bronze.member_cdc"))
             .config("spark.sql.streaming.checkpointLocation",
                     f"s3a://{bucket}/checkpoints/job_03_bronze_member_cdc/")
             .getOrCreate())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "${KAFKA_BROKERS}")
           .option("subscribe", topic("member.cdc.v1"))
           .option("kafka.group.id", consumer_group("bronze.member_cdc"))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 50_000)
           .load())

    parsed = (raw
              .select(F.col("value").cast("string").alias("raw_payload"),
                      F.col("timestamp").alias("ts_ingest"),
                      F.from_json(F.col("value").cast("string"), CDC_SCHEMA).alias("evt"))
              .filter(F.col("evt").isNotNull() & F.col("evt.member_firm_id").isNotNull()))

    enriched = (parsed.select(
        F.col("evt.cdc_op").alias("cdc_op"),
        F.to_timestamp(F.col("evt.cdc_ts")).alias("cdc_ts"),
        F.col("ts_ingest"),
        F.col("evt.member_firm_id").alias("member_firm_id"),
        F.col("evt.member_firm_name").alias("member_firm_name"),
        F.col("evt.sebi_registration").alias("sebi_registration"),
        F.col("evt.capital_adequacy").alias("capital_adequacy"),
        F.col("evt.suspension_history").alias("suspension_history"),
        F.col("evt.trader_id").alias("trader_id"),
        F.col("evt.trader_name").alias("trader_name"),
        F.col("evt.investor_acct").alias("investor_acct"),
        F.col("evt.investor_pan").alias("investor_pan"),
        F.col("evt.investor_email").alias("investor_email"),
        F.col("evt.investor_mobile").alias("investor_mobile"),
        F.col("evt.investor_demat").alias("investor_demat"),
        F.col("evt.investor_kyc_tier").alias("investor_kyc_tier"),
        F.col("raw_payload"),
        F.to_date(F.col("ts_ingest")).alias("ingest_date"),
    ))

    (enriched.writeStream
     .format("iceberg")
     .outputMode("append")
     .option("path", fqtn("bronze", "member_cdc"))
     .trigger(processingTime="30 seconds")  # CDC is hourly-cadence; less aggressive trigger
     .start()
     .awaitTermination())


if __name__ == "__main__":
    main()
