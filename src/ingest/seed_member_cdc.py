#!/usr/bin/env python3
"""
seed_member_cdc — one-time bulk load for member_cdc Bronze
==========================================================
The KAVACH CDC Kafka topic (argus.${SID}.member.cdc.v1) is fed by
Debezium in production; in the lab there's no live Oracle DB. This
script loads the flat members.csv + traders.csv + investors.csv files
into the same Bronze CDC table that JOB-03 normally populates, with
synthetic CDC ops.

Run once during Lab 1.2. Subsequent CDC streaming is handled by JOB-03
when the test environment includes a live KAVACH simulator.

Resource names (sink table, S3 paths, app name) are resolved from
src.common.naming using ${STUDENT_ID}.

PRD reference: §3 (INT-2 KAVACH ingest); referenced in lab-1-2-bronze-ingest.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F

from src.common.naming import fqtn, cde_job, s3_bucket


def main() -> None:
    bucket = s3_bucket()
    spark = (SparkSession.builder
             .appName(cde_job("bronze.seed_member_cdc"))
             .getOrCreate())

    landing = f"s3a://{bucket}/landing"

    # Members → one Bronze row per member firm with member-level fields populated
    members = (spark.read.csv(f"{landing}/members.csv", header=True, inferSchema=True)
               .withColumn("cdc_op", F.lit("INSERT"))
               .withColumn("cdc_ts", F.current_timestamp())
               .withColumn("ts_ingest", F.current_timestamp())
               .withColumn("trader_id", F.lit(None).cast("string"))
               .withColumn("trader_name", F.lit(None).cast("string"))
               .withColumn("investor_acct", F.lit(None).cast("string"))
               .withColumn("investor_pan", F.lit(None).cast("string"))
               .withColumn("investor_email", F.lit(None).cast("string"))
               .withColumn("investor_mobile", F.lit(None).cast("string"))
               .withColumn("investor_demat", F.lit(None).cast("string"))
               .withColumn("investor_kyc_tier", F.lit(None).cast("int"))
               .withColumn("raw_payload", F.to_json(F.struct("*")))
               .withColumn("ingest_date", F.current_date()))

    # Traders → enrich with member firm fields, set trader-level fields, NULL investor cols
    traders = spark.read.csv(f"{landing}/traders.csv", header=True, inferSchema=True)
    traders_cdc = (traders
                   .join(members.select("member_firm_id", "member_firm_name",
                                        "sebi_registration", "capital_adequacy",
                                        "suspension_history"), "member_firm_id", "left")
                   .withColumn("cdc_op", F.lit("INSERT"))
                   .withColumn("cdc_ts", F.current_timestamp())
                   .withColumn("ts_ingest", F.current_timestamp())
                   .withColumn("investor_acct", F.lit(None).cast("string"))
                   .withColumn("investor_pan", F.lit(None).cast("string"))
                   .withColumn("investor_email", F.lit(None).cast("string"))
                   .withColumn("investor_mobile", F.lit(None).cast("string"))
                   .withColumn("investor_demat", F.lit(None).cast("string"))
                   .withColumn("investor_kyc_tier", F.lit(None).cast("int"))
                   .withColumn("raw_payload", F.to_json(F.struct("*")))
                   .withColumn("ingest_date", F.current_date()))

    # Investors → enrich with member firm fields, NULL trader cols, populate investor PII
    investors = spark.read.csv(f"{landing}/investors.csv", header=True, inferSchema=True)
    investors_cdc = (investors
                     .join(members.select("member_firm_id", "member_firm_name",
                                          "sebi_registration", "capital_adequacy",
                                          "suspension_history"), "member_firm_id", "left")
                     .withColumn("cdc_op", F.lit("INSERT"))
                     .withColumn("cdc_ts", F.current_timestamp())
                     .withColumn("ts_ingest", F.current_timestamp())
                     .withColumn("trader_id", F.lit(None).cast("string"))
                     .withColumn("trader_name", F.lit(None).cast("string"))
                     .withColumn("raw_payload", F.to_json(F.struct("*")))
                     .withColumn("ingest_date", F.current_date()))

    # Project to the exact column order of <bronze schema>.member_cdc
    cols = ["cdc_op", "cdc_ts", "ts_ingest", "member_firm_id", "member_firm_name",
            "sebi_registration", "capital_adequacy", "suspension_history",
            "trader_id", "trader_name", "investor_acct", "investor_pan",
            "investor_email", "investor_mobile", "investor_demat",
            "investor_kyc_tier", "raw_payload", "ingest_date"]

    unified = (members.select(*cols)
               .unionByName(traders_cdc.select(*cols))
               .unionByName(investors_cdc.select(*cols)))

    unified.write.format("iceberg").mode("append").saveAsTable(fqtn("bronze", "member_cdc"))

    print(f"==> seed_member_cdc complete: {unified.count():,} rows written")
    spark.stop()


if __name__ == "__main__":
    main()
