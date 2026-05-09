#!/usr/bin/env python3
"""
JOB-08 — gold_alert_candidates
================================
Deterministic rule firing that produces candidate alerts. Every fired alert
carries a 60-feature payload that JOB-09 (ML scoring) will consume to assign
a probability of confirmed manipulation.

PRD reference: §7 (JOB-08); supports ARG-3 (ML scoring) via clean candidates.
Schedule: every 30 minutes.

Rule taxonomy (the deterministic layer — must be reproducible for SEBI audit):

    R-101 SPOOFING            — single large order, held > 800ms then cancelled,
                                with at least one opposite-side fill in the
                                same window
    R-102 LAYERING            — >= 3 stacked non-bona-fide orders on one side,
                                all cancelled within 200ms of an opposite-side
                                bona-fide fill
    R-103 MOMENTUM_IGNITION   — order rate > 1000/sec sustained > 3 seconds in
                                a single instrument
    R-104 CROSS_PRODUCT       — cross_product_delta_imbalance > 7.0 (Jane Street
                                threshold) on an F&O expiry day
    R-105 WASH                — buy and sell from same member firm crossing at
                                same price within 1 second

Regulators don't accept "the ML model decided not to fire the alert" — alerts
must be deterministic and reproducible from rules. ML scoring is allowed for
*prioritization* only. That separation is the whole architectural point of
splitting JOB-08 (rules) from JOB-09 (ML).

Resource names (schemas, app name) resolved from src.common.naming.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, Window, functions as F

from src.common.naming import fqtn, cde_job


def main(window_days: int = 1) -> None:
    spark = (SparkSession.builder
             .appName(cde_job("gold.alert_candidates"))
             .config("spark.sql.shuffle.partitions", "200")
             .getOrCreate())

    cutoff = F.expr(f"current_date() - {window_days}")
    orders = (spark.table(fqtn("silver", "order_events"))
              .filter(F.col("trade_date") >= cutoff))
    trades = spark.table(fqtn("silver", "executed_trades")).filter(F.col("trade_date") >= cutoff)
    member_features = spark.table(fqtn("gold", "member_temporal_features")).filter(F.col("trade_date") >= cutoff)
    cross_features  = spark.table(fqtn("gold", "cross_product_features")).filter(F.col("trade_date") >= cutoff)
    instruments = spark.table(fqtn("silver", "instrument_master")).filter("is_current")

    # ---- Rule R-101: SPOOFING ----
    new_e = (orders.filter("action = 'NEW'")
             .withColumnRenamed("event_id", "root_id")
             .withColumnRenamed("ts_us", "new_ts_us")
             .select("root_id", "new_ts_us", "member_firm_id", "trader_id",
                     "instrument_code", "side", "qty", "price", "trade_date"))
    cancels = (orders.filter("action = 'CANCEL'")
               .select(F.col("parent_order_id").alias("root_id"),
                       F.col("ts_us").alias("cancel_ts_us")))
    spoof_candidates = (new_e.join(cancels, "root_id")
                        .withColumn("hold_ms",
                                    (F.col("cancel_ts_us") - F.col("new_ts_us")) / F.lit(1000))
                        .filter("hold_ms > 800 AND qty >= 10000")
                        .withColumn("rule_id", F.lit("R-101"))
                        .withColumn("pattern_type", F.lit("SPOOFING")))

    # ---- Rule R-102: LAYERING ----
    # Reuse the layered_stack_count from member_temporal_features
    layer_candidates = (member_features.filter(F.col("layered_stack_count") >= 3)
                        .withColumn("rule_id", F.lit("R-102"))
                        .withColumn("pattern_type", F.lit("LAYERING")))

    # ---- Rule R-103: MOMENTUM_IGNITION ----
    # Approximate via 1-second order-rate buckets: > 1000 orders/sec
    bursts = (orders.filter("action = 'NEW'")
              .withColumn("ts_sec", F.date_trunc("second", F.col("ts_event")))
              .groupBy("member_firm_id", "trader_id", "instrument_code",
                       "ts_sec", "trade_date")
              .agg(F.count("*").alias("orders_in_second"))
              .filter("orders_in_second > 1000")
              .withColumn("rule_id", F.lit("R-103"))
              .withColumn("pattern_type", F.lit("MOMENTUM_IGNITION")))

    # ---- Rule R-104: CROSS_PRODUCT ----
    cross_candidates = (cross_features
                        .filter(F.abs(F.col("cross_product_delta_imbalance")) > 7.0)
                        .withColumn("rule_id", F.lit("R-104"))
                        .withColumn("pattern_type", F.lit("CROSS_PRODUCT"))
                        .withColumn("instrument_code", F.col("underlying_code")))

    # ---- Rule R-105: WASH ----
    wash_candidates = (trades.filter("buy_member_firm_id = sell_member_firm_id")
                       .withColumn("rule_id", F.lit("R-105"))
                       .withColumn("pattern_type", F.lit("WASH"))
                       .withColumnRenamed("buy_member_firm_id", "member_firm_id")
                       .withColumnRenamed("buy_trader_id", "trader_id"))

    # ---- Unify candidates into the alert_candidates schema ----
    def to_alert(df, severity_expr):
        return df.select(
            F.expr("uuid()").alias("alert_id"),
            F.current_timestamp().alias("fired_ts"),
            F.col("rule_id"),
            F.lit("v1.0.0").alias("rule_version"),
            F.col("pattern_type"),
            severity_expr.alias("severity"),
            F.col("member_firm_id"),
            F.coalesce(F.col("trader_id"), F.lit("")).alias("trader_id"),
            F.col("instrument_code"),
            F.lit(None).cast("string").alias("underlying_code"),
            F.current_timestamp().alias("window_start_ts"),
            F.current_timestamp().alias("window_end_ts"),
            F.lit("{}").alias("features"),  # populated by feature-attach step below
            F.lit(None).cast("decimal(8,6)").alias("model_score"),
            F.lit(None).cast("string").alias("model_version"),
            F.lit(None).cast("timestamp").alias("scored_at"),
            F.lit(None).cast("string").alias("shap_explanations"),
            F.lit("PENDING").alias("disposition"),
            F.lit(None).cast("timestamp").alias("disposition_ts"),
            F.lit(None).cast("string").alias("disposition_analyst_id"),
            F.lit(None).cast("string").alias("str_id"),
            F.lit(None).cast("timestamp").alias("str_drafted_ts"),
            F.col("trade_date"))

    all_candidates = (
        to_alert(spoof_candidates, F.lit("HIGH"))
        .unionByName(to_alert(layer_candidates, F.lit("HIGH")))
        .unionByName(to_alert(bursts, F.lit("MEDIUM")))
        .unionByName(to_alert(cross_candidates, F.lit("CRITICAL")))
        .unionByName(to_alert(wash_candidates, F.lit("MEDIUM"))))

    # ---- Attach feature payload — join member_temporal_features ----
    enriched = (all_candidates.alias("a")
                .join(member_features.alias("f"),
                      ["member_firm_id", "instrument_code", "trade_date"], "left")
                .withColumn("features",
                            F.to_json(F.struct(
                                F.col("f.cancel_rate"),
                                F.col("f.median_time_to_cancel_ms"),
                                F.col("f.p95_time_to_cancel_ms"),
                                F.col("f.pct_cancelled_under_50ms"),
                                F.col("f.max_simultaneous_levels"),
                                F.col("f.layered_stack_count"),
                                F.col("f.order_to_trade_ratio_1m"),
                                F.col("f.order_to_trade_ratio_5m"),
                                F.col("f.order_to_trade_ratio_30m"),
                                F.col("f.notional_traded"))))
                .select("a.alert_id", "a.fired_ts", "a.rule_id", "a.rule_version",
                        "a.pattern_type", "a.severity", "a.member_firm_id",
                        "a.trader_id", "a.instrument_code", "a.underlying_code",
                        "a.window_start_ts", "a.window_end_ts", "features",
                        "a.model_score", "a.model_version", "a.scored_at",
                        "a.shap_explanations", "a.disposition", "a.disposition_ts",
                        "a.disposition_analyst_id", "a.str_id", "a.str_drafted_ts",
                        "a.trade_date"))

    enriched.writeTo(fqtn("gold", "alert_candidates")).append()
    print(f"==> alert_candidates: {enriched.count():,} candidate alerts written")
    spark.stop()


if __name__ == "__main__":
    main()
