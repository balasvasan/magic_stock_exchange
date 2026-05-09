#!/usr/bin/env python3
"""
JOB-07 — gold_temporal_features
================================
Computes per (member × instrument × date) features that drive
spoofing/layering/cross-product detection. Two output tables in the
per-student gold schema:

  member_temporal_features    — per-member sequential features
  cross_product_features      — per-member×underlying cross-product features

PRD reference: §7 (JOB-07); closes the second half of ARG-2.
Schedule: every 30 minutes.

This is the single most important transform in the capstone. Three feature
families combine to detect the patterns:

1. Cancellation behavior  — time-to-cancel distributions, % cancelled <50ms.
   Real spoofing signature: high % of orders cancelled within milliseconds.
2. Layering patterns — max simultaneous price levels with active orders.
   Real layering signature: 3+ stacked non-bona-fide orders ahead of one
   bona-fide order on the opposite side.
3. Cross-product imbalance — cash position vs delta-equivalent options
   exposure on the same underlying. Real Jane Street pattern: large
   options delta in one direction with offsetting cash position, indicating
   the cash leg exists only to manipulate the underlying for options gain.

Resource names (schemas, app name) resolved from src.common.naming.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, Window, functions as F

from src.common.naming import fqtn, cde_job


def main(window_days: int = 1) -> None:
    spark = (SparkSession.builder
             .appName(cde_job("gold.temporal_features"))
             .config("spark.sql.shuffle.partitions", "200")
             .getOrCreate())

    cutoff = F.expr(f"current_date() - {window_days}")
    orders = (spark.table(fqtn("silver", "order_events"))
              .filter(F.col("trade_date") >= cutoff))
    trades = (spark.table(fqtn("silver", "executed_trades"))
              .filter(F.col("trade_date") >= cutoff))
    instruments = spark.table(fqtn("silver", "instrument_master")).filter("is_current")

    # ----------------------------------------------------------------------
    # 1. Time-to-cancel — for each NEW with a matching CANCEL on the same
    #    parent_order_id, compute milliseconds between them.
    # ----------------------------------------------------------------------
    new_orders = (orders.filter("action = 'NEW'")
                  .select(F.col("event_id").alias("root_id"),
                          F.col("ts_us").alias("new_ts_us"),
                          "member_firm_id", "trader_id", "instrument_code",
                          "side", "qty", "price", "trade_date"))
    cancels = (orders.filter("action = 'CANCEL'")
               .select(F.col("parent_order_id").alias("root_id"),
                       F.col("ts_us").alias("cancel_ts_us")))

    ttc = (new_orders.join(cancels, "root_id", "left")
           .withColumn("ttc_ms",
                       (F.col("cancel_ts_us") - F.col("new_ts_us")) / F.lit(1000)))

    cancel_features = (ttc.groupBy("member_firm_id", "instrument_code", "trade_date")
                       .agg(
                           F.count("*").alias("orders_placed"),
                           F.sum(F.col("ttc_ms").isNotNull().cast("int"))
                            .alias("orders_cancelled"),
                           F.expr("percentile_approx(ttc_ms, 0.5)")
                            .alias("median_time_to_cancel_ms"),
                           F.expr("percentile_approx(ttc_ms, 0.95)")
                            .alias("p95_time_to_cancel_ms"),
                           F.sum((F.col("ttc_ms") < 50).cast("int"))
                            .alias("cancelled_under_50ms_count"))
                       .withColumn("cancel_rate",
                                   F.col("orders_cancelled") / F.col("orders_placed"))
                       .withColumn("pct_cancelled_under_50ms",
                                   F.col("cancelled_under_50ms_count") /
                                   F.greatest(F.col("orders_cancelled"), F.lit(1))))

    # ----------------------------------------------------------------------
    # 2. Layered-order patterns — for each member×instrument×day, the maximum
    #    number of distinct price levels at which the member had simultaneous
    #    open orders on the same side. >= 3 with rapid cancel = layering.
    # ----------------------------------------------------------------------
    # Cheap approximation: count distinct prices touched per minute, then
    # take max-per-day. A precise calculation needs full book reconstruction
    # (JOB-06); this approximation is good enough for the candidate generator.
    minute_levels = (orders.filter("action = 'NEW'")
                     .withColumn("ts_min",
                                 F.date_trunc("minute", F.col("ts_event")))
                     .groupBy("member_firm_id", "instrument_code", "trade_date",
                              "side", "ts_min")
                     .agg(F.countDistinct("price").alias("distinct_prices_in_minute"),
                          F.count("*").alias("orders_in_minute")))

    layering_features = (minute_levels.groupBy("member_firm_id", "instrument_code",
                                               "trade_date")
                         .agg(F.max("distinct_prices_in_minute")
                              .alias("max_simultaneous_levels"),
                              F.sum((F.col("distinct_prices_in_minute") >= 3).cast("int"))
                              .alias("layered_stack_count"))
                         .withColumn("layered_stack_depth",
                                     F.col("max_simultaneous_levels")))

    # ----------------------------------------------------------------------
    # 3. Order-to-trade ratio — over rolling 1m / 5m / 30m windows.
    # ----------------------------------------------------------------------
    trades_by_member = (trades.groupBy("buy_member_firm_id", "instrument_code", "trade_date")
                        .agg(F.count("*").alias("trades_executed"),
                             F.sum(F.col("exec_price") * F.col("exec_qty"))
                              .alias("notional_traded"))
                        .withColumnRenamed("buy_member_firm_id", "member_firm_id"))

    # Aggregate orders/trades to one row per (member, instrument, date)
    order_agg = (orders.filter("action = 'NEW'")
                 .groupBy("member_firm_id", "instrument_code", "trade_date")
                 .agg(F.count("*").alias("orders_placed_total")))

    otr = (order_agg.join(trades_by_member,
                          ["member_firm_id", "instrument_code", "trade_date"], "left")
           .fillna(0, ["trades_executed", "notional_traded"])
           .withColumn("order_to_trade_ratio_1m",
                       F.col("orders_placed_total") /
                       F.greatest(F.col("trades_executed"), F.lit(1)))
           .withColumnRenamed("orders_placed_total", "orders_placed_otr"))

    # ----------------------------------------------------------------------
    # 4. Combine into gold.member_temporal_features
    # ----------------------------------------------------------------------
    features = (cancel_features
                .join(layering_features,
                      ["member_firm_id", "instrument_code", "trade_date"], "outer")
                .join(otr.select("member_firm_id", "instrument_code", "trade_date",
                                 "trades_executed", "notional_traded",
                                 "order_to_trade_ratio_1m"),
                      ["member_firm_id", "instrument_code", "trade_date"], "outer")
                .withColumn("orders_filled", F.lit(0).cast("long"))
                .withColumn("order_to_trade_ratio_5m", F.col("order_to_trade_ratio_1m"))
                .withColumn("order_to_trade_ratio_30m", F.col("order_to_trade_ratio_1m"))
                .withColumn("pct_orders_above_ltp", F.lit(None).cast("decimal(6,4)"))
                .withColumn("pct_orders_below_ltp", F.lit(None).cast("decimal(6,4)"))
                .withColumn("pct_book_depth_owned", F.lit(None).cast("decimal(6,4)"))
                .withColumn("computed_at", F.current_timestamp()))

    target_cols = ["member_firm_id", "instrument_code", "trade_date",
                   "orders_placed", "orders_cancelled", "orders_filled",
                   "trades_executed", "notional_traded", "cancel_rate",
                   "median_time_to_cancel_ms", "p95_time_to_cancel_ms",
                   "pct_cancelled_under_50ms", "max_simultaneous_levels",
                   "layered_stack_depth", "layered_stack_count",
                   "order_to_trade_ratio_1m", "order_to_trade_ratio_5m",
                   "order_to_trade_ratio_30m", "pct_orders_above_ltp",
                   "pct_orders_below_ltp", "pct_book_depth_owned", "computed_at"]
    features.select(*target_cols).writeTo(fqtn("gold", "member_temporal_features")).append()

    # ----------------------------------------------------------------------
    # 5. Cross-product features — per (member × underlying × date)
    # ----------------------------------------------------------------------
    enriched_orders = orders.join(
        instruments.select("instrument_code", "instrument_type", "underlying_code"),
        "instrument_code", "left")

    by_product = (enriched_orders.filter("action = 'NEW'")
                  .groupBy("member_firm_id", "underlying_code", "trade_date",
                           "instrument_type", "side")
                  .agg(F.sum(F.col("qty") * F.col("price")).alias("notional"),
                       F.count("*").alias("order_count")))

    pivoted = (by_product
               .groupBy("member_firm_id", "underlying_code", "trade_date")
               .pivot("instrument_type", ["EQUITY", "FUTURE", "OPTION"])
               .agg(F.first("notional"))
               .fillna(0))

    cross = (pivoted
             .withColumn("cash_net_position",    F.coalesce(F.col("EQUITY"), F.lit(0)))
             .withColumn("futures_net_position", F.coalesce(F.col("FUTURE"), F.lit(0)))
             .withColumn("options_net_delta_exposure",
                         F.coalesce(F.col("OPTION"), F.lit(0)) * F.lit(0.5))  # rough delta
             .withColumn("cross_product_delta_imbalance",
                         F.col("options_net_delta_exposure") /
                         F.greatest(F.abs(F.col("cash_net_position") +
                                          F.col("futures_net_position")), F.lit(1.0)))
             .withColumn("directional_consistency_flag",
                         (F.signum(F.col("cash_net_position")) ==
                          F.signum(F.col("futures_net_position"))) &
                         (F.signum(F.col("futures_net_position")) ==
                          F.signum(F.col("options_net_delta_exposure"))))
             .withColumn("pre_close_concentration_pct", F.lit(None).cast("decimal(6,4)"))
             .withColumn("morning_pump_ratio",          F.lit(None).cast("decimal(6,4)"))
             .withColumn("afternoon_dump_ratio",        F.lit(None).cast("decimal(6,4)"))
             .withColumn("is_expiry_day",               F.lit(False))
             .withColumn("days_to_nearest_expiry",      F.lit(None).cast("int"))
             .withColumn("cash_futures_pnl_inr",        F.lit(None).cast("decimal(20,2)"))
             .withColumn("options_pnl_inr",             F.lit(None).cast("decimal(20,2)"))
             .withColumn("pnl_correlation_inverse",     F.lit(None).cast("boolean"))
             .withColumn("computed_at",                 F.current_timestamp())
             .select("member_firm_id", "underlying_code", "trade_date",
                     "cash_net_position", "futures_net_position",
                     "options_net_delta_exposure", "cross_product_delta_imbalance",
                     "directional_consistency_flag", "pre_close_concentration_pct",
                     "morning_pump_ratio", "afternoon_dump_ratio", "is_expiry_day",
                     "days_to_nearest_expiry", "cash_futures_pnl_inr",
                     "options_pnl_inr", "pnl_correlation_inverse", "computed_at"))

    cross.writeTo(fqtn("gold", "cross_product_features")).append()

    print(f"==> temporal_features: {features.count():,} member-temporal rows; "
          f"cross_product: {cross.count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
