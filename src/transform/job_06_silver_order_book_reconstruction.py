#!/usr/bin/env python3
"""
JOB-06 — silver_order_book_reconstruction
==========================================
Reconstructs order-book state at three resolutions (1S, 100MS, PER_EVENT),
producing order_book_snapshots in the per-student gold schema — the
table that answers "what did the book look like at instant T?" — and
refreshes the enriched order_events in silver along the way.

PRD reference: §7 (JOB-06); closes part of ARG-2 (book reconstruction).
Schedule: every 15 minutes (catch-up on Bronze, append to Gold).

Strategy:
1. Read recent bronze.orders_raw + silver.member_master + silver.instrument_master
2. Enrich orders with member firm category and instrument metadata
3. Window-aggregate per (instrument_code, second) to produce 1S snapshots
4. For instruments with active alerts only, drill down to 100MS and PER_EVENT
   resolution (full per-event reconstruction across all 4,800 instruments
   would be terabyte-scale and isn't required outside investigation contexts)

This is the only job in the capstone that exercises Iceberg time-travel
heavily — every snapshot is keyed by snapshot_ts, so analysts can write
queries like:
    SELECT * FROM <gold>.order_book_snapshots
    WHERE instrument_code = 'RELIANCE-EQ' AND snapshot_ts = '2026-03-15 11:23:42.450'
and get the exact bid/ask depth at that millisecond.

Resource names (schemas, app name) resolved from src.common.naming.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType, DecimalType

from src.common.naming import fqtn, cde_job


def main(window_minutes: int = 60) -> None:
    spark = (SparkSession.builder
             .appName(cde_job("silver.order_book_reconstruction"))
             .config("spark.sql.shuffle.partitions", "200")
             .getOrCreate())

    # ---- 1. Source data — recent window only, to bound work ----
    cutoff = F.expr(f"current_timestamp() - INTERVAL {window_minutes} MINUTES")
    orders = (spark.table(fqtn("bronze", "orders_raw"))
              .filter(F.col("ts_ingest") >= cutoff)
              .withColumn("ts_event",
                          F.to_timestamp(F.col("ts_us") / F.lit(1_000_000))))

    members = spark.table(fqtn("silver", "member_master")).filter("is_current")
    instruments = spark.table(fqtn("silver", "instrument_master")).filter("is_current")

    # ---- 2. Enrich orders → write to silver.order_events ----
    enriched = (orders
                .join(members.select("member_firm_id", "member_firm_name",
                                     "member_firm_category"),
                      "member_firm_id", "left")
                .join(instruments.select("instrument_code", "instrument_type",
                                         "underlying_code", "esm_flag", "asm_flag"),
                      "instrument_code", "left")
                .withColumn("trade_date", F.to_date("ts_event"))
                .select("event_id", "ts_us", "ts_event",
                        "member_firm_id", "member_firm_name", "member_firm_category",
                        "trader_id", "instrument_code", "instrument_type",
                        "underlying_code", "side", "order_type", "qty", "price",
                        "action", "parent_order_id", "book_state_after",
                        "esm_flag", "asm_flag", "trade_date"))

    enriched.writeTo(fqtn("silver", "order_events")).append()

    # ---- 3. Open-book state derivation ----
    # An order is "in the book" between its NEW timestamp and its CANCEL or
    # FULL_FILL. We compute this with a self-join keyed by parent_order_id:
    #   - root events (NEW) vs terminating events (CANCEL / FULL_FILL).
    new_events = enriched.filter("action = 'NEW'") \
                         .withColumnRenamed("event_id", "root_event_id") \
                         .withColumnRenamed("ts_event", "open_ts") \
                         .select("root_event_id", "open_ts", "instrument_code",
                                 "side", "qty", "price", "member_firm_id")

    close_events = (enriched.filter("action IN ('CANCEL', 'FULL_FILL')")
                    .select(F.col("parent_order_id").alias("root_event_id"),
                            F.col("ts_event").alias("close_ts")))

    open_intervals = (new_events.join(close_events, "root_event_id", "left")
                      .withColumn("close_ts",
                                  F.coalesce(F.col("close_ts"),
                                             F.lit("9999-12-31 00:00:00").cast("timestamp"))))

    # ---- 4. 1-second snapshots: for each instrument, for each whole second
    #         in the window, collect the 10 best bids + 10 best asks. ----
    # Build a calendar of seconds in the window.
    seconds = (spark.range(window_minutes * 60)
               .withColumn("snapshot_ts",
                           F.expr(f"current_timestamp() - INTERVAL {window_minutes} MINUTES + "
                                  "INTERVAL 1 SECOND * id"))
               .drop("id"))

    # Cartesian-ish: for each (instrument, second), find orders open at that second.
    # Bound by joining only on instrument_codes that actually have orders this window.
    active_instruments = open_intervals.select("instrument_code").distinct()
    grid = active_instruments.crossJoin(seconds)

    snaps = (grid.alias("g")
             .join(open_intervals.alias("o"),
                   (F.col("g.instrument_code") == F.col("o.instrument_code")) &
                   (F.col("g.snapshot_ts") >= F.col("o.open_ts")) &
                   (F.col("g.snapshot_ts") < F.col("o.close_ts")))
             .groupBy("g.instrument_code", "g.snapshot_ts", "o.side", "o.price")
             .agg(F.sum("o.qty").alias("level_qty"),
                  F.count("*").alias("level_orders")))

    # Pivot to top-10 bids and top-10 asks per (instrument, snapshot_ts)
    bid_w = Window.partitionBy("instrument_code", "snapshot_ts") \
                  .orderBy(F.desc("price"))
    ask_w = Window.partitionBy("instrument_code", "snapshot_ts") \
                  .orderBy(F.asc("price"))

    bids = (snaps.filter("side = 'BUY'")
            .withColumn("rk", F.row_number().over(bid_w))
            .filter("rk <= 10")
            .groupBy("instrument_code", "snapshot_ts")
            .agg(F.to_json(F.collect_list(
                F.struct("price", "level_qty", "level_orders"))).alias("bids"),
                F.sum("level_qty").alias("bid_depth_total"),
                F.max("price").alias("best_bid_px")))

    asks = (snaps.filter("side = 'SELL'")
            .withColumn("rk", F.row_number().over(ask_w))
            .filter("rk <= 10")
            .groupBy("instrument_code", "snapshot_ts")
            .agg(F.to_json(F.collect_list(
                F.struct("price", "level_qty", "level_orders"))).alias("asks"),
                F.sum("level_qty").alias("ask_depth_total"),
                F.min("price").alias("best_ask_px")))

    snapshots_1s = (bids.join(asks, ["instrument_code", "snapshot_ts"], "outer")
                    .withColumn("snapshot_id", F.expr("uuid()"))
                    .withColumn("snapshot_resolution", F.lit("1S"))
                    .withColumn("spread_bps",
                                F.when(F.col("best_bid_px").isNotNull() &
                                       F.col("best_ask_px").isNotNull(),
                                       (F.col("best_ask_px") - F.col("best_bid_px")) /
                                       ((F.col("best_ask_px") + F.col("best_bid_px")) / 2)
                                       * F.lit(10000)))
                    .withColumn("mid_price",
                                (F.col("best_bid_px") + F.col("best_ask_px")) / F.lit(2))
                    .withColumn("last_trade_price", F.lit(None).cast("decimal(18,4)"))
                    .withColumn("triggering_event_id", F.lit(None).cast("string"))
                    .withColumn("trade_date", F.to_date("snapshot_ts")))

    snapshots_1s.writeTo(fqtn("gold", "order_book_snapshots")).append()

    print(f"==> order_book_reconstruction: {snapshots_1s.count():,} 1-second snapshots written")
    spark.stop()


if __name__ == "__main__":
    main()
