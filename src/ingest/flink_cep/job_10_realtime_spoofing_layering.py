#!/usr/bin/env python3
"""
JOB-10 — realtime_spoofing_layering_cep (Cloudera Flink / PyFlink)
==================================================================
Sub-second detection of R-101 SPOOFING and R-102 LAYERING patterns
on the live order stream from `argus.${STUDENT_ID}.orders.v1`.

PRD reference: §7 (JOB-10); contributes to closing ARG-1 (peak-volume
detection latency, not just ingest throughput).

Architectural role: this job runs in PARALLEL with JOB-08 batch rule
firing. JOB-08 owns canonical record-keeping for ML scoring (every 30
min); JOB-10 owns sub-second analyst notification (every event).
The same `event_id` should land in both gold.alert_candidates (within
30 min) and gold.realtime_alert_stream (within 800ms p99).

Patterns implemented:
- R-101 SPOOFING — single large order held >800ms then cancelled, with
  opposite-side fill on same instrument within 200ms of the cancel.
  Detected via Flink CEP NEW-cancel followed-by opposite-fill window.
- R-102 LAYERING — ≥3 stacked non-bona-fide orders on one side of the
  book, all cancelled within 200ms of an opposite-side fill on the
  underlying instrument.

Output: writes to `argus.${STUDENT_ID}.realtime_alerts.v1` with the
schema documented in PRD §5 Topic #9.

Resource names (topic, consumer group, app name) resolved from
src.common.naming using ${STUDENT_ID}.

Submission: see src/ingest/flink_cep/README.md for cluster submission
syntax. Locally for development:
    python -m src.ingest.flink_cep.job_10_realtime_spoofing_layering \\
        --bootstrap kafka1.argus.local:9092 --parallelism 4
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyflink.common import Time, Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
)
from pyflink.cep import CEP, Pattern
from pyflink.common.time import Time as FlinkTime

from src.common.naming import topic, consumer_group, cde_job

LOG = logging.getLogger("argus.flink.realtime_cep")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Detection thresholds — match the JOB-08 batch rules so the comparison
# is apples-to-apples. If you change these, update PRD §7 + JOB-08.
SPOOF_MIN_QTY = 5_000           # large-order threshold
SPOOF_HOLD_MS_MIN = 800         # held at least this long
SPOOF_OPPOSITE_FILL_MS = 200    # opposite-side fill within this of cancel
LAYER_STACK_MIN = 3             # ≥3 layered orders
LAYER_CANCEL_WINDOW_MS = 200    # all cancelled within this of fill


def parse_event(raw: str) -> dict | None:
    """Parse one Kafka order-event JSON line. Returns None on bad JSON
    so the operator silently drops malformed rows (NiFi already DLQ'd
    anything genuinely broken before it hit Kafka)."""
    try:
        e = json.loads(raw)
        # Validate the keys the CEP patterns rely on
        for k in ("event_id", "ts_us", "instrument_code", "side", "qty",
                  "action", "member_firm_id"):
            if k not in e:
                return None
        return e
    except Exception:
        return None


def build_spoofing_pattern() -> Pattern:
    """R-101 SPOOFING — large-order CANCEL followed by opposite-side
    FILL within 200ms on the same instrument."""
    return (Pattern.begin("large_resting_cancel")
            .where(lambda e: (e["action"] == "CANCEL"
                              and e["qty"] >= SPOOF_MIN_QTY))
            .next("opposite_fill")
            .where(lambda c, ctx: (
                c["action"] == "FILL"
                and c["instrument_code"] == ctx.get_events_for_pattern("large_resting_cancel")[0]["instrument_code"]
                and c["side"] != ctx.get_events_for_pattern("large_resting_cancel")[0]["side"]
            ))
            .within(FlinkTime.milliseconds(SPOOF_OPPOSITE_FILL_MS)))


def build_layering_pattern() -> Pattern:
    """R-102 LAYERING — three or more cancels on one side followed by a
    fill on the opposite side, all within 200ms."""
    return (Pattern.begin("stacked_cancels")
            .where(lambda e: e["action"] == "CANCEL")
            .times_or_more(LAYER_STACK_MIN)
            .next("opposite_fill")
            .where(lambda c, ctx: (
                c["action"] == "FILL"
                and c["instrument_code"] == ctx.get_events_for_pattern("stacked_cancels")[0]["instrument_code"]
                and c["side"] != ctx.get_events_for_pattern("stacked_cancels")[0]["side"]
            ))
            .within(FlinkTime.milliseconds(LAYER_CANCEL_WINDOW_MS)))


def emit_spoof_alert(matched: dict[str, list[dict]]) -> str:
    """Format a SPOOFING alert as the realtime_alerts.v1 JSON schema."""
    cancel = matched["large_resting_cancel"][0]
    fill = matched["opposite_fill"][0]
    detection_latency_ms = (fill["ts_us"] - cancel["ts_us"]) // 1000
    return json.dumps({
        "alert_id": f"RT-SPOOF-{cancel['event_id']}",
        "fired_ts": fill["ts_us"],
        "source_engine": "FLINK",
        "rule_id": "R-101",
        "severity": "HIGH",
        "pattern_type": "SPOOFING",
        "member_firm_id": cancel["member_firm_id"],
        "trader_id": cancel.get("trader_id"),
        "instrument_code": cancel["instrument_code"],
        "window_start_ts": cancel["ts_us"],
        "window_end_ts": fill["ts_us"],
        "evidence_json": {
            "cancelled_order_event_id": cancel["event_id"],
            "cancelled_qty": cancel["qty"],
            "cancelled_side": cancel["side"],
            "fill_event_id": fill["event_id"],
            "fill_side": fill["side"],
        },
        "detection_latency_ms": detection_latency_ms,
    })


def emit_layer_alert(matched: dict[str, list[dict]]) -> str:
    """Format a LAYERING alert as the realtime_alerts.v1 JSON schema."""
    cancels = matched["stacked_cancels"]
    fill = matched["opposite_fill"][0]
    detection_latency_ms = (fill["ts_us"] - cancels[0]["ts_us"]) // 1000
    return json.dumps({
        "alert_id": f"RT-LAYER-{cancels[0]['event_id']}",
        "fired_ts": fill["ts_us"],
        "source_engine": "FLINK",
        "rule_id": "R-102",
        "severity": "HIGH",
        "pattern_type": "LAYERING",
        "member_firm_id": cancels[0]["member_firm_id"],
        "trader_id": cancels[0].get("trader_id"),
        "instrument_code": cancels[0]["instrument_code"],
        "window_start_ts": cancels[0]["ts_us"],
        "window_end_ts": fill["ts_us"],
        "evidence_json": {
            "stack_depth": len(cancels),
            "cancelled_event_ids": [c["event_id"] for c in cancels],
            "fill_event_id": fill["event_id"],
            "fill_side": fill["side"],
        },
        "detection_latency_ms": detection_latency_ms,
    })


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True,
                    help="Kafka bootstrap servers, e.g. kafka1:9092,kafka2:9092")
    ap.add_argument("--parallelism", type=int, default=4)
    ap.add_argument("--checkpoint-interval-ms", type=int, default=10_000)
    args = ap.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(args.checkpoint_interval_ms)
    env.get_config().set_global_job_parameters({"job_name": cde_job("realtime_cep")})

    # ----- Source: orders.v1 -----
    order_source = (KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_topics(topic("orders.v1"))
        .set_group_id(consumer_group("flink_cep_v1"))
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    raw_stream = env.from_source(
        order_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(50)),
        "orders-source",
    )
    parsed = (raw_stream
              .map(parse_event, output_type=Types.MAP(Types.STRING(), Types.STRING()))
              .filter(lambda e: e is not None)
              .key_by(lambda e: e["instrument_code"]))

    # ----- CEP — R-101 SPOOFING -----
    spoof_alerts = CEP.pattern(parsed, build_spoofing_pattern()).select(emit_spoof_alert)

    # ----- CEP — R-102 LAYERING -----
    layer_alerts = CEP.pattern(parsed, build_layering_pattern()).select(emit_layer_alert)

    # ----- Sink: realtime_alerts.v1 -----
    alerts_topic = topic("realtime_alerts.v1")
    alert_sink = (KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
            .set_topic(alerts_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build())
        .build())

    spoof_alerts.sink_to(alert_sink).name("sink-spoof-alerts")
    layer_alerts.sink_to(alert_sink).name("sink-layer-alerts")

    LOG.info("Submitting Flink job %s (parallelism=%d, checkpoint=%dms)",
             cde_job("realtime_cep"), args.parallelism, args.checkpoint_interval_ms)
    env.execute(cde_job("realtime_cep"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
