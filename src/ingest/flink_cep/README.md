# PyFlink CEP — Real-Time Pattern Detection

This directory contains JOB-10, a PyFlink CEP application that detects R-101 SPOOFING and R-102 LAYERING patterns on the live order stream with sub-second latency.

## Files

| File | Purpose |
|---|---|
| `job_10_realtime_spoofing_layering.py` | The CEP job. ~180 lines. |
| `README.md` | This file. |

## How to submit to Cloudera Flink

The Cloudera Streaming Analytics platform runs Flink behind the Cloudera Manager UI. Submission steps:

1. **Package** the Python file with its dependencies into a `.zip`:
   ```bash
   cd src/ingest/flink_cep
   zip -r flink_cep_${STUDENT_ID}.zip job_10_realtime_spoofing_layering.py ../../common/naming.py
   ```

2. **Submit via the Flink CLI** on a CSA gateway node:
   ```bash
   flink run -py job_10_realtime_spoofing_layering.py \
       -pyfs ../../common/naming.py \
       --bootstrap "${KAFKA_BROKERS}" \
       --parallelism 4
   ```

   Or via Cloudera Manager → Flink → "Submit Job" UI.

3. **Verify** the job is running:
   - Cloudera Manager → Flink → Running Jobs → look for `argus_${STUDENT_ID}_realtime_cep`
   - Or from the CLI: `flink list -r | grep argus_${STUDENT_ID}`

## Local development (no cluster)

For testing logic before submitting:

```bash
# In the repo root, with PyFlink installed
python -m src.ingest.flink_cep.job_10_realtime_spoofing_layering \
    --bootstrap localhost:9092 --parallelism 1
```

This uses Flink's embedded mini-cluster — fine for verifying the CEP patterns fire on test data, not for production performance testing.

## State management

The job uses **keyed state** (keyed by `instrument_code`) for the CEP pattern matchers. With state TTL set to 60s (per PRD §17), an instrument with no events for 60s clears its state to keep memory bounded. CEP patterns themselves use a 200ms window for SPOOFING and LAYERING.

## Per-student namespacing

All resource names — Kafka topics, consumer groups, job names, savepoint paths — flow through `src/common/naming.py` and embed `${STUDENT_ID}`. So 16 students can submit `argus_s001_realtime_cep`, `argus_s002_realtime_cep`, ... to the same Flink cluster without collision.

## Latency budget

PRD §15 CP-02b requires p99 detection latency < 800ms from the cancel event to the alert appearing in `argus.${STUDENT_ID}.realtime_alerts.v1`. The 800ms budget breaks down as:

| Stage | Budget |
|---|---|
| Kafka → Flink consumer | ~50ms |
| Pattern match + windowing | ~100ms |
| State checkpoint barrier alignment | ~50ms |
| Sink to Kafka | ~50ms |
| **Total per-event** | **~250ms typical, p99 < 800ms** |

If you see p99 latencies above this, common causes: (1) `parallelism` too low for the partition count, (2) `checkpoint-interval-ms` too aggressive (try 30s if 10s is causing alignment stalls), (3) too many keys with low-volume traffic causing state imbalance.

## Comparison with JOB-08 (Spark batch)

JOB-08 in Module 3 implements the same R-101 / R-102 patterns as **batch SQL on Iceberg**, running every 30 minutes. Lab 1.5 (`labs/lab-1-5-throughput-comparison.md`) walks through running both engines on the same FLOW-SIM trace and measuring the latency delta — typically 5min p99 (batch) vs <800ms p99 (streaming).

This is not a competition. Both engines run in production. Batch wins for canonical record-keeping (replay-able from Iceberg snapshots, easy to audit). Streaming wins for analyst notification and circuit-breaker triggers.
