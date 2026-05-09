# Lab 1.2 — Bronze Ingest Deployment (CP-04)

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
> **Closes deficiency:** ARG-1 (peak volume crisis)
> **Source files:** [`src/ingest/job_01_bronze_orders.py`](../src/ingest/job_01_bronze_orders.py), [`src/ingest/job_02_bronze_trades.py`](../src/ingest/job_02_bronze_trades.py), [`src/ingest/job_03_bronze_member_cdc.py`](../src/ingest/job_03_bronze_member_cdc.py), [`src/ingest/job_04_bronze_external_feeds.py`](../src/ingest/job_04_bronze_external_feeds.py), four NiFi flow JSONs in [`src/ingest/nifi_flows/`](../src/ingest/nifi_flows/)

## Objectives

- Import the four NiFi flow exports into your NiFi canvas and run them
- Deploy the four Spark Structured Streaming Bronze ingest jobs to CDE
- Verify each Bronze table populates with the correct row counts after 10 minutes of streaming
- Confirm the DLQ rate is below the CP-04 threshold

## Why NiFi *and* Spark — a 60-second refresher

NiFi handles the **dirty work outside the platform**: tapping multicast, polling SFTP, calling REST APIs, validating record structure, attaching attributes (Atlas tags, source markers), routing failures to a DLQ. Spark Structured Streaming handles the **rigorous work inside the platform**: exactly-once writes to Iceberg, schema enforcement at the column level, partition routing, late-data handling. Splitting responsibilities this way is the standard CDF + CDE pattern.

## Procedure

### Step 1 — Import the four NiFi flows

In the NiFi UI, navigate to your assigned process group (`/ARGUS/{STUDENT_ID}/`) and import each flow:

```bash
# From the NiFi REST endpoint (replace ${NIFI_URL})
for f in src/ingest/nifi_flows/flow_*.json; do
    curl -X POST -F "file=@$f" \
        ${NIFI_URL}/nifi-api/process-groups/{group_id}/flow-snapshots/upload
done
```

Or do it through the UI: drag each JSON file onto the canvas, accept the import, and configure the parameter context with your `STUDENT_ID`, `KAFKA_BROKERS`, etc.

> 💡 **Tip:** The reference JSON files in `src/ingest/nifi_flows/` are simplified stubs. Your instructor's S3 asset bundle (`s3://argus-training-assets/argus-capstone/v1.0/nifi_flows/`) contains the full production XML exports with proper UUIDs and processor positions — use those if you have access.

After import, start each flow. SMM should show traffic on all 11 Kafka topics within 60 seconds.

### Step 2 — Deploy the four Spark Structured Streaming jobs to CDE

Each job is a standalone Python script. Deploy via the CDE CLI:

```bash
for job in job_01_bronze_orders job_02_bronze_trades job_03_bronze_member_cdc job_04_bronze_external_feeds; do
    cde job create --name "argus-${job}" \
        --type spark \
        --application-file "src/ingest/${job}.py" \
        --executor-memory "4g" \
        --executor-cores 2 \
        --num-executors 6
    cde job run --name "argus-${job}"
done
```

The number of executors is sized for the steady-state load. Lab 1.3 will scale executors up for the peak-volume test.

### Step 3 — Watch the Bronze tables populate

In Hue (or Impala-Shell), run:

```sql
SELECT 'orders_raw'      AS tbl, COUNT(*) FROM argus_${STUDENT_ID}_bronze.orders_raw
UNION ALL
SELECT 'trades_raw',          COUNT(*) FROM argus_${STUDENT_ID}_bronze.trades_raw
UNION ALL
SELECT 'member_cdc',          COUNT(*) FROM argus_${STUDENT_ID}_bronze.member_cdc
UNION ALL
SELECT 'instrument_cdc',      COUNT(*) FROM argus_${STUDENT_ID}_bronze.instrument_cdc
UNION ALL
SELECT 'external_feeds',      COUNT(*) FROM argus_${STUDENT_ID}_bronze.external_feeds
UNION ALL
SELECT 'legacy_alerts',       COUNT(*) FROM argus_${STUDENT_ID}_bronze.legacy_alerts;
```

Within 5–10 minutes of starting the streaming jobs, you should see non-zero counts in five of the six Bronze tables. `legacy_alerts` will be empty until you load it from `data/generated/legacy_alerts_history.csv` — that's a separate one-time batch task documented in the lab steps below.

### Step 4 — One-time batch load of `legacy_alerts`

`legacy_alerts` is the SMRITI archive — a nightly batch from the legacy vendor platform, not a stream. Load it once for Module 5 to use as ML training data:

```python
# In a Spark shell / CML notebook
df = spark.read.csv("s3a://${BUCKET_NAME}/landing/legacy_alerts_history.csv",
                   header=True, inferSchema=True)
df.write.format("iceberg").mode("append").saveAsTable("argus_${STUDENT_ID}_bronze.legacy_alerts")
```

After running this, the `argus_${STUDENT_ID}_bronze.legacy_alerts` count should match the row count of the source CSV (4.8M at full scale, ~25K at scale 0.001).

## Checkpoint CP-04 — Bronze tables populated

### Pass condition

After 10 minutes of streaming, all four checks below pass.

### Check 1 — All 6 Bronze tables have non-zero rows

Run the SQL query above. **Expected output**:

| `tbl` | row count (approx, at scale 0.05) |
|---|---:|
| `orders_raw` | 2,500,000 |
| `trades_raw` | 175,000 |
| `member_cdc` | 12,000 (scaled from KAVACH master + traders) |
| `instrument_cdc` | 4,800 (instrument master) |
| `external_feeds` | 350,000 (mostly BBO) |
| `legacy_alerts` | 25,000 (after Step 4 batch load) |

If any count is 0 after 10 minutes, the corresponding job has failed. Check the CDE job logs.

### Check 2 — Bronze row count matches Kafka offset

For `argus.${STUDENT_ID}.orders.v1`:

```bash
KAFKA_COUNT=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{sum += $3} END {print sum}')
echo "Kafka: $KAFKA_COUNT"
```

```sql
SELECT COUNT(*) AS bronze_count FROM argus_${STUDENT_ID}_bronze.orders_raw;
```

**Expected output**: `bronze_count` should be ≥ 99.9% of `KAFKA_COUNT`. The 0.1% gap is expected — DLQ + in-flight micro-batches.

### Check 3 — DLQ rate < 0.01%

```bash
DLQ_COUNT=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} --topic argus.${STUDENT_ID}.orders.dlq --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}')
echo "DLQ: $DLQ_COUNT, ratio: $(awk "BEGIN {print $DLQ_COUNT / $KAFKA_COUNT}")"
```

**Expected output**: ratio < 0.0001 (i.e. fewer than 1 in 10,000 records routed to DLQ).

### Check 4 — Streaming query latency under control

In the Spark UI for JOB-01, navigate to the Structured Streaming tab. Check:

- `inputRowsPerSecond` ≥ `processedRowsPerSecond` (no growing backlog)
- `batchDuration` p95 < 10 seconds
- `numActiveBatches` consistently 0 or 1 (no queued batches piling up)

If `numActiveBatches` is climbing, the job can't keep up — bump executors.

---

## Common failure mode — JOB-03 stuck on KAVACH stream with no rows produced

**Symptom**: JOB-03 starts, looks healthy in CDE logs, but `argus_${STUDENT_ID}_bronze.member_cdc` row count stays at 0 indefinitely.

**Diagnosis**: the KAVACH CDC topic (`argus.${STUDENT_ID}.member.cdc.v1`) is **log-compacted**. FLOW-SIM does not produce to this topic — the Day 1 generator wrote `consent_records.csv` and `members.csv` as files, not as Kafka events. So the topic has no traffic, and `JOB-03` waits forever for events that never arrive.

This is intentional: in production, KAVACH CDC is fed by Debezium against the live Oracle KYC database; in the lab, the equivalent is a one-time CSV bulk-load.

**Fix**: bulk-load `members.csv` and `traders.csv` directly into `argus_${STUDENT_ID}_bronze.member_cdc` via Spark, similar to the `legacy_alerts` step:

```python
# Combine members + traders into a single CDC-shaped DataFrame
members_df = spark.read.csv("s3a://${BUCKET_NAME}/landing/members.csv", header=True)
# ... join with traders.csv, project to argus_${STUDENT_ID}_bronze.member_cdc schema, write
```

A complete reference implementation lives at `src/ingest/seed_member_cdc.py` (added in Module 2).

---

## Pass condition for CP-04

All four checks pass and JOB-03 is producing rows after the seed step. When this passes, Bronze is alive and you're ready for the throughput test in [Lab 1.3](lab-1-3-throughput-test.md).
