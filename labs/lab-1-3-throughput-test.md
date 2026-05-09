# Lab 1.3 — Throughput at Peak (CP-03)

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
> **Closes deficiency:** ARG-1 — exactly the deficiency this lab proves the platform has fixed
> **Source files:** [`src/ingest/replay_simulator.py`](../src/ingest/replay_simulator.py) (continuous mode)

## Objectives

- Run FLOW-SIM in continuous mode at the F&O expiry-day peak rate of 150,000 events/sec
- Sustain that rate for at least 10 minutes
- Measure end-to-end ingest-to-Bronze latency and confirm p99 < 30 seconds
- Verify the platform absorbs the load without DLQ growth, queue lag, or executor failures

This is the lab that proves the legacy CEP engine's worst day — the day SEBI cited 14 missed manipulation episodes — would no longer be a problem.

## Why 150K events/sec

The legacy MSE platform was sized in 2017 for ~40K ev/s. Three structural changes between 2017 and 2025 made that sizing inadequate:

1. **Indian retail options participation exploded** (post-2020). Daily F&O contract volume doubled.
2. **Co-located algorithmic trading on MSE** roughly tripled, with each colo participant emitting 10× the order events of a standard member.
3. **Single-stock futures expanded** from a niche product to ~280 actively-quoted underlying names, multiplying the cross-product event volume.

The combined effect on F&O expiry Thursdays is a peak rate of ~150K events/sec across cash + futures + options — about 4× the legacy platform's design point. That's the number you'll target.

## Procedure

### Step 1 — Confirm the four ingest jobs from Lab 1.2 are running

```bash
cde job list | grep argus-job_0
```

You should see all four jobs in `RUNNING` state. If any are `FAILED` or `STOPPED`, fix Lab 1.2 before proceeding — running CP-03 against a degraded ingest layer will produce confusing results.

### Step 2 — Scale up Spark executors for the peak test

```bash
cde job update --name argus-job_01_bronze_orders --num-executors 12
cde job update --name argus-job_02_bronze_trades --num-executors 4
# JOB-03 and JOB-04 don't need more — their topics are lower volume
```

Rule of thumb: roughly one executor per 4 Kafka partitions on the topic the job consumes. JOB-01 reads `argus.${STUDENT_ID}.orders.v1` (48 partitions) so 12 executors = 4 partitions/executor.

### Step 3 — Run FLOW-SIM continuous at 150K ev/s for 10 minutes

```bash
python src/ingest/replay_simulator.py --mode continuous \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS} \
    --rate 150000 \
    --duration 600
```

**What you should see**: progress logs every ~5 seconds reporting actual ev/s. The actual rate should hold within ±10% of 150,000 for the full 10-minute window.

```
==> [continuous] target rate = 150,000 ev/s; per-topic split:
                argus.${STUDENT_ID}.orders.v1  →  120,000 ev/s
                argus.${STUDENT_ID}.trades.v1  →   12,000 ev/s
                argus.${STUDENT_ID}.bbo.v1     →   18,000 ev/s
        250,000 events sent  (148,500 ev/s avg)
        500,000 events sent  (149,200 ev/s avg)
        ...
==> continuous run: 90,000,000 events in 600.4s (149,901 ev/s avg)
```

> ⚠️ **Compliance gate:** This is one of the more demanding labs in the capstone. If your CDP environment is undersized, you will fall short of 150K ev/s no matter how cleanly your code is written. That's expected — the rubric for CP-03 accepts a measured-rate floor of 100K ev/s on the smallest sanctioned cluster, scaling linearly with cluster size up to 150K. Your instructor sets the threshold for your specific cluster.

### Step 4 — Measure ingest-to-Bronze latency

While FLOW-SIM is running, in another terminal run a continuous latency probe:

```sql
-- Run this every 30 seconds during the test, in Impala or Hue:
SELECT
    COUNT(*) AS rows_last_min,
    AVG(UNIX_TIMESTAMP(ts_ingest) - (ts_us / 1000000.0)) AS avg_latency_sec,
    APPROX_PERCENTILE(
        UNIX_TIMESTAMP(ts_ingest) - (ts_us / 1000000.0), 0.5
    ) AS p50_latency_sec,
    APPROX_PERCENTILE(
        UNIX_TIMESTAMP(ts_ingest) - (ts_us / 1000000.0), 0.99
    ) AS p99_latency_sec
FROM argus_${STUDENT_ID}_bronze.orders_raw
WHERE ts_ingest >= now() - INTERVAL 1 MINUTE;
```

**Expected output** under healthy load:

| metric | expected |
|---|---|
| `rows_last_min` | ~7,200,000 (i.e. 120,000 ev/s × 60s) |
| `avg_latency_sec` | < 5.0 |
| `p50_latency_sec` | < 3.0 |
| `p99_latency_sec` | < 30.0 ← **CP-03 pass threshold** |

If `p99_latency_sec` climbs above 30 seconds, the pipeline is falling behind. Most likely cause is JOB-01 needs more executors. Bump `--num-executors` and watch the latency drop on the next 1-minute window.

### Step 5 — Watch SMM for partition lag

In SMM, navigate to the consumer group for JOB-01 (`argus.bronze.orders_ingest`) and watch the per-partition lag. Healthy lag is < 100,000 messages per partition; lag growing without bound (e.g. 50K → 100K → 200K → 400K over 5 minutes) is the smoking gun for under-provisioned consumers.

## Checkpoint CP-03 — 150K ev/s sustained, p99 latency < 30s

### Pass condition

All three checks pass. **CP-03 is the largest single-component checkpoint in the capstone.**

### Check 1 — Sustained 150K ev/s rate (or cluster-adjusted floor)

FLOW-SIM's final log line shows an average rate within ±10% of the target. If you targeted 150K, the average should be ≥ 135K. If your instructor has set a cluster-adjusted floor (e.g. 100K for a small dev cluster), the average must clear that floor.

### Check 2 — p99 ingest-to-Bronze latency < 30 seconds

Latency probe from Step 4, sampled at 1-minute windows during the run, never exceeds 30 seconds at p99.

### Check 3 — DLQ rate < 0.01% during peak

```bash
# Compare the increase in DLQ vs total input topic during the 10-min window
DLQ_BEFORE=...    # captured before Step 3 started
DLQ_AFTER=...     # captured after Step 3 completed
DELTA_DLQ=$((DLQ_AFTER - DLQ_BEFORE))
DELTA_INPUT=90000000   # ~90M events sent
echo "DLQ rate during peak: $(awk "BEGIN {print $DELTA_DLQ / $DELTA_INPUT}")"
```

**Expected output**: < 0.0001 (i.e. < 1 in 10,000).

---

## Common failure mode — p99 latency climbs steadily despite sufficient executors

**Symptom**: average and p50 latency are healthy (~3 seconds), but p99 climbs from 8s → 15s → 25s → 45s over the 10-minute window.

**Diagnosis**: this is almost always a Spark **micro-batch sizing** issue. JOB-01 has `maxOffsetsPerTrigger=500_000` and `processingTime="10 seconds"`. At 120,000 ev/s on `argus.${STUDENT_ID}.orders.v1`, each 10-second trigger ingests 1.2M events — more than the 500K cap. Spark falls behind by 700K events per trigger, and that backlog only grows.

**Fix**: in `src/ingest/job_01_bronze_orders.py`, change:

```python
.option("maxOffsetsPerTrigger", 500_000)   # too small
.trigger(processingTime="10 seconds")
```

to:

```python
.option("maxOffsetsPerTrigger", 2_000_000)   # accommodate 150K ev/s × 13s headroom
.trigger(processingTime="10 seconds")
```

Then redeploy JOB-01. p99 should drop within two trigger cycles.

This is exactly the kind of misconfiguration the legacy MSE platform was full of — sized for an old workload, never re-tuned. The lesson the lab teaches is *configurations have to be reviewed against current load, not just inherited from the original deployment*.

---

## Pass condition for CP-03

All three checks pass. When they do, MSE has a streaming ingest layer that survives F&O expiry Thursday — the workload that brought down the legacy platform. The capstone's biggest "before/after" moment is now behind you. The next module (Module 2) starts using all that data to actually detect manipulation.
