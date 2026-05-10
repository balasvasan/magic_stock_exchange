# Lab 1.3 — Throughput at Peak (CP-03)

> 👋 **Module 1 first-timer?** Read [`docs/module-1-primer.md`](../docs/module-1-primer.md) before starting this lab. About 20 minutes.

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
> **Closes deficiency:** ARG-1 (peak volume crisis) — exactly the deficiency this lab proves the platform has fixed
> **Time:** ~45 minutes if executor sizing is right first try; up to 2 hours if you have to retune.
> **Source files:** [`src/ingest/replay_simulator.py`](../src/ingest/replay_simulator.py) (continuous mode)

This is the lab that proves the legacy CEP engine's worst day — the day SEBI cited 14 missed manipulation episodes — would no longer be a problem. **It's also the lab where you'll see what under-provisioning looks like in real time and learn how to diagnose and fix it.** That's a skill the surveillance platform team will use every quarter when load patterns shift.

## What you're going to do

In order:

1. **Confirm Lab 1.2's jobs are still healthy** — running the throughput test against a degraded ingest layer wastes time. (~3 min)
2. **Scale up Spark executors** for the peak test. (~5 min)
3. **Capture baseline metrics** — DLQ counts, latencies, etc. before the load arrives, so you can measure the delta. (~3 min)
4. **Run FLOW-SIM in continuous mode at 150K ev/s for 10 minutes** — the actual stress test. (~12 min)
5. **Measure ingest-to-Bronze latency** during the run with a sampling SQL query. (~during the run)
6. **Verify CP-03 pass conditions** — three named checks. (~5 min)

Total: about 45 minutes, but this is the most likely lab to need a retune-and-rerun cycle. If your executors are too small, you'll find out at minute 7 of the 10-minute test, fix it, and re-run — adding ~30 minutes per cycle.

## Before you begin — prerequisite checklist

- [ ] [Lab 1.2](lab-1-2-bronze-ingest.md) is complete and CP-04 passed
- [ ] All four Bronze ingest jobs are still in `RUNNING` state — quick check: `cde job list | grep "argus-${STUDENT_ID}"` should show 4 RUNNING entries
- [ ] You have **at least 12 worker nodes available** in your CDP environment (or a smaller floor sanctioned by your instructor — see "Cluster sizing" below)
- [ ] You can access **Spark UI for at least one of your running jobs** — quick check: `cde job describe --name argus-${STUDENT_ID}-job_01_bronze_orders` shows a Spark UI URL
- [ ] You can access **SMM (Streams Messaging Manager)** for partition-lag monitoring

## Why 150K events/sec — read this before Step 4

You might wonder *where does 150,000 come from?* It's not arbitrary.

The legacy MSE platform was sized in 2017 for ~40K ev/s. Three structural changes between 2017 and 2025 made that sizing inadequate:

1. **Indian retail options participation exploded** (post-2020). Daily F&O contract volume doubled.
2. **Co-located algorithmic trading on MSE** roughly tripled. Each colo participant emits roughly 10× the order events of a standard member firm because algos place and cancel constantly.
3. **Single-stock futures expanded** from a niche product to ~280 actively-quoted underlying names, multiplying cross-product event volume.

The combined effect on F&O expiry Thursdays is a peak rate of ~150K events/sec across cash + futures + options — about **4× the legacy platform's design point**. That mismatch is what brought the legacy platform down: every expiry Thursday it spent 6 hours behind, alerts arrived after the close, manipulation completed without intervention.

Module 1's job is to demonstrate the new platform survives that load. CP-03 is the demonstration. **Pass means: 150K ev/s sustained 10 minutes, no growing backlog, p99 ingest-to-Bronze latency < 30 seconds.**

### Cluster sizing — your floor may be lower than 150K

The numerical target depends on your cluster. CP-03 is calibrated against a "reference cluster" that has 6+ Spark executors per ingest job and ~200 GB cluster RAM. Smaller clusters can't physically hit 150K — and that's fine.

Your instructor will set your specific CP-03 floor. Common settings:

| Cluster size | CP-03 floor |
|---|---:|
| Reference (6+ executors per job) | 150K ev/s |
| Mid-size (~70% of reference) | 100K ev/s |
| Sandbox (<50% of reference) | 50K ev/s |

The lab text below targets 150K. If your floor is different, scale FLOW-SIM's `--rate` argument accordingly and apply the same proportional reduction to the expected row counts.

## Step 1 — Confirm the four ingest jobs are running

```bash
cde job list | grep "argus-${STUDENT_ID}"
```

**Expected output:** four lines, each ending in `RUNNING`. Like:

```
argus-s001-job_01_bronze_orders        RUNNING
argus-s001-job_02_bronze_trades        RUNNING
argus-s001-job_03_bronze_member_cdc    RUNNING
argus-s001-job_04_bronze_external_feeds RUNNING
```

If any are `FAILED` or `STOPPED`, **stop and fix Lab 1.2 first**. Running CP-03 against a degraded ingest layer produces confusing results — you'll think the throughput is bad when actually one job isn't even consuming.

## Step 2 — Scale up Spark executors for the peak test

The Lab 1.2 sizing was 6 executors per job, sized for steady-state load (~30K ev/s on the orders topic). At peak (~120K ev/s on orders, since 80% of the 150K total is order events), 6 executors will be overwhelmed.

```bash
# JOB-01 reads orders.v1 (48 partitions). Rule of thumb: 1 executor per 4 partitions.
cde job update --name argus-${STUDENT_ID}-job_01_bronze_orders --num-executors 12

# JOB-02 reads trades.v1 (24 partitions, ~12K ev/s peak). 4 executors is plenty.
cde job update --name argus-${STUDENT_ID}-job_02_bronze_trades --num-executors 4

# JOB-03 (member.cdc.v1) and JOB-04 (external feeds) are low-volume; leave at 6.
```

> 💡 **Why 1 executor per 4 partitions?** This is a heuristic, not a law. The reasoning: each executor runs one task per partition it's assigned (Spark's "1 task per Kafka partition" model). With 4 partitions/executor, each executor processes 4 streams in parallel. More than 4 starts to thrash the executor's CPU; fewer than 4 wastes parallelism. The right number depends on your cluster's CPU per executor — 4 is a good default for the lab's 2-vCore executors.

> 💡 **Why isn't this auto-scaling?** CDE supports auto-scaling but the lab disables it deliberately. Auto-scaling decisions take ~60 seconds to propagate, which means a sudden 4× spike (which is what 150K ev/s vs 30K ev/s steady-state is) will produce a backlog before scaling kicks in. Production-quality streaming systems use *static over-provisioning at peak* + auto-scaling for the long tail. The lab demonstrates the static part; auto-scaling is a Module 1 extension.

After running the updates, give CDE ~30 seconds to apply them. Verify:

```bash
cde job describe --name argus-${STUDENT_ID}-job_01_bronze_orders | grep -i executor
# Should show: numExecutors: 12
```

## Step 3 — Capture baseline metrics

Before the load arrives, snapshot DLQ and Bronze counts so you can measure the delta after the test. Run these and **save the outputs in a scratch file**:

```bash
# Baseline DLQ count
DLQ_BEFORE=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.dlq --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}')
echo "DLQ before: $DLQ_BEFORE"

# Baseline Kafka offset on orders
KAFKA_BEFORE=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{sum += $3} END {print sum}')
echo "Kafka offset before: $KAFKA_BEFORE"
```

Save these as `DLQ_BEFORE` and `KAFKA_BEFORE`. You'll subtract them from post-test values to see what arrived during the 10-minute peak.

## Step 4 — Run FLOW-SIM continuous at 150K ev/s for 10 minutes

This is the actual stress test:

```bash
python src/ingest/replay_simulator.py --mode continuous \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS} \
    --rate 150000 \
    --duration 600
```

**What `--rate` and `--duration` mean:**
- `--rate 150000` — target 150,000 events/sec across all topics combined (FLOW-SIM splits this proportionally: ~120K to orders, ~12K to trades, ~18K to bbo)
- `--duration 600` — run for 600 seconds (10 minutes), then stop

**What you should see during the run** — progress logs every ~5 seconds:

```
==> [continuous] target rate = 150,000 ev/s; per-topic split:
                argus.s001.orders.v1  →  120,000 ev/s
                argus.s001.trades.v1  →   12,000 ev/s
                argus.s001.bbo.v1     →   18,000 ev/s
        250,000 events sent  (148,500 ev/s avg)
        500,000 events sent  (149,200 ev/s avg)
        750,000 events sent  (149,800 ev/s avg)
        ...
        90,000,000 events sent (149,901 ev/s avg)
==> continuous run: 90,000,000 events in 600.4s (149,901 ev/s avg)
```

The actual rate should hold within ±10% of 150,000 throughout.

**While this is running**, in another terminal, run the latency probe (Step 5). Don't wait for FLOW-SIM to finish — the latency probe needs to sample DURING the load.

> ⚠️ **Compliance gate:** This is the most demanding lab in Module 1. If your CDP environment is undersized, you will fall short of 150K ev/s no matter how clean your code is. That's expected — see "Cluster sizing" above. Use your instructor's sanctioned floor.

## Step 5 — Measure ingest-to-Bronze latency

While FLOW-SIM is running, in another terminal open Hue or Impala-Shell. Run this query every 30 seconds:

```sql
SELECT
    COUNT(*)                                        AS rows_last_min,
    AVG(UNIX_TIMESTAMP(ts_ingest) - (ts_us / 1000000.0))   AS avg_latency_sec,
    APPROX_PERCENTILE(
        UNIX_TIMESTAMP(ts_ingest) - (ts_us / 1000000.0), 0.5
    )                                               AS p50_latency_sec,
    APPROX_PERCENTILE(
        UNIX_TIMESTAMP(ts_ingest) - (ts_us / 1000000.0), 0.99
    )                                               AS p99_latency_sec
FROM argus_${STUDENT_ID}_bronze.orders_raw
WHERE ts_ingest >= now() - INTERVAL 1 MINUTE;
```

> 💡 **What this query does in plain English:** for every order written to Bronze in the last 60 seconds, compute the time gap between when the order *happened* (`ts_us`, the event timestamp from the source system) and when our pipeline *wrote it* (`ts_ingest`). Average and p99 of that gap is the end-to-end ingest latency.

**Expected output during healthy peak load:**

| metric | expected |
|---|---|
| `rows_last_min` | ~7,200,000 (i.e. 120,000 ev/s × 60s) |
| `avg_latency_sec` | < 5.0 |
| `p50_latency_sec` | < 3.0 |
| `p99_latency_sec` | **< 30.0 ← CP-03 pass threshold** |

**What it looks like when the pipeline is keeping up:**
- p99 stays bounded around 8–15 seconds and doesn't grow over time
- `rows_last_min` is consistent (slight bounces around 7M)

**What it looks like when the pipeline is falling behind:**
- p99 climbs steadily: 12s → 25s → 38s → 55s
- `rows_last_min` is *less than* 7M (the pipeline can't keep up with input rate)

If you see falling-behind symptoms, see Common Failure Mode #1 below — most likely cause is `maxOffsetsPerTrigger` too small.

## Step 6 — Watch SMM for partition lag (parallel monitoring)

In SMM, navigate to the consumer group for JOB-01 (named like `argus.${STUDENT_ID}.spark_bronze_orders`) and watch the per-partition lag during the run.

**Healthy lag:** each partition shows `lag` < 100,000 messages, and the lag is stable or oscillating (not growing).

**Unhealthy lag:** lag is growing without bound on most partitions: 50K → 100K → 200K → 400K over 5 minutes. This is the smoking gun for under-provisioned consumers.

> 💡 **Why per-partition lag matters more than total lag:** if 1 partition has lag of 5,000,000 and 47 partitions have lag of 50,000 each, total lag is ~7.4M but the actual problem is one stuck partition (key skew). Watching per-partition reveals that. Watching total just shows "things are bad."

## Step 7 — Verify CP-03 pass conditions

After FLOW-SIM completes (~10 minutes), verify all three checks.

### Check 1 — Sustained ev/s rate met or exceeded the floor

FLOW-SIM's final log line shows the average rate for the run. **Pass if:** average ≥ your cluster's CP-03 floor (150K, 100K, or 50K depending on cluster size). **Fail if:** average is below the floor.

### Check 2 — p99 ingest-to-Bronze latency < 30 seconds throughout

The latency probe (Step 5) was sampled every 30 seconds during the run. **Pass if:** p99 latency stayed < 30 seconds at every sampling point. **Fail if:** p99 ever exceeded 30 seconds during the run.

### Check 3 — DLQ rate < 0.01% during peak

```bash
DLQ_AFTER=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.dlq --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}')

DELTA_DLQ=$((DLQ_AFTER - DLQ_BEFORE))
DELTA_INPUT=90000000     # ~90M events sent during the 10-min run

echo "DLQ before: $DLQ_BEFORE"
echo "DLQ after:  $DLQ_AFTER"
echo "DLQ delta:  $DELTA_DLQ"
echo "Rate:       $(awk "BEGIN {printf \"%.6f\", $DELTA_DLQ / $DELTA_INPUT}")"
```

**Pass if:** rate < 0.0001 (i.e., fewer than 1 in 10,000 records routed to DLQ during peak load). **Fail if:** rate ≥ 0.0001.

---

## Common failure mode #1 — p99 latency climbs steadily despite sufficient executors

**Symptom:** average and p50 latency are healthy (~3 seconds), but p99 climbs steadily during the run: 8s → 15s → 25s → 45s.

**Cause:** Spark **micro-batch sizing**. JOB-01 has these defaults:

```python
.option("maxOffsetsPerTrigger", 500_000)
.trigger(processingTime="10 seconds")
```

`maxOffsetsPerTrigger=500_000` says "process at most 500K events per trigger". `processingTime="10 seconds"` says "trigger every 10 seconds". So per 10-second window, Spark processes up to 500K events. That's **50,000 ev/s sustained** — well below 120,000 ev/s on `orders.v1`.

When the input rate exceeds the trigger capacity, Spark falls behind by `(input_rate − trigger_capacity) × 10s` events per trigger. At 120K input vs 50K capacity, that's 700K events of new backlog per 10-second window. Over 10 minutes, the backlog grows to 42M events — and p99 latency climbs accordingly.

**Diagnosis:** in Spark UI → Structured Streaming tab → most recent batch metrics:
- `inputRowsPerSecond` ~ 120K
- `processedRowsPerSecond` flat at ~50K
- `numActiveBatches` climbing
- `batchDuration` consistently at the trigger interval (10s)

**Fix:** raise `maxOffsetsPerTrigger` in `src/ingest/job_01_bronze_orders.py`:

```python
.option("maxOffsetsPerTrigger", 2_000_000)   # accommodate 150K ev/s with headroom
.trigger(processingTime="10 seconds")
```

The new cap is 200K ev/s sustained — handles 150K with a 33% headroom margin.

Re-deploy JOB-01:
```bash
cde job update --name argus-${STUDENT_ID}-job_01_bronze_orders \
    --application-file src/ingest/job_01_bronze_orders.py
cde job restart --name argus-${STUDENT_ID}-job_01_bronze_orders
```

p99 should drop within two trigger cycles (~20 seconds).

> 💡 **The lesson here:** the legacy MSE platform was full of this kind of misconfiguration — sizing inherited from an old workload, never re-tuned as load grew. Settings have to be reviewed against current load, not just inherited. This is exactly the operational discipline ARG-1 is supposed to instill.

## Common failure mode #2 — JOB-01 logs show `OutOfMemoryError` partway through

**Symptom:** JOB-01 starts the test, runs fine for 3–4 minutes, then crashes with `java.lang.OutOfMemoryError: Java heap space`.

**Cause:** executor memory is insufficient for the per-batch volume. At 120K ev/s with `maxOffsetsPerTrigger=2_000_000`, each batch is up to 2M events × ~500 bytes per event = ~1 GB per batch. With 12 executors, that's ~85 MB per executor per batch — fine if executor memory is 4 GB. But spilled-to-disk shuffles plus Iceberg writer buffers can push effective memory pressure past 80% of the 4 GB heap.

**Diagnosis:** in Spark UI → Executors tab, look for executors with high "GC Time" relative to "Task Time" (> 20% means the executor is GC-thrashing).

**Fix:** bump executor memory:
```bash
cde job update --name argus-${STUDENT_ID}-job_01_bronze_orders \
    --executor-memory 8g
cde job restart --name argus-${STUDENT_ID}-job_01_bronze_orders
```

## Common failure mode #3 — Test passes but CP-04 row-count check now fails

**Symptom:** CP-03 passes cleanly, but when you re-run CP-04 Check 2 (Bronze row count vs Kafka offset), the gap is now 4% instead of 0.1%.

**Cause:** the 10-minute peak run added 90M events to Kafka. JOB-01 is processing them, but if any executor failures happened during the run (e.g., one executor briefly went offline and was replaced), some events may have been deferred to a later batch. The gap closes within ~5 minutes after FLOW-SIM stops.

**Diagnosis:** wait 5 minutes, re-run the SQL count and the Kafka offset capture. The gap should now be back to <0.1%.

**Fix:** patience. If the gap doesn't close after 10 minutes, then there's a real problem — check executor logs for unhandled exceptions during the test window.

---

## Pass condition for CP-03

All three checks pass:
- ✅ Sustained the cluster-adjusted floor rate for 10 minutes
- ✅ p99 ingest-to-Bronze latency stayed < 30 seconds throughout
- ✅ DLQ rate < 0.01% during peak

When all three pass, MSE has a streaming ingest layer that survives F&O expiry Thursday — the workload that brought down the legacy platform.

## Wrap-up — what you can now do that you couldn't before

You can stress-test a streaming pipeline at production peak rates and measure end-to-end latency in real time. You can identify under-provisioning by reading Spark UI metrics, partition lag, and batch sizing parameters. You can diagnose the difference between "compute is too small" (executor count) and "memory is too tight" (executor heap) and "trigger sizing is wrong" (`maxOffsetsPerTrigger`).

Most importantly: **ARG-1 (the throughput half) is now provably closed.** The legacy platform's worst day no longer breaks anything.

Lab 1.4 is the next half of ARG-1: the *latency* half. Spark's 10-second trigger is fast enough for canonical persistence but too slow for sub-second pattern detection. That's why ARGUS introduces PyFlink CEP — and that's what you'll deploy next. Allow about 75 minutes for Lab 1.4.
