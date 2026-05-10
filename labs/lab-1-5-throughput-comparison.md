# Lab 1.5 — SSB SQL & Batch-vs-Stream Latency Comparison (CP-04b)

> 👋 **Module 1 first-timer?** Read [`docs/module-1-primer.md`](../docs/module-1-primer.md) before starting this lab. About 20 minutes — explains how SSB SQL fits with PyFlink CEP and Spark Streaming.

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
>
> ⏱ **Time budget:** ~75 minutes if SSB UI access works first try; up to 2 hours if the SSB cluster has connectivity issues.
>
> ✅ **Closes:** ARG-1 (analyst-driven detection capability), and demonstrates the full 3-engine architecture
>
> **Source files:** [`src/ingest/ssb/job_11_cross_product_imbalance.sql`](../src/ingest/ssb/job_11_cross_product_imbalance.sql), [`src/ingest/ssb/README.md`](../src/ingest/ssb/README.md), [`src/ingest/job_12_realtime_alert_persistence.py`](../src/ingest/job_12_realtime_alert_persistence.py)

## What you're going to do

In order:

1. **Verify JOB-12 is running** — the Spark Streaming job that persists Flink/SSB alerts to Iceberg. (~5 min)
2. **Read the SSB SQL** in `src/ingest/ssb/job_11_cross_product_imbalance.sql` — understand the HOP window logic. (~10 min)
3. **Deploy JOB-11 via SSB UI** — copy the SQL, fill in parameters, click Deploy. (~15 min)
4. **Generate Case 2 traffic** with FLOW-SIM and confirm the alert fires. (~10 min)
5. **Verify CP-04b pass condition** — alert lands within 60s of window close. (~10 min)
6. **Run the 3-engine comparison** — same Case 0, see the latency delta across Spark batch / PyFlink / SSB. (~15 min)
7. **Reflect** on the architectural lesson. (~5 min)

Total: about 75 minutes. The 3-engine comparison at the end is the high point of Module 1 — it's where the 3-engine architecture becomes intuitive.

## What this lab teaches

You'll deploy JOB-11, an SSB (SQL Stream Builder) job that detects R-104 cross-product imbalance — the Jane Street pattern. Unlike Lab 1.4 (PyFlink CEP, written in Python), JOB-11 is **declarative SQL** written in SSB's web UI. No Python, no submit script, no CI/CD — analysts deploy it themselves.

You'll then run the **batch-vs-stream comparison**: same R-104 pattern, three engines (JOB-08 Spark batch, JOB-10 PyFlink CEP, JOB-11 SSB SQL), and you'll measure the latency delta. This is the architectural insight the module exists to teach: **the right detection engine depends on what you need to do with the alert**.

## Prerequisites

- [ ] Lab 1.4 complete — JOB-10 (Flink CEP) is running
- [ ] You have access to the SSB web UI (your instructor will give you the URL)
- [ ] JOB-12 (`src/ingest/job_12_realtime_alert_persistence.py`) is running — instructor task or follow Step 1 below

## Step 1 — Verify JOB-12 is persisting realtime alerts

Before deploying JOB-11, make sure the realtime-alert persistence pipeline is working. Check the Iceberg table:

```sql
SELECT COUNT(*), source_engine
FROM argus_${STUDENT_ID}_gold.realtime_alert_stream
GROUP BY source_engine;
```

You should see at least one row with `source_engine='FLINK'` (from Lab 1.4). If the count is zero, JOB-12 isn't running. Submit it via:

```bash
spark-submit \
    --conf spark.kafka.bootstrap.servers=${KAFKA_BROKERS} \
    --conf spark.argus.bucket_name=${BUCKET_NAME} \
    src/ingest/job_12_realtime_alert_persistence.py
```

## Step 2 — Deploy JOB-11 via SSB UI

1. Open SSB in your browser. URL pattern: `https://ssb.<your-cdp>/streams-sql-console/`
2. Click **"Compose"** → **"New SSB Job"**
3. **Name** the job: `argus_${STUDENT_ID}_cross_product_imbalance` (replace `${STUDENT_ID}` with your actual ID)
4. **Set parameters** (Settings → Job Parameters):
   - `STUDENT_ID` = your student ID
   - `KAFKA_BROKERS` = your Kafka broker list
5. Resolve the SQL template through `envsubst` first to substitute variables:

   ```bash
   export STUDENT_ID=s001
   export KAFKA_BROKERS=kafka1.argus.local:9092,kafka2.argus.local:9092
   envsubst < src/ingest/ssb/job_11_cross_product_imbalance.sql | tee /tmp/job_11_resolved.sql
   ```

6. Copy the contents of `/tmp/job_11_resolved.sql` and paste into the SSB editor
7. Click **"Validate"** — SSB will compile to Flink and report any errors
8. Click **"Execute Job"** — should show `RUNNING` status

> 💡 **Tip:** if SSB's UI variable substitution works in your version, you can paste the original `${STUDENT_ID}` template and let SSB substitute. We use `envsubst` in this lab as the more portable approach.

## Step 3 — Generate Case 2 (Jane Street pattern) traffic

We need a planted cross-product imbalance event to flow through. Restart FLOW-SIM with a focus on the synthetic Case 2 events:

```bash
python src/ingest/replay_simulator.py \
    --mode continuous \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS} \
    --planted-cases 2 \
    --rate 5000 --duration 180
```

The `--planted-cases 2` flag (if your generator supports it) replays the Case 2 imbalance pattern. If your generator doesn't have this flag yet, just run plain continuous mode — Case 2 is in the synthetic data and will eventually replay.

## Step 4 — Verify CP-04b pass condition

The CP-04b condition: SSB job fires on Case 2 and writes to `argus.${STUDENT_ID}.realtime_alerts.v1` with `source_engine='SSB'`; row visible in `gold.realtime_alert_stream` within 60s.

```sql
SELECT alert_id, source_engine, rule_id, pattern_type,
       underlying_code, detection_latency_ms,
       fired_ts, ingested_at
FROM argus_${STUDENT_ID}_gold.realtime_alert_stream
WHERE source_engine = 'SSB'
  AND pattern_type = 'CROSS_PRODUCT_IMBALANCE'
ORDER BY fired_ts DESC
LIMIT 5;
```

**Pass condition**: at least one row returned. Detection latency typically 20-30 seconds (dominated by SSB's 60s window with 10s slide).

If no rows: check the SSB job status (should be RUNNING, not FAILED), check JOB-12 is consuming from `realtime_alerts.v1`, and verify FLOW-SIM is generating Case 2 events.

## Step 5 — The Three-Engine Comparison

This is the architectural insight of Module 1. You now have **three engines firing on the same patterns**:

| Engine | Job | Patterns | Latency | Where it stores results |
|---|---|---|---|---|
| **CDE/Spark batch** | JOB-08 | All 5 rules | 30 min | `gold.alert_candidates` |
| **PyFlink CEP** | JOB-10 | R-101, R-102 | <800ms p99 | `gold.realtime_alert_stream` (source_engine=FLINK) |
| **SSB SQL** | JOB-11 | R-104 | ~30s | `gold.realtime_alert_stream` (source_engine=SSB) |

JOB-08 won't run until Module 3 (Day 6), so you'll defer the full comparison until then. For now, run this query to see the realtime side:

```sql
-- The streaming engines side-by-side
SELECT
    source_engine,
    pattern_type,
    rule_id,
    COUNT(*) AS alert_count,
    PERCENTILE(detection_latency_ms, 0.5)  AS p50_latency_ms,
    PERCENTILE(detection_latency_ms, 0.99) AS p99_latency_ms,
    MIN(fired_ts) AS first_fired,
    MAX(fired_ts) AS last_fired
FROM argus_${STUDENT_ID}_gold.realtime_alert_stream
GROUP BY source_engine, pattern_type, rule_id
ORDER BY source_engine, pattern_type;
```

You should see PyFlink CEP much faster than SSB. That's expected — Flink CEP is event-driven; SSB SQL uses windowed aggregations that wait for the window to close.

## Step 6 — Reflect on the architecture

Take 10 minutes to reason through these questions before moving on. Module 3 (JOB-08 batch) and Module 7 (compliance) will revisit them.

1. **If you're a regulator filing an enforcement action**, do you cite the streaming alert (sub-second) or the batch alert (30 min)? Which one's easier to defend in court?

2. **If you're a surveillance analyst at 11:00 IST during F&O expiry**, which alert do you act on — the one that fired in 800ms or the one that fired 30 min ago?

3. **Why do we need both?** What does batch see that streaming can't, and vice versa?

The answers (don't read until you've thought about it):

> 1. **Batch wins for legal proof** — replay-able from Iceberg snapshots, full context, deterministic. Streaming wins for "we acted in real time."
> 2. **Streaming wins for analyst notification** — 800ms means the analyst can ping the trader before the next round of orders. 30 min means it's already over.
> 3. **Batch sees full historical context** — wash-trade rings spanning hours, identity-resolution-resolved entities, ML-scored prioritization. Streaming sees the live pattern but with bounded state. **Production surveillance uses both.** That's the whole point of this module.

## Lab summary

You've built the full real-time detection path: NiFi → Kafka → {PyFlink CEP, SSB SQL} → Iceberg, all per-student namespaced and running on the shared cluster. Combined with the Spark Bronze ingest from Lab 1.2, you now have a 3-engine streaming architecture that mirrors what production exchanges actually deploy.

CP-02b and CP-04b together close ARG-1's "peak detection latency" deficit. ARG-1's "peak ingest throughput" deficit was already closed by CP-03 in Lab 1.3.

## Common failure mode #1 — SSB job state goes to FAILED immediately

**Symptom:** you click Deploy in SSB UI, the job's status briefly shows DEPLOYING, then jumps straight to FAILED. The job logs reference the `instrument_master_${STUDENT_ID}` table.

**Cause:** the SSB SQL JOINs against an instrument-master table-from-topic that's populated by JOB-04 (external feeds Bronze ingest). If JOB-04 hasn't run, the topic is empty and the JOIN has nothing to match against, which manifests as a deploy-time failure.

**Diagnosis:**
```bash
# Confirm the instrument.cdc.v1 topic has rows
kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.instrument.cdc.v1 --time -1 \
    | awk -F: '{sum += $3} END {print "instrument.cdc rows:", sum}'
```
If the count is 0, JOB-04 hasn't populated it.

**Fix:** ensure JOB-04 from Lab 1.2 is running, give it a few minutes to fill the topic, then redeploy the SSB job.

## Common failure mode #2 — `Cannot detect underlying_code`

**Symptom:** SSB job runs but fires zero alerts. Logs reference an inability to classify instruments.

**Cause:** the SQL extracts the underlying equity code from the instrument code via string parsing (e.g., `BNXM-CE-1500` → `underlying=BNXM`). If your synthetic data generator produces instrument codes in a different format, the parsing fails silently, and every event is classified as 'UNKNOWN', which the WHERE clause filters out.

**Diagnosis:**
```bash
# Sample 10 instrument codes from the topic
kafka-console-consumer --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.instrument.cdc.v1 \
    --from-beginning --max-messages 10 \
    | jq -r '.instrument_code'
```
Expected format: `<UNDERLYING>-<PRODUCT>-<STRIKE_OR_EXPIRY>` (e.g., `BNXM-CE-1500`, `BNXM-FUT-2026-03`). If your codes don't follow this pattern, the parsing logic in JOB-11 needs adjustment.

**Fix:** either re-generate synthetic data with `--seed 42` (canonical format), or modify JOB-11's parsing to match your data's actual format.

## Common failure mode #3 — No alerts fire even though Case 2 is in the data

**Symptom:** the SSB job runs, the latency comparison query at the end returns rows for FLINK but not for SSB.

**Cause:** the threshold (`imbalance >= 70000` in the HAVING clause) doesn't match what your planted Case 2 produces.

**Diagnosis:** open `data/generated/orders_synthetic.jsonl.gz` and look at Case 2's imbalance values:
```bash
zcat data/generated/orders_synthetic.jsonl.gz \
    | grep '"planted_case_idx": 2' \
    | jq -r '.qty' | sort -n | tail
```
If the largest qty values are < 70,000, the threshold is too high.

**Fix:** lower the threshold in JOB-11's HAVING clause to match your data — for instance, `HAVING ABS(SUM(...)) >= 50000` — and redeploy.

## Common failure mode #4 — Latency stays > 60s

**Symptom:** CP-04b's pass condition says "within 60 seconds of window close", but your measured latency is 90+ seconds.

**Cause:** SSB's HOP window with a 60-second window and 10-second slide means the window closes 60 seconds after it opens. Plus a few seconds for SSB to compute aggregates and emit the alert. Latencies in the 60–90s range are normal for HOP windows.

**Fix:** if 60s is your hard ceiling, switch from HOP to TUMBLE in the SQL — TUMBLE windows fire as soon as they close, no slide overlap. The trade-off: TUMBLE windows have boundary effects (a Case 2 pattern that straddles a window boundary may not fire). For the lab, HOP at ~70s p99 is acceptable; pure SSB workloads in production tune this empirically.

## Wrap-up — what you can now do that you couldn't before

You can deploy a streaming SQL job through SSB's web UI without writing any Python or Java. You understand why the same pattern looks completely different in three engines (Spark batch SQL, PyFlink CEP imperative pattern, SSB SQL declarative window). You've measured a real latency delta between the three engines and can articulate the trade-offs.

Most importantly: **Module 1 is complete.** ARG-1 is provably closed. The legacy MSE platform's worst day — peak F&O expiry-Thursday volume with no real-time intervention — is no longer a problem.

Module 2 starts Day 4 afternoon and tackles ARG-2 (temporal feature engineering): identity resolution across brokers (the multi-broker manipulation pattern), order-book reconstruction with Iceberg time-travel, and the temporal/cross-product features that ML needs in Module 5. Allow about 6 hours total for Modules 2 + 3.
