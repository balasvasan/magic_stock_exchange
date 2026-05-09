# Lab 1.5 — SSB SQL & Batch-vs-Stream Latency Comparison (CP-04b)

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
>
> ⏱ **Time budget:** 90 minutes (Day 3 afternoon)
>
> ✅ **Closes:** ARG-1 (analyst-driven detection), and demonstrates the full real-time + batch architecture

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

## Common errors

**SSB job FAILED** — most common cause: the `instrument_master_${STUDENT_ID}` table-from-topic doesn't exist or is empty. Without instrument metadata, the JOIN can't classify CASH vs FUT vs OPT. Run JOB-04 first to populate `argus.${STUDENT_ID}.instrument.cdc.v1`.

**`Cannot detect underlying_code`** — check that your synthetic data generator is producing instruments with parseable underlying codes. The expected pattern is `BNXM-CE-1500` → underlying=BNXM, product_type=OPT_CALL.

**No alerts fire even though Case 2 is in the data** — verify the threshold matches what the planted case generates. Lab 1.5's calibration: imbalance >= 70000 (= 7.0 × 10000 lot size). If your generator produces smaller imbalances for Case 2, lower the threshold in JOB-11's HAVING clause and re-deploy.

**Latency >60s** — the 60s window has to close before alerts fire, so latencies in the 60-90s range are normal for SSB's HOP window. To get sub-second SSB latencies you'd switch to a TUMBLE or CUMULATE pattern, but the lab uses HOP because it's the most common analyst pattern.
