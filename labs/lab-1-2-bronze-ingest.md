# Lab 1.2 — Bronze Ingest Deployment (CP-04)

> 👋 **Module 1 first-timer?** Read [`docs/module-1-primer.md`](../docs/module-1-primer.md) before starting this lab. It explains NiFi, Spark Structured Streaming, and how the two work together. About 20 minutes.

> ℹ️ **Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection
> **Closes deficiency:** ARG-1 (peak volume crisis)
> **Time:** ~60 minutes if everything works first try; up to 3 hours if NiFi import or CDE job creation hits issues. **This is the longest lab in Module 1.**
> **Source files:**
> - 4 Spark Streaming jobs in [`src/ingest/job_01..04_*.py`](../src/ingest/)
> - 4 NiFi flow definitions in [`src/ingest/nifi_flows/`](../src/ingest/nifi_flows/)
> - Helper script for KAVACH CDC seeding in [`src/ingest/seed_member_cdc.py`](../src/ingest/seed_member_cdc.py)

## What you're going to do

In order:

1. **Import the four NiFi flows** to your NiFi canvas — these are the "outside world to Kafka" ingest paths. (~15 min)
2. **Configure each flow** with your `STUDENT_ID` and Kafka brokers. Start them. (~10 min)
3. **Deploy the four Spark Structured Streaming jobs to CDE** — these are the "Kafka to Iceberg Bronze" jobs. (~15 min)
4. **Verify Bronze tables populate** — watch row counts climb in five of the six Bronze tables. (~10 min)
5. **One-time batch load `legacy_alerts`** — this is the SMRITI nightly archive, batch-only. (~5 min)
6. **Verify CP-04 pass conditions** — four named checks. (~5 min)

Total: about 60 minutes. The "longest lab" rating comes from troubleshooting time, not happy-path execution time.

## Before you begin — prerequisite checklist

- [ ] [Lab 1.1](lab-1-1-flow-sim.md) is complete and CP-02 passed
- [ ] Your Kafka topics have data — quick re-check: `kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server ${KAFKA_BROKERS} --topic argus.${STUDENT_ID}.orders.v1 --time -1 | awk -F: '{sum += $3} END {print sum}'` should print a number > 100,000
- [ ] You have access to **NiFi UI** — your instructor will have given you a URL like `https://<cluster>.cdf.cloudera.site/nifi/`. Test by opening it and confirming you can log in.
- [ ] You have access to **CDE (Cloudera Data Engineering)** — your instructor will have given you a virtual cluster name. Test by running `cde job list` from your shell; you should see no error.
- [ ] You have access to **Hue or Impala-Shell** for running SQL against Iceberg tables. Test by running `SELECT 1;` and confirming you get a result.

If any prerequisite fails, **fix it before continuing**. Each is non-negotiable for this lab.

## Why NiFi *and* Spark Streaming — the architectural split

A common first question: *Spark Structured Streaming can read from Kafka directly. Why have NiFi in front of it?* They sound redundant.

They're not. They handle different problems.

**NiFi handles the dirty work outside the platform**:
- Tapping multicast feeds (TARANG match-engine emits via UDP multicast inside the trading VLAN)
- Polling SFTP servers (SEBI Intermediary Portal dumps regulatory notices nightly)
- Calling REST APIs (the BBO vendor exposes a JSON-over-HTTP feed)
- Validating record structure inline (drop or DLQ malformed records before they pollute the platform)
- Attaching attributes (the Atlas tag `PII_HIGH_${STUDENT_ID}` gets stamped on KAVACH CDC events at NiFi ingress, so by the time they hit Spark, lineage already shows the classification)

**Spark Structured Streaming handles the rigorous work inside the platform**:
- Exactly-once writes to Iceberg (no duplicates, no losses, even if a job restarts mid-batch)
- Schema enforcement at the column level (reject events that don't match the Bronze table's expected schema)
- Partition routing (event with `event_ts = 2026-05-09T...` lands in the `ingest_date=2026-05-09` partition)
- Late-data handling (events arriving 10 minutes after their `event_ts` still land in the right partition)

Could Spark do all of NiFi's job? Theoretically yes — there's a Spark connector for almost every source. In practice, **NiFi's drag-and-drop UI lets non-Java engineers reconfigure ingest paths**. When a new SEBI feed format arrives next quarter, an ops engineer adds a NiFi processor in 30 minutes. Doing the same in custom Spark code means a Java/Scala/Python pull request, code review, regression tests — days of engineering. The NiFi+Spark split is a deliberate choice that trades a little architectural complexity for a lot of operational flexibility.

The take-away: NiFi for source-to-Kafka, Spark for Kafka-to-Iceberg. Both are needed. The split is the right pattern.

## Step 1 — Import the four NiFi flows

In your NiFi UI, you have an assigned process group (folder) for your work. The path looks like `/ARGUS/${STUDENT_ID}/`. Your instructor will have created this for you.

There are four flow files in the repo that need importing:

```
src/ingest/nifi_flows/
├── flow_01_orders_landing.json
├── flow_02_trades_landing.json
├── flow_03_kavach_cdc.json
└── flow_04_external_feeds.json
```

### Option A — Import via NiFi UI (recommended for first-timers)

1. Open your NiFi UI in a browser.
2. Navigate to your process group `/ARGUS/${STUDENT_ID}/`.
3. For each flow JSON file: drag the file from your desktop onto the canvas. NiFi will prompt "Import flow definition?" — click Import.
4. After all four imports, your canvas should show four process group boxes, one per flow.

### Option B — Import via REST API (faster if you've used NiFi before)

```bash
# Get the process group ID for your student namespace
PG_ID=$(curl -s ${NIFI_URL}/nifi-api/process-groups/root/process-groups | \
        jq -r ".processGroups[] | select(.component.name==\"${STUDENT_ID}\") | .id")
echo "Your process group ID: $PG_ID"

# Import each flow
for f in src/ingest/nifi_flows/flow_*.json; do
    echo "Importing $f"
    curl -X POST -F "file=@$f" \
        ${NIFI_URL}/nifi-api/process-groups/${PG_ID}/flow-snapshots/upload
done
```

> 💡 **What's actually in those flow JSON files?** Each is a NiFi *flow definition* — a serialized graph of processors, connections, and parameter contexts. The reference files in `src/ingest/nifi_flows/` are simplified stubs designed to be readable in 5 minutes. Your instructor's S3 asset bundle (typically `s3://argus-training-assets/argus-capstone/<version>/nifi_flows/`) may contain richer production-style flows with proper UUIDs, processor positions, and bundle references. **Use the instructor bundle if available** — the stubs work but the bundle is what real production looks like.

After import, do not start the flows yet. We need to configure them first.

## Step 2 — Configure each flow with your STUDENT_ID

NiFi flows reference a **parameter context** — a set of named values (like environment variables) that processors pull from. Your imported flows have parameters they need filled in:

- `STUDENT_ID` — your assigned student identifier
- `KAFKA_BROKERS` — comma-separated broker list, e.g., `kafka1:9092,kafka2:9092,kafka3:9092`
- `BUCKET_NAME` — your S3 bucket from Lab 0.1
- `KAVACH_DB_HOST` — only used by `flow_03_kavach_cdc`; your instructor will provide

In the NiFi UI:

1. Open Controller Settings (top-right hamburger menu → Controller Settings).
2. Click the "Parameter Contexts" tab.
3. For each flow, find its parameter context (named like `argus_${STUDENT_ID}_orders_landing`, etc.).
4. Click the pencil icon to edit, fill in each parameter value, click Apply.

Once parameters are filled in, **start each flow**:

1. Right-click each flow's process group on the canvas.
2. Select "Start" from the menu.
3. The processors inside should turn green and start showing throughput counts.

**Expected outcome:** within 60 seconds, your four flows should show:
- `flow_01_orders_landing` — pulling from a multicast simulator, pushing to `argus.${STUDENT_ID}.orders.v1`
- `flow_02_trades_landing` — pulling from a Kafka mirror simulator, pushing to `argus.${STUDENT_ID}.trades.v1`
- `flow_03_kavach_cdc` — **probably idle** (no live KAVACH database in lab; we'll seed manually in Step 5)
- `flow_04_external_feeds` — pulling from BBO/SEBI/news simulators, pushing to multiple topics

Open SMM (Streams Messaging Manager) and confirm Kafka topics are receiving traffic. **If a flow shows red errors**, see Common Failure Mode #1 below.

## Step 3 — Deploy the four Spark Structured Streaming jobs to CDE

Now the consumer side. Each job reads one Kafka topic and writes to one Bronze Iceberg table.

```bash
for job in job_01_bronze_orders job_02_bronze_trades job_03_bronze_member_cdc job_04_bronze_external_feeds; do
    cde job create --name "argus-${STUDENT_ID}-${job}" \
        --type spark \
        --application-file "src/ingest/${job}.py" \
        --executor-memory "4g" \
        --executor-cores 2 \
        --num-executors 6
    cde job run --name "argus-${STUDENT_ID}-${job}"
done
```

> 💡 **What `cde job create` does:** registers a job definition in CDE's catalog (think of it as "save this job's specification"). `cde job run` actually starts execution. Separating registration from execution lets you re-run the same job many times without redefining it.

> 💡 **Why 6 executors?** Steady-state load on the orders topic is ~30K events/sec. One executor handles ~5K ev/s of streaming reads on a typical lab cluster. 6 executors gives ~30K capacity with no headroom. Lab 1.3 will scale this to 12 executors for the 150K peak test.

> 💡 **What does each job actually do?** Open `src/ingest/job_01_bronze_orders.py` — it's ~50 lines. It:
> 1. Defines the expected schema for incoming JSON events
> 2. Creates a Spark Streaming reader against the Kafka topic
> 3. Parses each Kafka message's `value` field as JSON, validates the schema, drops the offset/key columns
> 4. Writes the validated batch to `argus_${STUDENT_ID}_bronze.orders_raw` as Iceberg, partitioned by `ingest_date`
> 5. The whole thing runs continuously with a 10-second processing trigger
>
> All four jobs follow the same template — only the schema, topic, and target table differ.

After running `cde job run` for each, check status:

```bash
cde job list | grep "argus-${STUDENT_ID}"
```

**Expected output:** four jobs in state `RUNNING`. If any are `FAILED`, see Common Failure Mode #2.

## Step 4 — Watch the Bronze tables populate

In Hue (or `impala-shell`), run:

```sql
SELECT 'orders_raw'      AS tbl, COUNT(*) AS rows FROM argus_${STUDENT_ID}_bronze.orders_raw
UNION ALL
SELECT 'trades_raw',          COUNT(*)         FROM argus_${STUDENT_ID}_bronze.trades_raw
UNION ALL
SELECT 'member_cdc',          COUNT(*)         FROM argus_${STUDENT_ID}_bronze.member_cdc
UNION ALL
SELECT 'instrument_cdc',      COUNT(*)         FROM argus_${STUDENT_ID}_bronze.instrument_cdc
UNION ALL
SELECT 'external_feeds',      COUNT(*)         FROM argus_${STUDENT_ID}_bronze.external_feeds
UNION ALL
SELECT 'legacy_alerts',       COUNT(*)         FROM argus_${STUDENT_ID}_bronze.legacy_alerts;
```

**Expected output, after 5–10 minutes of streaming:**

| `tbl` | rows (approx, scale 0.05) |
|---|---:|
| `orders_raw` | 2,500,000 |
| `trades_raw` | 175,000 |
| `member_cdc` | 0 *(seeded in Step 5)* |
| `instrument_cdc` | 4,800 |
| `external_feeds` | 350,000 |
| `legacy_alerts` | 0 *(loaded in Step 5)* |

> 💡 **Why is `member_cdc` empty?** Read the Common Failure Mode section below — this is **expected behavior** in the lab and it has a specific fix in Step 5. It's not a bug.

> 💡 **Why is `legacy_alerts` empty?** It's a *batch* table loaded once from a CSV, not a streaming table. We'll load it in Step 5.

If the four streaming tables (`orders_raw`, `trades_raw`, `instrument_cdc`, `external_feeds`) all have non-zero rows, you're on track. If any is 0 after 10 minutes, see Common Failure Mode #3.

## Step 5 — One-time batch loads (legacy_alerts + member_cdc seed)

### 5a — Load `legacy_alerts` from the SMRITI archive CSV

`legacy_alerts` is the SMRITI archive — a nightly batch from the legacy vendor surveillance platform, **not a stream**. We load it once now, for Module 5 to use as ML training labels.

```python
# In a Spark shell or CML notebook
df = spark.read.csv(
    f"s3a://{os.environ['BUCKET_NAME']}/landing/legacy_alerts_history.csv",
    header=True, inferSchema=True
)
df.write.format("iceberg").mode("append").saveAsTable(
    f"argus_{os.environ['STUDENT_ID']}_bronze.legacy_alerts"
)
```

After this runs, the `legacy_alerts` count should match the source CSV row count — about 25,000 at scale 0.05, 4.8M at full scale.

### 5b — Seed `member_cdc` from the static KYC reference data

The KAVACH CDC topic is real-time-fed by Debezium against an Oracle KYC database in production. In the lab, we don't have a live Oracle, so we batch-load `members.csv` and `traders.csv` into the `member_cdc` table directly:

```bash
python src/ingest/seed_member_cdc.py
```

This script:
1. Reads `data/generated/members.csv` and `data/generated/traders.csv`
2. Joins them on `member_firm_id`
3. Reshapes to match the `member_cdc` Bronze schema (adds `cdc_op = 'INSERT'`, `cdc_ts = now()`, etc.)
4. Writes via Spark to `argus_${STUDENT_ID}_bronze.member_cdc`

After running, your `member_cdc` count should be ~12,000 (scaled from KAVACH master + traders).

> 💡 **Why isn't `member_cdc` real-time-streamed in the lab?** Because the lab doesn't have a real Oracle KYC database. In production, Debezium watches Oracle's redo log and emits one Kafka event per row change. Simulating that requires either a real database (cost) or a complex CDC simulator (engineering). The capstone takes the pragmatic path: batch-seed once, then in Module 7 the consent withdrawal flow simulates "live CDC" by writing additional events to the topic.

## Step 6 — Verify CP-04 pass conditions

CP-04 has **four checks**. All four must pass.

### Check 1 — All 6 Bronze tables have non-zero rows

Run the SQL UNION query from Step 4 again. **Pass if** all six rows show non-zero counts.

### Check 2 — Bronze row count matches Kafka offset

```bash
KAFKA_COUNT=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.v1 --time -1 \
    | awk -F: '{sum += $3} END {print sum}')
echo "Kafka has $KAFKA_COUNT messages"
```

Then in SQL:
```sql
SELECT COUNT(*) AS bronze_count FROM argus_${STUDENT_ID}_bronze.orders_raw;
```

**Pass if:** `bronze_count ≥ 0.999 × KAFKA_COUNT`. The 0.1% gap is expected (DLQ + in-flight micro-batches that haven't been committed yet). **Fail if:** the gap is > 1%.

### Check 3 — DLQ rate < 0.01%

```bash
DLQ_COUNT=$(kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server ${KAFKA_BROKERS} \
    --topic argus.${STUDENT_ID}.orders.dlq --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum+0}')

echo "DLQ count: $DLQ_COUNT"
echo "Ratio: $(awk "BEGIN {printf \"%.6f\", $DLQ_COUNT / $KAFKA_COUNT}")"
```

**Pass if:** ratio < 0.0001 (fewer than 1 in 10,000 records routed to DLQ). **Fail if:** ratio ≥ 0.0001.

### Check 4 — Streaming query latency under control

In Spark UI for JOB-01 (find via `cde job describe --name argus-${STUDENT_ID}-job_01_bronze_orders`):

1. Navigate to the "Structured Streaming" tab
2. Look at the most recent batch's metrics

**Pass if:**
- `inputRowsPerSecond` ≥ `processedRowsPerSecond` (no growing backlog)
- `batchDuration` p95 < 10 seconds
- `numActiveBatches` consistently 0 or 1

**Fail if:** `numActiveBatches` is climbing — the job can't keep up at current executor count. See Common Failure Mode #4.

---

## Common failure mode #1 — NiFi flow shows red error icons after import

**Symptom:** A processor inside an imported flow shows a red triangle icon. Hovering reveals "Cannot connect to bootstrap.servers" or "Parameter '${KAFKA_BROKERS}' not resolved" or similar.

**Cause:** Parameter context wasn't filled in, or NiFi can't reach Kafka brokers at the addresses you supplied.

**Diagnosis:**
1. Click the red processor → "View configuration" → "Properties" tab. Look for unresolved variables (`${KAFKA_BROKERS}` literal, not the broker list).
2. If parameters are filled in but Kafka still fails, test connectivity from the NiFi node:
   ```bash
   # SSH to the NiFi node, then:
   nc -zv kafka1.cluster 9092
   ```
   If `nc` says "connection refused" or hangs, NiFi can't reach Kafka. Network/firewall issue — escalate to instructor.

**Fix:**
- If parameters were unset: edit the parameter context (Step 2), supply real values, restart the flow.
- If network: instructor needs to fix the cluster's NiFi-to-Kafka security group or firewall rule.

## Common failure mode #2 — `cde job run` fails with "ApplicationFile not found"

**Symptom:** `cde job run` returns an error like `application file 'src/ingest/job_01_bronze_orders.py' does not exist in resource files`.

**Cause:** CDE doesn't have a copy of your source code. CDE jobs reference files from a "resource" — a per-cluster ZIP archive that contains your code.

**Fix:** before creating jobs, upload your source as a CDE resource:

```bash
cde resource create --name "argus-${STUDENT_ID}-src" --type files
cde resource upload --name "argus-${STUDENT_ID}-src" \
    --local-path src/

# Then update job creates to reference the resource:
cde job create --name "argus-${STUDENT_ID}-job_01" \
    --type spark \
    --mount-1-resource "argus-${STUDENT_ID}-src" \
    --application-file "ingest/job_01_bronze_orders.py" \
    ...
```

Note the application-file path is now relative to the resource mount, not your local filesystem.

## Common failure mode #3 — A streaming Bronze table has 0 rows after 10 minutes

**Symptom:** Step 4 query shows `orders_raw: 0` (or any other streaming table at zero) despite the corresponding job being in `RUNNING` state.

**Cause** (most common, in order of probability):
1. The job is reading the wrong topic name (typo in `STUDENT_ID`)
2. The job's consumer group already consumed all messages and is now waiting for new ones (and the lab is using bulk-loaded data, not live continuous data)
3. The job started but immediately errored before consuming anything; CDE shows it RUNNING because the driver is up

**Diagnosis:**
```bash
# Check the job's actual logs
cde job logs --name argus-${STUDENT_ID}-job_01_bronze_orders | tail -100
```

Look for:
- `subscribed to topic argus.<your-id>.orders.v1` near the start — confirms the job found the topic
- `No new offsets to commit` — confirms it caught up and is waiting (which is correct after bulk load)
- Stack traces — confirms an error you missed

**Fix:**
- If the topic name is wrong, the job's `STUDENT_ID` was set incorrectly. Update the CDE job spec and re-run.
- If the consumer caught up after reading bulk-load data, that's actually fine — the row count should still be non-zero. Re-check with `SELECT COUNT(*)`.
- If there's a stack trace, fix that root cause first.

## Common failure mode #4 — Bronze table populates but rows lag the Kafka offset

**Symptom:** Check 2 shows `bronze_count = 1,800,000` but `KAFKA_COUNT = 2,500,000`. Gap of 28%, well above the 0.1% threshold.

**Cause:** the Spark job can't keep up. Either:
- Too few executors (you specified 6, the load needs 12)
- Memory pressure (events are large; 4 GB executors are spilling to disk)
- Network bottleneck (executor↔Kafka latency dominates)

**Diagnosis:** Spark UI → Structured Streaming tab → look at `processedRowsPerSecond` over time. If it's flat at, say, 25K/s while `inputRowsPerSecond` is 50K/s, you have classic underprovisioning.

**Fix:** scale up executors:
```bash
cde job update --name argus-${STUDENT_ID}-job_01_bronze_orders --num-executors 12
# Wait ~30 seconds for the change to take effect; the job auto-scales
```
Then re-run Check 2 after another 5 minutes. The gap should close.

If 12 executors still can't keep up, check executor memory next:
```bash
cde job update --name argus-${STUDENT_ID}-job_01_bronze_orders --executor-memory 8g
```

---

## Pass condition for CP-04

All four checks pass:
- ✅ All 6 Bronze tables have non-zero rows (4 from streaming, 2 from batch loads)
- ✅ Bronze row count matches Kafka offset within 0.1%
- ✅ DLQ rate < 0.01%
- ✅ Streaming queries are not lagging

When all four pass, Bronze is alive and continuously consuming Kafka. You're ready for the throughput test in [Lab 1.3](lab-1-3-throughput-test.md).

## Wrap-up — what you can now do that you couldn't before

You have NiFi flows tapping simulated source systems and streaming events into Kafka. You have Spark Structured Streaming jobs reading those topics and writing exactly-once into Iceberg Bronze tables. You can verify the round-trip — from event arrival in NiFi through Kafka through Spark to Iceberg query — and you can identify when any link is broken.

Most importantly: **this is the canonical record path.** Every event that should ever appear in any analytics or ML feature comes through here. If a regulator asks "show me what your platform saw at 14:23:42 on May 9, 2026", the answer comes from Bronze.

Lab 1.3 stress-tests this same pipeline at the F&O expiry-day peak rate of 150K events/sec — the load that broke the legacy platform. Allow about 45 minutes for that one.
