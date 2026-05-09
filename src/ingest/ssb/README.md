# SQL Stream Builder — Analyst-Driven Streaming SQL

This directory contains JOB-11, an SSB SQL streaming application that detects R-104 cross-product imbalance (the Jane Street pattern) on the live order stream.

## Why SSB exists in ARGUS

JOB-10 is the engineering path: PyFlink CEP, requires Python skills, lives in version control, gets reviewed in PRs. JOB-11 is the **analyst path**: streaming SQL written in SSB's web UI by surveillance analysts who don't code. Both compile down to Flink underneath; the lab teaches when to choose which.

Real production exchanges use both. Engineers ship the high-volume, low-latency patterns (spoofing, layering — JOB-10). Analysts iterate on novel patterns they suspect from market color (cross-product imbalance — JOB-11) without filing tickets.

## Files

| File | Purpose |
|---|---|
| `job_11_cross_product_imbalance.sql` | The SSB SQL job. ~80 lines. |
| `README.md` | This file. |

## How to deploy

### Option A: SSB UI (the analyst workflow)

1. Open SSB in your browser: `https://ssb.<your-cdp>/streams-sql-console/`
2. Click **"Compose"** → **"New SSB Job"**
3. **Set variables** (Settings → Job Parameters):
   - `STUDENT_ID` = your student ID
   - `KAFKA_BROKERS` = your Kafka broker list
4. Paste the contents of `job_11_cross_product_imbalance.sql` into the editor
5. Click **"Validate"** — SSB compiles to Flink and reports any errors
6. Click **"Execute Job"**
7. Verify the job is running: SSB → "Jobs" → look for the running job named after your student ID

### Option B: REST API (the automation workflow)

For CI/CD or instructor scripts:

```bash
# Substitute STUDENT_ID + KAFKA_BROKERS at deploy time
envsubst < src/ingest/ssb/job_11_cross_product_imbalance.sql > /tmp/job_11_resolved.sql

# Submit via SSB REST
curl -u "$SSB_USER:$SSB_PASSWORD" \
    -X POST "https://ssb.<your-cdp>/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d @<(jq -n --arg sql "$(cat /tmp/job_11_resolved.sql)" '{name: "argus_'$STUDENT_ID'_cross_product_imbalance", sql: $sql}')
```

### Option C: Local validation (no SSB cluster)

For students who want to verify the SQL parses before submitting:

```bash
# Just check it through envsubst — no actual run, but catches syntax issues
export STUDENT_ID=s001
export KAFKA_BROKERS=localhost:9092
envsubst < job_11_cross_product_imbalance.sql | head -50
```

## Per-student namespacing

The SQL file uses `${STUDENT_ID}` in:
- All table names (`orders_stream_${STUDENT_ID}`, `instrument_master_${STUDENT_ID}`, `realtime_alerts_sink_${STUDENT_ID}`)
- Source/sink Kafka topic names (`argus.${STUDENT_ID}.orders.v1`, etc.)
- Consumer group ID (`argus.${STUDENT_ID}.ssb.cross_product_v1`)
- Generated alert_id prefix (`RT-XPROD-...`)

When SSB substitutes the variable, every student's SSB job runs in its own logical namespace on the shared SSB cluster.

## What this teaches

The same R-104 pattern is also implemented as a batch SQL rule in JOB-08 (Module 3). Lab 1.4 (`labs/lab-1-4-ssb-sql.md`) walks through:

1. Reading the JOB-08 batch SQL — same pattern, table over Iceberg
2. Reading this SSB SQL — same pattern, declared over Kafka stream
3. Submitting both, generating a planted Case 2 manipulation event, observing latency: 30 minutes (batch) vs ~2 seconds (SSB)

The lesson is that the *pattern* is the same — what changes is when you find out about it. Sub-second matters when an analyst can intervene; 30 minutes matters when you need full historical context for legal certainty.

## Latency budget

PRD §15 CP-04b requires the alert to land in `gold.realtime_alert_stream` within 60s of the planted event. Breakdown:

| Stage | Budget |
|---|---|
| Kafka → SSB | ~50ms |
| Window aggregation (10s slide on 60s window) | ~10s (waiting for window to close) |
| SQL evaluation + sink to Kafka | ~500ms |
| JOB-12 picks up from realtime_alerts.v1 → Iceberg | ~10s (Spark Streaming trigger) |
| **Total** | **~20-30s typical, < 60s p99** |

Note that SSB's latency is dominated by the SQL window semantics (60s window, 10s slide). For sub-second detection you'd use PyFlink CEP (JOB-10). For declarative, analyst-iterable SQL you accept the higher latency in exchange for removing engineering bottleneck.
