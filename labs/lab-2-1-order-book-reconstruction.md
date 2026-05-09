# Lab 2.1 — Order Book Reconstruction (CP-05)

> ℹ️ **Module:** 2 — Identity Resolution & Order Book Reconstruction
> **Closes deficiency:** ARG-2 part 1 (book reconstruction)
> **Source files:** [`src/transform/job_06_silver_order_book_reconstruction.py`](../src/transform/job_06_silver_order_book_reconstruction.py)

## Objectives

- Run JOB-06 to populate `argus_${STUDENT_ID}_silver.order_events` and `argus_${STUDENT_ID}_gold.order_book_snapshots`
- Use an Iceberg `FOR SYSTEM_TIME AS OF` query to retrieve historical book state
- Verify reconstructed bid/ask levels at the planted Case 0 layering window match what the synthetic data generator wrote

## Why this matters

Eleven weeks. That's how long MSE engineering took to reconstruct the order book for SEBI's 14 missed-manipulation episodes. Everything in this lab exists so that an analyst at a future SEBI inquiry can produce the same reconstruction with a 3-line query and have an answer in seconds.

## Procedure

### Step 1 — Confirm prerequisites

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current;
-- expect > 0; if 0, run Lab 2.2 first (JOB-05 must populate member_master)

SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master WHERE is_current;
-- expect ~ 4,800 at full scale, ~ 250 at lab scale 0.05
```

If either is 0, run JOB-05 (`silver_identity_resolution`) and the instrument-master batch loader first.

### Step 2 — Run JOB-06

```bash
cde job create --name argus-job_06_book_reconstruction \
    --type spark \
    --application-file src/transform/job_06_silver_order_book_reconstruction.py \
    --executor-memory 8g --executor-cores 4 --num-executors 8

cde job run --name argus-job_06_book_reconstruction
```

**What you should see**: the job log reports the Bronze window it processed and the snapshot count it wrote:

```
==> order_book_reconstruction: 47,328 1-second snapshots written
```

### Step 3 — Verify Silver enrichment

```sql
SELECT instrument_code, member_firm_category, esm_flag, asm_flag, COUNT(*)
FROM argus_${STUDENT_ID}_silver.order_events
WHERE trade_date = CURRENT_DATE
GROUP BY instrument_code, member_firm_category, esm_flag, asm_flag
ORDER BY 5 DESC LIMIT 20;
```

**Expected output**: 20 rows mixing instruments and member firm categories. The presence of `member_firm_category` and `esm_flag` confirms the Bronze `orders_raw` was successfully enriched with `member_master` + `instrument_master`. If those columns are uniformly NULL, the upstream masters aren't populated.

### Step 4 — Pick a planted case and find its window

```sql
-- Look up Case 0 (mid-cap pharma layering, member BNXM-0042)
-- by finding the cluster of CANCEL events from BNXM-0042 with hold time < 200ms
SELECT
    instrument_code,
    MIN(ts_event)  AS window_start,
    MAX(ts_event)  AS window_end,
    COUNT(*)       AS layered_orders
FROM argus_${STUDENT_ID}_silver.order_events
WHERE member_firm_id = 'BNXM-0042'
  AND action = 'NEW'
  AND trade_date >= CURRENT_DATE - INTERVAL 5 DAYS
GROUP BY instrument_code
HAVING COUNT(*) >= 5
ORDER BY layered_orders DESC LIMIT 1;
```

**Expected output**: one row. Note the `instrument_code` and `window_start` — you'll use them in the next step.

### Step 5 — Reconstruct the book at the layering window using time-travel

```sql
-- Replace <INSTR> and <TS> with values from Step 4.
-- This is the critical time-travel query — it must return correct depth.
SELECT
    snapshot_ts,
    bid_depth_total,
    ask_depth_total,
    spread_bps,
    bids,
    asks
FROM argus_${STUDENT_ID}_gold.order_book_snapshots
WHERE instrument_code = '<INSTR>'
  AND snapshot_ts BETWEEN '<TS>' AND '<TS>' + INTERVAL 2 SECONDS
ORDER BY snapshot_ts
LIMIT 5;
```

**Expected output**: 1–2 snapshot rows showing:

- `bid_depth_total` is **dramatically larger** than `ask_depth_total` (ratio > 5×) — this is the manufactured depth from the layering pattern
- `spread_bps` is unusually narrow (< 5 bps) — false-tight market caused by the layered orders
- `bids` JSON shows 5+ price levels — the layered stack
- `asks` JSON shows 1–2 price levels — the bona-fide opposite side

If the snapshot shows balanced depth and a normal spread, the time-travel reconstruction missed the layering window — most likely cause is JOB-06 only reconstructed the prior 60 minutes and the planted case is older. Bump `--window_minutes` and re-run.

### Step 6 — Iceberg time-travel against historical snapshot

This is the test that proves Iceberg's evidentiary capability:

```sql
-- Get the Iceberg snapshot ID just before JOB-06 ran (a few minutes ago)
SELECT snapshot_id, committed_at FROM argus_${STUDENT_ID}_gold.order_book_snapshots.snapshots
ORDER BY committed_at DESC LIMIT 5;
```

Pick a snapshot ID at least one commit ago. Then:

```sql
-- Query AS OF that snapshot — proves the data existed at that point
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.order_book_snapshots
FOR SYSTEM_VERSION AS OF <snapshot_id>;
```

**Expected output**: a non-zero row count, and the count from the older snapshot should be **less than** the current count (because JOB-06 has written more rows since).

## Checkpoint CP-05 — Order book reconstruction works

### Pass condition

All four checks pass.

### Check 1 — `argus_${STUDENT_ID}_gold.order_book_snapshots` populated

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.order_book_snapshots;
-- expect > 1,000 at lab scale; > 100,000 at full scale
```

### Check 2 — Time-travel query reproduces book state at planted timestamp

The query in Step 5 returns a row with `bid_depth_total > 5 × ask_depth_total` for Case 0's instrument and window. The asymmetry is the layering signature.

### Check 3 — Iceberg `FOR SYSTEM_VERSION AS OF` works

The query in Step 6 returns a non-zero row count for a historical snapshot, and that count is less than the current row count.

### Check 4 — Per-resolution snapshots present

```sql
SELECT snapshot_resolution, COUNT(*)
FROM argus_${STUDENT_ID}_gold.order_book_snapshots
GROUP BY snapshot_resolution;
```

**Expected output**: at least `1S` resolution should be present. `100MS` and `PER_EVENT` are produced on demand by drilldown queries during investigation, so 0 rows is acceptable for those at this checkpoint.

---

## Common failure mode — `FOR SYSTEM_TIME AS OF` returns "snapshot not found"

**Symptom**: the time-travel query in Step 6 fails with `Cannot find snapshot with id X`.

**Diagnosis**: the Iceberg snapshot has been **expired**. By default Iceberg expires snapshots older than 5 days; for `argus_${STUDENT_ID}_gold.order_book_snapshots` this is fine because reconstruction queries are typically against very recent windows. But if the snapshot ID you picked is older than the expiration window, the metadata is gone.

**Fix**: pick a more recent snapshot ID from `.snapshots`. For tables that need long retention — `argus_${STUDENT_ID}_gold.consent_audit` — the DDL sets `'history.expire.enabled' = 'false'`. That's why CP-19 (the COMPLIANCE GATE) tests against `consent_audit` specifically: it's the only table with permanently retained snapshots.

---

## Pass condition for CP-05

All four checks pass. When this passes, MSE has the evidentiary capability that took 11 weeks before — now it's 3 lines of SQL.
