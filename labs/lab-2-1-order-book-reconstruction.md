# Lab 2.1 — Order Book Reconstruction (CP-05)

> 👋 **Module 2 first-timer?** Read [`docs/module-2-primer.md`](../docs/module-2-primer.md) before this lab. About 20 minutes — explains SCD2, fuzzy matching, Iceberg time-travel.

> ℹ️ **Module:** 2 — Identity Resolution & Order Book Reconstruction
> **Closes deficiency:** ARG-2 part 1 (book reconstruction)
> **Time:** ~60 minutes if JOB-06 runs cleanly first try; up to 2 hours if executor sizing or window parameters need tuning.
> **Source files:** [`src/transform/job_06_silver_order_book_reconstruction.py`](../src/transform/job_06_silver_order_book_reconstruction.py)

> 💡 **Run Lab 2.2 BEFORE this lab.** JOB-06 joins against `silver.member_master` (Lab 2.2's output). Module 2 primer explains why.

## What you're going to do

In order:

1. **Confirm prerequisites** — `member_master` + `instrument_master` are populated. (~3 min)
2. **Run JOB-06** — populates `silver.order_events` (enriched) and `gold.order_book_snapshots`. (~15 min)
3. **Verify Silver enrichment** — check that `order_events` has `member_firm_category`, `esm_flag`, etc. (~5 min)
4. **Pick a planted case and find its window** — use Case 0 (mid-cap pharma layering, BNXM-0042). (~10 min)
5. **Reconstruct the book at the layering window** using time-travel. **This is the ARG-2 demonstration.** (~10 min)
6. **Verify Iceberg `FOR SYSTEM_VERSION AS OF`** works against historical snapshots. (~10 min)
7. **Verify CP-05 pass conditions** — four named checks. (~5 min)

Total: about 60 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 2.2](lab-2-2-fuzzy-match.md) is complete and CP-06 passed — `silver.member_master` exists with non-zero `is_current` rows
- [ ] `silver.instrument_master` exists with rows — quick check: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master WHERE is_current` should return ~250 at scale 0.05, or ~4,800 at full scale
- [ ] Bronze `orders_raw` is populated — quick check: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.orders_raw` should return > 100,000
- [ ] You have CDE access and ~8 executors available

## Why book reconstruction matters — read this before Step 2

Eleven weeks. That's how long MSE engineering took to reconstruct the order book for SEBI's 14 missed-manipulation episodes. The team had to:

1. Pull raw event logs from S3 (multi-day search)
2. Replay the events through a custom Python script (multi-day debugging)
3. Cross-reference timestamps against multiple log formats (multi-day reconciliation)
4. Hand-patch missing windows where logs were corrupted (a recurring frustration)
5. Produce a written reconstruction document for each of 14 episodes (writing time alone)

By the time the reconstructions were ready, the SEBI inspection had already produced its preliminary findings. The 11-week effort was largely retrospective justification, not real-time investigation support.

**Everything in this lab exists so that an analyst at a future SEBI inquiry can produce the same reconstruction with a 3-line query and have an answer in seconds.** That's the entire point of using Iceberg with time-travel — `FOR SYSTEM_TIME AS OF` makes the order book at any past moment a single SELECT statement away.

## Step 1 — Confirm prerequisites

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current;
-- expect > 0; if 0, run Lab 2.2 first

SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master WHERE is_current;
-- expect ~ 250 at scale 0.05, ~ 4,800 at full scale
```

If either is 0, run Lab 2.2 (for `member_master`) or `bash sql/provision_environment.sh` (for `instrument_master`) first.

## Step 2 — Run JOB-06

```bash
cde job create --name "argus-${STUDENT_ID}-job_06_book_reconstruction" \
    --type spark \
    --application-file src/transform/job_06_silver_order_book_reconstruction.py \
    --executor-memory 8g --executor-cores 4 --num-executors 8

cde job run --name "argus-${STUDENT_ID}-job_06_book_reconstruction"
```

> 💡 **Why 8g executors instead of 4g?** JOB-06 builds the order book in memory per partition and emits a snapshot every second. Holding the full bid/ask stack for ~250 instruments × the window duration produces a meaningful in-memory data structure. Smaller executors trigger spill-to-disk and slow the job. 8g is sized for typical lab loads.

**Expected output** (in CDE job logs):

```
==> order_book_reconstruction: 47,328 1-second snapshots written
```

The exact count depends on:
- How many instruments are active in the window
- The window duration (default `--window_minutes=60`)
- The data scale (more events = more activity = more distinct snapshots)

> 💡 **What does `1-second snapshot` mean?** JOB-06 produces one row per (instrument, second) capturing the book state as of that second. A typical busy instrument produces 60 rows per minute; a quiet one might produce 0 (no state change in that second). The `47,328` is summed across all instruments × all seconds with state changes.

The job should take 5–15 minutes. Monitor in CDE; once COMPLETED, proceed.

## Step 3 — Verify Silver enrichment

JOB-06 also writes `silver.order_events`, an enriched copy of `bronze.orders_raw` with `member_master` and `instrument_master` joined in. Verify the enrichment worked:

```sql
SELECT
    instrument_code,
    member_firm_category,
    esm_flag,
    asm_flag,
    COUNT(*) AS rows
FROM argus_${STUDENT_ID}_silver.order_events
WHERE trade_date >= CURRENT_DATE - INTERVAL 5 DAYS
GROUP BY instrument_code, member_firm_category, esm_flag, asm_flag
ORDER BY rows DESC LIMIT 20;
```

**Expected output:** 20 rows, mixing instruments and member-firm categories. The presence of non-NULL `member_firm_category` and `esm_flag` confirms the join worked.

> 💡 **What are these columns?**
> - `member_firm_category` — broker classification (`PROP`, `CLIENT`, `MIXED`) from `member_master`
> - `esm_flag` — Enhanced Surveillance Measures flag from `instrument_master` (SEBI's surveillance escalation tier)
> - `asm_flag` — Additional Surveillance Measures flag from `instrument_master` (SEBI's surveillance basic tier)
>
> Module 3's feature engineering will use these for filter conditions ("compute volume only for ESM-flagged instruments to surface the highest-risk subset").

If those columns are uniformly **NULL**, the upstream masters aren't populated. Re-check Step 1's prerequisite query.

## Step 4 — Pick a planted case and find its window

Case 0 (mid-cap pharma layering by member firm BNXM-0042) is the canonical test case for book reconstruction. Find its time window:

```sql
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

**Expected output:** **one row** showing:
- `instrument_code` — the mid-cap pharma instrument BNXM-0042 was layering
- `window_start` and `window_end` — narrow window (~1–2 seconds)
- `layered_orders` — typically 5–10

Note these values. You'll use them in Step 5.

> 💡 **Why does the planted case look like this in the SQL?** Spoofing/layering produces rapid-fire NEW orders on one side, all from the same member firm, on the same instrument, in a tight time window. The `HAVING COUNT(*) >= 5` filters out normal trading; only the planted case (and any organic patterns that look similar) passes. In production, this query is the front end of a manual investigation — analyst types it, confirms the case, drills into the book reconstruction.

## Step 5 — Reconstruct the book at the layering window using time-travel

This is the **critical query** — the ARG-2 demonstration. Replace `<INSTR>` and `<TS>` with the values from Step 4:

```sql
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

**Expected output:** 1–2 snapshot rows showing:
- `bid_depth_total` is **dramatically larger** than `ask_depth_total` (ratio > 5×) — manufactured depth from the layering pattern
- `spread_bps` is unusually narrow (< 5 bps) — false-tight market caused by layered orders
- `bids` JSON shows 5+ price levels — the layered stack
- `asks` JSON shows 1–2 price levels — bona-fide opposite side

> 💡 **What does the data tell you, in plain terms?** The layering pattern works by stacking large fake buy orders at multiple price levels just below the best ask, creating the illusion of strong buying interest. This pulls the actual best ask (a real seller) downward. When the real ask drops to the spoofer's target level, the spoofer cancels all the fake bids and either sells (if they were buyers wanting a low price) or doesn't trade at all (the goal was just to drive the price). The asymmetric depth in the snapshot is the structural signature.

If the snapshot shows balanced depth and a normal spread, the time-travel reconstruction missed the layering window. See Common Failure Mode #1.

## Step 6 — Iceberg `FOR SYSTEM_VERSION AS OF` against historical snapshot

This is the test that proves Iceberg's evidentiary capability — independent of the current state of the table:

```sql
-- Get available Iceberg snapshots for the order_book_snapshots table
SELECT snapshot_id, committed_at FROM argus_${STUDENT_ID}_gold.order_book_snapshots.snapshots
ORDER BY committed_at DESC LIMIT 5;
```

Pick a snapshot ID at least one commit ago (the second or third in the list). Then:

```sql
-- Query AS OF that snapshot
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.order_book_snapshots
FOR SYSTEM_VERSION AS OF <snapshot_id>;
```

**Expected output:** a non-zero row count. Then run the same COUNT(*) without the `FOR SYSTEM_VERSION AS OF` clause:

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.order_book_snapshots;
```

**The current count should be GREATER than the historical-snapshot count.** That's because JOB-06 may have produced more rows since the older snapshot was committed.

If both queries return the same count, no new commits have happened since (which is fine if you ran JOB-06 only once). If the AS OF query errors with "snapshot not found", see Common Failure Mode #2.

## Step 7 — Verify CP-05 pass conditions

CP-05 has **four checks**.

### Check 1 — `gold.order_book_snapshots` populated

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.order_book_snapshots;
```
**Pass if:** count > 1,000 at lab scale 0.05; > 100,000 at full scale. **Fail if:** 0 — JOB-06 didn't write anything.

### Check 2 — Time-travel query reproduces book at planted timestamp

The Step 5 query returns a row with `bid_depth_total > 5 × ask_depth_total` for Case 0's instrument and window. **Pass if:** the asymmetry is visible. **Fail if:** depth looks balanced — the reconstruction missed the layering window.

### Check 3 — Iceberg `FOR SYSTEM_VERSION AS OF` works

The Step 6 query returns a non-zero row count for a historical snapshot, and the count is less than (or equal to) the current row count. **Pass if:** both true. **Fail if:** the AS OF query errors out — snapshot is gone.

### Check 4 — Per-resolution snapshots present

```sql
SELECT snapshot_resolution, COUNT(*)
FROM argus_${STUDENT_ID}_gold.order_book_snapshots
GROUP BY snapshot_resolution;
```
**Pass if:** at least `1S` resolution present with > 0 rows. (`100MS` and `PER_EVENT` may show 0 — those are produced on demand by drilldown queries during deep investigations, not by default.)

---

## Common failure mode #1 — Step 5 query returns balanced depth (no layering signature)

**Symptom:** the time-travel query returns snapshots with `bid_depth_total ≈ ask_depth_total` and normal spread, even though Case 0 should show extreme asymmetry.

**Cause** (most common): JOB-06's `--window_minutes` parameter is too short. By default it reconstructs the prior 60 minutes of activity. If your planted case is older than that (e.g., the synthetic data was generated days ago), the snapshots for that window weren't created.

**Diagnosis:**
```sql
-- What time range does JOB-06's output cover?
SELECT MIN(snapshot_ts) AS earliest, MAX(snapshot_ts) AS latest
FROM argus_${STUDENT_ID}_gold.order_book_snapshots;
```
If your Case 0 window from Step 4 is outside this range, you've found the cause.

**Fix:** re-run JOB-06 with a longer window:
```bash
cde job update --name "argus-${STUDENT_ID}-job_06_book_reconstruction" \
    --extra-args "--window_minutes=2880"  # 48 hours
cde job run --name "argus-${STUDENT_ID}-job_06_book_reconstruction"
```

Wait for it to complete, then re-run Step 5.

## Common failure mode #2 — `FOR SYSTEM_VERSION AS OF` returns "snapshot not found"

**Symptom:** the time-travel query in Step 6 fails with `Cannot find snapshot with id <X>`.

**Cause:** the Iceberg snapshot has been **expired**. By default Iceberg expires snapshots older than 5 days. For `gold.order_book_snapshots`, this is normally fine because reconstruction queries target recent windows. But if you picked a snapshot ID older than 5 days (or your cluster's snapshot retention policy is shorter), the metadata is gone.

**Diagnosis:**
```sql
-- Confirm the snapshot exists in the table's snapshot history
SELECT snapshot_id, committed_at FROM argus_${STUDENT_ID}_gold.order_book_snapshots.snapshots
WHERE snapshot_id = <id_you_used>;
```
If this returns 0 rows, the snapshot is expired.

**Fix:** pick a more recent snapshot ID from the `.snapshots` view. For tables that need long retention (e.g., `gold.consent_audit` for compliance), the DDL sets `'history.expire.enabled' = 'false'`. Module 7's CP-19 (the COMPLIANCE GATE) tests against `consent_audit` specifically because it's the only table with permanently retained snapshots.

For `order_book_snapshots`, if you need longer retention for production:
```sql
ALTER TABLE argus_${STUDENT_ID}_gold.order_book_snapshots
SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '7776000000'  -- 90 days
);
```

## Common failure mode #3 — `silver.order_events` enrichment is NULL on every row

**Symptom:** Step 3's verification query shows `member_firm_category`, `esm_flag`, `asm_flag` are all NULL across all rows.

**Cause:** the JOIN in JOB-06 against `member_master` or `instrument_master` is failing. Either the master tables are empty, or the join key format doesn't match.

**Diagnosis:**
```sql
-- Are the masters populated?
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current;
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master WHERE is_current;

-- Do the join keys overlap?
SELECT COUNT(DISTINCT m.member_firm_id) AS in_master,
       COUNT(DISTINCT b.member_firm_id) AS in_bronze
FROM argus_${STUDENT_ID}_silver.member_master m
FULL OUTER JOIN argus_${STUDENT_ID}_bronze.orders_raw b
  ON m.member_firm_id = b.member_firm_id
WHERE m.is_current OR b.member_firm_id IS NOT NULL;
```

If both counts are similar (say, 380 each), the join is working — but the enrichment may have been done with a stale snapshot. Re-run JOB-06.

If `in_bronze` is much larger than `in_master` (e.g., 380 vs 5), the masters are missing rows that Bronze has — re-run Lab 2.2 to populate `member_master` fully.

**Fix:** ensure both masters are fully populated, then re-run JOB-06.

---

## Pass condition for CP-05

All four checks pass:
- ✅ `gold.order_book_snapshots` populated (> 1,000 rows at lab scale)
- ✅ Time-travel query reproduces the asymmetric book at Case 0's planted layering window
- ✅ Iceberg `FOR SYSTEM_VERSION AS OF` returns historical snapshot data
- ✅ At least `1S` resolution snapshots are present

When all four pass, MSE has the evidentiary capability that took 11 weeks before — now it's 3 lines of SQL.

## Wrap-up — what you can now do that you couldn't before

You can reconstruct the order book at any past timestamp using one Impala query. You can recognize a layering pattern by its book signature (asymmetric depth + tight spread). You can use Iceberg's `FOR SYSTEM_VERSION AS OF` and `FOR SYSTEM_TIME AS OF` for forensic queries against any past commit of the table.

Most importantly: **ARG-2 part 1 is now closed.** A SEBI investigator's question "what did the book look like at 11:23:42 on May 9, 2026 for instrument X?" is now a single SQL query with deterministic, evidence-grade output.

Module 3 builds on this — using `silver.order_events` (enriched event data) and `gold.order_book_snapshots` (historical book state) to compute the temporal and cross-product features ML needs in Module 5. Allow about 6 hours total for Modules 2 + 3 combined.
