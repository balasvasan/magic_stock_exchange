# Lab 3.1 — Temporal Features (CP-07)

> 👋 **Module 3 first-timer?** Read [`docs/module-3-primer.md`](../docs/module-3-primer.md) first. About 15 minutes — explains windowed aggregations, distributions vs thresholds, why rules are permissive.

> ℹ️ **Module:** 3 — Temporal & Cross-Product Feature Engineering
> **Closes deficiency:** ARG-2 part 2 (sequential features)
> **Time:** ~60 minutes if JOB-07 runs cleanly first try; up to 2 hours if NEW↔CANCEL join is broken (Common Failure Mode #1).
> **Source files:** [`src/transform/job_07_gold_temporal_features.py`](../src/transform/job_07_gold_temporal_features.py)

## What you're going to do

1. **Confirm prerequisites** — `silver.order_events` and `silver.executed_trades` populated. (~3 min)
2. **Run JOB-07** — populates `gold.member_temporal_features` and `gold.cross_product_features`. (~15 min)
3. **Sanity-check cancel-rate distribution** by member-firm category — this is the diagnostic that catches feature-engineering bugs. (~10 min)
4. **Locate the planted manipulation cases** — verify Case 0 surfaces with extreme feature values. (~10 min)
5. **Confirm the negative cases** (Case 6 legitimate MM, Case 7 legitimate news) also produce extreme features — by design. (~5 min)
6. **Verify CP-07 pass conditions** — four named checks. (~5 min)

Total: ~60 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 2.1](lab-2-1-order-book-reconstruction.md) is complete — `silver.order_events` exists and is enriched
- [ ] `silver.executed_trades` is populated — quick check: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.executed_trades` should return > 10,000
- [ ] You have CDE access and ~8 executors available

## Why distributions matter — read this before Step 3

Surveillance is a **distribution problem**, not a threshold problem.

A 90% cancel rate is normal for a tier-1 market maker on a quiet day, suspicious for a retail-broker proprietary desk on F&O expiry Thursday. The same numeric feature value means different things in different member-context buckets. The rules engine in Lab 3.2 fires candidates on simple thresholds because rules must be defensible. The ML model in Module 5 needs the **full distribution context** to deprioritize the false positives.

This lab populates the table that makes both possible: the per-member, per-day, per-instrument feature payload. The values themselves are mechanical (count NEWs, count CANCELs, divide); the *interpretation* — what's normal, what's anomalous — is downstream ML's job.

## Step 1 — Confirm prerequisites

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.order_events WHERE trade_date >= CURRENT_DATE - 5;
-- expect non-zero (Module 2 / JOB-06 must have run)

SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.executed_trades WHERE trade_date >= CURRENT_DATE - 5;
-- expect non-zero
```

Both must be > 0. If `order_events` is 0, run Lab 2.1. If `executed_trades` is 0, your trades.v1 Bronze ingest may not be running — re-check Lab 1.2.

## Step 2 — Run JOB-07

```bash
cde job create --name "argus-${STUDENT_ID}-job_07_temporal_features" \
    --type spark \
    --application-file src/transform/job_07_gold_temporal_features.py \
    --executor-memory 8g --executor-cores 4 --num-executors 8

cde job run --name "argus-${STUDENT_ID}-job_07_temporal_features"
```

**Expected output** (in CDE job logs):

```
==> temporal_features: 12,400 member-temporal rows; cross_product: 1,250 rows
```

(Numbers vary with `--scale` and active member count.)

> 💡 **What `member-temporal rows` means:** one row per (member_firm × instrument × trade_date) combination where there was activity. So 380 active members × ~30 instruments per member × 5 days ≈ 57,000 — but most member-instrument pairs have 0 activity on most days, so the actual count is much lower (~12K).

The job takes 5–15 minutes. Wait for COMPLETED.

## Step 3 — Sanity-check cancel-rate distribution

This is the most important diagnostic in Module 3. If feature engineering has bugs, you'll see them as degenerate distributions (everything is 1.0, or everything is 0.0).

```sql
SELECT
    member_firm_category,
    APPROX_PERCENTILE(cancel_rate, 0.50)               AS p50_cancel_rate,
    APPROX_PERCENTILE(cancel_rate, 0.95)               AS p95_cancel_rate,
    APPROX_PERCENTILE(median_time_to_cancel_ms, 0.50)  AS p50_ttc_ms,
    APPROX_PERCENTILE(pct_cancelled_under_50ms, 0.95)  AS p95_pct_under_50ms,
    COUNT(*)                                            AS rows
FROM argus_${STUDENT_ID}_gold.member_temporal_features f
JOIN argus_${STUDENT_ID}_silver.member_master m
  ON f.member_firm_id = m.member_firm_id AND m.is_current
WHERE trade_date >= CURRENT_DATE - 5
GROUP BY member_firm_category
ORDER BY p50_cancel_rate DESC;
```

**Expected output:** 4 rows, one per `member_firm_category`. Roughly:

| member_firm_category | p50_cancel_rate | p95_cancel_rate | p50_ttc_ms | p95_pct_under_50ms |
|---|---:|---:|---:|---:|
| TIER1_MM | 0.60–0.80 | 0.95+ | 200–500 | 0.10–0.20 |
| PROP_TRADER | 0.40–0.65 | 0.85+ | 300–800 | 0.05–0.15 |
| INSTITUTIONAL | 0.20–0.40 | 0.60+ | 500–2000 | 0.01–0.05 |
| RETAIL_BROKER | 0.10–0.30 | 0.40+ | 1000–5000 | 0.01–0.03 |

> 💡 **The pattern matters more than the exact numbers.** TIER1_MM should be *highest* in cancel rate (market makers cancel constantly to update quotes). RETAIL_BROKER should be *lowest* (retail orders are mostly placed and held). If the order is inverted, your member-firm categorization is wrong (re-check Lab 1.2's seed_member_cdc data).

If every row in the result shows `p50_cancel_rate ≈ 1.0` — see Common Failure Mode #1. **This is the most common bug**.

## Step 4 — Locate the planted manipulation cases

The five "real" planted cases (0–5, 9 from PRD §11) should show extreme values. Find Case 0:

```sql
-- Case 0: layering by BNXM-0042 in mid-cap pharma
SELECT
    member_firm_id,
    instrument_code,
    trade_date,
    orders_placed,
    orders_cancelled,
    cancel_rate,
    median_time_to_cancel_ms,
    pct_cancelled_under_50ms,
    layered_stack_count
FROM argus_${STUDENT_ID}_gold.member_temporal_features
WHERE member_firm_id = 'BNXM-0042'
  AND trade_date >= CURRENT_DATE - 5
ORDER BY layered_stack_count DESC NULLS LAST,
         pct_cancelled_under_50ms DESC NULLS LAST
LIMIT 5;
```

**Expected output:** at least one row where:
- `pct_cancelled_under_50ms` ≥ 0.50 — more than half the cancellations under 50ms (spoofing/layering signature)
- `layered_stack_count` ≥ 1 — at least one instance of multiple price levels stacked
- `cancel_rate` > 0.85 — overall cancel rate well above member's normal

> 💡 **Why ORDER BY NULLS LAST?** Some rows may have NULL `layered_stack_count` (no layering windows detected). Sorting these to the end makes the rows with detected layering appear first.

If no rows meet these thresholds, see Common Failure Mode #2 — your trade_date window may not contain the planted cases.

## Step 5 — Confirm the negative cases produce similar features

Case 6 is a legitimate tier-1 market maker (BNXM-0001). Case 7 is a legitimate news-driven move (BNXM-0156). **Both should show high cancel rates and short cancel times** — but the rules engine should fire on them, and Module 5's ML model should learn to deprioritize.

```sql
SELECT
    member_firm_id,
    instrument_code,
    cancel_rate,
    median_time_to_cancel_ms,
    pct_cancelled_under_50ms,
    layered_stack_count
FROM argus_${STUDENT_ID}_gold.member_temporal_features
WHERE member_firm_id IN ('BNXM-0001', 'BNXM-0156')
  AND trade_date >= CURRENT_DATE - 5
ORDER BY member_firm_id, trade_date;
```

**Expected output:** feature rows that look superficially similar to the manipulator cases — high cancel rate, low time-to-cancel.

The features alone don't distinguish legitimate from manipulator. **The member context** (BNXM-0001 is a TIER1_MM with a clean 5-year history; the Case 0 manipulator BNXM-0042 is a PROP_TRADER with prior SEBI matters) is what lets Module 5's ML model separate them. **This is exactly why ARG-3 needs ML and not just rules.**

## Step 6 — Verify CP-07 pass conditions

CP-07 has **four checks**.

### Check 1 — Feature rows present for prior trading window

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features
WHERE trade_date >= CURRENT_DATE - 5;
```
**Pass if:** > 1,000 at lab scale; > 100,000 at full scale. **Fail if:** 0 — JOB-07 wrote nothing.

### Check 2 — Distribution roughly matches expected member-category pattern

The Step 3 query produces 4 rows ordered by `p50_cancel_rate DESC`, with `TIER1_MM` first and `RETAIL_BROKER` last. **Pass if:** order matches expectation. **Fail if:** inverted — categorization is wrong.

### Check 3 — Case 0 surfaces

The Step 4 query returns at least one row for `BNXM-0042` with `pct_cancelled_under_50ms ≥ 0.50` AND `layered_stack_count ≥ 1`. **Pass if:** yes. **Fail if:** no row meets both.

### Check 4 — Cross-product features populated

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT underlying_code) AS underlyings,
    COUNT(DISTINCT member_firm_id) AS members,
    SUM(CASE WHEN ABS(cross_product_delta_imbalance) > 7.0 THEN 1 ELSE 0 END) AS jane_street_candidates
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE trade_date >= CURRENT_DATE - 5;
```
**Pass if:** non-zero `rows`, multi-digit `underlyings` and `members`, `jane_street_candidates ≥ 1`. **Fail if:** any of these are 0.

---

## Common failure mode #1 — Every member shows `cancel_rate = 1.0`

**Symptom:** Step 3's distribution shows `p50_cancel_rate = 1.0` for every category. Every member looks like they cancelled everything.

**Cause:** JOB-07's time-to-cancel join needs `parent_order_id` on cancel events to match `event_id` of new events. If the synthetic generator's cancel events have NULL `parent_order_id`, the join fails and the cancel-rate calc divides by 0 → returns 1.0 for everyone.

**Diagnosis:**
```bash
zcat data/generated/orders_synthetic.jsonl.gz | head -100000 \
  | grep '"action": "CANCEL"' \
  | python3 -c "import sys, json; n=sum(1 for l in sys.stdin if json.loads(l).get('parent_order_id')); print(f'cancels with parent_order_id: {n}')"
```
If output shows 0 (or much less than the total cancel count), the generator has a bug.

**Fix:** verify `data/generate_data.py`'s planted-case emission logic — every CANCEL block must set `"parent_order_id": oid` where `oid` is the originating NEW's UUID. Re-generate data with `--seed 42` if needed.

## Common failure mode #2 — Planted Case 0 has no row in the result

**Symptom:** Step 4's query returns 0 rows or rows that don't show extreme values.

**Cause:** the planted case events are outside the trade_date window. The synthetic generator timestamps events at `data/generate_data.py` runtime. If you generated data more than 7 days ago, today's `WHERE trade_date >= CURRENT_DATE - 5` window misses it.

**Diagnosis:**
```sql
-- What date range does Bronze cover?
SELECT MIN(trade_date), MAX(trade_date) FROM argus_${STUDENT_ID}_silver.order_events;
```
If MAX is older than 5 days ago, regenerate.

**Fix:** re-run synthetic data generator + FLOW-SIM oneshot bulk-load:
```bash
rm -rf data/generated/
python data/generate_data.py --seed 42 --out data/generated/
python src/ingest/replay_simulator.py --mode oneshot --data-dir data/generated/ --bootstrap-servers ${KAFKA_BROKERS}
# Wait 10 min for Bronze to repopulate, then re-run Lab 2.x and Lab 3.1 from Step 2.
```

## Common failure mode #3 — JOB-07 OOMs

**Symptom:** JOB-07 fails with `java.lang.OutOfMemoryError`.

**Cause:** the window-function aggregations are memory-intensive (group by member × instrument × day with sliding windows). Default 8g executors aren't enough at full scale.

**Fix:** bump executor memory:
```bash
cde job update --name "argus-${STUDENT_ID}-job_07_temporal_features" \
    --executor-memory 16g
cde job restart --name "argus-${STUDENT_ID}-job_07_temporal_features"
```

---

## Pass condition for CP-07

All four checks pass. With temporal and cross-product features in Gold, Module 5 has the 60-feature payload it needs to train models, and Module 3.2's rule engine has the input it needs to fire alert candidates.

## Wrap-up — what you can now do that you couldn't before

You can compute member-firm-day temporal features at scale using Spark window functions. You can interpret feature distributions to spot normal-vs-anomalous behavior across member categories. You understand why feature engineering is intentionally context-free (just compute the numbers) and ML is what adds context.

Lab 3.2 fires the deterministic rule alerts on top of these features. Allow ~75 minutes.
