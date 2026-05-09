# Lab 3.1 — Temporal Features (CP-07)

> ℹ️ **Module:** 3 — Temporal & Cross-Product Feature Engineering
> **Closes deficiency:** ARG-2 part 2 (sequential features)
> **Source files:** [`src/transform/job_07_gold_temporal_features.py`](../src/transform/job_07_gold_temporal_features.py)

## Objectives

- Run JOB-07 to populate `argus_${STUDENT_ID}_gold.member_temporal_features` and `argus_${STUDENT_ID}_gold.cross_product_features`
- Sanity-check the distributions of cancel rate and time-to-cancel across the member population
- Confirm the negative cases (6 — legitimate market maker, 7 — legitimate news) produce *high* cancel rates that the rule engine fires on but that the ML model in Module 5 will learn to deprioritize

## Why this matters

Surveillance is a distribution problem, not a threshold problem. A 90% cancel rate is normal for a tier-1 market maker on a quiet day and suspicious for a retail-broker proprietary desk on an expiry Thursday. The rules engine has to fire candidates on simple thresholds because it must be defensible to the regulator, but the ML model in Module 5 needs the full distribution context to deprioritize the false positives. This lab populates the table that makes both possible.

## Procedure

### Step 1 — Confirm prerequisites

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.order_events WHERE trade_date >= CURRENT_DATE - 5;
-- expect non-zero (Module 2 / JOB-06 must have run)

SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.executed_trades WHERE trade_date >= CURRENT_DATE - 5;
-- expect non-zero
```

### Step 2 — Run JOB-07

```bash
cde job create --name argus-job_07_temporal_features \
    --type spark \
    --application-file src/transform/job_07_gold_temporal_features.py \
    --executor-memory 8g --executor-cores 4 --num-executors 8

cde job run --name argus-job_07_temporal_features
```

**Expected output**:

```
==> temporal_features: 12,400 member-temporal rows; cross_product: 1,250 rows
```

(Numbers vary with `--scale`.)

### Step 3 — Sanity-check the distribution of cancel rates

```sql
SELECT
    member_firm_category,
    APPROX_PERCENTILE(cancel_rate, 0.50) AS p50_cancel_rate,
    APPROX_PERCENTILE(cancel_rate, 0.95) AS p95_cancel_rate,
    APPROX_PERCENTILE(median_time_to_cancel_ms, 0.50) AS p50_ttc_ms,
    APPROX_PERCENTILE(pct_cancelled_under_50ms, 0.95) AS p95_pct_under_50ms,
    COUNT(*) AS rows
FROM argus_${STUDENT_ID}_gold.member_temporal_features f
JOIN argus_${STUDENT_ID}_silver.member_master m
  ON f.member_firm_id = m.member_firm_id AND m.is_current
WHERE trade_date >= CURRENT_DATE - 5
GROUP BY member_firm_category
ORDER BY p50_cancel_rate DESC;
```

**Expected output**: 4 rows, one per `member_firm_category`. Roughly:

| member_firm_category | p50_cancel_rate | p95_cancel_rate | p50_ttc_ms | p95_pct_under_50ms |
|---|---:|---:|---:|---:|
| TIER1_MM | 0.60–0.80 | 0.95+ | 200–500 | 0.10–0.20 |
| PROP_TRADER | 0.40–0.65 | 0.85+ | 300–800 | 0.05–0.15 |
| INSTITUTIONAL | 0.20–0.40 | 0.60+ | 500–2000 | 0.01–0.05 |
| RETAIL_BROKER | 0.10–0.30 | 0.40+ | 1000–5000 | 0.01–0.03 |

The pattern matters more than the exact numbers: tier-1 market makers naturally have high cancel rates and short cancel times (that's the business of market making). Retail brokers naturally have low cancel rates and long cancel times. Any deviation from this pattern in a specific member-firm-day combination is a candidate signal.

### Step 4 — Locate the planted manipulation cases

The five "real" planted cases (0–5, 9 from PRD §11) should show up with extreme feature values. Find Case 0:

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

**Expected output**: at least one row where:

- `pct_cancelled_under_50ms` ≥ 0.50 (more than half the cancellations happened in under 50ms — the spoofing/layering signature)
- `layered_stack_count` ≥ 1
- `cancel_rate` > 0.85

If no row meets these thresholds, the planted Case 0 events didn't land in the trade_date window — most likely cause is the synthetic generator was run more than 7 days ago and the Bronze data hasn't been retained. Re-run `data/generate_data.py` (use the same `--seed 42`) and the FLOW-SIM oneshot bulk-load.

### Step 5 — Confirm the negative cases

Case 6 is a legitimate tier-1 market maker (BNXM-0001). Case 7 is a legitimate news-driven move (BNXM-0156). Both should have *high* cancel rates and short cancel times — but **the rule engine should fire on them, and Module 5's ML model should learn to deprioritize**.

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

**Expected output**: feature rows that look superficially similar to the manipulator cases — high cancel rate, low time-to-cancel. The features alone don't distinguish them. The **member context** (BNXM-0001 is a TIER1_MM with a clean 5-year history; the case-0 manipulator is a PROP_TRADER with prior SEBI matters) is what lets Module 5's ML model separate them. This is exactly why ARG-3 needs ML and not just rules.

## Checkpoint CP-07 — Temporal features computed for all members

### Pass condition

All four checks pass.

### Check 1 — Feature rows present for the prior trading window

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features
WHERE trade_date >= CURRENT_DATE - 5;
-- expect > 1,000 at lab scale; > 100,000 at full scale
```

### Check 2 — Distribution roughly matches expected member-category pattern

The query in Step 3 produces 4 rows ordered by `p50_cancel_rate DESC`, with `TIER1_MM` first and `RETAIL_BROKER` last. If the order is inverted or the categories are missing, the synthetic generator's category weights are wrong — re-check `members.csv`.

### Check 3 — Case 0 surfaces

The query in Step 4 returns at least one row for `BNXM-0042` with `pct_cancelled_under_50ms ≥ 0.50` and `layered_stack_count ≥ 1`.

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

**Expected output**: non-zero `rows`, multi-digit `underlyings` and `members`, and `jane_street_candidates` ≥ 1 (Case 2 should fire R-104 in JOB-08 next).

---

## Common failure mode — every member shows `cancel_rate = 1.0`

**Symptom**: the cancel-rate distribution in Step 3 is degenerate; every row shows 1.0 cancellation rate.

**Diagnosis**: JOB-07's time-to-cancel join requires `parent_order_id` on cancel events to match the `event_id` of new events. If the synthetic generator's `parent_order_id` field is set to `null` for cancels, the join fails and all NEWs look like they got cancelled (because the generator emits a CANCEL for almost every NEW in the planted patterns).

**Fix**: confirm the synthetic generator's CANCEL events carry `parent_order_id`:

```bash
zcat data/generated/orders_synthetic.jsonl.gz \
  | head -100000 | grep '"action": "CANCEL"' \
  | python3 -c "import sys, json; print(sum(1 for l in sys.stdin if json.loads(l).get('parent_order_id'))) "
```

Should return a number close to total CANCEL count. If it's 0, the generator has a bug — re-check `data/generate_data.py`'s planted-case emission logic; every CANCEL block sets `"parent_order_id": oid` where `oid` is the originating NEW's UUID.

---

## Pass condition for CP-07

All four checks pass. With temporal and cross-product features in Gold, the analyst toolbox is finally ready: queries against the order book are answered in seconds, member behavior is summarized in defensible numbers, and the cross-product imbalance metric flags the Jane Street pattern that defeated MSE before.
