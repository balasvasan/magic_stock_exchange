# Lab 3.3 — Cross-Product Detection (CP-09)

> 👋 **Module 3 first-timer?** Read [`docs/module-3-primer.md`](../docs/module-3-primer.md) first. The "cross-product features" concept is explained there.

> ℹ️ **Module:** 3 — Temporal & Cross-Product Feature Engineering
> **Closes deficiency:** ARG-2 part 2 (the Jane Street gap)
> **Time:** ~45 minutes if Case 2 fires R-104 first try; up to 90 minutes if cross-product features are NULL.
> **Source files:** [`src/transform/job_07_gold_temporal_features.py`](../src/transform/job_07_gold_temporal_features.py), [`src/transform/job_08_gold_alert_candidates.py`](../src/transform/job_08_gold_alert_candidates.py)

## What you're going to do

1. **Confirm prerequisites** — cross-product features and R-104 alerts populated. (~3 min)
2. **Locate Case 2's imbalance** — verify `cross_product_delta_imbalance ≥ 7.0` for the planted case. (~10 min)
3. **Inspect the R-104 CRITICAL alert** that fires on Case 2. (~10 min)
4. **Walk through what the alert reveals** — the directional consistency across cash + futures + options. (~10 min)
5. **Verify CP-09 pass conditions** — three named checks. (~5 min)
6. **Reflect** on the architectural insight: why this lab exists. (~5 min)

Total: ~45 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 3.1](lab-3-1-temporal-features.md) is complete — `gold.cross_product_features` is populated
- [ ] [Lab 3.2](lab-3-2-alert-candidates.md) is complete — `gold.alert_candidates` exists
- [ ] You can query Iceberg in Hue or Impala-Shell

## Why cross-product detection matters — read this before Step 2

The 2024–25 SEBI Jane Street order alleged ₹4,843 crore in unlawful gains across 18 trading days. The mechanic: **orchestrate cash + futures + options positions on the same underlying**.

Here's the simplified version of what Jane Street allegedly did:

1. **Cash leg:** large directional buying of an index-component stock to push the index price toward a target.
2. **Futures leg:** matching directional position in index futures, profiting from the index move.
3. **Options leg:** much larger position in index options at the target strike, profiting disproportionately when the index moved through the strike.

The cash + futures positions are essentially "marketing cost" — they push the price. The options profit is the actual goal, multiplied by leverage.

**Why was this hard for legacy MSE to detect?** Because cash, futures, and options live in three different trading systems with three different databases owned by three different tech teams. To detect the pattern, you need to:
1. Join the three product feeds to the same underlying instrument
2. Aggregate per (member × underlying × day) across all three products
3. Compute a derived metric that flags directional alignment

Most exchanges can't. Reproducing the Jane Street trades took SEBI's investigation team months. ARGUS makes it a 5-line SQL query, because:
1. **Identity resolution (Module 2)** gives one `entity_id` per trader across products.
2. **Iceberg + Spark** is one query layer over cash, futures, and options.
3. **JOB-07** computes `cross_product_delta_imbalance` per (member × underlying × day).

CP-09 verifies it works. Case 2 is the planted Jane Street-style pattern: `cross_product_delta_imbalance ≥ 7.0` with directional consistency across all three products.

## Step 1 — Confirm prerequisites

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE trade_date >= CURRENT_DATE - 5;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE rule_id = 'R-104' AND trade_date >= CURRENT_DATE - 5;
```

**Both must be > 0.** If `R-104` count is 0 but cross_product_features count is non-zero, re-run JOB-08 — JOB-08 must run after JOB-07 because R-104 reads from `cross_product_features`.

## Step 2 — Locate Case 2's imbalance

Case 2 is the planted marking-the-close pattern by member firm BNXM-0231:

```sql
SELECT
    member_firm_id,
    underlying_code,
    trade_date,
    cash_net_position,
    futures_net_position,
    options_net_delta_exposure,
    cross_product_delta_imbalance,
    directional_consistency_flag
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE member_firm_id = 'BNXM-0231'
  AND trade_date >= CURRENT_DATE - 5
ORDER BY ABS(cross_product_delta_imbalance) DESC NULLS LAST
LIMIT 5;
```

**Expected output:** at least one row with:
- `ABS(cross_product_delta_imbalance) ≥ 7.0` — large mismatch between products
- `directional_consistency_flag = TRUE` — all three products oriented the same direction (key Jane Street signature)
- `options_net_delta_exposure` is much larger in absolute terms than `cash_net_position` and `futures_net_position` combined — the leveraged amplification

If this returns 0 rows or all rows show `cross_product_delta_imbalance` near 0, see Common Failure Mode #1.

> 💡 **What `cross_product_delta_imbalance` actually computes:** roughly, `(options_net_delta_exposure / total_underlying_volume) / (cash_net_position + futures_net_position) / total_volume`. A value of 7.0 means the trader's options exposure is 7× more directional than their underlying activity would naturally produce. Normal traders are <1.0 (their options hedge their cash); manipulators are >5.0 (their options are the goal).

## Step 3 — Inspect the R-104 alert

```sql
SELECT
    a.alert_id,
    a.rule_id,
    a.severity,
    a.member_firm_id,
    a.instrument_code,
    a.trade_date,
    json_extract_scalar(a.feature_payload, '$.cross_product_delta_imbalance') AS imbalance,
    json_extract_scalar(a.feature_payload, '$.directional_consistency_flag') AS dir_consistent,
    json_extract_scalar(a.feature_payload, '$.entity_prior_str_count')      AS prior_strs
FROM argus_${STUDENT_ID}_gold.alert_candidates a
WHERE a.rule_id = 'R-104'
  AND a.member_firm_id = 'BNXM-0231'
  AND a.trade_date >= CURRENT_DATE - 5
ORDER BY ABS(CAST(json_extract_scalar(a.feature_payload, '$.cross_product_delta_imbalance') AS DOUBLE)) DESC NULLS LAST
LIMIT 1;
```

**Expected output:** one row with:
- `severity = 'CRITICAL'`
- `imbalance` value matching what you saw in Step 2 (≥ 7.0)
- `dir_consistent = TRUE`

> 💡 **Why `severity = CRITICAL` for R-104 specifically?** Because the Jane Street pattern, when present, is high-confidence manipulation — there's no legitimate trading strategy that produces a directional-consistent imbalance of 7×+. R-101 (spoofing) can fire on legitimate market makers; R-104 essentially can't. So R-104's severity is fixed at CRITICAL.

## Step 4 — Walk through what the alert reveals

For Case 2, look at the daily breakdown:

```sql
SELECT
    trade_date,
    cash_net_position,
    futures_net_position,
    options_net_delta_exposure,
    cross_product_delta_imbalance,
    directional_consistency_flag
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE member_firm_id = 'BNXM-0231'
  AND underlying_code = (SELECT underlying_code FROM argus_${STUDENT_ID}_gold.cross_product_features
                         WHERE member_firm_id = 'BNXM-0231'
                         ORDER BY ABS(cross_product_delta_imbalance) DESC LIMIT 1)
ORDER BY trade_date;
```

**Expected output:** a small number of rows (1–5 days). The day(s) with the highest `cross_product_delta_imbalance` are the manipulation events. Look at the *signs*:

- If `cash_net_position > 0` AND `futures_net_position > 0` AND `options_net_delta_exposure > 0`, all three legs are bullish — that's a directional-up squeeze pattern.
- If all three are < 0, that's a directional-down squeeze.

The `directional_consistency_flag = TRUE` confirms the three legs aren't hedging each other (which would be normal); they're stacking the same direction (which is the manipulation signature).

## Step 5 — Verify CP-09 pass conditions

CP-09 has **three checks**.

### Check 1 — Cross-product imbalance metric is non-NULL for active members

```sql
SELECT
    SUM(CASE WHEN cross_product_delta_imbalance IS NOT NULL THEN 1 ELSE 0 END) AS non_null,
    COUNT(*) AS total
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE trade_date >= CURRENT_DATE - 5;
```
**Pass if:** `non_null / total >= 0.80` (most members have computable imbalance — some don't if they only trade cash, no futures/options). **Fail if:** `non_null = 0` — the metric is broken (Common Failure Mode #1).

### Check 2 — At least 1 row with imbalance ≥ 7.0

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE ABS(cross_product_delta_imbalance) >= 7.0
  AND trade_date >= CURRENT_DATE - 5;
```
**Pass if:** ≥ 1 (Case 2 is in the data). **Fail if:** 0 — the planted pattern isn't reaching the imbalance threshold.

### Check 3 — R-104 alert fires on Case 2

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE rule_id = 'R-104'
  AND member_firm_id = 'BNXM-0231'
  AND severity = 'CRITICAL'
  AND trade_date >= CURRENT_DATE - 5;
```
**Pass if:** ≥ 1. **Fail if:** 0 — alert isn't firing despite imbalance being present.

---

## Common failure mode #1 — `cross_product_delta_imbalance` is NULL for every row

**Symptom:** Step 2 query shows the metric is NULL across all rows, including Case 2.

**Cause** (in decreasing likelihood):
1. The instrument-master mapping that joins futures/options to their underlying cash equity isn't working. Without it, JOB-07 can't aggregate per-underlying.
2. The futures or options product types aren't classified correctly in `instrument_master`.
3. A divide-by-zero in the imbalance formula (denominator zero when the trader has no cash/futures activity).

**Diagnosis:**
```sql
-- Check instrument_master for product type variety
SELECT product_type, COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master
WHERE is_current GROUP BY product_type;
```
**Expected:** at least `CASH`, `FUT`, and (`OPT_CALL` or `OPT_PUT`). If only `CASH` shows up, instrument categorization is broken — see Lab 1.5 Common Failure Mode #2 for the underlying-code parsing logic.

```sql
-- Check that BNXM-0231 has activity in all 3 product types
SELECT
    SUBSTR(instrument_code, INSTR(instrument_code, '-')+1, 3) AS product_signal,
    COUNT(*) AS rows
FROM argus_${STUDENT_ID}_silver.order_events
WHERE member_firm_id = 'BNXM-0231'
  AND trade_date >= CURRENT_DATE - 5
GROUP BY product_signal;
```
**Expected:** rows for cash + futures + options. If only one product type, Case 2's synthetic data is incomplete — regenerate with `--seed 42`.

**Fix:** if instrument categorization is broken, re-load `instrument_master` from `data/generated/instruments_synthetic.jsonl.gz`. If Case 2 doesn't have multi-product activity, regenerate data.

## Common failure mode #2 — Imbalance is computed but always < 5

**Symptom:** the metric is non-NULL across all rows but never crosses 7.0, even for the planted Case 2.

**Cause:** the synthetic data's planted Case 2 doesn't have a strong enough directional pattern. This usually means the data generator was run with a non-canonical seed.

**Diagnosis:**
```bash
# Confirm the seed
md5sum data/generated/orders_synthetic.jsonl.gz
# Compare to your instructor's expected hash for --seed 42
```

**Fix:** regenerate with `--seed 42`:
```bash
rm -rf data/generated/
python data/generate_data.py --seed 42 --out data/generated/
# Re-run FLOW-SIM, JOB-06, JOB-07, JOB-08 in order
```

## Common failure mode #3 — R-104 alert fires but feature_payload doesn't include the imbalance

**Symptom:** Step 3 query returns the alert but the `imbalance` extracted field is NULL.

**Cause:** JOB-08's feature_payload assembly is dropping the cross_product_delta_imbalance from the JSON — usually a bug in the field-mapping list.

**Diagnosis:** open `src/transform/job_08_gold_alert_candidates.py` and search for `cross_product_delta_imbalance`. It should appear in the columns list passed to `to_json(struct(...))`.

**Fix:** add the field to the columns list if it's missing, redeploy JOB-08, re-run.

---

## Pass condition for CP-09

All three checks pass:
- ✅ `cross_product_delta_imbalance` non-NULL for ≥80% of rows
- ✅ At least 1 row with imbalance ≥ 7.0 (Case 2)
- ✅ R-104 CRITICAL alert fires on BNXM-0231

When all three pass, the Jane Street pattern is detectable on ARGUS. The 18 trading days, the ₹4,843 crore — that pattern would be flagged in real time. **ARG-2 is now fully closed.**

## Reflection — what this capability means

Take a moment to think about what you've built.

The 2024–25 Jane Street investigation took SEBI's team multiple months to reconstruct. They had to manually correlate trades across cash, futures, and options data sources that didn't talk to each other. The reconstruction was retrospective — by the time it was ready, the trades had already happened, the gains had been booked.

What you've built here detects the same pattern **on the day it happens**. The imbalance metric is a simple SQL aggregation; the rule fires alongside any other manipulation alert; the analyst sees a CRITICAL severity and routes it for investigation within hours, not months.

That's the difference between a surveillance platform that *documents* manipulation after the fact and one that *detects* it as it occurs. ARG-2 was the deficiency that made this impossible on the legacy MSE platform; CP-05, CP-06, CP-07, CP-08, and CP-09 together close it.

## Wrap-up — what you can now do that you couldn't before

You can detect the Jane Street cross-product manipulation pattern on the day it occurs, using a single SQL query against ARGUS's Gold layer. You understand why most exchanges can't do this — and why ARGUS can. You can interpret the `cross_product_delta_imbalance` metric and the `directional_consistency_flag` to assess pattern severity.

**Module 3 is complete.** You've built the full feature engineering pipeline. Module 4 next exposes governed views over these tables; Module 5 trains an ML model on the alert candidates. Allow about 4 hours total for Module 4.
