# Lab 3.2 — Alert Candidates (CP-08)

> 👋 **Module 3 first-timer?** Read [`docs/module-3-primer.md`](../docs/module-3-primer.md) first. About 15 minutes.

> ℹ️ **Module:** 3 — Temporal & Cross-Product Feature Engineering
> **Closes deficiency:** ARG-2 part 2 (alert candidate generation)
> **Time:** ~75 minutes if all 10 cases surface; up to 2 hours if rule thresholds need adjustment.
> **Source files:** [`src/transform/job_08_gold_alert_candidates.py`](../src/transform/job_08_gold_alert_candidates.py)

## What you're going to do

1. **Confirm prerequisites** — temporal + cross-product features populated. (~3 min)
2. **Run JOB-08** — fires deterministic rule-based alerts. (~10 min)
3. **Inspect alerts by rule** — see what each rule caught. (~10 min)
4. **Verify all 10 planted manipulation cases (0–9) surface** — this is CP-08's core test. (~30 min)
5. **Inspect the 60-feature payload** that ML will consume in Module 5. (~10 min)
6. **Verify CP-08 pass conditions** — three named checks. (~5 min)

Total: ~75 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 3.1](lab-3-1-temporal-features.md) is complete — `gold.member_temporal_features` and `gold.cross_product_features` populated
- [ ] You have CDE access and ~6 executors available

## Why deterministic rules matter — read this before Step 2

You might wonder: *if Module 5's ML model is the smart one, why bother with deterministic rules at all?*

Three reasons.

**Reason 1 — Defensibility.** A regulator (SEBI, DPB) doesn't accept "the ML decided not to fire" as a reason for missed manipulation. They accept "the rules fired the alert; the ML deprioritized it; here's the SHAP explanation." The rule layer is what makes the platform defensible. ML alone wouldn't be.

**Reason 2 — Reproducibility.** A deterministic rule can be replayed against historical data and produce the same output every time. Any junior analyst can verify "the alert fired because cancel_rate was 0.92 and pct_under_50ms was 0.71." ML model outputs are stochastic and shift over time as models are retrained — fine for ranking, not for evidence.

**Reason 3 — Floor on detection.** Rules are simple thresholds. If the ML model has bugs (or hasn't been retrained, or was sabotaged), rules still fire. The platform has a non-zero detection floor regardless of ML state.

The trade-off: **rules are intentionally permissive.** They WILL fire on legitimate market makers (Case 6) and legitimate news-driven moves (Case 7). The 92% false-positive rate that ARG-3 talks about comes from rules; Module 5's ML model is what reduces it to ~30% by ranking.

**Don't try to fix the false-positive rate by tightening rules.** You'll miss real manipulation. Keep rules permissive; let ML rank.

## Step 1 — Confirm prerequisites

```sql
SELECT 'temporal' AS f, COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features
  WHERE trade_date >= CURRENT_DATE - 5
UNION ALL
SELECT 'cross_product', COUNT(*) FROM argus_${STUDENT_ID}_gold.cross_product_features
  WHERE trade_date >= CURRENT_DATE - 5;
```

**Both expected > 0.** If either is 0, run Lab 3.1 first.

## Step 2 — Run JOB-08

```bash
cde job create --name "argus-${STUDENT_ID}-job_08_alert_candidates" \
    --type spark \
    --application-file src/transform/job_08_gold_alert_candidates.py \
    --executor-memory 6g --executor-cores 2 --num-executors 6

cde job run --name "argus-${STUDENT_ID}-job_08_alert_candidates"
```

**Expected output** (in CDE job logs):

```
==> alert_candidates: 247 candidate alerts written
```

(Number varies with `--scale` and the planted-case emission count; expect 100–500 at lab scale.)

> 💡 **What does each row in `alert_candidates` represent?** One member-firm × instrument × trade_date combination that crossed at least one rule's threshold. Each row carries: the member's `entity_id` (from Module 2's identity resolution), the instrument and date, the rule(s) that fired (`rule_id`), the severity, and a JSON `feature_payload` with all 60 features the ML model in Module 5 will score.

## Step 3 — Inspect alerts by rule

```sql
SELECT rule_id, severity, COUNT(*) AS alert_count
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE trade_date >= CURRENT_DATE - 5
GROUP BY rule_id, severity
ORDER BY rule_id, severity;
```

**Expected output:** rows for each (rule, severity) combination, like:

| rule_id | severity | alert_count |
|---|---|---:|
| R-101 | HIGH | 47 |
| R-101 | CRITICAL | 8 |
| R-102 | HIGH | 32 |
| R-103 | MEDIUM | 18 |
| R-104 | CRITICAL | 5 |
| ... | | |

> 💡 **What the rule IDs mean:**
> - **R-101 SPOOFING** — `pct_cancelled_under_50ms ≥ 0.50 AND cancel_rate ≥ 0.85`
> - **R-102 LAYERING** — `layered_stack_count ≥ 5`
> - **R-103 WASH** — cancellation pattern with self-cross signature
> - **R-104 CROSS_PRODUCT_IMBALANCE** — `ABS(cross_product_delta_imbalance) ≥ 7.0` (Jane Street)
>
> Each rule has its own thresholds and severities defined in `src/transform/job_08_*.py`. Severities map to Module 5's downstream prioritization: CRITICAL alerts get scored first, MEDIUM alerts get scored last.

## Step 4 — Verify all 10 planted manipulation cases surface

This is CP-08's core test. The synthetic data plants exactly 10 cases (indices 0–9) and JOB-08 must produce at least one alert per case.

```sql
-- Check that each planted case has a corresponding alert
WITH planted_cases AS (
    SELECT 0 AS case_idx, 'BNXM-0042' AS expected_member, 'R-101' AS expected_rule UNION ALL
    SELECT 1, 'BNXM-0073', 'R-101' UNION ALL
    SELECT 2, 'BNXM-0231', 'R-104' UNION ALL  -- Jane Street
    SELECT 3, 'BNXM-0098', 'R-101' UNION ALL  -- momentum ignition
    SELECT 4, 'BNXM-0104', 'R-102' UNION ALL  -- multi-product layering
    SELECT 5, 'BNXM-0167', 'R-103' UNION ALL  -- wash trading
    SELECT 6, 'BNXM-0001', 'R-101' UNION ALL  -- legitimate MM (will fire, ML will deprioritize)
    SELECT 7, 'BNXM-0156', 'R-101' UNION ALL  -- legitimate news (same)
    SELECT 8, 'BNXM-0089', 'R-103' UNION ALL  -- coordinated wash
    SELECT 9, 'BNXM-0042', 'R-102'           -- multi-day layering (BNXM-0042 again)
)
SELECT
    p.case_idx,
    p.expected_member,
    p.expected_rule,
    COUNT(a.alert_id) AS alerts_fired
FROM planted_cases p
LEFT JOIN argus_${STUDENT_ID}_gold.alert_candidates a
  ON a.member_firm_id = p.expected_member
  AND a.rule_id = p.expected_rule
  AND a.trade_date >= CURRENT_DATE - 5
GROUP BY p.case_idx, p.expected_member, p.expected_rule
ORDER BY p.case_idx;
```

**Expected output:** 10 rows. **Every row should show `alerts_fired ≥ 1`.**

If any case shows 0 alerts:
- Cases 0–5 missing → rule threshold issue, see Common Failure Mode #1
- Case 9 missing specifically → multi-day window aggregation issue, see Common Failure Mode #2
- Case 2 missing → cross-product feature issue, see Lab 3.3 Common Failure Mode #1
- Cases 6, 7 missing → these legitimate cases SHOULD fire — same fix path as 0–5

## Step 5 — Inspect the 60-feature payload

For one alert, look at the full feature payload Module 5's ML model will see:

```sql
SELECT
    alert_id,
    rule_id,
    member_firm_id,
    instrument_code,
    severity,
    feature_payload
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE rule_id = 'R-101'
  AND member_firm_id = 'BNXM-0042'
  AND trade_date >= CURRENT_DATE - 5
LIMIT 1;
```

**Expected output:** one row. The `feature_payload` is a JSON string with ~60 key-value pairs. Pretty-print it:

```sql
-- Use json_extract or JSON_QUERY (engine-dependent) to drill in
SELECT
    alert_id,
    json_extract_scalar(feature_payload, '$.cancel_rate')              AS cancel_rate,
    json_extract_scalar(feature_payload, '$.pct_cancelled_under_50ms') AS pct_under_50ms,
    json_extract_scalar(feature_payload, '$.layered_stack_count')      AS stack_count,
    json_extract_scalar(feature_payload, '$.member_firm_category')     AS firm_category,
    json_extract_scalar(feature_payload, '$.entity_prior_str_count')   AS prior_strs
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE alert_id = '<alert_id_from_above>';
```

> 💡 **The 60 features are grouped:**
> - ~25 temporal features (cancel rate, time-to-cancel, ratios)
> - ~10 cross-product features (delta imbalance, directional flags)
> - ~10 entity context features (firm category, prior STR count, asset class)
> - ~10 instrument context features (ESM/ASM flags, liquidity, volatility)
> - ~5 market context features (expiry day flag, sector, market session)
>
> Module 5 will train an XGBoost model on this payload + historical labels (the legacy SMRITI alerts in `bronze.legacy_alerts`). The model output is a `manipulation_probability` score 0–1 that re-ranks the alerts.

## Step 6 — Verify CP-08 pass conditions

CP-08 has **three checks**.

### Check 1 — `alert_candidates` has rows for the prior trading window

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE trade_date >= CURRENT_DATE - 5;
```
**Pass if:** > 50. **Fail if:** 0 — JOB-08 didn't fire any alerts (rule thresholds may be set too high, or features are bad).

### Check 2 — All 4 rule IDs present

```sql
SELECT COUNT(DISTINCT rule_id) FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE trade_date >= CURRENT_DATE - 5;
```
**Pass if:** count = 4 (R-101, R-102, R-103, R-104). **Fail if:** any rule didn't fire.

### Check 3 — All 10 planted cases surface

The Step 4 query returns 10 rows, every one with `alerts_fired ≥ 1`. **Pass if:** all 10 fire. **Fail if:** any show 0.

---

## Common failure mode #1 — Some planted cases missing

**Symptom:** Step 4's query shows several cases with `alerts_fired = 0`.

**Cause** (in decreasing likelihood):
1. The case's underlying feature values aren't extreme enough — the rules are too tight for that synthetic data variant.
2. The case's events are outside the 5-day trade_date window — same fix as Lab 3.1 Common Failure Mode #2.
3. The synthetic data was regenerated with a non-canonical seed.

**Diagnosis:** for the missing case (say, Case 1), look at the underlying features:
```sql
SELECT cancel_rate, pct_cancelled_under_50ms, layered_stack_count
FROM argus_${STUDENT_ID}_gold.member_temporal_features
WHERE member_firm_id = 'BNXM-0073'
  AND trade_date >= CURRENT_DATE - 5;
```
Compare against rule thresholds (R-101: `pct_cancelled_under_50ms ≥ 0.50 AND cancel_rate ≥ 0.85`). If features are below threshold, the planted case wasn't extreme enough.

**Fix:** regenerate data with `--seed 42` (the canonical seed is calibrated to clear thresholds). Don't loosen rule thresholds — that breaks Module 5's training labels.

## Common failure mode #2 — Case 9 (multi-day layering) doesn't fire

**Symptom:** Cases 0–8 fire but Case 9 (multi-day) shows 0 alerts.

**Cause:** R-102's window is per-day. Case 9's pattern only manifests across multiple days. JOB-07 has a multi-day rolling-window aggregation specifically for this; if it's missing, Case 9 invisible.

**Diagnosis:**
```sql
-- Check whether multi-day rolling features are populated
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features
WHERE rolling_5day_layered_stack_count > 0;
```
If 0, the rolling-window logic in JOB-07 isn't producing rows.

**Fix:** check `src/transform/job_07_gold_temporal_features.py` for the rolling-window CTE; ensure it's not commented out. Re-run JOB-07, then re-run JOB-08.

## Common failure mode #3 — Alert count is huge (10,000+)

**Symptom:** JOB-08 produces tens of thousands of alerts instead of hundreds.

**Cause:** rule thresholds are too loose, or the upstream features have an outlier-amplification bug (e.g., divide-by-zero produces Inf which clears any threshold).

**Diagnosis:**
```sql
-- Look at the distribution of cancel_rate
SELECT MIN(cancel_rate), MAX(cancel_rate), AVG(cancel_rate)
FROM argus_${STUDENT_ID}_gold.member_temporal_features;
```
If MAX is > 1.0 or AVG is suspiciously high, features are bad.

**Fix:** re-check Lab 3.1 Common Failure Mode #1 (parent_order_id join issue). If features are clean and you still get too many alerts, the rule thresholds need tightening — but only do this if you'd otherwise miss the manipulation signal. The default thresholds are calibrated; trust them.

---

## Pass condition for CP-08

All three checks pass. With alert candidates populated, the analyst toolbox is finally usable: deterministic rules produce defensible candidates, and the 60-feature payload is ready for Module 5's ML scoring.

## Wrap-up — what you can now do that you couldn't before

You can produce regulator-defensible deterministic alerts on top of feature data. You understand the rule/ML division of labor: rules generate candidates (defensible), ML ranks them (signal-prioritization). You can read JOB-08's threshold logic and predict which member-firm-days will surface as candidates.

Lab 3.3 verifies the cross-product detection (Jane Street pattern) specifically. Allow ~45 minutes.
