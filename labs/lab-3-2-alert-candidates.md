# Lab 3.2 — Alert Candidates (CP-08)

> ℹ️ **Module:** 3 — Temporal & Cross-Product Feature Engineering
> **Closes deficiency:** ARG-2 part 2 (alert candidate generation)
> **Source files:** [`src/transform/job_08_gold_alert_candidates.py`](../src/transform/job_08_gold_alert_candidates.py)

## Objectives

- Run JOB-08 to fire deterministic rule-based alerts into `argus_${STUDENT_ID}_gold.alert_candidates`
- Verify all 10 planted manipulation cases (indices 0–9) surface as alerts
- Inspect the 60-feature payload that JOB-09 (Module 5) will consume

## Why this matters

This is the moment of truth for the rules engine. The PRD calls for a deterministic rule layer that produces auditable, regulator-defensible candidates — alerts that any junior analyst can replay against historical data and reproduce. The ML model in Module 5 only ranks these candidates; it doesn't generate them. That separation is what keeps the platform defensible: SEBI doesn't accept "the ML decided not to fire" as a reason for a missed manipulation, but they do accept "the rules fired the alert; the ML deprioritized it; here's the SHAP explanation."

## Procedure

### Step 1 — Confirm prerequisites

```sql
SELECT 'temporal' AS f, COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features
  WHERE trade_date >= CURRENT_DATE - 5
UNION ALL
SELECT 'cross_product', COUNT(*) FROM argus_${STUDENT_ID}_gold.cross_product_features
  WHERE trade_date >= CURRENT_DATE - 5;
-- both expected > 0 (Lab 3.1 must have run JOB-07 first)
```

### Step 2 — Run JOB-08

```bash
cde job create --name argus-job_08_alert_candidates \
    --type spark \
    --application-file src/transform/job_08_gold_alert_candidates.py \
    --executor-memory 6g --executor-cores 2 --num-executors 6

cde job run --name argus-job_08_alert_candidates
```

**Expected output**:

```
==> alert_candidates: 247 candidate alerts written
```

(Number varies with `--scale` and the planted-case emission count; expect 100–500 at lab scale.)

### Step 3 — Inspect the alerts by rule

```sql
SELECT
    rule_id,
    pattern_type,
    severity,
    COUNT(*) AS n
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE trade_date >= CURRENT_DATE - 5
GROUP BY rule_id, pattern_type, severity
ORDER BY rule_id;
```

**Expected output**: 5 rows, one per rule R-101 through R-105, with non-zero counts. The exact split depends on synthetic data, but representative output looks like:

| rule_id | pattern_type | severity | n |
|---|---|---|---:|
| R-101 | SPOOFING | HIGH | 18 |
| R-102 | LAYERING | HIGH | 25 |
| R-103 | MOMENTUM_IGNITION | MEDIUM | 9 |
| R-104 | CROSS_PRODUCT | CRITICAL | 4 |
| R-105 | WASH | MEDIUM | 6 |

If R-104 has 0 rows, JOB-07's cross-product features didn't produce any imbalance > 7.0 — most likely cause is the planted Case 2 (Jane Street) didn't land in the time window. Re-check Lab 3.1 / CP-07 first.

### Step 4 — Verify all 10 manipulation cases surface

The 10 planted cases (0–9) should each fire at least one alert. Verify by joining against the planted member firm IDs:

```sql
WITH planted AS (
    SELECT
        c.case_idx, c.pattern, c.member_firm_id, c.is_real_manipulation
    FROM (VALUES
        (0, 'LAYERING',            'BNXM-0042', TRUE),
        (1, 'SPOOFING',            'BNXM-0117', TRUE),
        (2, 'MARKING_THE_CLOSE',   'BNXM-0231', TRUE),
        (3, 'MOMENTUM_IGNITION',   'BNXM-0089', TRUE),
        (4, 'CROSS_PRODUCT_LAYER', 'BNXM-0042', TRUE),
        (5, 'WASH_TRADE',          'BNXM-0276', TRUE),
        (6, 'LEGITIMATE_MM',       'BNXM-0001', FALSE),
        (7, 'LEGITIMATE_NEWS',     'BNXM-0156', FALSE),
        (8, 'AMBIGUOUS',           'BNXM-0203', FALSE),
        (9, 'MULTI_DAY_LAYERING',  'BNXM-0117', TRUE)
    ) c(case_idx, pattern, member_firm_id, is_real_manipulation)
)
SELECT
    p.case_idx, p.pattern, p.member_firm_id, p.is_real_manipulation,
    COUNT(a.alert_id) AS alerts_fired
FROM planted p
LEFT JOIN argus_${STUDENT_ID}_gold.alert_candidates a
       ON a.member_firm_id = p.member_firm_id
      AND a.trade_date >= CURRENT_DATE - 5
GROUP BY p.case_idx, p.pattern, p.member_firm_id, p.is_real_manipulation
ORDER BY p.case_idx;
```

**Expected output**: 10 rows. Every row should have `alerts_fired > 0`. Cases 6–8 (negative / ambiguous) firing alerts is correct and expected — that's the false-positive flood the legacy platform produces, and exactly what Module 5's ML model is trained to deprioritize.

### Step 5 — Inspect the feature payload

```sql
SELECT
    alert_id, member_firm_id, instrument_code, severity,
    features
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE rule_id = 'R-104'   -- the most interesting alerts
  AND trade_date >= CURRENT_DATE - 5
LIMIT 3;
```

**Expected output**: each row's `features` column is a JSON object with non-null values for the headline features:

```json
{
  "cancel_rate": 0.91,
  "median_time_to_cancel_ms": 78,
  "p95_time_to_cancel_ms": 423,
  "pct_cancelled_under_50ms": 0.34,
  "max_simultaneous_levels": 6,
  "layered_stack_count": 4,
  "order_to_trade_ratio_1m": 12.5,
  "order_to_trade_ratio_5m": 8.4,
  "order_to_trade_ratio_30m": 5.2,
  "notional_traded": 47200000
}
```

JOB-09 (Module 5) reads this JSON and feeds the values into the XGBoost ranker. The richer the features, the better the model can separate true positives (Case 0) from false positives (Case 6).

## Checkpoint CP-08 — All 10 manipulation cases surface

### Pass condition

All three checks pass.

### Check 1 — Five rules all fire at least once

The query in Step 3 returns 5 rows, all with non-zero counts.

### Check 2 — All 10 planted cases produce ≥ 1 alert each

The query in Step 4 returns 10 rows; every row has `alerts_fired > 0`. This is the central CP-08 assertion.

### Check 3 — Feature payloads are non-empty

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE features IS NULL OR features = '{}'
  AND trade_date >= CURRENT_DATE - 5;
-- expect: 0 (or very small — < 1% of total alerts)
```

If this returns a non-trivial count, the feature-attach JOIN in JOB-08 isn't joining cleanly — probably a member×instrument mismatch between `alert_candidates` and `member_temporal_features`. The two should share a key.

---

## Common failure mode — Case 9 (multi-day layering) doesn't fire

**Symptom**: 9 of 10 planted cases produce alerts; Case 9 (BNXM-0117 multi-day layering across the full expiry week) shows `alerts_fired = 0`.

**Diagnosis**: Case 9 is intentionally subtle — the layering activity is spread across 5 trading days, with no single day showing layered_stack_count ≥ 3. JOB-08's R-102 rule is per-day, so it doesn't catch the multi-day pattern.

This is **a feature, not a bug**. Case 9 is meant to demonstrate the limits of single-day rule-based detection. It's left as a Module 5 / Module 7 extension exercise: train the ML model on multi-day rolling features (Module 5 bonus), or build a custom Atlas lineage query that surfaces sustained activity across cases (Module 7 stretch).

**For CP-08 pass condition**: Case 9 firing alerts on at least one of its 5 days is sufficient. The sub-rule R-101 SPOOFING or R-102 LAYERING should fire on at least one day's worth of BNXM-0117 activity. If even that's empty, the synthetic generator is producing very tame Case 9 events — re-generate.

---

## Pass condition for CP-08

All three checks pass. The candidate-generation layer is now producing actionable, auditable alerts that downstream systems (the ML model in Module 5, the GenAI drafter in Module 6) can consume.
