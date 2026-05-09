# Lab 3.3 — Cross-Product Detection (CP-09)

> ℹ️ **Module:** 3 — Temporal & Cross-Product Feature Engineering
> **Closes deficiency:** ARG-2 part 2 (the Jane Street gap)
> **Source files:** [`src/transform/job_07_gold_temporal_features.py`](../src/transform/job_07_gold_temporal_features.py), [`src/transform/job_08_gold_alert_candidates.py`](../src/transform/job_08_gold_alert_candidates.py)

## Objectives

- Confirm Case 2 (the planted Jane Street-style marking-the-close pattern) shows a `cross_product_delta_imbalance` ≥ 7.0
- Verify R-104 fires a CRITICAL alert on Case 2
- Walk through what the alert reveals — and what it would have prevented if MSE had this capability when SEBI's January 2025 inspection started

## Why this matters

The 2024–25 SEBI Jane Street order alleged ₹4,843 crore in unlawful gains across 18 trading days, achieved by orchestrating cash + futures + options positions on the same underlying. The cash leg moved the index toward a strike price; the options leg profited from the move. To detect this, you need cross-product correlation across products that, at most exchanges, are owned by separate trading-tech teams with separate databases. ARG-2 cited this as one of the key reasons MSE engineering took 11 weeks to reproduce SEBI's missed episodes — the data model didn't support cross-product joins. JOB-07's `cross_product_features` is the table that fixes that. CP-09 verifies it works.

## Procedure

### Step 1 — Confirm prerequisites

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.cross_product_features WHERE trade_date >= CURRENT_DATE - 5;
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates WHERE rule_id = 'R-104' AND trade_date >= CURRENT_DATE - 5;
```

Both should be > 0. If `R-104` count is 0, re-run JOB-08 (Lab 3.2) — JOB-08 must run after JOB-07 because R-104 reads from `cross_product_features`.

### Step 2 — Locate Case 2's imbalance

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

**Expected output**: at least one row with:

- `cross_product_delta_imbalance` ≥ 7.0 (the Jane Street threshold)
- `directional_consistency_flag` = TRUE (cash, futures, options all pointing the same way)
- `options_net_delta_exposure` substantially larger than `cash_net_position + futures_net_position`

If `cross_product_delta_imbalance` is null or below 7.0, the synthetic generator's Case 2 emission isn't producing enough options activity. Most likely cause is the F&O instrument seeding in `data/generate_data.py` only created 5 strikes per underlying — bump that to 11 strikes and regenerate.

### Step 3 — Verify R-104 alert was fired on Case 2

```sql
SELECT
    alert_id, fired_ts, rule_id, severity,
    member_firm_id, underlying_code, instrument_code,
    SUBSTR(features, 1, 200) AS features_preview
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE rule_id = 'R-104'
  AND member_firm_id = 'BNXM-0231'
  AND trade_date >= CURRENT_DATE - 5
ORDER BY fired_ts DESC LIMIT 5;
```

**Expected output**: at least one row with `severity = CRITICAL`. The `features` JSON should include the cross-product imbalance value as a feature for the ML model.

### Step 4 — Inspect the directional pattern

The Jane Street pattern requires that cash, futures, and options all point in the **same direction** in a way that suggests the cash leg exists only to manipulate the underlying price for options gain.

```sql
-- For Case 2, look at the directional breakdown
SELECT
    member_firm_id,
    underlying_code,
    trade_date,
    SIGN(cash_net_position)              AS cash_dir,
    SIGN(futures_net_position)           AS fut_dir,
    SIGN(options_net_delta_exposure)     AS opt_dir,
    cash_net_position,
    futures_net_position,
    options_net_delta_exposure,
    options_net_delta_exposure / GREATEST(ABS(cash_net_position + futures_net_position), 1) AS imbalance
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE member_firm_id = 'BNXM-0231'
  AND trade_date >= CURRENT_DATE - 5
ORDER BY trade_date DESC LIMIT 3;
```

**Expected output**: the three direction columns (`cash_dir`, `fut_dir`, `opt_dir`) all show the same sign on at least one row. This is what makes the pattern manipulative — coordinated, not coincidental.

### Step 5 — What would this have caught at SEBI?

Open the synthetic-data manipulation report (`compliance_test_cases.csv`) and locate Case 2's `expected_disposition`:

```bash
grep '^2,' data/generated/compliance_test_cases.csv
```

Should return:

```
2,CROSS_PRODUCT_MANIPULATION,MARKING_THE_CLOSE,BNXM-0231,True,CONFIRMED_MANIPULATION,3,"CP-08, CP-09, CP-13",LARGE-cap BANKING; see PRD §11
```

The case is tagged `CONFIRMED_MANIPULATION` — it's a real manipulation pattern. CP-09's job is to demonstrate ARGUS detects it; CP-13 (Module 5) demonstrates the ML model preserves it as a high-priority alert despite the noise from Cases 6–7.

## Checkpoint CP-09 — Cross-product features detect Jane Street pattern

### Pass condition

All four checks pass.

### Check 1 — Case 2 has `cross_product_delta_imbalance` ≥ 7.0

The Step 2 query returns at least one row for `BNXM-0231` with `cross_product_delta_imbalance ≥ 7.0`.

### Check 2 — R-104 alert fired on Case 2

The Step 3 query returns at least one row with `severity = CRITICAL`.

### Check 3 — Directional consistency confirmed

The Step 4 query returns at least one row where `cash_dir`, `fut_dir`, and `opt_dir` all share the same sign (or the `directional_consistency_flag` column reads TRUE in Step 2).

### Check 4 — Cross-product features cover at least 2 instrument types

```sql
SELECT
    SUM(CASE WHEN cash_net_position    != 0 THEN 1 ELSE 0 END) AS rows_with_cash,
    SUM(CASE WHEN futures_net_position != 0 THEN 1 ELSE 0 END) AS rows_with_futures,
    SUM(CASE WHEN options_net_delta_exposure != 0 THEN 1 ELSE 0 END) AS rows_with_options
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE trade_date >= CURRENT_DATE - 5;
```

**Expected output**: at least two of the three columns are non-zero. If only `cash` is non-zero, the synthetic data didn't seed F&O instruments — re-check `data/generate_data.py`'s instrument generation step.

---

## Common failure mode — `cross_product_delta_imbalance` is null

**Symptom**: the `cross_product_delta_imbalance` column is null for every row.

**Diagnosis**: JOB-07's pivot on `instrument_type` requires the source `instruments.csv` to populate `EQUITY`, `FUTURE`, and `OPTION` rows. If the synthetic generator skipped F&O seeding (perhaps `--scale` was set extremely low), the pivot silently produces nulls in the FUTURE and OPTION columns, and the imbalance ratio degenerates to 0/0 = null.

**Fix**: verify the instrument inventory:

```sql
SELECT instrument_type, COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master
WHERE is_current GROUP BY instrument_type;
```

You should see rows for `EQUITY`, `FUTURE`, and `OPTION`. If `FUTURE` or `OPTION` is missing, regenerate data at `--scale 0.05` or higher (the F&O block in the generator only kicks in at non-trivial scale).

---

## Pass condition for CP-09

All four checks pass. With cross-product features in place, the platform can finally detect the manipulation pattern that defined SEBI's biggest enforcement action of 2025. The remaining question — separating the real manipulators from the false positives — is what Module 5's ML model answers.
