# Lab 4.1 — Governed Views (CP-10)

> ℹ️ **Module:** 4 — Governed Views in CDW
> **Closes deficiency:** ARG-5 part 1 (PII access governance)
> **Source files:** [`sql/governed_views.sql`](../sql/governed_views.sql)

## Objectives

- Deploy the 7 governed views into the `argus_${STUDENT_ID}_views` schema
- Verify each view returns rows under the right role
- Confirm column masking on PAN behaves correctly across roles

## Why this matters

Before this module, every analyst with access to `argus_${STUDENT_ID}_silver.member_master` saw full PAN, email, and mobile numbers for 24 million investors. That's a DPDP §16 failure waiting to be found. After this module, the same analyst sees `XXXXX****X` in place of PAN — enough context to confirm an alert is on the right person, without ever exposing the raw identifier.

## Procedure

### Step 1 — Confirm prerequisites

```sql
-- Module 3 must have populated alert_candidates and feature tables
SELECT
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates WHERE trade_date >= CURRENT_DATE - 5) AS alerts,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features WHERE trade_date >= CURRENT_DATE - 5) AS features,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current) AS members;
```

All three counts must be > 0. If `alerts` is 0, run Module 3 / JOB-08 first.

### Step 2 — Create the views

```bash
impala-shell -f sql/governed_views.sql
```

**Expected output**: 7 view-creation statements complete without error. Verify:

```sql
SHOW VIEWS IN argus_${STUDENT_ID}_views;
```

**Expected output**: 7 rows — `vw_alert_queue`, `vw_member_analytics`, `vw_cross_product_alerts`, `vw_surveillance_audit`, `vw_consent_audit`, `vw_kpi_daily`, `vw_model_performance`. (`vw_active_investigations` is added in Lab 4.2 with the policies, so 7 here is correct.)

### Step 3 — Sanity-check each view returns rows

```sql
SELECT 'vw_alert_queue'           AS view_name, COUNT(*) FROM argus_${STUDENT_ID}_views.vw_alert_queue
UNION ALL SELECT 'vw_member_analytics',         COUNT(*) FROM argus_${STUDENT_ID}_views.vw_member_analytics
UNION ALL SELECT 'vw_cross_product_alerts',     COUNT(*) FROM argus_${STUDENT_ID}_views.vw_cross_product_alerts
UNION ALL SELECT 'vw_surveillance_audit',       COUNT(*) FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
UNION ALL SELECT 'vw_consent_audit',            COUNT(*) FROM argus_${STUDENT_ID}_views.vw_consent_audit
UNION ALL SELECT 'vw_kpi_daily',                COUNT(*) FROM argus_${STUDENT_ID}_views.vw_kpi_daily
UNION ALL SELECT 'vw_model_performance',        COUNT(*) FROM argus_${STUDENT_ID}_views.vw_model_performance;
```

**Expected output** (at lab scale 0.05):

| view_name | row_count |
|---|---:|
| `vw_alert_queue` | 100–500 (pending alerts) |
| `vw_member_analytics` | ~12,000 |
| `vw_cross_product_alerts` | 5–50 |
| `vw_surveillance_audit` | 100–500 |
| `vw_consent_audit` | 0 (until Module 7 writes audit events) |
| `vw_kpi_daily` | 0 (until JOB-09 / Module 5 writes KPIs) |
| `vw_model_performance` | 0 (until model is scored — Module 5) |

It's expected for the bottom three to be empty here; Modules 5 and 7 populate them.

### Step 4 — Inspect a single alert end-to-end through the view

```sql
SELECT
    alert_id,
    rule_id,
    pattern_type,
    severity,
    member_firm_id,
    member_firm_name,
    member_firm_category,
    instrument_code,
    sector,
    market_cap_bucket,
    fired_ts
FROM argus_${STUDENT_ID}_views.vw_alert_queue
ORDER BY fired_ts DESC LIMIT 5;
```

**Expected output**: 5 rows. Each row should have non-null `member_firm_name` and non-null `sector` — confirming the view's joins to `member_master` and `instrument_master` are working.

### Step 5 — Test PAN masking (preview — full Ranger test in Lab 4.2)

The view definition projects `m.investor_pan` directly. Without Ranger applied, this column shows the raw PAN. With Ranger applied (Lab 4.2), it should be masked. For now, just confirm the column is exposed:

```sql
DESCRIBE argus_${STUDENT_ID}_views.vw_surveillance_audit;
```

**Expected output**: column listing includes `investor_pan` and `investor_pan_hash`. Both columns will be subject to Ranger policies in Lab 4.2.

## Checkpoint CP-10 — Governed views deployed and joining cleanly

### Pass condition

All four checks pass.

### Check 1 — All 7 views exist

```sql
SELECT COUNT(*) FROM information_schema.views
WHERE table_schema = 'argus_${STUDENT_ID}_views';
-- expect: 7
```

### Check 2 — `vw_alert_queue` joins to member_master successfully

```sql
SELECT COUNT(*) AS total,
       SUM(CASE WHEN member_firm_name IS NULL THEN 1 ELSE 0 END) AS unjoined
FROM argus_${STUDENT_ID}_views.vw_alert_queue;
```

**Expected**: `unjoined / total` < 0.05. If most alerts have NULL `member_firm_name`, the JOIN to `member_master` is failing — most likely cause is `member_master` doesn't have member-level rows (only investor or trader rows). Re-run JOB-05 with the seed step from Lab 1.2.

### Check 3 — `vw_cross_product_alerts` includes Case 2 (BNXM-0231)

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_cross_product_alerts
WHERE member_firm_id = 'BNXM-0231'
  AND ABS(cross_product_delta_imbalance) >= 5.0;
-- expect: >= 1
```

If 0, JOB-07 didn't produce a cross-product imbalance for Case 2 — go back to Lab 3.3 / CP-09.

### Check 4 — `vw_surveillance_audit` carries the SEBI_AUDIT_TRAIL_${STUDENT_ID} classification

```sql
SELECT DISTINCT atlas_classification
FROM argus_${STUDENT_ID}_views.vw_surveillance_audit;
-- expect single row: 'SEBI_AUDIT_TRAIL_${STUDENT_ID}'
```

The hard-coded string is what Module 4's Ranger policies use to recognize statutory views and bypass the consent filter. If this returns NULL or a different value, check the view DDL — it should `CAST('SEBI_AUDIT_TRAIL_${STUDENT_ID}' AS STRING) AS atlas_classification`.

---

## Common failure mode — `vw_alert_queue` is empty even though `alert_candidates` has rows

**Symptom**: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates WHERE disposition = 'PENDING'` returns thousands, but `vw_alert_queue` is empty.

**Diagnosis**: the view filters on `WHERE a.disposition = 'PENDING'`. JOB-08 writes alerts with `disposition = 'PENDING'` by default. If you've manually run a disposition-update statement, or if the JOB-08 schema changed, the filter excludes everything.

**Fix**: confirm:

```sql
SELECT disposition, COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates
GROUP BY disposition;
```

Should show `PENDING` as the dominant value. If it shows other values like `NULL`, the JOB-08 default is broken. Re-check the SELECT projection in `src/transform/job_08_gold_alert_candidates.py` — it should set `F.lit("PENDING").alias("disposition")`.

---

## Pass condition for CP-10

All four checks pass. The views are deployed; what determines who sees what is now Lab 4.2's Ranger policies.
