# Lab 4.1 — Governed Views (CP-10)

> 👋 **Module 4 first-timer?** Read [`docs/module-4-primer.md`](../docs/module-4-primer.md) first. About 10 minutes.

> ℹ️ **Module:** 4 — Governed Views in CDW
> **Closes deficiency:** ARG-5 part 1 (PII access governance)
> **Time:** ~45 minutes if `impala-shell` access works first try; up to 90 minutes if connectivity to CDW needs setup.
> **Source files:** [`sql/governed_views.sql`](../sql/governed_views.sql)

## What you're going to do

1. **Confirm prerequisites** — Module 3 outputs are populated. (~3 min)
2. **Create the 7 views** by running the SQL DDL through `impala-shell`. (~5 min)
3. **Sanity-check each view returns rows** — except the 3 that depend on Modules 5–7. (~10 min)
4. **Inspect a single alert end-to-end through `vw_alert_queue`** — confirm joins are working. (~10 min)
5. **Verify the SEBI_AUDIT_TRAIL classification on `vw_surveillance_audit`** — needed for Module 7. (~5 min)
6. **Verify CP-10 pass conditions** — four checks. (~5 min)

Total: ~45 minutes.

## Before you begin — prerequisite checklist

- [ ] Module 3 is complete — `gold.alert_candidates`, `gold.member_temporal_features`, `gold.cross_product_features` all populated
- [ ] You have access to `impala-shell` or Hue with Impala connection
- [ ] The schema `argus_${STUDENT_ID}_views` exists — created during Day 1 provisioning. Quick check: `SHOW SCHEMAS` in Impala should list it. If not, run `CREATE SCHEMA argus_${STUDENT_ID}_views`.

## Why governed views matter — read this before Step 2

Before this module, every analyst with access to `silver.member_master` saw full PAN, email, and mobile numbers for ~12,000 investors (synthetic; in production it's ~24M). That's a DPDP §16 failure waiting to be found.

After this module, the analyst's access is via `vw_surveillance_audit` instead. The view shows enough context to confirm an alert is on the right person, but PAN comes through Ranger column-masking (Lab 4.2) as `XXXXX****X`. The DPO who legitimately needs the full PAN sees it; everyone else sees the mask.

The view also pre-joins everything an analyst typically needs (alert_candidates × member_master × instrument_master) so analysts don't need to remember join keys, and gives a stable interface that survives upstream table changes.

## Step 1 — Confirm prerequisites

```sql
SELECT
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates WHERE trade_date >= CURRENT_DATE - 5) AS alerts,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features WHERE trade_date >= CURRENT_DATE - 5) AS features,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current) AS members;
```

All three counts must be > 0. If `alerts` is 0, complete Module 3 first.

## Step 2 — Create the 7 views

```bash
impala-shell -i ${IMPALA_HOST}:21000 -f sql/governed_views.sql
```

> 💡 **Why `impala-shell` instead of Hue?** The DDL file uses parameter substitution patterns (`${STUDENT_ID}`) that work with `impala-shell -f`. You can paste the DDL into Hue too, but you'd have to substitute the values manually first. The shell's `-f` is simpler and idempotent.

> 💡 **What if the schema needs to be created first?** The DDL starts with `CREATE SCHEMA IF NOT EXISTS argus_${STUDENT_ID}_views`. You don't need to create it separately.

**Expected output:** 7 view-creation statements complete without error. Verify:

```sql
SHOW VIEWS IN argus_${STUDENT_ID}_views;
```

**Expected output:** 7 rows — `vw_alert_queue`, `vw_member_analytics`, `vw_cross_product_alerts`, `vw_surveillance_audit`, `vw_consent_audit`, `vw_kpi_daily`, `vw_model_performance`.

> 💡 **An 8th view (`vw_active_investigations`) is added in Lab 4.2** alongside the policies — so seeing 7 here is correct.

## Step 3 — Sanity-check each view returns rows

```sql
SELECT 'vw_alert_queue'           AS view_name, COUNT(*) AS rows FROM argus_${STUDENT_ID}_views.vw_alert_queue
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
| `vw_consent_audit` | 0 *(populated by Module 7)* |
| `vw_kpi_daily` | 0 *(populated by Module 5)* |
| `vw_model_performance` | 0 *(populated by Module 5)* |

> 💡 **Why are 3 views empty?** `vw_consent_audit` mirrors `gold.consent_audit`, which Module 7's workflow writes. `vw_kpi_daily` and `vw_model_performance` mirror tables that Module 5's ML scoring writes. They're empty here because those modules haven't run yet — that's correct, not broken.

If `vw_alert_queue` is empty, see Common Failure Mode #1.

## Step 4 — Inspect a single alert end-to-end

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

**Expected output:** 5 rows. Each must have:
- non-null `member_firm_name` (proves JOIN to `member_master` worked)
- non-null `sector` (proves JOIN to `instrument_master` worked)
- non-null `severity` and `fired_ts`

> 💡 **What pre-joining bought you:** without the view, the analyst would have to write a 4-table JOIN every time they wanted alert + firm context + instrument context. With the view, they SELECT from `vw_alert_queue` and get all of it. That's a 10× productivity gain for the surveillance team.

If most rows have NULL `member_firm_name`, see Common Failure Mode #2.

## Step 5 — Verify SEBI_AUDIT_TRAIL classification on `vw_surveillance_audit`

This is the magic that makes the DPDP §6(4) statutory exception work in Module 7. The view declares itself as a SEBI audit trail; Ranger policies look for that classification when deciding whether to bypass the consent filter for `compliance_dpo`.

```sql
SELECT DISTINCT atlas_classification
FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
LIMIT 1;
```

**Expected output:** single row with value `'SEBI_AUDIT_TRAIL_${STUDENT_ID}'` (with your actual student ID substituted).

> 💡 **What the column does:** the view's DDL has `CAST('SEBI_AUDIT_TRAIL_${STUDENT_ID}' AS STRING) AS atlas_classification`. The constant string is what Ranger's row-filter policy in Lab 4.2 looks for to recognize this view as a statutory exception. The Atlas tag itself (Lab 7.1) is a separate but related mechanism — both work together for full DPDP §7 enforcement.

If this returns NULL or a different value, see Common Failure Mode #3.

## Step 6 — Verify CP-10 pass conditions

CP-10 has **four checks**.

### Check 1 — All 7 views exist

```sql
SELECT COUNT(*) FROM information_schema.views
WHERE table_schema = 'argus_${STUDENT_ID}_views';
```
**Pass if:** count = 7. **Fail if:** any are missing.

### Check 2 — `vw_alert_queue` joins to member_master successfully

```sql
SELECT COUNT(*) AS total,
       SUM(CASE WHEN member_firm_name IS NULL THEN 1 ELSE 0 END) AS unjoined
FROM argus_${STUDENT_ID}_views.vw_alert_queue;
```
**Pass if:** `unjoined / total < 0.05`. **Fail if:** higher — the JOIN is failing for most rows.

### Check 3 — `vw_cross_product_alerts` includes Case 2 (BNXM-0231)

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_cross_product_alerts
WHERE member_firm_id = 'BNXM-0231'
  AND ABS(cross_product_delta_imbalance) >= 5.0;
```
**Pass if:** ≥ 1. **Fail if:** 0 — go back to Lab 3.3 (CP-09).

### Check 4 — `vw_surveillance_audit` carries SEBI_AUDIT_TRAIL classification

The Step 5 query returns the expected value. **Pass if:** matches. **Fail if:** NULL or different — view DDL is wrong.

---

## Common failure mode #1 — `vw_alert_queue` is empty even though `alert_candidates` has rows

**Symptom:** Step 3 shows `vw_alert_queue: 0` but `gold.alert_candidates` has thousands of rows.

**Cause:** the view filters on `WHERE a.disposition = 'PENDING'`. JOB-08 writes alerts with `disposition = 'PENDING'` by default. If JOB-08's projection is broken, the column may be NULL or a different value, and the view's filter excludes everything.

**Diagnosis:**
```sql
SELECT disposition, COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates
GROUP BY disposition;
```
**Expected:** `PENDING` should be the dominant value. If you see NULL or other values, JOB-08's projection is bad.

**Fix:** check `src/transform/job_08_gold_alert_candidates.py` — should set `F.lit("PENDING").alias("disposition")` in the SELECT. Re-deploy and re-run JOB-08.

## Common failure mode #2 — Most alerts have NULL `member_firm_name`

**Symptom:** Step 4's query returns rows but `member_firm_name` is NULL on most.

**Cause:** the JOIN from `alert_candidates` to `member_master` isn't matching. The view joins on `member_firm_id`, and either Bronze didn't have full member data or the keys don't match the format.

**Diagnosis:**
```sql
-- Are member rows present?
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current AND member_firm_id IS NOT NULL;

-- Do the keys overlap?
SELECT COUNT(DISTINCT a.member_firm_id) AS in_alerts,
       COUNT(DISTINCT m.member_firm_id) AS in_master,
       SUM(CASE WHEN m.member_firm_id IS NOT NULL THEN 1 ELSE 0 END) AS matched
FROM argus_${STUDENT_ID}_gold.alert_candidates a
LEFT JOIN argus_${STUDENT_ID}_silver.member_master m
  ON a.member_firm_id = m.member_firm_id AND m.is_current
WHERE a.trade_date >= CURRENT_DATE - 5;
```

If `in_master` is much smaller than `in_alerts`, run `seed_member_cdc.py` (Lab 1.2 Step 5b) to populate member-level rows.

**Fix:** re-run the seed step, then re-run JOB-05 (Lab 2.2). Views auto-pick-up underlying data; no need to recreate them.

## Common failure mode #3 — `vw_surveillance_audit` `atlas_classification` is NULL

**Symptom:** Step 5 query returns NULL or a row missing the column entirely.

**Cause:** the view DDL is missing the `CAST('SEBI_AUDIT_TRAIL_${STUDENT_ID}' AS STRING) AS atlas_classification` projection. This may have been edited out, or the parameter substitution didn't apply.

**Diagnosis:**
```sql
SHOW CREATE TABLE argus_${STUDENT_ID}_views.vw_surveillance_audit;
```
Look for the `atlas_classification` column in the SELECT.

**Fix:** drop and recreate the view from the canonical `sql/governed_views.sql`:
```sql
DROP VIEW argus_${STUDENT_ID}_views.vw_surveillance_audit;
-- then re-run the relevant CREATE VIEW from sql/governed_views.sql
```

## Common failure mode #4 — `impala-shell` can't connect

**Symptom:** running the DDL fails with "could not connect to localhost:21000".

**Cause:** `impala-shell` defaults to localhost. In CDP, you have to point it at your CDW endpoint.

**Fix:**
```bash
# Get your CDW JDBC URL from the CDW console, extract the impala host
impala-shell -i <impala-host>:21000 -k -f sql/governed_views.sql
# `-k` is for Kerberos; if your cluster doesn't use it, omit
```

If you can't determine the host, ask your instructor.

---

## Pass condition for CP-10

All four checks pass. The views are deployed and joining cleanly. Ranger policies in Lab 4.2 will determine *who* sees *what* through these views.

## Wrap-up — what you can now do that you couldn't before

You can deploy a governed view layer in Impala that pre-joins multiple Gold and Silver tables, providing a stable analyst-facing interface. You understand why governed views are the right primitive for analytics access (vs. direct table grants). You can verify view joins are working at the data level.

Lab 4.2 puts the access controls on top — column masking on PAN, row filtering for consent. Allow ~75 minutes.
