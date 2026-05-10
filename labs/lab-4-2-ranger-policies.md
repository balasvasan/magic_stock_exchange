# Lab 4.2 — Ranger Policies (CP-11)

> 👋 **Module 4 first-timer?** Read [`docs/module-4-primer.md`](../docs/module-4-primer.md) first. About 10 minutes.

> ℹ️ **Module:** 4 — Governed Views in CDW
> **Closes deficiency:** ARG-5 part 1 (PII access governance)
> **Time:** ~75 minutes if Ranger admin UI access works first try; up to 3 hours if role assignments need fixing.
> **Source files:** [`sql/ranger_policies.sql`](../sql/ranger_policies.sql), [`sql/governed_views.sql`](../sql/governed_views.sql) (the `vw_active_investigations` view that gets added in this lab)

## What you're going to do

1. **Confirm prerequisites** — Lab 4.1 complete, Atlas tags from CP-17 applied (or apply minimal tags). (~3 min)
2. **Add the `vw_active_investigations` view** — the 8th governed view, deferred from Lab 4.1. (~5 min)
3. **Deploy 3 Ranger policy families:**
   - 3a. Tag-based access policy on `PII_HIGH` (~10 min)
   - 3b. Column-masking policy on `investor_pan` (~10 min)
   - 3c. Row-filter policy on `member_temporal_features` (the DPDP §6(4) filter) (~15 min)
4. **Test policies as each role:** `surveillance_l1`, `compliance_dpo`, `research_analyst`. (~20 min)
5. **Verify CP-11 pass conditions** — three checks (one per policy family). (~10 min)

Total: ~75 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 4.1](lab-4-1-governed-views.md) is complete and CP-10 passed
- [ ] You have access to the **Ranger admin UI** (your instructor will provide URL + credentials)
- [ ] [Lab 7.1](lab-7-1-atlas-classifications.md) is complete OR you've applied minimal Atlas tags (`PII_HIGH`, `SEBI_AUDIT_TRAIL`) for this lab — quick check: `curl ${ATLAS_URL}/api/atlas/v2/types/typedef/name/PII_HIGH_${STUDENT_ID}` returns 200
- [ ] You can `SET ROLE` for `surveillance_l1`, `compliance_dpo`, `research_analyst` — quick check: `SHOW ROLES` lists all three

> 💡 **Lab dependency note:** Lab 4.2 depends on Atlas tags from Lab 7.1 for the tag-based policy. The capstone runs Module 4 before Module 7 in the schedule, so either (a) your instructor pre-applies minimal tags for this lab, or (b) skip the tag-based policy step and complete it later after Lab 7.1.

## Why Ranger policies matter — read this before Step 3

Ranger is the **authorization layer**. It decides who sees what, and for PII specifically, *how* they see it.

Three policy types in ARGUS:

**1. Tag-based access** — written against Atlas classifications, not table names. One policy ("users in role `research_analyst` cannot see `PII_HIGH` columns") covers every column tagged `PII_HIGH` across every table. Tags propagate downstream via Atlas; your policy adapts automatically.

**2. Column-masking** — show a different value depending on the user's role. The column still appears in the result set, just with a transformed value (`XXXXX****X` for masked PAN). Analysts confirm "yes this alert is on the right person" without seeing the raw identifier.

**3. Row-filtering** — silently inject a WHERE clause. The user runs `SELECT * FROM features` and gets only the rows they're allowed to see; the rows for consent-withdrawn investors are filtered out before they leave the database.

The architectural insight: **all three operate at query time, not at storage time.** The data itself never gets duplicated for different roles. One Iceberg table; many views over it; many policies layered on top. That's how production data platforms scale governance.

## Step 1 — Confirm prerequisites

```sql
-- Atlas tags exist
SHOW TBLPROPERTIES argus_${STUDENT_ID}_silver.member_master ('atlas.classification');
-- (you may also need: curl Atlas API to verify)

-- Roles exist
SHOW ROLES;
-- Should list at minimum: surveillance_l1, surveillance_l2, compliance_dpo, compliance_admin, research_analyst
```

If roles don't exist:
```sql
CREATE ROLE surveillance_l1;
CREATE ROLE surveillance_l2;
CREATE ROLE compliance_dpo;
CREATE ROLE compliance_admin;
CREATE ROLE research_analyst;
```

Your user needs to have these roles granted before Step 4 — your instructor handles this typically.

## Step 2 — Add the `vw_active_investigations` view

This is the 8th view, deferred from Lab 4.1 because it depends on Ranger policies being available. The view filters to alerts with `disposition IN ('UNDER_INVESTIGATION', 'ESCALATED')`.

```sql
CREATE VIEW IF NOT EXISTS argus_${STUDENT_ID}_views.vw_active_investigations AS
SELECT
    a.alert_id,
    a.rule_id,
    a.severity,
    a.member_firm_id,
    m.member_firm_name,
    a.instrument_code,
    a.disposition,
    a.assigned_to,
    a.fired_ts,
    a.last_updated_ts,
    DATEDIFF(CURRENT_DATE, CAST(a.fired_ts AS DATE)) AS days_open
FROM argus_${STUDENT_ID}_gold.alert_candidates a
LEFT JOIN argus_${STUDENT_ID}_silver.member_master m
  ON a.member_firm_id = m.member_firm_id AND m.is_current
WHERE a.disposition IN ('UNDER_INVESTIGATION', 'ESCALATED')
;
```

Verify:
```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_active_investigations;
```
**Expected:** 0 initially (no investigations dispositioned yet); will populate as analysts work the queue.

## Step 3a — Tag-based access policy on PII_HIGH

Open the Ranger admin UI in a browser. Navigate to: **Service Manager** → **HADOOP_SQL** → your service name (typically `cm_hive` or `hive`) → **Tag-Based Policies** tab.

Create a new policy:

| Field | Value |
|---|---|
| Policy Name | `argus_${STUDENT_ID}_pii_high_block` |
| Tag | `PII_HIGH_${STUDENT_ID}` (the tag you'll apply in Lab 7.1, or a temp version for this lab) |
| Allow Conditions: | (leave empty — implicit deny) |
| Deny Conditions: |  |
| - Roles | `research_analyst` |
| - Permissions | `select` |

Save. **Wait 60 seconds** for the Ranger plugin to refresh.

> 💡 **What this policy does:** any column tagged `PII_HIGH_${STUDENT_ID}` is blocked for `research_analyst` role. The table query won't error; the column just doesn't appear in results (or returns NULL depending on Impala version).

## Step 3b — Column-masking policy on `investor_pan`

Same Ranger admin UI. Navigate to: **Service Manager** → **HADOOP_SQL** → your service → **Masking** tab.

Create a new policy:

| Field | Value |
|---|---|
| Policy Name | `argus_${STUDENT_ID}_mask_pan_for_l1` |
| Database | `argus_${STUDENT_ID}_views` |
| Table | `vw_surveillance_audit` |
| Column | `investor_pan` |
| Mask Conditions: | |
| - Roles | `surveillance_l1`, `surveillance_l2` |
| - Mask Type | `Custom` |
| - Custom Expression | `concat(substr(investor_pan, 1, 5), '****', substr(investor_pan, -1, 1))` |

Save. **Wait 60 seconds** for the plugin to refresh.

> 💡 **What this policy does:** when `surveillance_l1` or `surveillance_l2` queries `vw_surveillance_audit`, the `investor_pan` column shows a masked value like `ABCDE****F` instead of `ABCDE1234F`. The DPO (no policy applies) sees the full PAN. The mask is a SQL expression evaluated at query time; the underlying data is unchanged.

## Step 3c — Row-filter policy on `member_temporal_features`

This is the DPDP §6(4) consent enforcement. Same Ranger admin UI → **Row Level Filter** tab.

Create a new policy:

| Field | Value |
|---|---|
| Policy Name | `argus_${STUDENT_ID}_dpdp_consent_filter_features` |
| Database | `argus_${STUDENT_ID}_gold` |
| Table | `member_temporal_features` |
| Row Filter Conditions: | |
| - Roles | `research_analyst` |
| - Filter Expression | `member_firm_id IN (SELECT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current AND consent_status = 'ACTIVE' AND consent_purpose LIKE '%ANALYTICS%')` |

Save. **Wait 60 seconds** for refresh.

> 💡 **What this policy does:** when `research_analyst` queries `member_temporal_features`, the WHERE clause is silently appended. They see only rows for member firms where any tied investor has `consent_status = 'ACTIVE'` and the consent covers ANALYTICS. Consent-withdrawn investors are invisible. Module 7's CP-18 lab is what *exercises* this policy in anger.

## Step 4 — Test policies as each role

### As `surveillance_l1`

```sql
SET ROLE surveillance_l1;

SELECT alert_id, member_firm_id, investor_pan FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
LIMIT 5;
```

**Expected:** rows return, but `investor_pan` shows `ABCDE****F` style mask.

```sql
SELECT investor_email FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current LIMIT 5;
```

**Expected:** if `investor_email` is tagged `PII_HIGH`, this either errors or returns NULL (depending on Impala/Ranger version).

### As `compliance_dpo`

```sql
SET ROLE compliance_dpo;

SELECT alert_id, member_firm_id, investor_pan FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
LIMIT 5;
```

**Expected:** rows return with **full unmasked** PAN.

### As `research_analyst`

```sql
SET ROLE research_analyst;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features;
```

**Expected:** non-zero count, but **less than** what `compliance_admin` sees (the row-filter excludes consent-withdrawn investors).

```sql
SELECT investor_pan FROM argus_${STUDENT_ID}_silver.member_master LIMIT 5;
```

**Expected:** error (PII_HIGH tag-based deny) or NULL column.

## Step 5 — Verify CP-11 pass conditions

CP-11 has **three checks** — one per policy family.

### Check 1 — Column-masking applies for surveillance_l1

```sql
SET ROLE surveillance_l1;
SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
WHERE investor_pan LIKE '%****%';
```
**Pass if:** count > 0 (every PAN is masked). **Fail if:** 0 (mask not applied) — see Common Failure Mode #1.

### Check 2 — Compliance DPO bypasses mask

```sql
SET ROLE compliance_dpo;
SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
WHERE investor_pan IS NOT NULL AND investor_pan NOT LIKE '%****%';
```
**Pass if:** count > 0 (DPO sees real PANs). **Fail if:** 0 (mask is applied to DPO too — policy roles are wrong).

### Check 3 — Row-filter applies for research_analyst

First, count without filter (as admin):
```sql
SET ROLE compliance_admin;
SELECT COUNT(*) AS admin_count FROM argus_${STUDENT_ID}_gold.member_temporal_features;
```

Then count with filter (as research_analyst):
```sql
SET ROLE research_analyst;
SELECT COUNT(*) AS analyst_count FROM argus_${STUDENT_ID}_gold.member_temporal_features;
```

**Pass if:** `analyst_count < admin_count` (the filter removed some rows). **Fail if:** equal — filter not applied (Common Failure Mode #2).

---

## Common failure mode #1 — Column-mask doesn't apply

**Symptom:** as `surveillance_l1`, querying `vw_surveillance_audit.investor_pan` shows the raw PAN, not the mask.

**Cause** (in decreasing likelihood):
1. **Ranger policy cache hasn't refreshed** — wait 60 seconds and retry.
2. **You're not actually in the role** — run `SHOW CURRENT ROLES` to verify.
3. **The policy is missing the column** — check the policy's column field (`investor_pan` exactly).
4. **The view isn't using `cm_hive` service** — Ranger has multiple services; the policy's service must match the cluster's service.

**Fix sequence:**
```bash
# Force Ranger plugin refresh from admin UI: Audit → Plugins → "Refresh"
```
Then re-test. If still failing, check Ranger admin UI's "Audit" tab for your query — it shows what policy was applied (or wasn't).

## Common failure mode #2 — Row-filter not applied

**Symptom:** as `research_analyst`, the count of `member_temporal_features` matches the admin count — the filter isn't doing anything.

**Cause:** the filter expression has a SQL error (Ranger silently disables broken filter policies), or the role isn't being picked up.

**Diagnosis:**
1. In Ranger admin UI, open the row-filter policy. Click "Test Run" if available.
2. Check Ranger admin UI's "Audit" tab — your query should show the filter that was applied. If "no policy applied", the role match is failing.

**Fix:**
- Verify the filter expression syntax against your Impala version (some versions need `IN (LIST)` not subqueries).
- Try a simpler filter first: `member_firm_id IS NOT NULL` and confirm that applies. If yes, your real filter has a SQL bug.

## Common failure mode #3 — `SET ROLE` returns "user does not have role"

**Symptom:** `SET ROLE compliance_dpo` errors with "User <you> does not have role compliance_dpo".

**Cause:** your user wasn't granted the role.

**Fix:**
```sql
GRANT ROLE compliance_dpo TO USER <your-user>;
```
You typically need admin rights to do this; ask your instructor.

## Common failure mode #4 — Tag-based policy not blocking

**Symptom:** as `research_analyst`, querying `member_master.investor_email` (tagged PII_HIGH) returns the value, not NULL or error.

**Cause:** Atlas tag isn't applied to the column, or the cluster suffix in Ranger doesn't match Atlas's qualified-name suffix.

**Diagnosis:** Lab 7.1 Common Failure Mode #1 covers this in detail.

**Fix:** ensure Lab 7.1 Step 5 ran with `attached >= 6, missing = 0`. The tag-based Ranger policy can't apply if the tag itself isn't on the column.

---

## Pass condition for CP-11

All three checks pass:
- ✅ Column-mask applies for surveillance_l1 (PANs masked)
- ✅ Compliance DPO bypasses mask (DPO sees real PANs)
- ✅ Row-filter applies for research_analyst (count is reduced)

When all three pass, ARGUS has the full role-based access control that DPDP §16 demands. Module 7's compliance work (CP-18) builds on these policies; without them, CP-18 has nothing to enforce.

## Wrap-up — what you can now do that you couldn't before

You can deploy three flavors of Ranger policies (tag-based, column-mask, row-filter) and verify each applies for the right role. You understand why the three operate at query time, not storage time. You can diagnose why a Ranger policy isn't applying by checking cache, role membership, and the audit log.

**Module 4 is complete.** Module 5 next trains an ML model on the alert candidates from Module 3. Allow about 5 hours total for Module 5.
