# Lab 4.2 — Ranger Policies (CP-11)

> ℹ️ **Module:** 4 — Governed Views in CDW
> **Closes deficiency:** ARG-5 part 1 (DPDP §6(4) consent enforcement, role-based PII access)
> **Source files:** [`sql/ranger_policies.sql`](../sql/ranger_policies.sql)

## Objectives

- Deploy the three Ranger policy families (consent row filter, PII column mask, time-bound investigation access)
- Verify the same view returns different effective shapes for different roles
- Confirm the planted DPDP §6(4) consent-withdrawal cases (15–19) are filtered for non-statutory roles but visible to the DPO

## Why this matters

This lab is the technical demonstration of DPDP compliance. The five planted consent-withdrawal cases (indices 15–19) are investors who have withdrawn analytics consent under DPDP §6(4). When the surveillance team runs daily analytics rollups, those investors must be excluded — but when the DPO opens a SEBI inquiry, those same investors must be visible because surveillance retention is statutory under DPDP §7. Both behaviors must work, automatically, from the same data, with the same view definitions, evaluated at query time.

## Procedure

### Step 1 — Deploy the policies

Two paths:

**Path A — CDP 7.3+ with Hive Ranger plugin SQL extensions:**

```bash
envsubst < sql/ranger_policies.sql | hive -f -
```

**Path B — Older CDP, manual via Ranger UI:**

Open the Ranger UI → Service Manager → `cm_hive` → Add Policy. For each policy block in `sql/ranger_policies.sql`, paste the policy fields into the corresponding form. There are 9 individual policies to enter (3 families × multiple table targets); the Ranger UI's "Save Policy" workflow takes ~2 minutes per policy.

After deployment, verify in the Ranger UI that all 9 policies show up under `cm_hive` for the `argus_*` databases.

### Step 2 — Set up test roles in Hue / Beeline

Confirm the four roles exist:

```sql
SHOW ROLES;
-- expect at minimum: surveillance_analyst, compliance_dpo, research_analyst, investigation_lead
```

If any are missing, they need to be created by your CDP administrator. The role definitions and grants are environment-specific and outside the scope of this lab.

### Step 3 — Test PII column mask as `surveillance_analyst`

```sql
-- Switch to surveillance_analyst role
SET ROLE surveillance_analyst;

-- Pull a few investor rows from the master
SELECT
    investor_acct,
    investor_pan,         -- should be masked
    investor_email,       -- should be masked
    investor_mobile       -- should be masked
FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current AND investor_acct IS NOT NULL
LIMIT 5;
```

**Expected output**: 5 rows where:

| Column | Expected value |
|---|---|
| `investor_pan` | `XXXXX****X` (every row) |
| `investor_email` | `redacted@example.in` (or similar — local-part redacted, domain preserved) |
| `investor_mobile` | `+91-XXXX-XXXXXX` |

If any of these columns shows the actual value, the column-mask policy isn't applied — the most common cause is the Ranger plugin needs to refresh policies (it caches for a few minutes by default). Wait 5 minutes and retry, or force a policy refresh from the Ranger UI.

### Step 4 — Test PII column mask as `compliance_dpo`

```sql
SET ROLE compliance_dpo;

SELECT
    investor_acct,
    investor_pan,
    investor_email,
    investor_mobile
FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current AND investor_acct IS NOT NULL
LIMIT 5;
```

**Expected output**: 5 rows showing **actual PAN values** (e.g. `ABCDE1234F`), real emails, and real mobile numbers. The compliance_dpo role bypasses every column-mask policy.

This is the privileged DPO tier under DPDP §16 (Significant Data Fiduciary). Access here should be auditable but not technically blocked.

### Step 5 — Test DPDP §6(4) consent row filter

The five planted consent-withdrawal cases (indices 15–19) are at investor accounts listed in `data/generated/compliance_test_cases.csv`. Verify they're filtered correctly:

```sql
-- Find the planted withdrawn investor account IDs
SELECT investor_acct, consent_status FROM argus_${STUDENT_ID}_silver.member_master
WHERE consent_status = 'WITHDRAWN' AND is_current
ORDER BY investor_acct LIMIT 10;
```

**Expected output**: 5 rows with `consent_status = 'WITHDRAWN'`. Note the `investor_acct` values; you'll use them next.

#### As `research_analyst` — should NOT see withdrawn investors

```sql
SET ROLE research_analyst;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features f
WHERE f.member_firm_id IN (
    SELECT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master
    WHERE consent_status = 'WITHDRAWN' AND is_current
);
-- expect: 0  (consent filter excludes withdrawn investors)
```

The Ranger row-filter policy `dpdp_consent_filter_features` adds an implicit `WHERE consent_purpose LIKE '%ANALYTICS%'` clause to every query the research_analyst runs against `member_temporal_features`. Withdrawn investors lack `ANALYTICS` in their consent purpose, so they're filtered.

#### As `compliance_dpo` — SHOULD see withdrawn investors (statutory)

```sql
SET ROLE compliance_dpo;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
WHERE investor_acct IN (
    SELECT investor_acct FROM argus_${STUDENT_ID}_silver.member_master
    WHERE consent_status = 'WITHDRAWN' AND is_current
);
-- expect: > 0 (statutory access under DPDP §7)
```

The `vw_surveillance_audit` view is tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}` which causes the Ranger policy to bypass the consent filter — DPDP §7 legitimate-use exception covers statutory market surveillance.

### Step 6 — Test time-bound investigation access

```sql
SET ROLE investigation_lead;

-- Without an active investigation, the lead can see no alert rows:
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates WHERE trade_date >= CURRENT_DATE - 5;
-- expect: 0 unless there's a row in vw_active_investigations matching member_firm_id
```

If `vw_active_investigations` has rows, the count will reflect alerts for those open cases. If it's 0, the time-bound filter blocks all access — which is correct. The investigation_lead role is least-privilege by default, gaining access only during active matters.

## Checkpoint CP-11 — Three Ranger policy families enforced correctly

### Pass condition

All five checks pass.

### Check 1 — `surveillance_analyst` sees masked PAN

The query in Step 3 returns 5 rows; every row's `investor_pan` value is `XXXXX****X`.

### Check 2 — `compliance_dpo` sees full PAN

The query in Step 4 returns 5 rows; every row's `investor_pan` value matches the expected 10-character PAN format `[A-Z]{5}[0-9]{4}[A-Z]`.

### Check 3 — `research_analyst` filtered from consent-withdrawn investors

The first query in Step 5 returns **0** when the research_analyst role is active. Cases 15–19 are correctly hidden from analytics processing.

### Check 4 — `compliance_dpo` sees consent-withdrawn investors via `vw_surveillance_audit`

The second query in Step 5 returns **> 0** when the compliance_dpo role is active. Cases 15–19 remain visible for statutory surveillance under DPDP §7.

### Check 5 — `investigation_lead` is restricted by `vw_active_investigations`

Step 6's query returns rows only for member firms that have an active investigation case open. If `vw_active_investigations` is empty, the count is 0; if it has rows, the count reflects the corresponding alert subset.

---

## Common failure mode — column-mask policy doesn't apply

**Symptom**: as `surveillance_analyst`, the PAN column shows the actual value, not `XXXXX****X`.

**Diagnosis**: three possibilities, in order of likelihood:

1. **Ranger plugin policy cache is stale.** Ranger caches policies for 30 seconds by default; if you deployed the policy and ran the query immediately, the plugin hasn't picked it up. Wait one minute and retry.
2. **The policy targets the wrong column path.** Ranger column policies are keyed by `{database}.{table}.{column}`. If you put `argus_${STUDENT_ID}_bronze.member_cdc.investor_pan` instead of `argus_${STUDENT_ID}_silver.member_master.investor_pan`, the Silver-layer query bypasses the mask.
3. **The role assignment didn't take effect.** Hive in CDP requires `SET ROLE` to be issued in the same session as the query. If your client opens a fresh session per query (some BI tools do), the SET ROLE is dropped. Use a sticky session or set the default role for the user.

**Fix sequence**:

```sql
-- Force a policy refresh from the Ranger CLI
-- (run as Ranger admin, not as the test role)
ranger-admin --refresh-policies --service cm_hive

-- Then in the test session:
SET ROLE surveillance_analyst;
SELECT current_user(), current_role();   -- confirms identity + role
SELECT investor_pan FROM argus_${STUDENT_ID}_silver.member_master LIMIT 1;
```

If after all three the mask still doesn't apply, the policy was probably deployed against the wrong service in the Ranger UI (e.g. `cm_kafka` instead of `cm_hive`). Re-check in the Ranger UI.

---

## Pass condition for CP-11

All five checks pass. The DPDP §6(4) consent regime is enforced at the query layer, automatically, with no analyst intervention required. Cases 15–19 are filtered from non-statutory queries and visible to the DPO. The platform passes the technical bar of DPDP compliance — the statutory bar (audit trail, erasure capability) is what Module 7 closes.
