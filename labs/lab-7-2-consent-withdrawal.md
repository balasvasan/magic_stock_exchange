# Lab 7.2 — DPDP §6(4) Consent Withdrawal (CP-18)

> ℹ️ **Module:** 7 — SDX Governance & DPDP Compliance
> **Closes deficiency:** ARG-5 part 2 (consent enforcement at query time)
> **Source files:** [`src/governance/ccpa_optout_enforcement.py`](../src/governance/ccpa_optout_enforcement.py)

## Objectives

- Run consent-withdrawal workflow for the 5 planted cases at indices 15–19
- Verify each withdrawal writes a `CONSENT_WITHDRAWN` audit row with pre/post Iceberg snapshots
- Confirm `argus_${STUDENT_ID}_silver.member_master.consent_status` updates to `WITHDRAWN` for the affected investors
- Verify Ranger row-filter `dpdp_consent_filter` (deployed in Module 4) now excludes those investors from non-statutory queries
- Confirm DPO bypass — `vw_surveillance_audit` continues to show those investors under DPDP §7

## Why this matters

The five planted cases are MSE's smallest, easiest test of DPDP compliance. If the platform can't correctly process five consent withdrawals end-to-end — script execution, audit row, master-table update, Ranger enforcement, DPO bypass — it can't process the thousands of withdrawals that DPDP-compliant operations will require monthly. Lab 7.2 isn't proving the workflow scales; it's proving the workflow exists.

## Procedure

### Step 1 — Find the planted withdrawal cases

```sql
SELECT investor_acct, consent_status, consent_purpose, investor_pan_hash
FROM argus_${STUDENT_ID}_silver.member_master
WHERE consent_status = 'WITHDRAWN'
  AND is_current
ORDER BY investor_acct
LIMIT 10;
```

Expected output: 5 rows. Each is one of the planted cases at indices 15–19 (per `data/generated/compliance_test_cases.csv`). Note the `investor_acct` values; you'll use them in Step 2.

If you see 0 rows, the synthetic generator's consent records didn't make it into Silver. Re-check Lab 2.2 / JOB-05 — the SCD2 master needs the consent fields populated from `consent_records.csv`. (The seed script at `src/ingest/seed_member_cdc.py` does this; if you skipped it, run it now.)

### Step 2 — Reset their consent to ACTIVE for the test

The planted cases come pre-WITHDRAWN in the synthetic data — that's so Module 4's CP-11 has data to work with. For Lab 7.2, the test is to *run the withdrawal workflow*, so we'll temporarily reset to ACTIVE, then withdraw fresh:

```sql
UPDATE argus_${STUDENT_ID}_silver.member_master
SET consent_status = 'ACTIVE',
    consent_purpose = 'TRADING,SURVEILLANCE,ANALYTICS,MARKETING'
WHERE investor_acct IN (
    -- replace with the 5 actual values from Step 1
    'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
) AND is_current;
```

### Step 3 — Run the workflow for each of the 5 investors

```bash
for acct in INV-XXXXXXXX INV-XXXXXXXX INV-XXXXXXXX INV-XXXXXXXX INV-XXXXXXXX; do
    python src/governance/ccpa_optout_enforcement.py \
        --investor-acct "$acct" \
        --withdraw-purposes "ANALYTICS,MARKETING" \
        --request-id "REQ-LAB72-$acct" \
        --requestor-channel CONSENT_MANAGER
done
```

**Expected output per call**:

```
Consent withdrawal request REQ-LAB72-INV-XXXXXXXX for investor INV-XXXXXXXX
==> Consent updated. Audit row: AUDIT-XXXXXXXXXXXX
    Status:    WITHDRAWN
    Purposes:  TRADING,SURVEILLANCE
    Snapshots: pre=abc123def456 post=abc123def789
    Ranger row-filter dpdp_consent_filter applies on next query.
```

### Step 4 — Verify audit rows

```sql
SELECT
    audit_id,
    event_type,
    investor_acct,
    consent_purpose,
    pre_action_snapshot,
    post_action_snapshot,
    notes
FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE event_type = 'CONSENT_WITHDRAWN'
  AND request_id LIKE 'REQ-LAB72-%'
ORDER BY event_ts DESC;
```

**Expected output**: 5 rows. Each row has:
- `consent_purpose` containing `ANALYTICS` and/or `MARKETING` (the withdrawn purposes)
- non-empty `pre_action_snapshot` and `post_action_snapshot` (Iceberg snapshot IDs)
- distinct snapshot IDs between pre and post (proves the master table changed)

### Step 5 — Verify master table state

```sql
SELECT investor_acct, consent_status, consent_purpose
FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_acct IN (-- the 5 values --)
  AND is_current;
```

**Expected output**: 5 rows with `consent_status = 'WITHDRAWN'` and `consent_purpose = 'TRADING,SURVEILLANCE'` (statutory purposes retained, optional purposes withdrawn).

### Step 6 — Verify Ranger filter applied (research_analyst role)

```sql
SET ROLE research_analyst;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features f
WHERE f.member_firm_id IN (
    SELECT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master
    WHERE investor_acct IN (-- the 5 values --)
);
```

**Expected output**: 0. The `dpdp_consent_filter_features` Ranger policy (Module 4) joins through `consent_status = 'ACTIVE' AND consent_purpose LIKE '%ANALYTICS%'`, which now excludes these investors.

If you see > 0, the Ranger policy isn't refreshing — wait 60 seconds (policy cache TTL) and retry, or force a Ranger plugin refresh from the admin UI.

### Step 7 — Verify DPO bypass via `vw_surveillance_audit`

```sql
SET ROLE compliance_dpo;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
WHERE investor_acct IN (-- the 5 values --);
```

**Expected output**: > 0. The `vw_surveillance_audit` view is tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}`, which bypasses the consent filter under DPDP §7 statutory exception. The DPO can still see these investors for surveillance purposes, even though their analytics consent is withdrawn.

## Checkpoint CP-18 — Cases 15–19 filtered for non-statutory; visible to DPO

### Pass condition

All five checks pass.

### Check 1 — 5 audit rows written

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE event_type = 'CONSENT_WITHDRAWN' AND request_id LIKE 'REQ-LAB72-%';
-- expect: 5
```

### Check 2 — Master table updated

The Step 5 query returns 5 rows with `consent_status = 'WITHDRAWN'`.

### Check 3 — Pre/post snapshots distinct

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id LIKE 'REQ-LAB72-%'
  AND pre_action_snapshot != post_action_snapshot
  AND pre_action_snapshot IS NOT NULL
  AND post_action_snapshot IS NOT NULL;
-- expect: 5
```

### Check 4 — research_analyst sees 0 from these investors

The Step 6 query (with `SET ROLE research_analyst`) returns 0.

### Check 5 — compliance_dpo sees > 0 via `vw_surveillance_audit`

The Step 7 query (with `SET ROLE compliance_dpo`) returns > 0.

---

## Common failure mode — Step 6 returns rows even after withdrawal

**Symptom**: as `research_analyst`, querying `argus_${STUDENT_ID}_gold.member_temporal_features` for the withdrawn investors returns rows when it should return 0.

**Diagnosis**: three possibilities, in decreasing likelihood:

1. **Ranger policy cache hasn't refreshed.** The Hive Ranger plugin caches policies for 30–60 seconds. If you ran the withdrawal and immediately queried, the cached policy still allows the rows. **Wait 60 seconds and retry.**
2. **The Ranger filter targets the wrong column.** The `dpdp_consent_filter_features` policy joins through `member_firm_id`. If the investor's withdrawal didn't propagate to a `consent_status = WITHDRAWN` flag on their member firm row, the filter doesn't bite. Check `argus_${STUDENT_ID}_silver.member_master` for the investor — they should have a row with their own `investor_acct` set, distinct from the member firm's master row.
3. **The role's grants include a `BYPASS` flag.** Run `SHOW CURRENT ROLE` and confirm `research_analyst` is the active role; if it's not, the underlying user might have admin grants that override the row filter.

**Fix sequence**:

```bash
# Force policy refresh
ranger-admin --refresh-policies --service cm_hive

# Confirm role + filter
SET ROLE research_analyst;
SELECT current_user(), current_role();
SELECT consent_status, consent_purpose FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_acct = 'INV-XXXXXXXX' AND is_current;
```

If `consent_status` is still `ACTIVE`, the workflow didn't update Silver — re-run `ccpa_optout_enforcement.py` for that investor.

---

## Pass condition for CP-18

All five checks pass. With consent enforcement working at query time, MSE has the operational mechanism that DPDP §6(4) requires. The 5 planted cases prove the workflow; production scale just runs the same workflow more often.
