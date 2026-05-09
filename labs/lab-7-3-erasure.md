# Lab 7.3 — DPDP §12 Erasure with Iceberg Time-Travel Proof (CP-19 — **COMPLIANCE GATE**)

> ⚠️ **THIS IS THE COMPLIANCE GATE.** CP-19 is non-negotiable: failing it means failing the capstone regardless of overall score. Read every step before running anything. The PRD treats this checkpoint as a separate pass condition because a surveillance platform that can't prove erasure cannot be deployed at any Indian financial-services customer in 2026.

> ℹ️ **Module:** 7 — SDX Governance & DPDP Compliance
> **Closes deficiency:** ARG-5 part 2 (the central erasure capability)
> **Source files:** [`src/governance/gdpr_erasure_workflow.py`](../src/governance/gdpr_erasure_workflow.py)

## Objectives

- Run the DPDP §12 erasure workflow for the 3 planted cases at indices 20–22
- Verify each erasure writes both a `ERASURE_REQUESTED` and a `ERASURE_COMPLETED` audit row
- Use Iceberg `FOR SYSTEM_VERSION AS OF` queries to prove (a) the data existed before erasure and (b) the data is gone after
- Confirm the audit trail is preserved despite operational data being erased — the entire point of `consent_audit` having `history.expire.enabled=false`
- Confirm statutory data tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}` is retained under DPDP §7 with the natural-identity link severed

## Why this matters

In a SEBI / DPB inspection, the inspector will pick a recent erasure request and ask MSE to demonstrate the erasure completed correctly. MSE must produce three artefacts: (1) the audit row showing the request was received, (2) a query showing the data existed at the request time, and (3) a query showing the data is gone now. If any of the three is missing or unreproducible, MSE has failed its DPDP §12 obligation. The Iceberg `FOR SYSTEM_VERSION AS OF` mechanism is what makes (2) cryptographically reproducible — the snapshot ID stored in `consent_audit.pre_action_snapshot` is a content-addressed reference to the table state at that exact moment.

## Pre-flight check — confirm `consent_audit` has `history.expire.enabled=false`

This is the single most critical prerequisite. If snapshot expiration is on, the Iceberg metadata for old snapshots gets garbage-collected and the time-travel proof becomes irreproducible. **Verify before going any further:**

```sql
DESCRIBE FORMATTED argus_${STUDENT_ID}_gold.consent_audit;
```

Look in the `Table Parameters:` section for the line `history.expire.enabled    false`. If you don't see it, **STOP** and re-apply the Day 1 DDL:

```sql
ALTER TABLE argus_${STUDENT_ID}_gold.consent_audit
SET TBLPROPERTIES ('history.expire.enabled' = 'false');
```

If `history.expire.enabled` is `true` or unset, re-run `sql/gold_ddl.sql` to recreate the table with the correct property — but be aware this loses any existing audit rows. In a clean lab environment that's fine; in production it would be a serious incident.

## Procedure

### Step 1 — Find the 3 planted erasure cases

```sql
SELECT investor_acct, consent_status, investor_pan_hash
FROM argus_${STUDENT_ID}_silver.member_master
WHERE consent_status = 'ERASED' AND is_current
ORDER BY investor_acct;
```

**Expected output**: 3 rows (planted cases at indices 20–22). Note their `investor_acct` values and especially their `investor_pan_hash` values — the erasure workflow keys on the hash.

If you see 0 rows, the synthetic generator's erasure consents didn't make it into Silver. Same fix as Lab 7.2: re-run `seed_member_cdc.py` then `JOB-05`.

### Step 2 — Reset the cases to ACTIVE for the test

The planted cases come pre-ERASED in the synthetic data so Module 4's CP-11 has data to work with. For Lab 7.3, the test is to *run the erasure workflow*, so reset to ACTIVE first:

```sql
UPDATE argus_${STUDENT_ID}_silver.member_master
SET consent_status = 'ACTIVE',
    consent_purpose = 'TRADING,SURVEILLANCE,ANALYTICS,MARKETING'
WHERE investor_acct IN ('INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX')
  AND is_current;
```

Capture the `investor_pan_hash` for each — you'll pass them to the workflow:

```sql
SELECT investor_acct, investor_pan_hash FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_acct IN ('INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX')
  AND is_current;
```

### Step 3 — Capture the **pre-erasure** Iceberg snapshot IDs (your evidence baseline)

```sql
-- Run this BEFORE running the erasure workflow
SELECT 'silver.member_master' AS tbl, snapshot_id, committed_at
FROM argus_${STUDENT_ID}_silver.member_master.snapshots
ORDER BY committed_at DESC LIMIT 1
UNION ALL
SELECT 'bronze.member_cdc', snapshot_id, committed_at
FROM argus_${STUDENT_ID}_bronze.member_cdc.snapshots
ORDER BY committed_at DESC LIMIT 1;
```

**Save these snapshot IDs.** You'll use them in Step 6 to prove the data existed before erasure.

### Step 4 — Run the erasure workflow for each of 3 investors

```bash
for hash in HASH_FOR_CASE_20 HASH_FOR_CASE_21 HASH_FOR_CASE_22; do
    python src/governance/gdpr_erasure_workflow.py \
        --investor-pan-hash "$hash" \
        --request-id "REQ-LAB73-$hash" \
        --requestor-channel CONSENT_MANAGER \
        --actioned-by lab_dpo_user
done
```

**Expected output per call**:

```
Erasure request REQ-LAB73-... for investor hash <prefix>...
Audit row written: AUDIT-XXXXXXXXXXXX (ERASURE_REQUESTED)
  [delete]  argus_${STUDENT_ID}_silver.member_master — 1 rows match hash
  [delete]  argus_${STUDENT_ID}_bronze.member_cdc — 1 rows match hash
Vector-store sweep removed 0 embeddings
Audit row written: AUDIT-XXXXXXXXXXXX (ERASURE_COMPLETED)
==> Erasure complete. Time-travel proof available via:
    argus_${STUDENT_ID}_silver.member_master — pre=abc123def456, post=abc123def789
    argus_${STUDENT_ID}_bronze.member_cdc — pre=..., post=...
```

### Step 5 — Verify audit rows preserved

```sql
SELECT
    audit_id,
    event_type,
    investor_pan_hash,
    request_id,
    legal_basis,
    pre_action_snapshot,
    post_action_snapshot,
    affected_row_count,
    notes
FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id LIKE 'REQ-LAB73-%'
ORDER BY event_ts;
```

**Expected output**: 6 rows total — for each of 3 investors, a `ERASURE_REQUESTED` row and a `ERASURE_COMPLETED` row. Each `ERASURE_COMPLETED` row should have:
- `legal_basis = 'ERASURE_§12'`
- non-empty `pre_action_snapshot` and `post_action_snapshot`
- `affected_row_count >= 1`
- `notes` describing what happened

### Step 6 — The CRITICAL test: time-travel proof

This is the cryptographic evidence that erasure happened. For one of the 3 investors, take their `investor_pan_hash` and the `pre_action_snapshot` from the audit row:

```sql
-- Pull the audit row's snapshot IDs for one investor
SELECT pre_action_snapshot, post_action_snapshot
FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id = 'REQ-LAB73-HASH_FOR_CASE_20'
  AND event_type = 'ERASURE_COMPLETED';
```

The `pre_action_snapshot` value is a JSON map like `{"argus_${STUDENT_ID}_silver.member_master": "1234567890123456", ...}`. Extract the value for `argus_${STUDENT_ID}_silver.member_master`. Then:

```sql
-- (a) Query the silver master AS OF the pre-erasure snapshot.
-- The investor's row should be present.
SELECT investor_acct, investor_pan_hash, consent_status
FROM argus_${STUDENT_ID}_silver.member_master FOR SYSTEM_VERSION AS OF <pre_snapshot_id>
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
-- expect: 1 row (or more, for SCD2 history rows)
```

```sql
-- (b) Query the silver master at current state.
-- The investor's row should be GONE.
SELECT investor_acct, investor_pan_hash, consent_status
FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
-- expect: 0 rows
```

```sql
-- (c) Query the consent_audit at current state.
-- The audit rows should still be there — preserved despite the operational data being erased.
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
-- expect: >= 2 (REQUESTED + COMPLETED rows)
```

If query (a) returns 1+ rows, query (b) returns 0 rows, and query (c) returns 2+ rows, **you have proven DPDP §12 compliance with cryptographic evidence**. This is what CP-19 verifies.

### Step 7 — Statutory retention check (`SEBI_AUDIT_TRAIL_${STUDENT_ID}` tables)

For the same investor hash, confirm the surveillance archive is still intact:

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.legacy_alerts
WHERE alert_id IN (
    SELECT alert_id FROM argus_${STUDENT_ID}_bronze.legacy_alerts
    WHERE member_firm_id IN (
        SELECT DISTINCT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master
            FOR SYSTEM_VERSION AS OF <pre_snapshot_id>
        WHERE investor_pan_hash = 'HASH_FOR_CASE_20'
    )
);
-- expect: > 0 — alerts retained under DPDP §7 / SEBI 8-year retention
```

The investor's natural identity (PAN, email, mobile) has been erased from the operational master, but their *behavior history* (alerts, dispositions) is retained because surveillance is a statutory legitimate use under DPDP §7. SEBI can still investigate; the investor's privacy is restored.

## Checkpoint CP-19 — DPDP §12 erasure provable via Iceberg time-travel ⚠️ **COMPLIANCE GATE**

### Pass condition

**ALL FIVE CHECKS** must pass. Failure on any single check fails the capstone overall.

### Check 1 — `consent_audit` has `history.expire.enabled = false`

```sql
SHOW TBLPROPERTIES argus_${STUDENT_ID}_gold.consent_audit ('history.expire.enabled');
-- expect: 'false'
```

### Check 2 — 6 audit rows for the 3 erasures

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id LIKE 'REQ-LAB73-%';
-- expect: 6 (3 REQUESTED + 3 COMPLETED)
```

### Check 3 — Pre-snapshot query returns the investor's row

For each of the 3 erased investors, the Step 6(a) query returns ≥ 1 row.

### Check 4 — Post-erasure (current) query returns 0 rows

For each of the 3 erased investors, the Step 6(b) query returns 0 rows.

### Check 5 — Audit row preserved

For each of the 3 erased investors, the Step 6(c) query returns ≥ 2 rows.

If any of Checks 3, 4, or 5 fails for any of the 3 investors, the COMPLIANCE GATE fails.

---

## Common failure mode — query (a) `FOR SYSTEM_VERSION AS OF` returns "snapshot not found"

**Symptom**: the time-travel query in Step 6(a) fails with `Cannot find snapshot with id <X>` or returns 0 rows even though the investor was definitely there before erasure.

**Diagnosis**: the snapshot has been **expired**. By default Iceberg expires snapshots older than 5 days. If you ran the erasure 6+ days ago and tried to time-travel today, the metadata is gone.

For `argus_${STUDENT_ID}_gold.consent_audit` this can never happen because Day 1's DDL sets `history.expire.enabled=false`. But for `argus_${STUDENT_ID}_silver.member_master` and `argus_${STUDENT_ID}_bronze.member_cdc`, the default 5-day expiration applies. The audit row stores the snapshot ID, but if the underlying table's metadata has been expired, the snapshot is unreachable.

This is a real production concern: in a SEBI inspection 6 months after an erasure, MSE needs the snapshots to still be queryable. The fix is to extend snapshot retention on the operational tables:

**Fix**:

```sql
ALTER TABLE argus_${STUDENT_ID}_silver.member_master
SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '94608000000',  -- 3 years
    'history.expire.min-snapshots-to-keep' = '100'
);

ALTER TABLE argus_${STUDENT_ID}_bronze.member_cdc
SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '94608000000',
    'history.expire.min-snapshots-to-keep' = '100'
);
```

For the lab, if you hit this within the 5-day default and the snapshot is gone: re-run from Step 2 (reset the planted cases to ACTIVE) and execute the workflow fresh — you'll get new snapshot IDs that are current.

This failure mode is the single most common reason CP-19 gets cited in real DPDP inspections, so Lab 7.3 is calibrated to catch it. In production deployments, the storage cost of 3-year snapshot retention on PII tables is non-negligible but is the price of evidentiary-grade erasure.

---

## Pass condition for CP-19

All 5 checks pass for all 3 erased investors. When this passes, MSE can defend its DPDP §12 obligation in any DPB or SEBI inspection — with cryptographic-grade Iceberg snapshots as evidence. The capstone's most consequential checkpoint is closed.

When CP-19 passes, ARGUS is done.
