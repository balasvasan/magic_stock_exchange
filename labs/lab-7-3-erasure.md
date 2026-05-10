# Lab 7.3 — DPDP §12 Erasure with Iceberg Time-Travel Proof (CP-19 — **COMPLIANCE GATE**)

> ⚠️ **THIS IS THE COMPLIANCE GATE.** CP-19 is non-negotiable: failing it means failing the capstone regardless of overall score. Read every step before running anything. The PRD treats this checkpoint as a separate pass condition because a surveillance platform that can't prove erasure cannot be deployed at any Indian financial-services customer in 2026.

> 👋 **Module 7 first-timer?** STOP. Read [`docs/module-7-primer.md`](../docs/module-7-primer.md) before this lab. It explains DPDP §12, Iceberg time-travel as proof mechanism, and the difference between filtering (CP-18) and deletion (CP-19). About 25 minutes — and absolutely required for this lab.

> ℹ️ **Module:** 7 — SDX Governance & DPDP Compliance
> **Closes deficiency:** ARG-5 part 2 (the central erasure capability)
> **Time:** ~75–90 minutes if pre-flight is clean and snapshots aren't expired; up to 4 hours if you have to recover from snapshot expiration mid-test.
> **Source files:** [`src/governance/gdpr_erasure_workflow.py`](../src/governance/gdpr_erasure_workflow.py) — file is named `gdpr_*` for legacy reasons; its actual logic implements DPDP §12.

## What you're going to do — read this entire section before running anything

The CP-19 proof has a specific shape. Understanding the shape *first* prevents the most common failure (running the workflow before snapshots are captured properly). Here's the logic:

**The DPDP §12 proof model:**
1. **Before erasure**: snapshot N. The investor's PII is in `silver.member_master` and `bronze.member_cdc`.
2. **Capture snapshot N's ID** — write it down. This is your *evidence baseline*.
3. **Execute erasure** — the workflow deletes rows AND captures snapshot N+1's ID into the audit row.
4. **After erasure**: query `FOR SYSTEM_VERSION AS OF N` and see the data was there.
5. **Now**: query the table with no time-travel and confirm the data is gone.
6. **The audit table itself**: still has the records of what happened (because `history.expire.enabled=false`).

The "proof" is the diff between snapshot N and current state. The snapshot ID is recorded immutably in the audit row, so anyone — regulator, judge, future MSE compliance team — can replay the proof from the audit row alone.

**Steps you'll execute, in order:**

1. **Pre-flight check** — verify `consent_audit` has snapshot expiration disabled. **If this is wrong, the entire proof fails later.** (~10 min)
2. **Find the 3 planted erasure cases** in `silver.member_master`. (~5 min)
3. **Reset them to ACTIVE** so the workflow can run fresh. (~3 min)
4. **Capture pre-erasure snapshot IDs** — your evidence baseline. (~5 min)
5. **Run the erasure workflow** for each of 3 investors. (~15 min)
6. **Verify audit rows** are preserved (request + complete pair per investor). (~5 min)
7. **The CRITICAL test — time-travel proof.** For each erased investor, demonstrate the data was there (query AS OF pre-snapshot) AND is now gone (query current). (~25 min)
8. **Statutory retention check** — confirm `legacy_alerts` (SEBI 8-year retention) is intact. (~5 min)
9. **Verify CP-19 pass conditions** — five named checks, all of which must pass for the capstone to succeed. (~5 min)

Total: about 75–90 minutes. **Don't rush; this is the lab where students most often realize at minute 70 that they made a mistake at minute 5 and have to start over.** The pre-flight is what prevents that.

## Before you begin — prerequisite checklist

- [ ] [Lab 7.1](lab-7-1-atlas-classifications.md) is complete and CP-17 passed
- [ ] [Lab 7.2](lab-7-2-consent-withdrawal.md) is complete and CP-18 passed
- [ ] You have access to `silver.member_master`, `bronze.member_cdc`, `gold.consent_audit`, `bronze.legacy_alerts`
- [ ] You can execute Iceberg time-travel queries (`FOR SYSTEM_VERSION AS OF`) — quick check: `SELECT * FROM argus_${STUDENT_ID}_gold.consent_audit FOR SYSTEM_VERSION AS OF (SELECT MAX(snapshot_id) FROM argus_${STUDENT_ID}_gold.consent_audit.snapshots) LIMIT 1` should run without error
- [ ] You have credentials to run `gdpr_erasure_workflow.py` (this script does DELETE operations against Bronze and Silver — your user needs DELETE privilege on those tables)
- [ ] **A clear 90-minute window** — the lab works best end-to-end without interruptions, since pre/post snapshot timing matters

## Pre-flight check — confirm `consent_audit` has `history.expire.enabled=false`

**This is the single most critical prerequisite of the entire capstone.** If snapshot expiration is on, your time-travel proof becomes irreproducible the moment the snapshot expires (default: 5 days). When CP-19 is audited a year later, the data is gone but the *proof* is also gone — that's worse than the original violation, because you can't even demonstrate good-faith effort.

```sql
DESCRIBE FORMATTED argus_${STUDENT_ID}_gold.consent_audit;
```

Look in the `Table Parameters:` section. You should see:
```
history.expire.enabled    false
```

> 💡 **What `history.expire.enabled=false` does:** it tells Iceberg's snapshot expiration job to skip this table. Snapshots accumulate forever. For most tables this is wasteful (snapshot metadata grows over time) but for the audit table, it's required — the snapshots ARE the audit evidence.

If you don't see this property, **STOP**. Apply it now:

```sql
ALTER TABLE argus_${STUDENT_ID}_gold.consent_audit
SET TBLPROPERTIES ('history.expire.enabled' = 'false');
```

Then re-run the DESCRIBE FORMATTED and confirm the property is present.

> ⚠️ **If `history.expire.enabled` was `true` and snapshots have already expired**, your previous audit rows from Lab 7.2 may have unreachable snapshot IDs. Lab 7.2's CP-18 may show as passed but the proof is fragile. For the lab, this is recoverable — re-run Lab 7.2 fresh after fixing the property. In production, this would be a serious incident requiring a remediation report to the DPB.

## Step 1 — Find the 3 planted erasure cases

The synthetic data plants 3 erasure scenarios at planted-case indices 20–22.

```sql
SELECT investor_acct, consent_status, investor_pan_hash
FROM argus_${STUDENT_ID}_silver.member_master
WHERE consent_status = 'ERASED' AND is_current
ORDER BY investor_acct;
```

**Expected output:** exactly 3 rows.

Note three things for each row:
- `investor_acct` — the investor account ID
- `investor_pan_hash` — a SHA-256 hash of the PAN, which the workflow keys on (we don't pass the raw PAN around; we pass its hash)

If you see **0 rows**, the synthetic data's erasure consents didn't make it into Silver. Same fix as Lab 7.2: re-run `seed_member_cdc.py` then JOB-05.

> 💡 **Why does the workflow key on the hash, not the PAN?** Two reasons. (1) The PAN is a high-sensitivity PII; we minimize where it appears in code paths and logs. (2) The hash is what the audit table stores — using the hash as the key means the audit is reproducible even after the PAN is erased. After erasure, the audit row says "we erased the user with PAN-hash X" but doesn't reveal the PAN itself. That's the right privacy property.

## Step 2 — Reset the cases to ACTIVE for the test

The planted cases come pre-ERASED in the synthetic data so Module 4's CP-11 has data to work with. For Lab 7.3, the test is to *run the erasure workflow*, so we reset to ACTIVE first:

```sql
UPDATE argus_${STUDENT_ID}_silver.member_master
SET consent_status = 'ACTIVE',
    consent_purpose = 'TRADING,SURVEILLANCE,ANALYTICS,MARKETING'
WHERE investor_acct IN (
    -- Replace with the 3 actual values from Step 1:
    'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
)
  AND is_current;
```

Then capture the `investor_pan_hash` for each — you'll pass them to the workflow:

```sql
SELECT investor_acct, investor_pan_hash
FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_acct IN (
    'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
)
  AND is_current;
```

Save these 3 hash values. They look like 64-character hex strings.

## Step 3 — Capture the **pre-erasure** Iceberg snapshot IDs (your evidence baseline)

This is critical. Run this query **BEFORE** running the erasure workflow:

```sql
-- Most-recent snapshot of each table that will be modified
SELECT 'silver.member_master' AS tbl, snapshot_id, committed_at
FROM argus_${STUDENT_ID}_silver.member_master.snapshots
ORDER BY committed_at DESC LIMIT 1
UNION ALL
SELECT 'bronze.member_cdc', snapshot_id, committed_at
FROM argus_${STUDENT_ID}_bronze.member_cdc.snapshots
ORDER BY committed_at DESC LIMIT 1;
```

**Save these snapshot IDs.** Write them down. Copy them to a scratch file. You'll need them in Step 7.

> 💡 **Why save them now and not later?** The audit table will store snapshot IDs from the workflow's perspective, but capturing them yourself before running gives you an independent baseline. If the workflow has a bug and reports incorrect snapshot IDs, you have ground truth.

## Step 4 — Run the erasure workflow for each of 3 investors

```bash
for hash in HASH_FOR_CASE_20 HASH_FOR_CASE_21 HASH_FOR_CASE_22; do
    python src/governance/gdpr_erasure_workflow.py \
        --investor-pan-hash "$hash" \
        --request-id "REQ-LAB73-$hash" \
        --requestor-channel CONSENT_MANAGER \
        --actioned-by lab_dpo_user
done
```

(Substitute the actual 64-char hash values from Step 2 for `HASH_FOR_CASE_20`, etc.)

**Expected output per call:**

```
Erasure request REQ-LAB73-... for investor hash <prefix>...
Audit row written: AUDIT-XXXXXXXXXXXX (ERASURE_REQUESTED)
  [delete]  argus_s001_silver.member_master — 1 rows match hash
  [delete]  argus_s001_bronze.member_cdc — 1 rows match hash
Vector-store sweep removed 0 embeddings
Audit row written: AUDIT-XXXXXXXXXXXX (ERASURE_COMPLETED)
==> Erasure complete. Time-travel proof available via:
    argus_s001_silver.member_master — pre=abc123def456, post=abc123def789
    argus_s001_bronze.member_cdc — pre=..., post=...
```

> 💡 **What just happened, in detail:**
> 1. The workflow wrote an `ERASURE_REQUESTED` audit row to `gold.consent_audit` *before* deleting anything. This is intentional — if the workflow crashes mid-way, the request is still recorded.
> 2. It captured the *current* (pre-erasure) snapshot ID of each table it's about to modify.
> 3. It executed `DELETE FROM ... WHERE investor_pan_hash = ...` on each operational table.
> 4. After all deletes succeeded, it wrote an `ERASURE_COMPLETED` audit row containing both `pre_action_snapshot` and `post_action_snapshot` IDs.
> 5. The `pre_action_snapshot` field is a JSON map: `{"silver.member_master": "<id>", "bronze.member_cdc": "<id>"}`. This is your time-travel handle for each affected table.

> ⚠️ **If the workflow exits with an error mid-run**, see Common Failure Mode #2. **Do not** re-run blindly — the partial state may already have an `ERASURE_REQUESTED` row but no `ERASURE_COMPLETED`, and re-running could create duplicate request rows.

## Step 5 — Verify audit rows preserved

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

**Expected output:** **6 rows total** — for each of the 3 investors:
- One `ERASURE_REQUESTED` row (no snapshot fields populated)
- One `ERASURE_COMPLETED` row with all three of: `pre_action_snapshot`, `post_action_snapshot`, `affected_row_count` populated

Each `ERASURE_COMPLETED` row should have:
- `legal_basis = 'ERASURE_§12'`
- non-empty `pre_action_snapshot` (a JSON map)
- non-empty `post_action_snapshot` (a JSON map)
- `affected_row_count >= 1`

If you only see 3 rows (just the COMPLETED, no REQUESTED), or 3 rows of just REQUESTED with no COMPLETED — the workflow had a failure. See Common Failure Mode #2.

## Step 6 — Cross-check the audit's snapshot IDs against your baseline from Step 3

This is a sanity check — the snapshots the workflow recorded should match the baseline you captured in Step 3.

```sql
-- For one investor, extract the audit's pre-snapshot for silver.member_master
SELECT
    request_id,
    json_extract_scalar(pre_action_snapshot, '$.silver.member_master') AS audit_pre_snap
FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id = 'REQ-LAB73-HASH_FOR_CASE_20'
  AND event_type = 'ERASURE_COMPLETED';
```

(Adjust the JSON path syntax if your engine doesn't support `json_extract_scalar` — Impala uses `get_json_object`.)

**Expected outcome:** the `audit_pre_snap` value should match the `silver.member_master` snapshot ID you captured in Step 3 (or be very close — if any other write happened between Step 3 and Step 4, there might be a snapshot in between).

If they don't match at all, an unexpected write happened (e.g., a stale Spark Streaming job is still updating `silver.member_master`). Investigate; the audit's snapshot is canonical for proof, but a mismatch is a sign something else is going on.

## Step 7 — The CRITICAL test: time-travel proof

This is **the cryptographic evidence** that erasure happened correctly. CP-19 succeeds or fails on this step. For one of the 3 investors, take their `investor_pan_hash` and the `pre_action_snapshot` from the audit row.

### 7(a) — Confirm the data WAS there at the pre-erasure snapshot

```sql
-- Pull the audit row's snapshot IDs
SELECT pre_action_snapshot, post_action_snapshot
FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id = 'REQ-LAB73-HASH_FOR_CASE_20'
  AND event_type = 'ERASURE_COMPLETED';
```

The `pre_action_snapshot` value is a JSON map. Extract the `silver.member_master` value — call it `<pre_snap>`.

```sql
-- Query silver.member_master AS OF the pre-erasure snapshot
SELECT investor_acct, investor_pan_hash, consent_status
FROM argus_${STUDENT_ID}_silver.member_master FOR SYSTEM_VERSION AS OF <pre_snap>
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
```

**Expected output:** ≥ 1 row (one or more SCD2 history rows for the investor — likely 1 with `is_current = TRUE`, possibly more if the investor's profile has been updated before).

> 💡 **Reading FOR SYSTEM_VERSION AS OF:** this is Iceberg time-travel. It says "treat the table as if it were exactly the state at snapshot `<pre_snap>`". Iceberg looks up that snapshot's manifest, reads the data files referenced by it, and applies any delete files relative to that snapshot. The result is the table as it appeared at that point in time. **This works because the snapshot wasn't expired** — that's why the pre-flight matters so much.

If this returns **0 rows**, see Common Failure Mode #1 — the snapshot is gone or the wrong ID was captured.

### 7(b) — Confirm the data is GONE at current state

```sql
-- Query silver.member_master at current state (no FOR SYSTEM_VERSION)
SELECT investor_acct, investor_pan_hash, consent_status
FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
```

**Expected output:** **0 rows**.

If you see > 0 rows, the workflow's DELETE didn't actually delete the row, or your hash is wrong. Re-check: `SELECT investor_pan_hash FROM ... WHERE investor_acct = 'INV-XXXXXXXX'` to make sure you're using the right hash for the right investor.

### 7(c) — Confirm the audit row is preserved

```sql
-- Query the consent_audit at current state
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
```

**Expected output:** ≥ 2 (the ERASURE_REQUESTED + ERASURE_COMPLETED rows for this investor).

> 💡 **Why is this a separate check?** Because in a naive implementation, "DELETE the user's data" would include the audit row, breaking the proof. The audit table is structured to keep its own records intact even when the user's operational data is erased. The `investor_pan_hash` field in `consent_audit` is *not* PII (it's a one-way hash), so retaining it doesn't violate §12.

If this returns 0, the workflow is wrong — it deleted the audit row alongside the user data. Flag to your instructor; this is a fundamental correctness issue.

### Repeat 7(a), 7(b), 7(c) for the other 2 investors

You need the proof to hold for **all 3 investors**. CP-19 fails if any one of them has a broken proof.

**If 7(a) returns ≥ 1, 7(b) returns 0, and 7(c) returns ≥ 2 for all 3 investors, you have proven DPDP §12 compliance with cryptographic-grade evidence.** That's what CP-19 verifies.

## Step 8 — Statutory retention check (`SEBI_AUDIT_TRAIL` tables)

For the same investor hash, confirm the surveillance archive is still intact — DPDP §7 statutory exception under SEBI's 8-year retention.

First, get the investor's member_firm_id from the pre-erasure snapshot:

```sql
SELECT DISTINCT member_firm_id
FROM argus_${STUDENT_ID}_silver.member_master FOR SYSTEM_VERSION AS OF <pre_snap>
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
```

Then check legacy_alerts for that firm:

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.legacy_alerts
WHERE member_firm_id = 'FIRM_VALUE_FROM_ABOVE';
```

**Expected output:** > 0 (alerts retained under DPDP §7 / SEBI 8-year retention).

> 💡 **What's happening here:** the investor's natural identity (PAN, email, mobile) has been erased from the operational master. Their *behavior history* (which trades, which alerts) is retained because surveillance is a statutory legitimate use. SEBI can still investigate the firm's alerts; the investor's individual privacy is restored *to the extent §12 requires*. This is what "erasure with statutory carve-out" looks like in practice.

## Step 9 — Verify CP-19 pass conditions

CP-19 has **5 checks** and **all must pass**, **for all 3 investors**. There is no partial credit.

### Check 1 — `consent_audit` has `history.expire.enabled = false`

```sql
SHOW TBLPROPERTIES argus_${STUDENT_ID}_gold.consent_audit ('history.expire.enabled');
```
**Pass if:** value is `'false'`. **Fail if:** value is `'true'` or unset.

### Check 2 — 6 audit rows for the 3 erasures (REQUESTED + COMPLETED pair each)

```sql
SELECT event_type, COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id LIKE 'REQ-LAB73-%'
GROUP BY event_type;
```
**Pass if:** result is `ERASURE_REQUESTED: 3` and `ERASURE_COMPLETED: 3`. **Fail if:** any imbalance.

### Check 3 — Pre-snapshot query returns the investor's row (for all 3)

For each of the 3 erased investors, run Step 7(a). **Pass if:** all 3 return ≥ 1 row. **Fail if:** any return 0.

### Check 4 — Post-erasure (current) query returns 0 rows (for all 3)

For each of the 3 erased investors, run Step 7(b). **Pass if:** all 3 return 0 rows. **Fail if:** any return > 0.

### Check 5 — Audit row preserved (for all 3)

For each of the 3 erased investors, run Step 7(c). **Pass if:** all 3 return ≥ 2 rows. **Fail if:** any return < 2.

**ANY failure on ANY check for ANY investor → CP-19 FAILS → CAPSTONE FAILS.**

---

## Common failure mode #1 — Time-travel query 7(a) fails with "snapshot not found"

**Symptom:** the time-travel query in Step 7(a) fails with `Cannot find snapshot with id <X>` or returns 0 rows even though the investor was definitely there before erasure.

**Cause:** the snapshot has been **expired**. By default, Iceberg expires snapshots older than 5 days on most tables. If you're running this lab and the relevant table has had its snapshots expired, the time-travel handle is gone.

**`gold.consent_audit` itself is safe** because the pre-flight set `history.expire.enabled=false` on it. But for `silver.member_master` and `bronze.member_cdc`, the default 5-day retention applies. The audit row stores the snapshot ID, but if the underlying table's metadata has been expired since then, the snapshot is unreachable.

**Diagnosis:**
```sql
-- Check whether the snapshot still exists in the table's snapshot history
SELECT snapshot_id, committed_at FROM argus_${STUDENT_ID}_silver.member_master.snapshots
WHERE snapshot_id = <pre_snap>;
```
If this returns 0 rows, the snapshot has been expired.

**Fix (for the lab — recoverable):**

Re-run from Step 2: reset the planted cases to ACTIVE and execute the workflow fresh. You'll get new snapshot IDs that are current (and won't expire for at least 5 days).

**Fix (for production — extend snapshot retention):**

This is a real production concern. In a SEBI inspection 6 months after an erasure, MSE needs the snapshots to still be queryable.

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

This failure mode is the single most common reason CP-19 gets cited in real DPDP inspections. Lab 7.3 is calibrated to catch it. The storage cost of 3-year snapshot retention on PII tables is non-negligible but is the price of evidentiary-grade erasure.

## Common failure mode #2 — Workflow exits partway through with an error

**Symptom:** `gdpr_erasure_workflow.py` exits with a non-zero return code, possibly after writing the `ERASURE_REQUESTED` audit row but before writing `ERASURE_COMPLETED`.

**Cause:** various — could be an intermittent network blip to S3, a DELETE failing on one of the operational tables, or a permission issue.

**Diagnosis:**
```sql
-- Check what got written
SELECT event_type, COUNT(*)
FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id = 'REQ-LAB73-<the failing one>'
GROUP BY event_type;
```
If you see `ERASURE_REQUESTED: 1` but no `ERASURE_COMPLETED`, the workflow died mid-run.

**Fix:** the workflow is **not idempotent** — re-running with the same `request_id` will create another `ERASURE_REQUESTED` row, bloating the audit. The right recovery:

1. Determine what state the actual data is in:
   ```sql
   SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
   WHERE investor_pan_hash = '<the hash>';
   ```
   If this returns 0, the DELETE actually succeeded but the audit row didn't get written — you need to manually write the COMPLETED row (talk to instructor).

   If this returns ≥ 1, the DELETE didn't run yet — you can re-run the workflow with a *different* request ID (e.g., `REQ-LAB73-RETRY-<hash>`).

2. For the lab, the simpler path is: re-run the workflow with a new request-id suffix, accept that the original REQUEST row exists with no matching COMPLETE, and document this in your lab notes. CP-19's Check 2 looks for ≥3 of each event type with `request_id LIKE 'REQ-LAB73-%'` — extra REQUESTED rows from a retry don't break that.

## Common failure mode #3 — `legacy_alerts` lookup in Step 8 returns 0 rows

**Symptom:** Step 8's `legacy_alerts` count is 0 — no alerts retained for the firm.

**Cause:** the synthetic data didn't generate any legacy alerts for the investor's firm, OR the `member_firm_id` in `silver.member_master` doesn't match the `member_firm_id` in `legacy_alerts` (data linkage issue).

**Diagnosis:**
```sql
-- Confirm the firm has any alerts at all
SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.legacy_alerts
WHERE member_firm_id = '<firm from step 8>';

-- Compare formats — sometimes the IDs have prefix mismatches
SELECT DISTINCT member_firm_id FROM argus_${STUDENT_ID}_bronze.legacy_alerts LIMIT 5;
SELECT DISTINCT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master FOR SYSTEM_VERSION AS OF <pre_snap> LIMIT 5;
```
The two LIMIT 5 outputs should have member_firm_id values in the same format.

**Fix:** if formats match but counts are 0, re-run the synthetic data generator with --seed 42 and re-load `legacy_alerts` (Lab 1.2 Step 5a). The seed-42 data is calibrated to ensure each planted erasure case has at least one corresponding firm-level alert.

## Common failure mode #4 — Step 7(b) returns rows even though the workflow reported success

**Symptom:** the workflow reports `ERASURE_COMPLETED` and `[delete] silver.member_master — 1 rows match hash`, but Step 7(b) at current state still returns rows.

**Cause:** Iceberg DELETE in MOR (merge-on-read) mode produces *delete files*, not row mutations. If the table reader doesn't know about the new delete files (e.g., metadata cache), the read might return the deleted rows.

**Diagnosis:**
```sql
-- Force a metadata refresh
REFRESH METADATA argus_${STUDENT_ID}_silver.member_master;

-- Re-run Step 7(b)
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_pan_hash = 'HASH_FOR_CASE_20';
```

If this now returns 0, the cause was metadata caching.

**Fix:** the REFRESH METADATA in your diagnosis step is the fix. Add it as a final step of the workflow if you find it's a recurring issue. Alternatively, use COW (copy-on-write) tables for any table where erasure is required — COW writes a new full file per affected partition, making DELETE immediately visible without metadata refresh.

---

## Pass condition for CP-19

ALL 5 CHECKS pass for ALL 3 investors:
- ✅ `consent_audit` has snapshot expiration disabled
- ✅ 6 audit rows (3 REQUESTED + 3 COMPLETED) for the 3 erasures
- ✅ Pre-snapshot time-travel query returns the row (for all 3)
- ✅ Current-state query returns 0 (for all 3)
- ✅ Audit row is preserved at current state (for all 3)

**When all 5 pass, CP-19 is closed and the COMPLIANCE GATE is open.**

MSE can defend its DPDP §12 obligation in any DPB or SEBI inspection, with cryptographic-grade Iceberg snapshots as evidence. The capstone's most consequential checkpoint is closed. **ARGUS is done.**

## Wrap-up — what you can now do that you couldn't before

You can execute a DPDP §12 erasure end-to-end: from request capture through operational deletion to time-travel proof. You understand why the audit table is structured to outlive the data it audits, and why Iceberg snapshot retention on PII tables is non-negotiable for evidentiary erasure. You can defend, in front of any regulator, the proof that an erasure happened correctly.

Most importantly: **the capstone is structurally complete.** ARG-1 through ARG-5 are all closed. ARGUS demonstrates a CDP-based surveillance platform that:
- Sustains 150K events/sec with sub-second pattern detection (ARG-1)
- Performs identity resolution and cross-product feature engineering at scale (ARG-2)
- Produces an alert dataset with reduced false-positive rate via XGBoost ML (ARG-3)
- Drafts STRs from RAG-augmented LLMs with provable lineage to source events (ARG-4)
- Honors DPDP §6(4) consent withdrawal and §12 erasure with cryptographic proof (ARG-5)

You've built — and proven the correctness of — a SEBI-defensible market surveillance platform. Submit your capstone. You're done.
