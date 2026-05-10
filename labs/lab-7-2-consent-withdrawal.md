# Lab 7.2 — DPDP §6(4) Consent Withdrawal (CP-18)

> 👋 **Module 7 first-timer?** Read [`docs/module-7-primer.md`](../docs/module-7-primer.md) first. Critical for this lab — the §6(4) "non-statutory data" concept and the PMLA carve-out are explained in detail there. About 25 minutes.

> ℹ️ **Module:** 7 — SDX Governance & DPDP Compliance
> **Closes deficiency:** ARG-5 part 2 (consent enforcement at query time)
> **Time:** ~45 minutes if Module 4 (Ranger policies) is in place; up to 2 hours if Ranger policies need redeployment.
> **Source files:** [`src/governance/ccpa_optout_enforcement.py`](../src/governance/ccpa_optout_enforcement.py) — note the file is named `ccpa_*` for legacy reasons but its actual logic implements DPDP §6(4). Same workflow.

## What you're going to do

In order:

1. **Find the 5 planted withdrawal cases** in `silver.member_master`. (~5 min)
2. **Reset their consent to ACTIVE** so we can run the withdrawal workflow fresh. (~3 min)
3. **Run the consent-withdrawal workflow** for each of the 5 investors. (~10 min)
4. **Verify audit rows** were written with pre/post snapshot IDs. (~5 min)
5. **Verify the master table state** updated. (~3 min)
6. **Verify Ranger row-filter applied** — research_analyst should see 0 from these investors. (~5 min)
7. **Verify DPO bypass** — compliance_dpo via the `vw_surveillance_audit` view should still see them. (~5 min)
8. **Verify CP-18 pass conditions** — five named checks. (~5 min)

Total: about 45 minutes. If Ranger policies haven't been deployed (Lab 4.2 missed), this jumps to 2 hours; that's the most common time-sink.

## Before you begin — prerequisite checklist

- [ ] [Lab 7.1](lab-7-1-atlas-classifications.md) is complete and CP-17 passed — Atlas tags (especially `DPDP_CONSENT_REQUIRED` and `SEBI_AUDIT_TRAIL`) must be applied
- [ ] Module 4 Lab 4.2 is complete and Ranger policies deployed — specifically the `dpdp_consent_filter_features` row-filter policy and the `compliance_dpo` role with `SEBI_AUDIT_TRAIL` bypass
- [ ] `silver.member_master` has consent fields populated for the planted cases — quick check: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE consent_status IS NOT NULL` should show > 0
- [ ] You have credentials for both `research_analyst` and `compliance_dpo` roles (your instructor will provide; or run `SHOW ROLES` to see what's available to your user)
- [ ] You can reach the **Ranger admin UI** if needed for policy refresh

## Why DPDP §6(4) matters — read this before Step 3

You might wonder: *if a person withdraws consent, why don't I just delete their data?* That feels intuitive. It's also wrong, and getting it wrong is a regulatory violation in two directions.

**DPDP §6(4) is the "consent withdrawal with statutory carve-out" rule.** It says: when a Data Principal withdraws consent, the Data Fiduciary must stop processing that data — *except* for data they're legally required to retain under another law.

For MSE, the "another law" is mostly **PMLA (Prevention of Money Laundering Act)**. PMLA Regulation 2(1)(da) requires exchanges to retain trading records for 5 years after a confirmed manipulation case is closed. So if Trader Tarun withdraws consent, but he's tied to a confirmed wash-trading case from 2024, Tarun's trades on the suspect days **must be kept** — by law — until 2029.

The DPDP-correct response is: **filter, don't delete**. Make non-statutory data *invisible* to non-compliance roles. Keep the statutory data (PMLA-tagged rows) intact and accessible to compliance roles only. This is what CP-18 demonstrates.

**Two failure directions:**
- ❌ Delete the trader's whole record on withdrawal → violates PMLA retention. Regulator action: SEBI fines for record-loss. **Worse failure** because it breaks audit-trail evidence.
- ❌ Keep the trader's whole record visible to all roles → violates DPDP §6(4). Regulator action: DPDP Authority fine.

**The right answer:** Ranger row-filter policy hides non-statutory rows from non-compliance roles. Statutory rows stay visible to compliance_dpo only. Both regulators are satisfied.

CP-19 (the next lab) is the *deletion* of non-statutory data — that's §12 erasure, which only applies after consent has been withdrawn AND the statutory retention window has elapsed. CP-18 is the visibility-control layer.

## Step 1 — Find the planted withdrawal cases

The synthetic data plants 5 consent-withdrawal scenarios at planted-case indices 15–19 in `data/generated/compliance_test_cases.csv`. They appear in `silver.member_master` as rows where `consent_status = 'WITHDRAWN'`.

```sql
SELECT investor_acct, consent_status, consent_purpose, investor_pan_hash
FROM argus_${STUDENT_ID}_silver.member_master
WHERE consent_status = 'WITHDRAWN'
  AND is_current
ORDER BY investor_acct
LIMIT 10;
```

**Expected output:** exactly 5 rows. Each is one of the planted cases.

> 💡 **What `is_current` means:** `silver.member_master` is an SCD-Type-2 table (Slowly Changing Dimension) — every change to an investor's profile creates a new row, and `is_current = TRUE` flags the latest. This is the core SCD2 pattern Lab 2.2 set up. Always filter `WHERE is_current` when querying current state; queries without it return historical rows too.

**Note the 5 `investor_acct` values** — you'll use them in Step 2 and beyond. Copy them somewhere you can reference. They look like `INV-XXXXXXXX` where each `X` is a digit.

If you see **0 rows**, the synthetic generator's consent records didn't make it into Silver. Re-check Lab 2.2 / JOB-05; specifically the seed_member_cdc step from Lab 1.2 Step 5b. Run it now if skipped.

## Step 2 — Reset their consent to ACTIVE for the test

The 5 planted cases come pre-WITHDRAWN in the synthetic data. That's so Module 4's CP-11 has data to work with. For Lab 7.2, the test is to **run the withdrawal workflow itself**, so we'll temporarily reset to ACTIVE, then withdraw fresh.

```sql
UPDATE argus_${STUDENT_ID}_silver.member_master
SET consent_status = 'ACTIVE',
    consent_purpose = 'TRADING,SURVEILLANCE,ANALYTICS,MARKETING'
WHERE investor_acct IN (
    -- Replace with the 5 actual values from Step 1:
    'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
) AND is_current;
```

> 💡 **Why are we resetting?** In production, you wouldn't — withdrawal is one-way. For lab pedagogy, we need to be able to *run* the withdrawal workflow against rows that aren't already withdrawn. The reset is a teaching shortcut. Real workflow: a withdrawal is permanent until a new consent is recorded.

> 💡 **What does `consent_purpose = 'TRADING,SURVEILLANCE,ANALYTICS,MARKETING'` mean?** DPDP requires recording *what specific purpose* a consent applies to. ARGUS tracks four:
> - `TRADING` — process orders/trades (statutory; can't be withdrawn while trading is active)
> - `SURVEILLANCE` — manipulation detection (statutory under SEBI/PMLA)
> - `ANALYTICS` — internal analysis, ML training
> - `MARKETING` — promotional communications
>
> A typical withdrawal removes ANALYTICS and MARKETING but keeps TRADING and SURVEILLANCE because those are statutory.

## Step 3 — Run the workflow for each of the 5 investors

```bash
for acct in INV-XXXXXXXX INV-XXXXXXXX INV-XXXXXXXX INV-XXXXXXXX INV-XXXXXXXX; do
    python src/governance/ccpa_optout_enforcement.py \
        --investor-acct "$acct" \
        --withdraw-purposes "ANALYTICS,MARKETING" \
        --request-id "REQ-LAB72-$acct" \
        --requestor-channel CONSENT_MANAGER
done
```

**Expected output per call:**

```
Consent withdrawal request REQ-LAB72-INV-XXXXXXXX for investor INV-XXXXXXXX
==> Consent updated. Audit row: AUDIT-XXXXXXXXXXXX
    Status:    WITHDRAWN
    Purposes:  TRADING,SURVEILLANCE
    Snapshots: pre=abc123def456 post=abc123def789
    Ranger row-filter dpdp_consent_filter applies on next query.
```

> 💡 **What just happened, step by step:**
> 1. The script captured the *current* Iceberg snapshot ID of `silver.member_master` — that's `pre`.
> 2. It updated the row for `investor_acct` to `consent_status = 'WITHDRAWN'` with `consent_purpose = 'TRADING,SURVEILLANCE'` (statutory only).
> 3. The Iceberg write produced a new snapshot ID — that's `post`.
> 4. It wrote an audit row to `gold.consent_audit` recording the change, with both snapshot IDs and the request ID.
> 5. Ranger's row-filter policy now sees `consent_status = WITHDRAWN` and excludes the investor from non-compliance queries.

The pre/post snapshots are the *evidence trail*. If a regulator later asks "show me what changed at 14:23:42 on May 9, 2026", you can run a time-travel query against snapshot `pre` and snapshot `post` and produce the diff.

## Step 4 — Verify audit rows

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

**Expected output:** 5 rows. Each must have:
- `consent_purpose` containing `ANALYTICS` and `MARKETING` (the *withdrawn* purposes — recording what the user asked to remove)
- non-empty `pre_action_snapshot` and `post_action_snapshot`
- **Distinct** snapshot IDs between pre and post (proves the master table actually changed; identical snapshots = the workflow didn't write anything)

> ⚠️ **If pre = post**, the workflow ran but the underlying UPDATE didn't change any rows — that means the `investor_acct` you targeted wasn't in `is_current` state, or it had `consent_status = 'WITHDRAWN'` already (the reset in Step 2 didn't take). Re-run Step 2, then re-run the workflow.

## Step 5 — Verify master table state

```sql
SELECT investor_acct, consent_status, consent_purpose
FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_acct IN (
    -- the 5 values from Step 1
    'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
)
  AND is_current;
```

**Expected output:** 5 rows, each showing:
- `consent_status = 'WITHDRAWN'`
- `consent_purpose = 'TRADING,SURVEILLANCE'` — only statutory purposes remain

If `consent_purpose` shows the original `'TRADING,SURVEILLANCE,ANALYTICS,MARKETING'`, the workflow's UPDATE didn't apply correctly. Check the script's logs for errors.

## Step 6 — Verify Ranger row-filter applied (research_analyst role)

This is the test of CP-18: a non-compliance role should now see *zero* rows tied to these withdrawn investors in non-statutory tables.

```sql
SET ROLE research_analyst;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features f
WHERE f.member_firm_id IN (
    SELECT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master
    WHERE investor_acct IN (
        'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
    )
);
```

**Expected output:** `0`.

> 💡 **What just happened:** the `dpdp_consent_filter_features` Ranger policy intercepts every query against `member_temporal_features` from a `research_analyst` user and adds a hidden join to `member_master` with the predicate `consent_status = 'ACTIVE' AND consent_purpose LIKE '%ANALYTICS%'`. Since you withdrew ANALYTICS in Step 3, those rows now fail the join — they're invisible.

If you see **> 0**, the filter isn't applying. See Common Failure Mode #1 below.

## Step 7 — Verify DPO bypass via `vw_surveillance_audit`

The DPO (Data Protection Officer) needs to see the withdrawn investors *for surveillance purposes*, even though their analytics consent is gone. DPDP §7 statutory exception covers this.

```sql
SET ROLE compliance_dpo;

SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
WHERE investor_acct IN (
    'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX', 'INV-XXXXXXXX'
);
```

**Expected output:** `> 0` (one row per investor's surveillance-relevant data).

> 💡 **Why does the DPO see them?** The view `vw_surveillance_audit` is tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}` (an Atlas classification from CP-17). The Ranger policy that hides non-statutory data has an **explicit exception**: tables tagged `SEBI_AUDIT_TRAIL` are not filtered when accessed by `compliance_dpo`. This is the §7 statutory exception expressed as a Ranger policy.

If you see **0**, either the view isn't tagged correctly (CP-17 issue) or the exception in the Ranger policy is wrong (CP-11 / Lab 4.2 issue). The fix is upstream, not in this lab.

## Step 8 — Verify CP-18 pass conditions

CP-18 has **five checks**.

### Check 1 — 5 audit rows written

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE event_type = 'CONSENT_WITHDRAWN' AND request_id LIKE 'REQ-LAB72-%';
```
**Pass if:** count = 5. **Fail if:** count < 5.

### Check 2 — Master table updated

The Step 5 query returns 5 rows with `consent_status = 'WITHDRAWN'`. **Pass if:** all 5 show WITHDRAWN. **Fail if:** any show ACTIVE.

### Check 3 — Pre/post snapshots distinct

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.consent_audit
WHERE request_id LIKE 'REQ-LAB72-%'
  AND pre_action_snapshot != post_action_snapshot
  AND pre_action_snapshot IS NOT NULL
  AND post_action_snapshot IS NOT NULL;
```
**Pass if:** count = 5. **Fail if:** count < 5 — at least one withdrawal didn't actually change the table.

### Check 4 — research_analyst sees 0 from withdrawn investors

The Step 6 query returns 0. **Pass if:** 0. **Fail if:** > 0 — Ranger policy not applied.

### Check 5 — compliance_dpo sees > 0 via `vw_surveillance_audit`

The Step 7 query returns > 0. **Pass if:** > 0. **Fail if:** 0 — the §7 statutory exception isn't bypassing for the DPO role.

---

## Common failure mode #1 — Step 6 returns rows even after withdrawal

**Symptom:** as `research_analyst`, querying `member_temporal_features` for the withdrawn investors returns rows when it should return 0.

**Cause:** three possibilities, in decreasing likelihood.

**Cause A — Ranger policy cache hasn't refreshed.**

The Hive Ranger plugin caches policies for 30–60 seconds for performance. If you ran the withdrawal and immediately queried, the cached policy still allows the rows.

Diagnosis: wait 90 seconds and retry. If it now returns 0, this was the cause.

Fix: just patience. In production, the cache TTL is tuned to your latency tolerance — for emergencies, the Ranger admin UI has a "Refresh policies" button.

**Cause B — The Ranger filter targets the wrong column.**

The `dpdp_consent_filter_features` policy joins through `member_firm_id` (NOT `investor_acct`). If the investor's withdrawal didn't propagate to a `consent_status = WITHDRAWN` flag on their member firm row, the filter doesn't bite.

Diagnosis:
```sql
-- Check whether the investor's withdrawal actually propagated to the firm row
SELECT investor_acct, member_firm_id, consent_status
FROM argus_${STUDENT_ID}_silver.member_master
WHERE investor_acct = 'INV-XXXXXXXX' AND is_current;
```
If `member_firm_id` is NULL or `consent_status` is `ACTIVE`, the workflow's UPDATE didn't propagate correctly.

Fix: re-run the workflow. If it still doesn't work, check `ccpa_optout_enforcement.py`'s UPDATE clause — make sure it's matching on `investor_acct` and updating the row that has `is_current = TRUE`.

**Cause C — Your role's grants include a BYPASS flag.**

Some superuser roles override row-filters. Diagnosis:
```sql
SELECT current_user(), current_role();
SHOW CURRENT ROLES;
```
If `current_role()` returns something other than `research_analyst`, you're not actually in that role.

Fix: explicitly set the role:
```sql
SET ROLE research_analyst;
```

## Common failure mode #2 — Workflow script fails with "snapshot lookup error"

**Symptom:** `ccpa_optout_enforcement.py` exits with `Failed to read pre-action snapshot for argus_${STUDENT_ID}_silver.member_master`.

**Cause:** Iceberg snapshot lookup requires Iceberg metadata to be available. If `silver.member_master` was just created and has no commits yet, `current_snapshot()` returns NULL.

**Diagnosis:**
```sql
SELECT * FROM argus_${STUDENT_ID}_silver.member_master.snapshots;
```
If this returns 0 rows, the table has no snapshots, which is impossible if there's data — implies the metadata is corrupted.

**Fix:** if the table has data (`SELECT COUNT(*) > 0`), refresh metadata:
```sql
REFRESH METADATA argus_${STUDENT_ID}_silver.member_master;
```
Then re-run the workflow.

## Common failure mode #3 — `SET ROLE` fails with "user does not have role"

**Symptom:** `SET ROLE research_analyst` returns an error like `User <you> does not have role research_analyst`.

**Cause:** Module 4 / Lab 4.2 hasn't been completed — the role doesn't exist or your user wasn't granted it.

**Diagnosis:**
```sql
SHOW ROLES;
```
Should list `research_analyst`, `compliance_dpo`, and others.

```sql
SHOW ROLE GRANT USER <your-user>;
```
Should show your user has been granted the relevant roles.

**Fix:** complete Lab 4.2 first. If Lab 4.2 is supposed to be done already, escalate to instructor — your user's role grants weren't provisioned.

## Common failure mode #4 — DPO query returns 0 rows from `vw_surveillance_audit`

**Symptom:** Step 7 query returns 0 even though the investors clearly had data before withdrawal.

**Cause:** the view `vw_surveillance_audit` either doesn't exist (CP-11 wasn't completed) or isn't tagged with `SEBI_AUDIT_TRAIL_${STUDENT_ID}` (CP-17 partial failure).

**Diagnosis:**
```sql
SHOW CREATE TABLE argus_${STUDENT_ID}_views.vw_surveillance_audit;
```
If this errors with "table not found", the view doesn't exist — Module 4 / Lab 4.1 incomplete.

```bash
# Check if the view has the SEBI_AUDIT_TRAIL tag in Atlas
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/hive_table?attr:qualifiedName=argus_${STUDENT_ID}_views.vw_surveillance_audit@${ATLAS_CLUSTER_NAME:-default}" \
    | jq '.entity.classifications[]? | .typeName'
```
If `SEBI_AUDIT_TRAIL_${STUDENT_ID}` doesn't appear, the tag wasn't applied.

**Fix:** complete Lab 4.1 (creates the view) and Lab 7.1 (applies the SEBI_AUDIT_TRAIL tag). Re-run Step 7.

---

## Pass condition for CP-18

All five checks pass:
- ✅ 5 audit rows written
- ✅ Master table updated to WITHDRAWN
- ✅ Pre/post Iceberg snapshots are distinct (proof of change)
- ✅ research_analyst sees 0 rows from withdrawn investors (filter works)
- ✅ compliance_dpo sees > 0 rows via SEBI_AUDIT_TRAIL view (statutory exception works)

When all five pass, MSE has the operational mechanism that DPDP §6(4) requires. The 5 planted cases prove the workflow; production scale just runs the same workflow more often.

## Wrap-up — what you can now do that you couldn't before

You can run an end-to-end DPDP §6(4) consent withdrawal: from intake (the workflow script) to enforcement (the Ranger row-filter) to bypass (the SEBI statutory exception via tagged views). You understand the difference between **filtering** (what §6(4) requires — make non-statutory data invisible) and **deletion** (what §12 requires — physically erase, with proof). You can navigate the audit trail in `consent_audit` to trace any specific withdrawal back to its pre-action and post-action Iceberg snapshots.

Most importantly: **DPDP §6(4) compliance is now operational.** Module 7's CP-18 checkpoint passes when the workflow demonstrates correct behavior on five planted cases.

Lab 7.3 is the **COMPLIANCE GATE** — DPDP §12 erasure with Iceberg time-travel proof. **This is the lab you cannot fail.** Allow about 90 minutes; the pre-flight check alone is worth 15 minutes of careful attention.
