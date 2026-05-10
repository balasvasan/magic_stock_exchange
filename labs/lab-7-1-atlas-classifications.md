# Lab 7.1 — Atlas Classifications & Lineage (CP-17)

> 👋 **Module 7 first-timer?** Read [`docs/module-7-primer.md`](../docs/module-7-primer.md) before starting. It explains DPDP Act sections, Atlas, Ranger, and Iceberg time-travel — about 25 minutes. **Module 7 is the compliance module and CP-19 is the COMPLIANCE GATE.** Don't skip the primer.

> ℹ️ **Module:** 7 — SDX Governance & DPDP Compliance
> **Closes deficiency:** ARG-5 part 2 (lineage + classification)
> **Time:** ~60 minutes if Atlas is healthy and tags apply cleanly first try; up to 2.5 hours if the Atlas Hive hook isn't configured or the cluster suffix is wrong (the most common Atlas issue).
> **Source files:** [`src/governance/atlas_classifications.json`](../src/governance/atlas_classifications.json), [`src/governance/apply_atlas_tags.py`](../src/governance/apply_atlas_tags.py)

## What you're going to do

In order:

1. **Confirm Bronze, Silver, and Gold tables have data** so Atlas knows about them. (~3 min)
2. **Confirm Atlas is reachable** from your environment. (~2 min)
3. **Validate the classifications JSON** has all 6 PRD-locked tags. (~2 min)
4. **Dry-run the applier** to see which (tag × column) targets exist before changing anything. (~5 min)
5. **Apply the classifications for real** — Phase 1 creates the type definitions, Phase 2 attaches them to columns. (~10 min)
6. **Verify in the Atlas UI** that tags appear on the expected entities. (~10 min)
7. **Verify tag propagation** — Bronze → Silver derivation should automatically inherit tags. (~10 min)
8. **Verify CP-17 pass conditions** — four named checks. (~5 min)

Total: about 60 minutes. The biggest time-sink, if anything goes wrong, is Step 5/Failure Mode #1 (Atlas qualified-name suffix mismatch).

## Before you begin — prerequisite checklist

- [ ] Modules 1, 2, and 3 are complete. Bronze tables (`member_cdc`, `orders_raw`), Silver tables (`member_master`, `order_events`), and Gold tables (`alert_candidates`) all have at least some rows in them.
- [ ] You have an Atlas URL (your instructor will provide), and `ATLAS_URL`, `ATLAS_USER`, `ATLAS_PASSWORD` env vars are set in your shell — quick check: `echo $ATLAS_URL` should print a URL, not blank.
- [ ] You can reach Atlas from this shell: `curl -s -o /dev/null -w "%{http_code}" -u $ATLAS_USER:$ATLAS_PASSWORD $ATLAS_URL/api/atlas/v2/types/typedefs/headers` should return 200.
- [ ] **The Hive Atlas hook is enabled in Cloudera Manager.** Quick check (your instructor probably already did this): in CM, navigate to Hive → Configuration → search for "atlas". The "Hive Atlas Hook" checkbox must be ticked. If it isn't, ask your instructor to enable it and restart Hive — Lab 7.1 cannot succeed without this hook.
- [ ] You have access to the **Atlas web UI** in a browser, with credentials.

## Why classifications matter — read this before Step 5

Atlas classifications might feel like academic metadata busywork. They're not. Three concrete things they do:

**Classifications make policy expression possible.** Ranger policies (Lab 4.2 and 7.2) say things like "users in role `surveillance_l1` cannot read columns tagged `PII_HIGH`". Without tags, Ranger has to be configured per-column-per-table — hundreds of policies. With tags, one policy covers every PII column in every table, present and future.

**Classifications propagate downstream automatically.** When you tag a Bronze column `PII_HIGH`, any Silver/Gold column that derives from it inherits the tag without anyone re-tagging. If a JOB-05 transform reads `bronze.member_cdc.investor_pan` and produces `silver.member_master.investor_pan`, the Silver column gets `PII_HIGH` automatically because Atlas tracks the lineage. **This is the only sustainable way to manage classifications across hundreds of tables**.

**Classifications + lineage = the auditable answer to DPDP.** When a regulator asks "where does Investor X's PAN appear in the platform?", the answer is "every entity tagged `PII_HIGH` whose lineage traces back to a row containing X's PAN" — a single graph query. Without classifications and lineage, the answer requires forensic engineering through the codebase.

The take-away: classifications aren't documentation. They're the substrate every downstream policy and audit query is written against.

## Step 1 — Confirm prerequisites

Atlas only knows about tables that Hive has touched at least once. Confirm your tables have data:

```sql
SELECT
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.member_cdc)              AS bronze_member,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current) AS silver_master,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates)         AS gold_alerts;
```

**Expected output:** all three counts > 0. If any is 0, run the corresponding upstream module first:
- `bronze_member = 0` → re-run Lab 1.2 Step 5b (the seed_member_cdc script)
- `silver_master = 0` → run Module 2 Lab 2.2 (identity resolution)
- `gold_alerts = 0` → run Module 3 Lab 3.2 (alert candidates)

## Step 2 — Confirm Atlas is reachable

```bash
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    ${ATLAS_URL}/api/atlas/v2/types/typedefs/headers \
    | jq '.classificationDefs | length'
```

**Expected output:** a number ≥ 0 (the count of classifications already in your Atlas instance — could be 0 if you're the first student, or larger if previous students have run the lab).

**Failure modes to watch for:**
- HTTP 401 → wrong `ATLAS_USER` or `ATLAS_PASSWORD`
- Connection refused → wrong `ATLAS_URL` or VPN/network issue
- Empty response → ask your instructor whether Atlas is up

If Atlas isn't reachable, **stop and fix this first**. The rest of the lab depends on it.

## Step 3 — Validate the classifications JSON

The 6 PRD-locked tags are the heart of CP-17. They cannot drift — production Ranger policies reference these specific names.

```bash
python3 -c "
import json
cfg = json.load(open('src/governance/atlas_classifications.json'))
classes = cfg['classifications']
print(f'Found {len(classes)} classifications')
expected = {'PII_HIGH', 'PII_LOW', 'FINANCIAL_SENSITIVE', 'SURVEILLANCE_RESTRICTED',
            'DPDP_CONSENT_REQUIRED', 'SEBI_AUDIT_TRAIL'}
actual = {c['name'] for c in classes}
assert actual == expected, f'mismatch: {actual ^ expected}'
print(f'  All 6 PRD-locked tags present')
"
```

**Expected output:**
```
Found 6 classifications
  All 6 PRD-locked tags present
```

If you see a mismatch, the file has been edited and the PRD lock is broken. **Do not proceed**. Restore from the canonical version:

```bash
git checkout src/governance/atlas_classifications.json
```

> 💡 **Why these specific tags?** Each maps to a real DPDP/SEBI requirement:
> - `PII_HIGH` — fields the DPDP Act treats as "sensitive personal data" (PAN, Aadhaar, biometrics)
> - `PII_LOW` — names, member IDs, contact details (still personal but lower sensitivity)
> - `FINANCIAL_SENSITIVE` — trading positions, P&L, settlement details
> - `SURVEILLANCE_RESTRICTED` — disposition labels, manipulation case statuses
> - `DPDP_CONSENT_REQUIRED` — fields whose use depends on §6 consent (used by the §6(4) row filter in Lab 7.2)
> - `SEBI_AUDIT_TRAIL` — fields SEBI requires retained for 5+ years (used by the PMLA-statutory carve-out in Lab 7.3)
>
> Atlas applies the `_${STUDENT_ID}` suffix at registration time so each student's tags don't collide with other students'.

## Step 4 — Dry-run the applier

The dry-run shows you what Atlas API calls *would* be made, without making them. Use this to catch issues before changing anything.

```bash
python src/governance/apply_atlas_tags.py \
    --atlas-url ${ATLAS_URL} \
    --classifications src/governance/atlas_classifications.json \
    --dry-run
```

**Expected output:** progress logs reporting each `(tag, target)` pair the script would attach. The dry-run runs in two phases:
- **Phase 1** — type creations: 6 lines like `[would create] type PII_HIGH_${STUDENT_ID}`
- **Phase 2** — column-level attachments: dozens of lines like `[would tag] PII_HIGH_${STUDENT_ID} → argus_${STUDENT_ID}_bronze.member_cdc.investor_pan`

The dry-run **does not** actually call POST against Atlas. It only validates that:
- The targets in the JSON resolve to real columns/tables
- The Atlas qualified-name lookups would succeed (no `[miss]` lines)

If you see lots of `[miss]` lines, see Common Failure Mode #1 below — most likely cause is a cluster-suffix mismatch.

## Step 5 — Apply for real

Once the dry-run is clean, apply the classifications:

```bash
python src/governance/apply_atlas_tags.py \
    --atlas-url ${ATLAS_URL} \
    --classifications src/governance/atlas_classifications.json
```

**Expected output:**

```
Loaded 6 classifications from src/governance/atlas_classifications.json
Phase 1 — ensuring classification types exist in Atlas
  [create]  type PII_HIGH_s001
  [create]  type PII_LOW_s001
  [create]  type FINANCIAL_SENSITIVE_s001
  [create]  type SURVEILLANCE_RESTRICTED_s001
  [create]  type DPDP_CONSENT_REQUIRED_s001
  [create]  type SEBI_AUDIT_TRAIL_s001
Phase 2 — attaching classifications to columns/tables
  PII_HIGH_s001
    [tag ]  PII_HIGH_s001 → argus_s001_bronze.member_cdc.investor_pan
    [tag ]  PII_HIGH_s001 → argus_s001_bronze.member_cdc.trader_name
    [tag ]  PII_HIGH_s001 → argus_s001_bronze.member_cdc.investor_email
    ...
  PII_LOW_s001
    [tag ]  PII_LOW_s001 → argus_s001_bronze.member_cdc.member_firm_id
    ...
==> Done — attached 22, missing 0
```

Pay attention to the **last line**: `attached = 22, missing = 0`.

> 💡 **What `attached` and `missing` count:** `attached` is the number of (tag, target) pairs the script successfully applied. `missing` is the number it tried to apply but couldn't because Atlas didn't recognize the target. Some `missing` is expected when the Atlas hook is still indexing recent table writes; **`missing` should converge to 0 within ~5 minutes** of the upstream tables being written.

**Pass criteria for this step:** `attached >= 6` (at minimum the 6 type creations). Ideally `attached = 22` (or whatever your count is — it depends on the JSON config). `missing = 0` is ideal but `missing < 5` is acceptable if Hive metadata is still propagating.

If `attached = 0` and `missing = N` for large N → **Common Failure Mode #1** below. Don't proceed past Step 5 until this is fixed.

## Step 6 — Verify in the Atlas UI

Open the Atlas web UI in a browser. Search for `PII_HIGH_${STUDENT_ID}` in the search box at the top.

**Expected outcome:** the search returns the classification you just created. Click it.

In the classification's detail view, click the **Tagged Entities** tab. You should see at least 8 column-level entities listed. The first few should look like:

| Entity | Type | Path |
|---|---|---|
| investor_pan | hive_column | argus_${STUDENT_ID}_bronze.member_cdc.investor_pan |
| trader_name | hive_column | argus_${STUDENT_ID}_bronze.member_cdc.trader_name |
| investor_pan | hive_column | argus_${STUDENT_ID}_silver.member_master.investor_pan |
| investor_email | hive_column | argus_${STUDENT_ID}_silver.member_master.investor_email |
| ... | | |

> 💡 **What "tagged entities" tab shows:** every column or table in your platform that currently has this classification attached. If you see a column you didn't explicitly tag (e.g., `silver.member_master.investor_pan`), that's tag *propagation* working — Step 7 verifies this directly.

Then search for the table `argus_${STUDENT_ID}_silver.member_master`. In the table's detail view:

- The **Classifications** panel should show at least 3 classifications: `PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, `DPDP_CONSENT_REQUIRED_${STUDENT_ID}` — some attached directly to the table, some inherited from columns.
- The **Lineage** tab should show a graph: `argus_${STUDENT_ID}_bronze.member_cdc` ➝ `argus_${STUDENT_ID}_silver.member_master` (the JOB-05 identity-resolution transform). The arrow should be present without you drawing anything — Atlas built it from the Hive query plan.

If lineage is missing → see Common Failure Mode #2.

## Step 7 — Verify tag propagation works

When a Bronze column tagged `PII_HIGH` flows into a Silver column via JOB-05, Atlas's tag-propagation feature should automatically apply the same tag to the Silver column. Verify:

```bash
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/hive_column?attr:qualifiedName=argus_${STUDENT_ID}_silver.member_master.investor_email@${ATLAS_CLUSTER_NAME:-default}" \
    | jq '{
        directly_tagged:    [.entity.classifications[]? | .typeName],
        propagated_tagged:  [.entity.propagatedClassifications[]? | .typeName]
      }'
```

**Expected output** — a JSON like:
```json
{
  "directly_tagged":   ["PII_HIGH_s001"],
  "propagated_tagged": ["DPDP_CONSENT_REQUIRED_s001"]
}
```

The exact split (which tags are direct vs propagated) depends on your atlas_classifications.json. **Pass if:** `PII_HIGH_${STUDENT_ID}` appears in either list. Both count for compliance — Atlas treats direct and propagated identically.

## Step 8 — Verify CP-17 pass conditions

CP-17 has **four checks**.

### Check 1 — All 6 tag types registered in Atlas

```bash
for t in PII_HIGH PII_LOW FINANCIAL_SENSITIVE SURVEILLANCE_RESTRICTED DPDP_CONSENT_REQUIRED SEBI_AUDIT_TRAIL; do
    code=$(curl -s -o /dev/null -w "%{http_code}" -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
        "${ATLAS_URL}/api/atlas/v2/types/typedef/name/${t}_${STUDENT_ID}")
    echo "  ${t}_${STUDENT_ID}: HTTP $code"
done
```

**Pass if:** all 6 lines show `HTTP 200`. **Fail if:** any line shows 404 (the type wasn't created) or 401 (auth issue).

### Check 2 — Applier reported `attached >= 6, missing < 5`

The Step 5 final line. **Pass if:** `attached >= 6` and `missing < 5`. **Fail if:** `attached < 6` (Phase 2 didn't tag enough columns).

### Check 3 — `silver.member_master` shows ≥ 3 classifications in Atlas UI

Search the table in Atlas UI → look at the Classifications panel.

**Pass if:** at least 3 of `{PII_HIGH, PII_LOW, FINANCIAL_SENSITIVE, SURVEILLANCE_RESTRICTED, DPDP_CONSENT_REQUIRED, SEBI_AUDIT_TRAIL}` (each suffixed with `_${STUDENT_ID}`) appear.

### Check 4 — Lineage Bronze → Silver → Gold visible

In Atlas UI, view lineage for `argus_${STUDENT_ID}_gold.alert_candidates`.

**Pass if:** the lineage graph shows upstream connections back to `silver.order_events` and `silver.member_master`, which trace back to `bronze.orders_raw` and `bronze.member_cdc`. The full chain should be visible.

**Fail if:** any link in the chain is broken (e.g., Gold has no upstream connection).

---

## Common failure mode #1 — `attached = 0, missing = N`

**Symptom:** the applier runs cleanly but Phase 2 prints only `[miss]` lines, ending with `attached = 0, missing = 22`.

**Cause:** Atlas doesn't know about any of the Hive entities the applier is trying to tag. Two possible roots:

**Root cause A — The Hive Atlas hook is not enabled.**

The Hive Atlas hook is the component that emits an event to Atlas every time a Hive query touches a table. Without it, Atlas never learns about your tables, and column lookups all 404.

Diagnosis:
```bash
# Check if any of your tables exist in Atlas
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/search/basic?typeName=hive_table&query=argus_${STUDENT_ID}_bronze.member_cdc" \
    | jq '.entities | length'
```
If this returns 0, Atlas has no record of your Bronze table — Hive hook isn't running.

Fix: in Cloudera Manager → Hive → Configuration → search "atlas" → check "Hive Atlas Hook" → restart Hive. **Then re-write to your Bronze tables** so the hook fires on those writes (re-run Lab 1.2 Step 5a's small batch load is fastest).

**Root cause B — Atlas qualified-name cluster suffix is wrong.**

This is the #1 most common Atlas issue. Atlas fully-qualifies entity names with a cluster suffix, like `argus_s001_bronze.member_cdc@default` or `argus_s001_bronze.member_cdc@cm`. If the script's expected suffix doesn't match what Atlas is using, every lookup 404s.

Diagnosis:
```bash
# Confirm what cluster suffix Atlas is using by inspecting any existing entity:
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/search/basic?typeName=hive_table&query=argus_${STUDENT_ID}_bronze.orders_raw" \
    | jq '.entities[0].attributes.qualifiedName'
# expect something like: "argus_s001_bronze.orders_raw@<cluster_name>"
```

The thing after `@` is the cluster suffix. If it's not `default`, you need to either:
1. Set `ATLAS_CLUSTER_NAME` env var: `export ATLAS_CLUSTER_NAME=<cluster_name>` and re-run the applier (it picks up the env var).
2. Or pass `--cluster-name <cluster_name>` as a CLI arg to `apply_atlas_tags.py`.

After fix, re-run Step 5. `missing` should drop to 0 (or near it).

## Common failure mode #2 — Lineage graph is empty for `gold.alert_candidates`

**Symptom:** Atlas UI search for `gold.alert_candidates` succeeds, but the Lineage tab shows just the table itself with no inputs or outputs.

**Cause:** Hive Atlas hook didn't capture the JOB-08 query plan. This usually means JOB-08 was run via a path that bypassed the hook (e.g., direct Spark write to Iceberg without going through Hive Metastore properly).

**Diagnosis:**
```bash
# Look for hive_process entities that produced gold.alert_candidates
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/search/basic?typeName=hive_process&query=alert_candidates" \
    | jq '.entities | length'
```
If this returns 0, no process was captured — that's the cause.

**Fix:** in `src/silver_to_gold/job_08_alert_candidates.py`, ensure the write is going through Hive (not bypassing it):
```python
# Good: writes through Hive Metastore, hook fires
df.writeTo(f"argus_{STUDENT_ID}_gold.alert_candidates").append()

# Bad: bypasses Hive, Atlas misses it
df.write.format("iceberg").mode("append").save(f"s3a://{BUCKET}/argus_{STUDENT_ID}_gold/alert_candidates")
```
After fix, re-run JOB-08. Lineage should appear within 60 seconds.

## Common failure mode #3 — Tags applied successfully but propagation isn't working

**Symptom:** Step 5 reports `attached = 22, missing = 0`, but Step 7's propagation check shows `propagated_tagged: []` for Silver columns.

**Cause:** Atlas tag-propagation has to be enabled at the type level — sometimes new types are created with propagation off by default.

**Diagnosis:**
```bash
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/types/typedef/name/PII_HIGH_${STUDENT_ID}" \
    | jq '.options.propagationEnabled // .options'
```
If this prints `false` or null, propagation is off.

**Fix:** the `apply_atlas_tags.py` script's classification JSON should set `"propagatable": true` on each type (already set in the canonical version). If it's missing, restore the JSON from `git checkout src/governance/atlas_classifications.json` and re-run Step 5. The script will see the type already exists but with wrong options and update it.

---

## Pass condition for CP-17

All four checks pass:
- ✅ All 6 tag types registered (HTTP 200 from typedef API)
- ✅ Applier attached ≥ 6 (and missing < 5)
- ✅ `silver.member_master` shows ≥ 3 classifications
- ✅ Bronze → Silver → Gold lineage is visible

When all four pass, the platform is **mapped**. Every PII column is tagged. Every Gold column has traceable derivation back to its Bronze source. The DPDP audit answer for "which tables hold investor X's PII" is now an Atlas search instead of a multi-week investigation.

## Wrap-up — what you can now do that you couldn't before

You can deploy Atlas classifications to a CDP cluster and verify their attachment via the API. You understand why classifications propagate downstream automatically and what that means for managing policy at scale. You can navigate the Atlas UI to verify lineage between Bronze, Silver, and Gold tables.

Most importantly: **the platform is now auditable for DPDP `where is data X?` questions.** Before CP-17, that question required forensic engineering; after, it's a single Atlas search.

Lab 7.2 builds on this — using the `DPDP_CONSENT_REQUIRED` and `SEBI_AUDIT_TRAIL` tags from CP-17 to express row-level Ranger policies that distinguish DPDP §6(4) consent-withdrawn data from PMLA-statutory data. Allow about 45 minutes for that one.
