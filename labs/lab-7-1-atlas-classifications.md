# Lab 7.1 — Atlas Classifications & Lineage (CP-17)

> ℹ️ **Module:** 7 — SDX Governance & DPDP Compliance
> **Closes deficiency:** ARG-5 part 2 (lineage + classification)
> **Source files:** [`src/governance/atlas_classifications.json`](../src/governance/atlas_classifications.json), [`src/governance/apply_atlas_tags.py`](../src/governance/apply_atlas_tags.py)

## Objectives

- Apply all 6 PRD-locked Atlas classifications (`PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, `FINANCIAL_SENSITIVE_${STUDENT_ID}`, `SURVEILLANCE_RESTRICTED_${STUDENT_ID}`, `DPDP_CONSENT_REQUIRED_${STUDENT_ID}`, `SEBI_AUDIT_TRAIL_${STUDENT_ID}`) to their target columns and tables
- Verify lineage capture from Bronze through Silver to Gold is visible in the Atlas UI
- Confirm tag propagation works — when a column has a tag, downstream columns derived from it inherit the tag automatically

## Why this matters

Atlas is the answer to the first three of the four DPDP audit questions ("which tables hold investor X's PII", "what's the consent state", "where did this Gold column come from"). Before classifications are applied, the data exists but the regulator has no map. After Lab 7.1, every PII-bearing column is tagged, every Bronze-to-Gold derivation is captured, and a DPDP inspection can be answered with a single Atlas search rather than a forensic engineering effort.

## Procedure

### Step 1 — Confirm prerequisites

The Bronze, Silver, and Gold tables need to have been written to at least once so that Atlas knows about them. Confirm:

```sql
SELECT
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.member_cdc)             AS bronze_member,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current) AS silver_master,
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates)         AS gold_alerts;
```

All three counts must be > 0. If any is zero, run the corresponding upstream module first (Module 1 for member_cdc, Module 2 for member_master, Module 3 for alert_candidates).

Also confirm Atlas is reachable:

```bash
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} ${ATLAS_URL}/api/atlas/v2/types/typedefs/headers \
    | jq '.classificationDefs | length'
```

**Expected output**: a number ≥ 0. If the call fails with 401 or connection refused, fix Atlas connectivity before proceeding.

### Step 2 — Validate the classifications JSON

```bash
python3 -c "
import json
cfg = json.load(open('src/governance/atlas_classifications.json'))
classes = cfg['classifications']
print(f'Found {len(classes)} classifications')
expected = {'PII_HIGH_${STUDENT_ID}', 'PII_LOW_${STUDENT_ID}', 'FINANCIAL_SENSITIVE_${STUDENT_ID}', 'SURVEILLANCE_RESTRICTED_${STUDENT_ID}',
            'DPDP_CONSENT_REQUIRED_${STUDENT_ID}', 'SEBI_AUDIT_TRAIL_${STUDENT_ID}'}
actual = {c['name'] for c in classes}
assert actual == expected, f'mismatch: {actual ^ expected}'
print(f'  All 6 PRD-locked tags present')
"
```

**Expected output**: `Found 6 classifications` and `All 6 PRD-locked tags present`. If you see a mismatch, the file has been edited and the PRD lock is broken — restore from the canonical version.

### Step 3 — Dry-run the applier

```bash
python src/governance/apply_atlas_tags.py \
    --atlas-url ${ATLAS_URL} \
    --classifications src/governance/atlas_classifications.json \
    --dry-run
```

**Expected output**: progress logs reporting each (tag × column) target. Phase 1 should show 6 type creations (or "exists, skipping" if you've run before); Phase 2 should enumerate every column targeted across the 6 tags. The dry-run does not actually call POST against Atlas — it only validates the targets and lookup paths.

### Step 4 — Apply for real

Once dry-run is clean:

```bash
python src/governance/apply_atlas_tags.py \
    --atlas-url ${ATLAS_URL} \
    --classifications src/governance/atlas_classifications.json
```

**Expected output**:

```
Loaded 6 classifications from src/governance/atlas_classifications.json
Phase 1 — ensuring classification types exist in Atlas
  [create]  type PII_HIGH_${STUDENT_ID}
  [create]  type PII_LOW_${STUDENT_ID}
  ...
Phase 2 — attaching classifications to columns/tables
  PII_HIGH_${STUDENT_ID}
    [tag ]  PII_HIGH_${STUDENT_ID} → argus_${STUDENT_ID}_bronze.member_cdc.investor_pan
    [tag ]  PII_HIGH_${STUDENT_ID} → argus_${STUDENT_ID}_bronze.member_cdc.trader_name
    ...
==> Done — attached 22, missing 0
```

If `missing > 0`, some target columns aren't yet known to Atlas. The Hive hook indexes columns asynchronously after the first write; if `missing` is small (< 5), wait 5 minutes and re-run. If `missing` is large, the issue is more fundamental — check that the Hive Atlas hook is enabled in Cloudera Manager.

### Step 5 — Verify in Atlas UI

Open the Atlas web UI and search for the `PII_HIGH_${STUDENT_ID}` classification. Click the classification name → "Tagged Entities" tab. You should see at least 8 column entities listed (4 from `argus_${STUDENT_ID}_bronze.member_cdc` + 4 from `argus_${STUDENT_ID}_silver.member_master`).

Then search for the Hive table `argus_${STUDENT_ID}_silver.member_master`:
- The classification panel should show `PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, and `DPDP_CONSENT_REQUIRED_${STUDENT_ID}` (the latter via wildcard/whole-table tag).
- The "Lineage" tab should show a graph connecting `argus_${STUDENT_ID}_bronze.member_cdc` → `argus_${STUDENT_ID}_silver.member_master` (the JOB-05 transform).

### Step 6 — Verify tag propagation works

When a Bronze column tagged `PII_HIGH_${STUDENT_ID}` flows into a Silver column via JOB-05's identity-resolution job, Atlas's tag-propagation feature should automatically apply the same tag to the Silver column without you re-tagging it. Verify by querying Atlas for any Silver column:

```bash
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/entity/uniqueAttribute/type/hive_column?attr:qualifiedName=argus_${STUDENT_ID}_silver.member_master.investor_email@default" \
    | jq '.entity.classifications[] | .typeName'
```

**Expected output**: `"PII_HIGH_${STUDENT_ID}"` should appear, either because we explicitly tagged it (it's in the JSON) or via propagation. If the entity has propagated classifications, you'll see them in `.entity.propagatedClassifications` instead — both count.

## Checkpoint CP-17 — Atlas tags applied + lineage captured

### Pass condition

All four checks pass.

### Check 1 — All 6 tag types registered in Atlas

```bash
for t in PII_HIGH_${STUDENT_ID} PII_LOW_${STUDENT_ID} FINANCIAL_SENSITIVE_${STUDENT_ID} SURVEILLANCE_RESTRICTED_${STUDENT_ID} DPDP_CONSENT_REQUIRED_${STUDENT_ID} SEBI_AUDIT_TRAIL_${STUDENT_ID}; do
    code=$(curl -s -o /dev/null -w "%{http_code}" -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
        "${ATLAS_URL}/api/atlas/v2/types/typedef/name/$t")
    echo "  $t: HTTP $code"
done
# expect: HTTP 200 for all 6
```

### Check 2 — `applier` log reports `attached >= 6`

The Step 4 final line shows `attached >= 6, missing` count is acceptable (often 0; up to 5 is fine if Hive metadata propagation is still catching up).

### Check 3 — `argus_${STUDENT_ID}_silver.member_master` shows ≥ 3 classifications in Atlas UI

Search the table in Atlas UI; the classification panel lists at least `PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, `DPDP_CONSENT_REQUIRED_${STUDENT_ID}`.

### Check 4 — Lineage Bronze → Silver → Gold visible

In Atlas UI, the lineage graph for `argus_${STUDENT_ID}_gold.alert_candidates` shows upstream connections back to `argus_${STUDENT_ID}_silver.order_events` and `argus_${STUDENT_ID}_silver.member_master`, which in turn trace back to `argus_${STUDENT_ID}_bronze.orders_raw` and `argus_${STUDENT_ID}_bronze.member_cdc`. The full chain should be visible without manual edges.

---

## Common failure mode — `attached = 0, missing = N`

**Symptom**: the applier runs cleanly, prints nothing in Phase 2 except `[miss]` lines, and reports `attached = 0`.

**Diagnosis**: Atlas doesn't know about any of the Hive entities. Two possible causes:

1. **The Hive Atlas hook is not enabled.** Cloudera Manager → Hive → Configuration → search for "atlas" → ensure `Hive Atlas Hook` is checked. Restart Hive after enabling.
2. **The qualified-name cluster suffix is wrong.** The applier uses `@default`. If your cluster is configured as `@cm` or some other cluster name, the lookups all 404. Check Atlas UI for any existing entity → look at its `qualifiedName` to find the suffix → update `apply_atlas_tags.py`'s `hive_column_qualified_name` and `hive_table_qualified_name` defaults.

**Fix sequence**:

```bash
# Confirm what cluster suffix Atlas is using by inspecting any existing entity:
curl -s -u ${ATLAS_USER}:${ATLAS_PASSWORD} \
    "${ATLAS_URL}/api/atlas/v2/search/basic?typeName=hive_table&query=argus_${STUDENT_ID}_bronze.orders_raw" \
    | jq '.entities[0].attributes.qualifiedName'
# expect something like "argus_${STUDENT_ID}_bronze.orders_raw@<cluster_name>"
```

Then patch `apply_atlas_tags.py` to use that suffix and re-run.

---

## Pass condition for CP-17

All four checks pass. The platform is now mapped — every PII column has a tag, every Gold column has a traceable derivation. The DPDP audit answer for "which tables hold investor X's PII" is now an Atlas search instead of a multi-week investigation.
