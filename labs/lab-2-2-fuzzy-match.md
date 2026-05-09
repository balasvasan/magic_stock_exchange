# Lab 2.2 — Fuzzy-Match Identity Resolution (CP-06)

> ℹ️ **Module:** 2 — Identity Resolution & Order Book Reconstruction
> **Closes deficiency:** ARG-2 part 1 (identity resolution)
> **Source files:** [`src/transform/job_05_silver_identity_resolution.py`](../src/transform/job_05_silver_identity_resolution.py)

## Objectives

- Run JOB-05 to populate `argus_${STUDENT_ID}_silver.member_master` with SCD2 effective windows
- Verify all 5 fuzzy-match cases (10–14) are merged into single canonical entities
- Understand the multi-signal fuzzy-match algorithm and why exact-match alone is inadequate

## Why this matters

Sophisticated manipulators distribute their activity across multiple identifiers. A single human places coordinated orders through three brokers using a PAN typo here, a name spelling variant there, and two demat accounts at different brokers. To the legacy MSE platform that's three independent participants, none of which individually crosses a manipulation threshold. To the ARGUS platform — after JOB-05 runs — it's one canonical entity whose combined activity easily clears the threshold.

The synthetic data generator plants exactly five such cases at indices 10–14. Each case has a base investor row plus two variants: one with a single-character PAN typo, one with a name spelling variant and a different broker. JOB-05's fuzzy-match logic must identify all three as a single canonical entity.

## Procedure

### Step 1 — Confirm Bronze CDC is seeded

```sql
SELECT entity_kind, COUNT(*) FROM (
    SELECT
        CASE
            WHEN investor_acct IS NOT NULL THEN 'INVESTOR'
            WHEN trader_id    IS NOT NULL THEN 'TRADER'
            ELSE 'MEMBER'
        END AS entity_kind
    FROM argus_${STUDENT_ID}_bronze.member_cdc
) t GROUP BY entity_kind;
```

**Expected output** (at lab scale 0.05):

| entity_kind | count |
|---|---:|
| MEMBER | 380 |
| TRADER | ~600 |
| INVESTOR | ~12,500 |

The 12,500 includes the 12,495 base investors **plus 10 fuzzy-match variants** (5 cases × 2 variants each — the base investor is already counted).

If `INVESTOR` count is below 5,000, the seed step in Lab 1.2 wasn't run — go back and run `seed_member_cdc.py`.

### Step 2 — Run JOB-05

```bash
cde job create --name argus-job_05_identity_resolution \
    --type spark \
    --application-file src/transform/job_05_silver_identity_resolution.py \
    --executor-memory 4g --executor-cores 4 --num-executors 4

cde job run --name argus-job_05_identity_resolution
```

**What you should see**:

```
==> silver_identity_resolution: merged 12,495 entities into member_master
```

The merged count should match the base investor count, NOT the base + variant count. That's because the 5 fuzzy-match cases (10 variant rows) are merged INTO their base investors — not added as new entities.

### Step 3 — Verify fuzzy matches resolved

```sql
-- Look for entities whose known_aliases array has > 1 element
SELECT
    entity_id,
    SIZE(known_aliases) AS alias_count,
    known_aliases
FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current
  AND SIZE(known_aliases) > 1
ORDER BY alias_count DESC;
```

**Expected output**: 5 rows — one per fuzzy-match case (indices 10–14). Each row's `known_aliases` array contains 2 or 3 values (the canonical key plus the variants that merged into it).

### Step 4 — Confirm specific planted cases merged

The synthetic generator's `compliance_test_cases.csv` lists the variants for each fuzzy case. Pick case 10 and verify all variants resolve to a single `entity_id`:

```sql
-- Substitute the actual variants from data/generated/compliance_test_cases.csv
SELECT
    investor_acct,
    member_firm_id,
    investor_pan,
    investor_email,
    entity_id
FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current
  AND investor_acct IN (
      '<base_acct_from_csv>',
      'INV-FUZZY-10-V1',
      'INV-FUZZY-10-V2'
  )
ORDER BY investor_acct;
```

**Expected output**: 3 rows that all share the same `entity_id`. The investors have slightly different `investor_pan` values (single-char typo) and `investor_email` (added suffix) but the resolution logic recognized them as the same human.

If only 1 or 2 rows share an `entity_id`, the fuzzy-match thresholds are too strict for your synthetic data — most likely cause is the `levenshtein_le_one` rule on PAN strings is failing because the typo flipped the wrong character. The generator uses position-modulo-26 letter rotation so all typos are within edit-distance 1, but if you regenerated data with a different `--seed`, the typos might not satisfy the rule. Re-generate with `--seed 42`.

### Step 5 — Verify SCD2 effective windows

```sql
-- For one of the merged entities, look at its SCD2 history
SELECT
    member_firm_id,
    investor_acct,
    effective_from,
    effective_to,
    is_current
FROM argus_${STUDENT_ID}_silver.member_master
WHERE entity_id = '<one-of-the-merged-ids>'
ORDER BY effective_from;
```

**Expected output**: each constituent record (base + variants) has its own row with `effective_from` populated and `effective_to` either populated (if a later CDC superseded it) or NULL (if it's still current). At least one row should have `is_current = TRUE`.

## Checkpoint CP-06 — All 5 fuzzy-match cases merged

### Pass condition

All four checks pass.

### Check 1 — `member_master` populated

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current;
```

**Expected output**: matches the base investor count from `data/generated/investors.csv` (no inflation from variant rows).

### Check 2 — Exactly 5 entities have multi-element `known_aliases`

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current AND SIZE(known_aliases) > 1;
-- expect: 5
```

### Check 3 — Each merged entity has at least 2 different `member_firm_id` values across its history

```sql
SELECT entity_id, COUNT(DISTINCT member_firm_id) AS firms
FROM argus_${STUDENT_ID}_silver.member_master
WHERE entity_id IN (
    SELECT entity_id FROM argus_${STUDENT_ID}_silver.member_master
    WHERE is_current AND SIZE(known_aliases) > 1
)
GROUP BY entity_id;
-- expect 5 rows, each with firms >= 2 (variants 10-14 use different brokers)
```

### Check 4 — `investor_pan_hash` is consistent within an entity

The `investor_pan_hash` column is what Module 7's DPDP §12 erasure workflow joins on. For an entity that merged across variants with PAN typos, each constituent row will have a *different* hash (because the PAN strings differ by 1 char). That's fine — the entity_id is the canonical join key, not the hash. But each row's hash must be the SHA-256 of its own PAN string, not NULL or empty:

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current
  AND investor_pan IS NOT NULL
  AND (investor_pan_hash IS NULL OR investor_pan_hash = '');
-- expect: 0
```

---

## Common failure mode — fuzzy match merges too aggressively

**Symptom**: more than 5 rows have `SIZE(known_aliases) > 1`, sometimes hundreds. Two unrelated investors with similar names are being merged.

**Diagnosis**: the fuzzy-match rule requires "at least 2 of 3 signals." If your synthetic data has heavy name-collision (two investors named Aarav Sharma at the same broker) plus a coincidentally-close demat prefix, the algorithm merges them.

**Fix**: tighten the name-similarity threshold. In `job_05_silver_identity_resolution.py`, change:

```python
(F.levenshtein("a.name_norm",  "b.name_norm") <= F.lit(2)).cast("int")
```

to:

```python
(F.levenshtein("a.name_norm",  "b.name_norm") <= F.lit(1)).cast("int")
```

Edit-distance 1 on the normalized name is much more restrictive and reduces false merges. Re-run JOB-05 — `member_master` is rebuilt from scratch each run, so the previous incorrect merges are corrected.

This is the kind of trade-off that real production identity resolution has to manage continuously: too loose = false merges and lost privacy; too tight = missed manipulation. The lab's setting is calibrated for the synthetic data, but in production you'd run a validation harness against a labeled set of known matches monthly.

---

## Pass condition for CP-06

All four checks pass. When this passes, the manipulator-with-3-broker-accounts pattern that defeats rules-based surveillance can no longer hide on MSE.
