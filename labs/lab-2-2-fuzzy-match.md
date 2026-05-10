# Lab 2.2 — Fuzzy-Match Identity Resolution (CP-06)

> 👋 **Module 2 first-timer?** Read [`docs/module-2-primer.md`](../docs/module-2-primer.md) before this lab. It explains SCD2, fuzzy matching, and the multi-broker manipulation pattern. About 20 minutes.

> ℹ️ **Module:** 2 — Identity Resolution & Order Book Reconstruction
> **Closes deficiency:** ARG-2 part 1 (identity resolution)
> **Time:** ~75 minutes if it works first try; up to 2 hours if fuzzy-match thresholds need tuning.
> **Source files:** [`src/transform/job_05_silver_identity_resolution.py`](../src/transform/job_05_silver_identity_resolution.py)

> 💡 **Run this lab BEFORE Lab 2.1** even though the numbering suggests otherwise. Lab 2.1's order-book reconstruction joins against `silver.member_master` (JOB-05's output table). The Module 2 primer explains why labs are numbered this way.

## What you're going to do

In order:

1. **Confirm Bronze CDC is seeded** — `bronze.member_cdc` must have ~12,500+ investor rows. (~3 min)
2. **Run JOB-05** — populates `silver.member_master` with SCD2 records. (~20 min)
3. **Verify fuzzy matches resolved** — find the 5 entities with multi-element `known_aliases`. (~10 min)
4. **Confirm specific planted cases merged** — pick Case 10, verify all 3 variants share the same `entity_id`. (~10 min)
5. **Verify SCD2 effective windows** — confirm `effective_from` / `effective_to` semantics work. (~10 min)
6. **Verify CP-06 pass conditions** — four named checks. (~5 min)

Total: about 75 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 1.2](lab-1-2-bronze-ingest.md) is complete and Bronze tables are populated
- [ ] **Specifically `bronze.member_cdc` has rows** — quick check: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.member_cdc` should return > 5,000. If it's 0, the seed step in Lab 1.2 Step 5b wasn't run; run `python src/ingest/seed_member_cdc.py` now.
- [ ] You can submit Spark jobs to CDE — quick check: `cde job list` runs without error
- [ ] You can query Iceberg tables in Hue or Impala-Shell

## Why fuzzy matching matters — read this before Step 2

You might wonder: *if every investor has a PAN, can't we just join on PAN?* That feels intuitive. It's also wrong, and it's exactly the assumption that defeated MSE's legacy surveillance.

**Sophisticated manipulators distribute their activity across multiple identifiers on purpose.** A single human places coordinated orders through three brokers. They use a PAN typo at one broker (one digit different from their actual PAN). A name-spelling variant at another. Two demat accounts at the third. To the legacy MSE platform, that's three independent participants — none of whom individually crosses any manipulation threshold. To the ARGUS platform, after JOB-05 runs, it's **one canonical entity** whose combined activity easily clears the threshold.

The synthetic data plants exactly five such cases at indices 10–14. Each case has a base investor row plus two variants — one with a single-character PAN typo, one with a name spelling variant and a different broker. JOB-05's fuzzy-match logic must identify all three as a single canonical entity.

The trade-off: **2-of-3 signals must match** for a merge.
- Tighter (3-of-3): no false merges, but multi-broker manipulators slip through if any one signal is too divergent.
- Looser (1-of-3): every "Aarav Sharma" in the data merges into one entity (hundreds of false merges).

The 2-of-3 rule sits at the calibration sweet spot for the synthetic data. Production rules use weighted ML matchers across dozens of signals and get re-tuned monthly.

## Step 1 — Confirm Bronze CDC is seeded

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

> 💡 **The `INVESTOR` count of 12,500 includes the 12,495 base investors PLUS 10 fuzzy-match variants** (5 cases × 2 variants each — the base investor is already counted in 12,495). After JOB-05 runs, the output `member_master` will have ~12,495 unique entities — the 10 variants will have merged into 5 of those entities.

If `INVESTOR` count is below 5,000, the seed step in Lab 1.2 wasn't run — run it now:
```bash
python src/ingest/seed_member_cdc.py
```

## Step 2 — Run JOB-05

```bash
cde job create --name "argus-${STUDENT_ID}-job_05_identity_resolution" \
    --type spark \
    --application-file src/transform/job_05_silver_identity_resolution.py \
    --executor-memory 4g --executor-cores 4 --num-executors 4

cde job run --name "argus-${STUDENT_ID}-job_05_identity_resolution"
```

**Expected output** (in CDE job logs):

```
==> silver_identity_resolution: merged 12,495 entities into member_master
```

> 💡 **The merged count should match the BASE investor count, NOT base + variants.** If it shows 12,505 instead of 12,495, the fuzzy-match logic didn't merge the 10 variant rows — they were treated as 10 new investors. See Common Failure Mode #1 below.

> 💡 **Why isn't this a streaming job?** `silver.member_master` is rebuilt from scratch each run because SCD2 logic is much simpler in batch (compute the full history, write all rows) than in streaming (incrementally update effective_to on prior rows). The trade-off: identity resolution lags Bronze CDC by up to ~10 minutes (the JOB-05 cadence). For surveillance, 10-minute lag is fine; for real-time fraud blocking, you'd want streaming SCD2.

The job should take 2–10 minutes depending on cluster size. Monitor in CDE UI; once it shows COMPLETED, proceed.

## Step 3 — Verify fuzzy matches resolved

```sql
SELECT
    entity_id,
    SIZE(known_aliases) AS alias_count,
    known_aliases
FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current
  AND SIZE(known_aliases) > 1
ORDER BY alias_count DESC;
```

**Expected output:** **exactly 5 rows** — one per fuzzy-match case (indices 10–14). Each row's `known_aliases` array contains 2 or 3 values (the canonical key plus the variants that merged into it).

> 💡 **What `known_aliases` is:** a Spark array column that JOB-05 populates with all the `investor_acct` values that resolved to this entity. So Tarun's row has `known_aliases = ['INV-FUZZY-10', 'INV-FUZZY-10-V1', 'INV-FUZZY-10-V2']` — three accounts, one entity.

If you see **0 rows**, fuzzy matching didn't fire — see Common Failure Mode #2.
If you see **more than 5 rows** (especially many more), fuzzy matching is over-aggressive — see Common Failure Mode #1.

## Step 4 — Confirm specific planted cases merged

The synthetic generator's `compliance_test_cases.csv` lists the variants for each fuzzy case. Pick Case 10 and verify all variants resolve to a single `entity_id`:

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

**Expected output:** **3 rows that all share the same `entity_id`**. The investors have:
- Slightly different `investor_pan` values (single-char typo)
- Different `investor_email` (added suffix or domain variant)
- Different `member_firm_id` (the variants are at different brokers)

But the same `entity_id` — proof that JOB-05 saw past the surface differences.

> 💡 **What does `entity_id` actually look like?** It's a deterministic SHA-256 hash of the canonical key (typically the canonical PAN, name, and email after normalization). All variants of the same person produce the same hash; different people produce different hashes. This means `entity_id` is stable across re-runs of JOB-05 — important for joining historical analyses.

If only 1 or 2 rows share an `entity_id` (or each row has a different one), fuzzy match isn't resolving the variants. See Common Failure Mode #2.

## Step 5 — Verify SCD2 effective windows

```sql
-- For one of the merged entities, look at its SCD2 history
SELECT
    member_firm_id,
    investor_acct,
    effective_from,
    effective_to,
    is_current
FROM argus_${STUDENT_ID}_silver.member_master
WHERE entity_id = '<entity_id_from_step_4>'
ORDER BY effective_from;
```

**Expected output:** each constituent record (base + variants) has its own row with:
- `effective_from` populated (a timestamp from the CDC event)
- `effective_to` either populated (if a later CDC event superseded it) or NULL (if it's still current)
- At least one row has `is_current = TRUE`

> 💡 **Why are there multiple rows per entity?** Because SCD2: each variant's record is a separate temporal entry. If Tarun's V1 account at broker BNXM came online 2024-01-15 and his V2 account at broker KCAP came online 2024-08-30, those are two distinct historical events. The SCD2 chain captures both with their effective windows.

> 💡 **What does `is_current` semantically mean?** "This row's data is the latest known state for this entity." For a merged entity, exactly one of the constituent rows should have `is_current = TRUE` — typically the one with the latest CDC timestamp. The others have `is_current = FALSE` and `effective_to` set.

## Step 6 — Verify CP-06 pass conditions

CP-06 has **four checks**.

### Check 1 — `member_master` populated to expected count

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current;
```
**Pass if:** count matches the base investor count (~12,495 at scale 0.05). **Fail if:** count is much lower (rows missing) or much higher (variants weren't merged).

### Check 2 — Exactly 5 entities have multi-element `known_aliases`

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current AND SIZE(known_aliases) > 1;
```
**Pass if:** count = 5. **Fail if:** count ≠ 5.

### Check 3 — Each merged entity spans ≥ 2 broker firms

```sql
SELECT entity_id, COUNT(DISTINCT member_firm_id) AS firms
FROM argus_${STUDENT_ID}_silver.member_master
WHERE entity_id IN (
    SELECT entity_id FROM argus_${STUDENT_ID}_silver.member_master
    WHERE is_current AND SIZE(known_aliases) > 1
)
GROUP BY entity_id;
```
**Pass if:** 5 rows, each with `firms >= 2` (variants 10–14 use different brokers). **Fail if:** any entity shows `firms = 1` (variants didn't span brokers — synthetic data issue).

### Check 4 — `investor_pan_hash` populated for all rows

The hash column is what Module 7's DPDP §12 erasure workflow joins on. It must never be NULL or empty for non-NULL PANs:

```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.member_master
WHERE is_current
  AND investor_pan IS NOT NULL
  AND (investor_pan_hash IS NULL OR investor_pan_hash = '');
```
**Pass if:** 0. **Fail if:** > 0 — JOB-05 didn't compute the hash for some rows.

---

## Common failure mode #1 — Fuzzy match merges too aggressively (>> 5 rows)

**Symptom:** Step 3's `SIZE(known_aliases) > 1` query returns dozens or hundreds of rows.

**Cause:** the fuzzy-match thresholds are too loose for your synthetic data. Two unrelated investors with similar names plus a coincidentally-close demat prefix get merged.

**Diagnosis:** sample some of the over-merged entities:
```sql
SELECT entity_id, investor_acct, investor_pan, member_firm_id
FROM argus_${STUDENT_ID}_silver.member_master
WHERE entity_id IN (
    SELECT entity_id FROM argus_${STUDENT_ID}_silver.member_master
    WHERE is_current AND SIZE(known_aliases) > 1
)
ORDER BY entity_id LIMIT 30;
```
If you see entities where the merged rows clearly aren't the same person (different PANs entirely, different names, etc.), the algorithm is too loose.

**Fix:** in `src/transform/job_05_silver_identity_resolution.py`, tighten the name-similarity threshold:

```python
# Change this:
(F.levenshtein("a.name_norm", "b.name_norm") <= F.lit(2)).cast("int")

# To this:
(F.levenshtein("a.name_norm", "b.name_norm") <= F.lit(1)).cast("int")
```

Edit-distance 1 on the normalized name is much more restrictive. Re-run JOB-05 — `member_master` is rebuilt from scratch each run, so previous merges are corrected.

## Common failure mode #2 — Fuzzy match doesn't fire (0 rows with multi-aliases)

**Symptom:** Step 3's query returns 0 rows. Each variant got its own `entity_id`.

**Cause:** typically the synthetic data was regenerated with a non-default `--seed`, and the planted variants now don't satisfy any of the 3 signal rules.

**Diagnosis:**
```sql
-- Compare the actual PAN values of one fuzzy case
SELECT investor_acct, investor_pan, LENGTH(investor_pan) AS pan_len
FROM argus_${STUDENT_ID}_bronze.member_cdc
WHERE investor_acct IN (
    '<base_acct>', 'INV-FUZZY-10-V1', 'INV-FUZZY-10-V2'
);
```
If the PANs differ by more than 1 character, the typo rule won't catch them.

**Fix:** regenerate synthetic data with the canonical seed:
```bash
rm -rf data/generated/
python data/generate_data.py --seed 42 --out data/generated/
# Re-run Lab 1.2's seed step + this lab from Step 1
```

The `--seed 42` data is calibrated to ensure all planted variants satisfy the 2-of-3 fuzzy rule.

## Common failure mode #3 — JOB-05 fails with 'instrument_master not found'

**Symptom:** JOB-05 fails immediately with `Table 'argus_${STUDENT_ID}_silver.instrument_master' not found`.

**Cause:** JOB-05 reads `instrument_master` for context (some logic uses instrument metadata). The instrument master is populated by a separate batch loader that may not have run.

**Diagnosis:**
```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master;
```
If this errors with "table not found", the table doesn't exist (DDL not applied). If it returns 0, the table exists but is empty.

**Fix:** if the table doesn't exist, re-run the silver DDL:
```bash
impala-shell -q "$(cat sql/silver_ddl.sql | sed 's/\${STUDENT_ID}/'${STUDENT_ID}'/g')"
```
If it exists but is empty, run the instrument-master batch loader (often part of provisioning):
```bash
bash sql/provision_environment.sh
```

## Common failure mode #4 — JOB-05 succeeds but `entity_id` is NULL on every row

**Symptom:** `member_master` is populated but every row has `entity_id = NULL`. Step 3 returns 0 rows because `known_aliases` can't be sized.

**Cause:** the entity-ID computation function in JOB-05 hit a NULL input and silently produced NULL output. Most often this is because `investor_pan` is NULL on rows the script expected to have it.

**Diagnosis:**
```sql
SELECT
    SUM(CASE WHEN entity_id IS NULL THEN 1 ELSE 0 END)         AS null_entity,
    SUM(CASE WHEN investor_pan IS NULL THEN 1 ELSE 0 END)      AS null_pan,
    COUNT(*)                                                    AS total
FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current;
```
If `null_entity` and `null_pan` are both ~equal to `total`, the input rows had NULL PANs.

**Fix:** check `bronze.member_cdc` for NULL PANs:
```sql
SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.member_cdc
WHERE investor_acct IS NOT NULL AND investor_pan IS NULL;
```
If non-zero, the seed (`seed_member_cdc.py`) didn't populate PANs correctly. Re-run the seed; verify PANs land before re-running JOB-05.

---

## Pass condition for CP-06

All four checks pass:
- ✅ `member_master` populated to base investor count
- ✅ Exactly 5 entities have multi-element `known_aliases`
- ✅ Each merged entity spans ≥ 2 broker firms
- ✅ `investor_pan_hash` populated for all rows with non-NULL PANs

When all four pass, the manipulator-with-3-broker-accounts pattern that defeated rule-based surveillance can no longer hide on MSE.

## Wrap-up — what you can now do that you couldn't before

You can take a stream of CDC events from a KYC source and produce a canonical entity master with SCD2 history. You can identify multi-broker manipulators where one human distributes activity across PAN typos, name variants, and demat-ID variants. You can read JOB-05's fuzzy-match logic and predict which input rows will merge.

Most importantly: **`entity_id` is now the canonical join key for everything downstream.** Module 3 features will compute against `entity_id`, not `investor_acct`. Tarun's coordinated 3-broker activity becomes one trader's behavior, with combined volume that easily clears any manipulation threshold.

Lab 2.1 builds on this — using `silver.member_master` to enrich order events, then reconstructing the order book at any past timestamp via Iceberg time-travel. Allow about 60 minutes.
