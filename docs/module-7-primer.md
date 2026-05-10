# Module 7 Primer — Read This Before Lab 7.1

> 📊 **Visual reference**: [Module 7 SDX governance flow](../assets/diagrams/08_module7_sdx.md) ([SVG](../assets/diagrams/08_module7_sdx.svg))

> 👋 **New to data-protection regulation, Atlas, or Ranger?** This primer is for you. Module 7 is the **compliance module** — and CP-19 is the COMPLIANCE GATE. Failing CP-19 means failing the capstone. About 25 minutes; please read it carefully.

This is a **primer**, not a procedure. The actual hands-on work is in `labs/lab-7-1-atlas-classifications.md` through `labs/lab-7-3-erasure.md`. Read this first; do those next.

If you've worked with PII regulation (DPDP, GDPR, CCPA) before, you can skim. If you haven't, please read every section — Module 7's labs ask you to *prove* compliance, and you can't prove what you don't understand.

## The big picture in one paragraph

Module 7 is where ARGUS becomes auditable to a regulator. Every event the platform ingests (orders, KAVACH KYC records, trader profiles, consent decisions) gets a classification tag at ingest time. Every transformation the platform performs gets recorded as lineage. When a person exercises their DPDP §12 right to erasure, the platform deletes their data and proves the deletion held by querying a *prior* Iceberg snapshot and showing the data was there before, gone after, with a verifiable timestamp. **The capstone passes or fails on CP-19's ability to demonstrate this end-to-end** — not just "I deleted some rows" but "here is cryptographic evidence the rows were deleted, and the audit trail is itself immutable." If you remember nothing else from this primer, remember: **CP-19 is the compliance gate, and it requires Iceberg time-travel proof, not just a DELETE statement**.

## The technologies and concepts you'll meet in Module 7

Six things matter today.

### India's DPDP Act (Digital Personal Data Protection Act, 2023)

The DPDP Act is India's general data protection law, enacted in August 2023 and operationally enforced from late 2024. It applies to MSE because MSE processes personal data of Indian residents (KAVACH KYC records, trader contact details, consent decisions).

Five sections of DPDP matter for ARGUS:

- **§6** — A Data Principal (the person whose data it is) can withdraw consent at any time. The "Data Fiduciary" (MSE) must stop processing within "a reasonable time".
- **§6(4)** — Consent withdrawal does not affect data the Fiduciary is *legally obligated* to retain under another law. For MSE, that means **PMLA (Prevention of Money Laundering Act) records cannot be deleted on consent withdrawal alone** — they must be kept for the statutory period.
- **§8** — A Data Fiduciary must publish how it processes personal data, including categories collected, purposes, and retention periods.
- **§12** — A Data Principal can request **erasure** of personal data when the purpose has been fulfilled or consent is withdrawn (subject to §6(4)'s legal-obligation carve-out). The Fiduciary must comply within "a reasonable time" — interpreted operationally as 30 days.
- **§14** — A Data Principal has the right to nominate someone to exercise these rights on their death or incapacity.

The lab focuses on §6(4) (consent withdrawal with PMLA carve-out) in CP-18 and §12 (erasure with proof) in CP-19.

> ⚠️ **The PMLA carve-out is non-trivial.** A trader withdraws consent. Their general activity logs can be deleted. But trades that are part of a confirmed manipulation case or marked under PMLA Regulation 2(1)(da) are *legally required* to be retained for 5 years (or longer if the case is open). Module 7 makes you build the SQL filter that distinguishes these. Get it wrong → either you delete data you legally must keep (regulatory violation) or you keep data you legally must delete (privacy violation). Both are bad.

### Apache Atlas — classifications and lineage

Atlas is the **metadata catalog**. Two things it does for us:

**Classifications** — attach a tag (e.g. `PII_HIGH`, `STATUTORY_RECORD`, `CONSENT_REQUIRED`) to a column or table. Tags are inherited downstream automatically: tag a Bronze column, and any Silver/Gold column derived from it gets the tag too. This is how `member_firm_id` (tagged `PII_LOW` in Bronze) propagates the same tag through every Silver and Gold table that uses it, without anyone re-tagging anything.

**Lineage** — record the data flow: which tables fed into which tables, via which job. When a regulator asks "where did this Gold alert's PAN-number derived score come from?" the answer is a lineage graph that goes back to the original Bronze ingest. Atlas builds this graph automatically from Spark and Hive query plans.

In Module 7 you'll deploy 6 classification tags via `src/governance/apply_atlas_tags.py`, attach them to the right columns, and verify lineage propagates them downstream.

> 💡 **Why "tags" instead of just naming the column `pii_high_member_firm_id`?** Because column names are stable, but classification policy changes. Today `member_firm_id` is `PII_LOW`. If SEBI's interpretation tightens next year and member firm IDs become `PII_HIGH`, you change one tag in Atlas and every downstream policy adjusts. Renaming columns to follow policy would mean rewriting every query in the platform.

### Apache Ranger — policies and row-level filtering

Ranger is the **authorization layer**. It enforces who can see what. Two flavors of policies you'll meet:

**Tag-based access policies** — written against Atlas classifications, not table names. A policy like "users in role `surveillance_l1` cannot see columns tagged `PII_HIGH`" applies *automatically* to every column with that tag, in every table, present and future. You don't have to maintain a list.

**Row-level filtering policies** — restrict which rows a query returns, based on the user's role. The DPDP §6(4) example: "if user is in role `compliance_dpo` and they're querying `legacy_alerts`, exclude rows where the underlying member has consented withdrawal AND the row is not flagged as PMLA-statutory."

Ranger is how the lab demonstrates "non-statutory data is hidden from the DPO after consent withdrawal" — not by physically deleting (that's CP-19), but by row-filtering the query results.

### Iceberg time-travel — the audit mechanism

This is the most important concept in Module 7, because **CP-19 is built on it**.

When you DELETE rows from an Iceberg table, the rows are not actually erased from S3 — they're marked as deleted in a new snapshot. The old snapshot (with the rows still present) remains queryable for as long as the table's snapshot retention allows. You can write:

```sql
SELECT * FROM consent_audit FOR SYSTEM_VERSION AS OF 12345
```

...and get the table state as of snapshot 12345.

This is normally a feature for debugging and rollback. In Module 7, **it's the audit mechanism**. Here's the proof flow CP-19 demands:

1. **Before erasure**: snapshot N. Trader Tarun's PAN appears in `consent_audit`.
2. **You execute the erasure** workflow. This produces snapshot N+1.
3. **After erasure**: snapshot N+1. Tarun's PAN is gone from `consent_audit`.
4. **Audit query (any time later)**: query `FOR SYSTEM_VERSION AS OF N` and see Tarun's row was there. Query `FOR SYSTEM_VERSION AS OF N+1` and confirm it's not. The transition between snapshots is the proof.

For this to work, snapshots must not be expired. ARGUS sets `history.expire.enabled=false` on `consent_audit` (and you'll verify this in Lab 7.3 pre-flight). If snapshot N gets expired, the audit trail is broken. **Don't skip the pre-flight check.**

> ⚠️ **Why time-travel rather than a separate audit log?** Because a separate audit log can be tampered with — someone deletes the audit row alongside the data. Iceberg snapshots are append-only by design; you can't rewrite history. The proof is structural, not policy.

### The "non-statutory data" filter (DPDP §6(4) ↔ PMLA Regulation 2(1)(da))

This is the trickiest single concept in Module 7. Here's the whole thing in one diagram:

```
                           ┌─────────────────────┐
                           │ Trader withdraws    │
                           │ consent             │
                           └──────────┬──────────┘
                                      │
                           ┌──────────▼──────────┐
                           │ For each row in     │
                           │ legacy_alerts       │
                           │ tied to this trader │
                           └──────────┬──────────┘
                                      │
                          ┌───────────▼───────────┐
                          │ Is the row tagged as  │
                          │ STATUTORY_RECORD or   │
                          │ part of a confirmed   │
                          │ PMLA case?            │
                          └─────┬───────────┬─────┘
                                │           │
                              YES          NO
                                │           │
                    ┌───────────▼─┐   ┌─────▼────────────┐
                    │ KEEP        │   │ HIDE from DPO    │
                    │ (PMLA       │   │ via Ranger row   │
                    │  retention) │   │ filter (CP-18)   │
                    └─────────────┘   └─────┬────────────┘
                                            │
                                  ┌─────────▼─────────┐
                                  │ At §12 erasure:   │
                                  │ DELETE the row    │
                                  │ via Iceberg, then │
                                  │ prove via         │
                                  │ time-travel       │
                                  │ (CP-19)           │
                                  └───────────────────┘
```

CP-18 and CP-19 are the two halves: filter (CP-18 — make it invisible) followed by delete-with-proof (CP-19 — physically erase). **Both are required for §6/§12 compliance.**

### MLflow registry signature checks (returns in Module 7)

You met MLflow in Module 5. In Module 7, you'll verify that the model registered there has not been silently swapped or modified — by checking its registry signature against an expected hash. This is part of CP-17's lineage integrity proof: the regulator should be able to verify that the model that scored a specific alert is the model that's currently deployed.

## What Module 7 closes — ARG-5 (and finishes ARG-4)

ARG-5 has three parts:

1. **No data lineage**: legacy MSE couldn't answer "where did this alert score come from" — CP-17 (Atlas + lineage) closes this.
2. **No consent management**: legacy MSE had no way to honor a consent withdrawal — CP-18 (DPDP §6(4) with row filter) closes this.
3. **No erasure capability**: legacy MSE couldn't delete personal data on request — CP-19 (DPDP §12 with time-travel proof) closes this.

ARG-4 (40-minute hand-written STRs, the SAR drafting bottleneck) was substantially closed in Module 6. Module 7 verifies that **the STRs themselves are auditable**: each generated STR has provenance pointing back to the underlying alerts and source events.

## What Module 7 sets up — the lab-by-lab map

Three labs, in order:

| Lab | What you do | Checkpoint | Time budget |
|---|---|---|---|
| 7.1 — Atlas classifications + lineage | Deploy 8 classification tags, attach to columns, verify lineage propagates | CP-17 (lineage captured, all tags applied) | ~60 min |
| 7.2 — DPDP §6(4) consent withdrawal | Implement row-level filter for non-statutory data; verify DPO sees only statutory rows after withdrawal | CP-18 (consent enforcement) | ~45 min |
| 7.3 — DPDP §12 erasure with proof | Pre-flight Iceberg snapshot retention; execute erasure; query time-travel for proof | **CP-19 — COMPLIANCE GATE** | ~75–90 min |

Total Module 7 wall-clock: about 4–5 hours of hands-on work. Reading time, troubleshooting, and instructor Q&A roughly doubles that. **Budget the full Day 9** for Module 7 and don't try to compress it.

## Things you'll find confusing the first time, and what to do about them

### "Atlas tags don't seem to apply to the columns I asked them to"

The most common cause: a hostname mismatch between Atlas's qualified-name format and the actual table address. Atlas qualified names look like `argus_s001_bronze.orders_raw@cluster_name`. If your script generates qualified names with a different cluster suffix than what Atlas is using, the API accepts the request (HTTP 200) but the tag attaches to nothing. Lab 7.1's Common Failure Mode #1 walks through the diagnosis — read it carefully if your tag count comes out as 0.

### "DPDP §6(4) sounds like I should delete things, but the lab tells me to filter, not delete"

Right. §6(4) is consent withdrawal with the PMLA carve-out: data legally required to be retained stays, but everything else becomes invisible. Filtering (Ranger row filter) is the right primitive — actual deletion happens in §12 (CP-19) for non-statutory data only. If you delete in §6(4), you'll fail CP-18 because a regulator query against `consent_audit` will show data missing that should still legally exist.

### "Time-travel queries fail with 'snapshot not found'"

Two common causes: (a) the snapshot expired (Iceberg's default snapshot retention is 5 days), or (b) you're using the wrong snapshot ID. Lab 7.3 has a pre-flight that disables expiration on `consent_audit` *for the lifetime of the lab*. **Don't skip it.** If you do, your snapshots from earlier in the day may be gone by the time CP-19 runs.

### "I can delete a row but I can't prove the deletion held"

That's exactly what CP-19 demands you fix. The proof has two parts: (1) snapshot N has the row, (2) snapshot N+1 doesn't. You write the queries; Iceberg provides the structural guarantee that snapshot N can't be retroactively modified. The "proof" is the diff between the two snapshots, captured immutably by Iceberg.

### "The same trader appears under multiple member firms — does that affect erasure?"

Yes — this is exactly what Module 2's identity resolution fixed. Tarun Patel might be `BNXM-T-00042` under one broker and `KCAP-T-00891` under another (the multi-broker manipulation pattern). DPDP §12 erasure operates on the *Data Principal* — the person — not the per-broker ID. So when Tarun requests erasure, you delete *both* personae's rows, not just the one he submits. Module 2's `entity_master` table is what links them; you'll use its `entity_id` as the deletion key.

### "Why can't I just write 'DELETE FROM consent_audit WHERE pan = ...' and call it done?"

Three reasons. (1) Ranger may not let you — production roles separate `compliance_dpo` (can SELECT, can authorize deletes) from `compliance_admin` (can execute deletes). (2) The deletion has to leave an audit trail in `consent_audit` itself before the row is gone. (3) The proof — CP-19 — requires the Iceberg snapshot diff. A direct DELETE without the workflow loses the audit context. Use `src/governance/gdpr_erasure_workflow.py`, which encodes the full sequence; don't shortcut.

## Success at end of Module 7

By the time you finish Lab 7.3, you should be able to:

- Explain why Atlas tags propagate downstream automatically and why that matters for compliance auditing
- Distinguish between DPDP §6 (general consent), §6(4) (consent withdrawal with statutory carve-out), and §12 (erasure right)
- Identify which rows in `legacy_alerts` are PMLA-statutory and which are not, by SQL alone
- Execute a Ranger row-filter policy and verify a DPO role sees what they're supposed to and not what they aren't
- Execute the §12 erasure workflow and produce two Iceberg snapshot IDs that prove the data was there and is now gone
- Write a forensic audit query — given an erasure timestamp, retrieve the pre-erasure state of any specific row

If any of those feel impossible right now, that's expected — that's why we have the labs. By Day 9 evening, all six should feel routine.

## What's NOT in Module 7

Module 7 is governance and compliance. **It does not do**:

- New ML model training (that was Module 5)
- New STR drafting (that was Module 6)
- New ingest paths (Module 1)
- Performance tuning (Module 1, Module 5)

If you find yourself thinking "but the model accuracy could be better" or "but ingestion latency could be lower" — those are Modules 5 and 1, both finished. Today, just focus on proving the platform is auditable and DPDP-compliant.

---

When you're ready, head to [Lab 7.1 — Atlas Classifications & Lineage](../labs/lab-7-1-atlas-classifications.md). Allow about 60 minutes if everything works first try.
