# Module 7 — SDX Governance & DPDP Compliance

> 📊 **Visual reference**: [Module 7 SDX governance + CP-19 gate](../assets/diagrams/08_module7_sdx_governance.md) ([SVG](../assets/diagrams/08_module7_sdx_governance.svg))

> **Closes deficiency:** ARG-5 part 2 — no data lineage, no consent governance, no DPDP-compliant erasure
> **Day:** 9
> **Checkpoints:** CP-17, CP-18, **CP-19 (COMPLIANCE GATE)**
> **Weight:** 15% of capstone

> ⚠️ **CP-19 is the COMPLIANCE GATE — non-negotiable.** Failing CP-19 means failing the capstone regardless of overall score. Read the [Lab 7.3](../labs/lab-7-3-erasure.md) procedure carefully before running.

## What's broken

MSE's surveillance platform processes personal data on millions of investors but cannot answer four questions a SEBI inspector or DPO would ask. Which downstream tables contain personal data of investor X? When was investor X's consent obtained, and what was the stated purpose? If investor X invokes their right to erasure under DPDP §12, can MSE demonstrate erasure across all systems? When an analyst opens an investigation file, who else accessed the file, and was each access logged with a justified business purpose?

There is no Atlas, no equivalent metadata service, no automated capture of derivation chains. Access control is coarse-grained table-level grants. Personal data fields (PAN, Aadhaar reference, registered email, mobile, demat account) are mixed into operational tables with no classification tagging. Consent records exist in the broker member firm's KYC system but are not federated into MSE's surveillance data plane, so purpose limitation under DPDP §6(4) cannot be enforced at query time. Erasure requests are handled manually by a junior analyst running ad-hoc DELETE statements — a process that has produced two known referential-integrity incidents.

Under DPDP, the Data Protection Board can impose penalties up to ₹250 crore for failure to implement reasonable security safeguards. The DPDP Rules 2025 took effect November 2025, putting MSE in active enforcement scope. SEBI separately requires audit-trail integrity for all surveillance data. Independent estimate of regulatory exposure under combined DPDP + SEBI scrutiny: ₹150–300 crore worst case.

## What you build

Three governance pillars on top of the SDX (Shared Data Experience) layer.

**Atlas classifications** apply six tags — `PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, `FINANCIAL_SENSITIVE_${STUDENT_ID}`, `SURVEILLANCE_RESTRICTED_${STUDENT_ID}`, `DPDP_CONSENT_REQUIRED_${STUDENT_ID}`, `SEBI_AUDIT_TRAIL_${STUDENT_ID}` — to specific columns and tables across Bronze, Silver, and Gold. The tag definitions live in `atlas_classifications.json` (PRD-locked) and are applied via `apply_atlas_tags.py` against the Atlas REST API. Once applied, Atlas captures lineage automatically as Spark and Impala write through the layers, so you can trace any Gold column back to its Bronze sources without writing custom code.

**DPDP §6(4) consent withdrawal enforcement** (`ccpa_optout_enforcement.py` — kept that filename per framework convention; the actual regulation is DPDP §6(4)). When an investor withdraws consent for non-statutory purposes (analytics, marketing), the script updates `argus_${STUDENT_ID}_silver.member_master.consent_status` and writes a row to `argus_${STUDENT_ID}_gold.consent_audit` with pre/post Iceberg snapshot IDs. Statutory purposes (TRADING, SURVEILLANCE) cannot be withdrawn — they're covered by DPDP §7 legitimate-use exception. The Module 4 Ranger row-filter `dpdp_consent_filter` picks up the new consent_status at next query time without any application change.

**DPDP §12 erasure with Iceberg time-travel proof** (`gdpr_erasure_workflow.py` — same naming caveat). When an investor invokes the right to erasure, the script (1) writes a request audit row capturing pre-action snapshot IDs, (2) deletes PII rows from the operational Bronze and Silver tables that hold them, (3) sweeps Milvus for any embeddings derived from the investor's data, (4) writes a completion audit row with both pre-action and post-action snapshot IDs. The audit row's snapshots let any future SEBI / DPO inquiry run `SELECT … FROM table FOR SYSTEM_VERSION AS OF <pre_snap>` to see the data existed before erasure, and `FOR SYSTEM_VERSION AS OF <post_snap>` to see it gone — definitive cryptographic-grade evidence that the erasure happened. The `consent_audit` table itself has `history.expire.enabled=false` (Day 1 DDL), so the audit row never expires.

Surveillance / order / trade data tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}` is **retained** under DPDP §7 statutory exception with the natural-identity link severed via SHA-256 hashing — k-anonymized retention. SEBI's 8-year archival requirement is satisfied; the investor's PAN is gone from every operational table while their hashed surveillance record persists.

## CDP services used

- **Apache Atlas (SDX)** — classification tags, automatic lineage capture, entity GUID lookup, REST API
- **Apache Ranger (SDX)** — enforcement of consent filter and column masks (Module 4 deployed; Module 7 verifies)
- **Apache Iceberg time-travel** — `FOR SYSTEM_VERSION AS OF` queries against captured snapshot IDs prove erasure
- **Cloudera AI Inference / Milvus** — vector store sweep on erasure
- **Cloudera Data Engineering / Spark** — runs the consent + erasure workflow scripts as authorized DPO jobs

## Source files

| File | Purpose |
|---|---|
| [`src/governance/atlas_classifications.json`](../src/governance/atlas_classifications.json) | 6 PRD-locked tag definitions with column targets |
| [`src/governance/apply_atlas_tags.py`](../src/governance/apply_atlas_tags.py) | Idempotent Atlas REST applier |
| [`src/governance/ccpa_optout_enforcement.py`](../src/governance/ccpa_optout_enforcement.py) | DPDP §6(4) consent withdrawal workflow |
| [`src/governance/gdpr_erasure_workflow.py`](../src/governance/gdpr_erasure_workflow.py) | DPDP §12 erasure with Iceberg time-travel proof |

## Labs

| Lab | What it does | Checkpoint |
|---|---|---|
| [Lab 7.1 — Atlas Classifications](../labs/lab-7-1-atlas-classifications.md) | Apply 6 tags; verify lineage captured Bronze→Gold | CP-17 |
| [Lab 7.2 — DPDP §6(4) Consent Filter](../labs/lab-7-2-consent-withdrawal.md) | Withdraw consent for cases 15–19; verify Ranger filter; verify DPO bypass | CP-18 |
| [Lab 7.3 — DPDP §12 Erasure (COMPLIANCE GATE)](../labs/lab-7-3-erasure.md) | Erase cases 20–22; prove with `FOR SYSTEM_VERSION AS OF`; verify audit preserved | **CP-19** |

## Measurable outcome

By end of module:

- All 6 Atlas classifications applied; lineage graph in Atlas UI shows full Bronze→Silver→Gold chain
- Cases 15–19 (5 planted DPDP §6(4) withdrawals) correctly filtered from non-statutory queries; visible to DPO via `vw_surveillance_audit`
- Cases 20–22 (3 planted DPDP §12 erasures) erased from operational tables; `argus_${STUDENT_ID}_gold.consent_audit` rows preserved with pre/post snapshot IDs
- `FOR SYSTEM_VERSION AS OF <pre_snap>` query returns the erased investor's row; same query AS OF current returns 0 rows
- Statutory tables tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}` retain the surveillance record without natural-identity columns

## What this fixes

Before ARGUS, a DPDP audit on MSE would produce a list of failures starting with "no row-level policy enforcement" and ending with "no demonstrable erasure capability." After ARGUS, the same audit is answered in minutes from `argus_${STUDENT_ID}_views.vw_consent_audit` — every consent grant, every modification, every withdrawal, every erasure request, every completion, with cryptographic snapshot IDs that prove what existed when. SEBI's 8-year retention requirement is satisfied via the `SEBI_AUDIT_TRAIL_${STUDENT_ID}` tag's bypass of the consent filter. The ₹150–300 crore regulatory exposure that defined ARG-5 is, in principle, eliminated.

> 💡 **Tip:** The most common reason CP-19 fails is forgetting the `history.expire.enabled=false` property on `argus_${STUDENT_ID}_gold.consent_audit`. If Iceberg expires the snapshots, the time-travel queries return `Cannot find snapshot with id X` and the proof is gone. Day 1's DDL sets the property; Module 7 verifies it. If you've manually altered the table since, restore the property: `ALTER TABLE argus_${STUDENT_ID}_gold.consent_audit SET TBLPROPERTIES ('history.expire.enabled' = 'false')`.

> ⚠️ **CP-19 is the COMPLIANCE GATE.** Failure here is non-negotiable. The capstone framework treats CP-19 as a separate pass condition: if the overall score is 95% but CP-19 fails, the capstone score is FAIL. The reason is operational: a surveillance platform that can't prove erasure can't be deployed at any Indian financial-services customer in 2026. Take the time to validate every step of Lab 7.3.
