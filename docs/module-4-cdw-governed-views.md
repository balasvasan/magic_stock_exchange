# Module 4 — Governed Views in CDW

> 📊 **Visual reference**: [Module 4 governed views by role](../assets/diagrams/05_module4_cdw_governed.md) ([SVG](../assets/diagrams/05_module4_cdw_governed.svg))

> 👋 **New to Cloudera Data Warehouse or Ranger policies?** Read [`docs/module-4-primer.md`](module-4-primer.md) first. About 10 minutes.


> **Closes deficiency:** ARG-5 part 1 — coarse-grained access; no row/column governance on PII
> **Day:** 6
> **Checkpoints:** CP-10, CP-11
> **Weight:** 10% of capstone

## What's broken

MSE's legacy surveillance database has table-level grants and nothing else. Every analyst with read access to `member_kyc` sees full PAN, email, mobile, and Aadhaar reference for all 24 million investors. The DPO has no separate role; she just queries the same tables as everyone else. There is no row-level filter for DPDP §6(4) consent withdrawal — when an investor withdraws analytics consent, MSE has no technical mechanism to enforce that withdrawal at query time, so any internal report that aggregates investor-level data continues to include their data anyway. The Show Cause Notice from SEBI didn't cite this directly, but the DPDP Rules 2025 phased rollout in November 2025 puts MSE in active enforcement scope, and a DPDP audit on the legacy stack would find a list of failures starting with "no row-level policy enforcement."

## What you build

A CDW (Apache Impala) layer of governed views over the Gold tables, with Ranger row-filter and column-masking policies that enforce role-aware access. Three view families serve three audiences:

- **Surveillance views** — for analysts working the alert queue. Full notional and feature payloads, but PAN / email / mobile / trader names are masked at query time by Ranger column policies.
- **Compliance views** — for the DPO and Compliance team. Full PII visible, plus the consent audit trail. The `vw_surveillance_audit` view is tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}` which means the DPDP §6(4) consent row filter does *not* apply — statutory surveillance under DPDP §7 legitimate-use exception.
- **Analytics views** — for the research team. No PII whatsoever, only aggregates, daily KPIs, and model-performance metrics.

Three Ranger policy families enforce the boundaries:

1. **`dpdp_consent_filter`** — row-level filter that removes consent-withdrawn investors from non-statutory queries
2. **`pii_column_mask`** — column-level masking on PAN, email, mobile, trader name; different masks for different roles
3. **`surveillance_time_bound_access`** — investigation_lead role gets full-PII access only during an active investigation case

## CDP services used

- **Cloudera Data Warehouse (CDW) / Apache Impala** — SQL serving, view materialization
- **Apache Ranger (SDX)** — row-filter and column-masking policies, role-based access control
- **Apache Atlas (SDX)** — classification tags (`PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, `SURVEILLANCE_RESTRICTED_${STUDENT_ID}`, `DPDP_CONSENT_REQUIRED_${STUDENT_ID}`, `SEBI_AUDIT_TRAIL_${STUDENT_ID}`, `FINANCIAL_SENSITIVE_${STUDENT_ID}`) drive the masking decisions

## Source files

| File | Purpose |
|---|---|
| [`sql/governed_views.sql`](../sql/governed_views.sql) | 7 views across surveillance / compliance / analytics tiers |
| [`sql/ranger_policies.sql`](../sql/ranger_policies.sql) | 3 policy families (9 individual policies across multiple tables) |

## Labs

| Lab | What it does | Checkpoint |
|---|---|---|
| [Lab 4.1 — Governed Views](../labs/lab-4-1-governed-views.md) | Deploy the 7 views; confirm role-based PAN masking works | CP-10 |
| [Lab 4.2 — Ranger Policies](../labs/lab-4-2-ranger-policies.md) | Deploy 3 Ranger policy families; verify cases 15–19 are filtered for non-statutory roles | CP-11 |

## Measurable outcome

By end of module:

- All 7 views compile and return rows
- A user assuming the `surveillance_analyst` role sees PAN as `XXXXX****X` in `vw_alert_queue`
- The same query as `compliance_dpo` returns the actual PAN value
- A query from `research_analyst` against `vw_member_analytics` filters out the 5 planted consent-withdrawal cases (15–19)
- The same query as `compliance_dpo` against `vw_surveillance_audit` returns those 5 investors normally — under the DPDP §7 statutory exception

## What this fixes

Before ARGUS, an analyst working a routine spoofing alert had read access to the full PAN of every investor on the platform. After ARGUS, the same alert lands in a view that masks PAN to `XXXXX****X` — enough to confirm the alert is on the right person without exposing the raw identifier. When the alert is escalated to a confirmed investigation, the investigation_lead role gains full-PII access for the duration of the case, and that access auto-revokes when the case closes. Every access is logged in Atlas; every classification on every column is queryable. A DPDP audit can be answered in minutes from `vw_consent_audit` rather than weeks of forensic engineering.

> 💡 **Tip:** Ranger's column-masking policies are evaluated at query time, not at view materialization. That means the same view (`vw_alert_queue`) returns different effective shapes for different users — a single view definition serves three roles. This is the right CDP pattern for governance: the view defines the columns, Ranger defines who sees what.

> ⚠️ **Compliance gate:** The DPDP §6(4) consent filter must NOT apply to views tagged `SEBI_AUDIT_TRAIL_${STUDENT_ID}`. If a consent-withdrawn investor was once involved in a confirmed manipulation case, the surveillance audit trail must continue to show their data — that's statutory under DPDP §7. Module 7's CP-19 (the COMPLIANCE GATE) tests this precisely: an erased investor's PII is gone from the operational tables, but their alert history persists in the audit views with their PAN replaced by hash.
