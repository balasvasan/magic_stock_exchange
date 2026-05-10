# Module 4 Primer — Read This Before Lab 4.1

> 📊 **Visual reference**: [Module 4 governed views + Ranger](../assets/diagrams/05_module4_cdw.md) ([SVG](../assets/diagrams/05_module4_cdw.svg))

> 👋 **New to Cloudera Data Warehouse, governed views, or Ranger?** This primer is for you. About 10 minutes — Module 4 is the most concept-light module, but the policy layer matters for Module 7's compliance work.

This is a **primer**, not a procedure. The actual hands-on work is in Module 4's two labs. Read this first.

## The big picture in one paragraph

Module 4 is the **governance layer**. Modules 1–3 produced raw events and feature tables; Modules 5–6 will train models and draft STRs on them. **Between those layers**, Module 4 wraps everything in *governed views* (Lab 4.1) and applies *Ranger policies* (Lab 4.2) so the right people see the right data and the wrong people don't see PII. Two short labs: deploy 7 views over the Gold layer, deploy 3 Ranger policy families on top. By the end of Module 4, an analyst querying `vw_surveillance_audit` sees member-firm context but masked PAN; the DPO querying the same view sees full PAN; a research analyst querying `member_temporal_features` sees only the rows for investors who haven't withdrawn analytics consent. The data is one set; the views and policies make it appear differently to different roles.

## Concepts you'll meet

### Cloudera Data Warehouse (CDW) — the SQL analytics layer

CDP's CDW is a managed Apache Impala (and Hive) deployment optimized for BI-style SQL queries. Where CDE/Spark is for jobs that build datasets, **CDW is for queries that consume them**. Analysts in BI tools (Tableau, Cloudera Data Visualization) connect to CDW; SQL queries run on Impala against the Iceberg tables in S3.

Why have CDW at all when Spark can also run SQL? Because:
- **Impala is faster for interactive queries** (sub-second on Gold tables vs Spark's 10–30s for the same query). Impala's planner is tuned for BI; Spark's is tuned for ETL.
- **Impala respects Ranger policies natively** with column-masking, row-filtering, and tag-based access — Spark needs more wiring.
- **CDW handles concurrency** for many analyst sessions; a single Spark cluster gets bottlenecked.

In Module 4, you'll deploy the views via `impala-shell` and verify the views work in Hue or any Impala client.

### Governed views — controlled access patterns

A "governed view" is just a SQL VIEW, but with one difference from a typical analyst's ad-hoc view: **it's the canonical access path that applications and BI tools are required to use** (raw Gold tables don't get analyst-direct grants). The view does three things:

1. **Pre-joins** related tables (alert_candidates × member_master × instrument_master) so analysts don't have to remember join keys
2. **Filters** to the analyst-relevant subset (e.g., `disposition = 'PENDING'` on the alert queue)
3. **Provides a stable interface** — the underlying tables can change, but the view contract holds

ARGUS deploys 7 governed views into `argus_${STUDENT_ID}_views`:
- `vw_alert_queue` — pending alerts for analyst triage
- `vw_member_analytics` — member-level summary for research queries
- `vw_cross_product_alerts` — Jane Street-pattern alerts specifically
- `vw_surveillance_audit` — full audit with PAN, tagged for compliance
- `vw_consent_audit` — DPDP audit trail (populated by Module 7)
- `vw_kpi_daily` — daily platform metrics (populated by Module 5)
- `vw_model_performance` — ML model performance metrics (populated by Module 5)

Lab 4.1 deploys these and verifies they work.

### Ranger policies — three flavors

Apache Ranger handles authorization. ARGUS uses three policy types:

**1. Tag-based access policies** — written against Atlas classifications, not table names.

Example: "users in role `surveillance_l1` cannot see columns tagged `PII_HIGH`". This applies automatically to every column with that tag, in every table, present and future. Tag-based is the right primitive for PII because tags propagate downstream.

**2. Column-masking policies** — show a different value depending on the user's role.

Example: "for users in role `surveillance_l1`, mask the `investor_pan` column with `XXXXX****X`". The column still exists in the result; it just shows masked content. Analysts can confirm an alert is on the right person without seeing the raw PAN.

**3. Row-filtering policies** — silently inject a WHERE clause based on user role.

Example: "for users in role `research_analyst`, add `WHERE consent_status = 'ACTIVE' AND consent_purpose LIKE '%ANALYTICS%'`". The query returns only rows the user is allowed to see; rows for consent-withdrawn investors are silently filtered out.

Lab 4.2 deploys all three. Module 7 (CP-18, the consent withdrawal lab) is what *exercises* the row-filtering policy in anger.

### Roles in ARGUS

ARGUS defines five roles:

| Role | Sees PAN as | Sees consent-withdrawn rows? |
|---|---|---|
| `surveillance_l1` (junior analyst) | Masked `XXXXX****X` | No |
| `surveillance_l2` (senior analyst) | Masked `XXXXX****X` | Yes (on `vw_surveillance_audit` only) |
| `compliance_dpo` | Full | Yes (on tagged statutory views) |
| `compliance_admin` | Full | Yes (everywhere) |
| `research_analyst` | None (column hidden) | No |

Lab 4.2 deploys these grants. The roles are referenced everywhere downstream — Module 7 in particular needs `research_analyst` and `compliance_dpo` for CP-18.

## What Module 4 closes — ARG-5 part 1

ARG-5 has two parts. Module 4 closes part 1 (PII access governance). Module 7 closes part 2 (consent enforcement + erasure).

## Module 4's labs

| Lab | What you do | Checkpoint | Time |
|---|---|---|---|
| 4.1 — Governed views | Deploy 7 views into `argus_${STUDENT_ID}_views`; verify joins work | CP-10 | ~45 min |
| 4.2 — Ranger policies | Deploy 3 policy families; verify role-based access works | CP-11 | ~75 min |

## Things confusing the first time

### "Why don't I just grant analysts direct access to gold tables?"

Three reasons. (1) Tables change schema; views provide a stable contract. (2) Every analyst would have to remember the same JOINs; the view does them once. (3) Ranger policies are easier to write against views than across many gold tables — one column-mask on `vw_surveillance_audit.investor_pan` covers every consumer of that view.

### "What's the difference between a tag-based policy and a column-mask policy?"

Tag-based decides *whether* you can see the column at all (boolean access). Column-mask decides *how the value appears when you do see it* (transformation). PII_HIGH is usually masked (you see the column with masked content); PII restricted (much rarer) is fully blocked.

### "Ranger policies don't seem to apply — what's wrong?"

Ranger has a 30–60 second policy cache TTL. Wait that long after deploying or modifying. Lab 4.2 covers diagnosis. Also confirm you're querying through CDW/Impala, not directly through Spark — Spark's Ranger integration requires extra config (Spark/Sentry plugin) that the lab doesn't set up.

## Success at end of Module 4

- Deploy a governed view in Impala with the right joins and filters
- Write a Ranger column-mask policy and verify it applies for the right role
- Write a Ranger row-filter policy and verify it filters by user role
- Diagnose Ranger policy issues (cache, role membership, plugin status)
- Trace why a specific user sees what they see

## What's NOT in Module 4

- New ML training (Module 5)
- Atlas classifications (Module 7 — though the views *use* tags Module 7 will create)
- Real-time streaming (Module 1)

---

When ready, head to [Lab 4.1 — Governed Views](../labs/lab-4-1-governed-views.md). Allow ~45 minutes.
