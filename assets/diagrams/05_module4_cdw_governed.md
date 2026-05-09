# Module 4 — Governed Views + Ranger Policies

Day 6 · Closes **ARG-5 (Part 1)** · CP-10 (views) · CP-11 (policy enforcement)

## Same data, three roles, three different shapes

```mermaid
flowchart TB
    classDef gold     fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px
    classDef view     fill:#1a1632,stroke:#6366f1,color:#e5e7eb,stroke-width:2px
    classDef policy   fill:#1a1632,stroke:#6366f1,color:#6366f1,stroke-width:1.5px
    classDef analyst  fill:#161b27,stroke:#f96302,color:#e5e7eb
    classDef dpo      fill:#1a1632,stroke:#6366f1,color:#e5e7eb,stroke-width:2px
    classDef research fill:#161b27,stroke:#f96302,color:#e5e7eb

    G["argus_${SID}_gold tables<br/>+ silver.member_master with consent_status"]:::gold

    V["7 governed views in argus_${SID}_views<br/>vw_alert_queue · vw_member_360 ·<br/>vw_surveillance_audit · vw_research_aggregates · ..."]:::view

    P["RANGER POLICIES (runtime enforcement)<br/>dpdp_consent_filter (row) ·<br/>pii_column_mask (column) ·<br/>surveillance_time_bound_access (row + temporal)"]:::policy

    R1["surveillance_analyst<br/>━━━━━━━━━━<br/>PAN = MASKED ('XXXXX1234X')<br/>email = MASKED · mobile = MASKED<br/>consent-withdrawn FILTERED OUT<br/>model_score + SHAP visible"]:::analyst

    R2["compliance_dpo<br/>━━━━━━━━━━<br/>PAN = FULL (statutory bypass)<br/>consent-withdrawn rows VISIBLE<br/>(DPDP §7 legitimate use)<br/>Erasure audit trail visible"]:::dpo

    R3["research_analyst<br/>━━━━━━━━━━<br/>PII columns DROPPED entirely<br/>Group-by aggregates only<br/>k-anonymity (k ≥ 5)<br/>Cases 15-19 NOT visible"]:::research

    G --> V
    V -.through.-> P
    P -.enforces.-> R1 & R2 & R3
```

## What each role sees for the same alert

Sample alert: a planted Case 15 LAYERING by `BNXM-0117` involving investor PAN `ABCDE1234F`, who withdrew consent for ANALYTICS on Day 8.

| Field | surveillance_analyst | compliance_dpo | research_analyst |
|---|---|---|---|
| `alert_id` | `ALERT-X1234` | `ALERT-X1234` | aggregated |
| `member_firm_id` | `BNXM-0117` | `BNXM-0117` | aggregated |
| `investor_pan` | `XXXXX1234F` | `ABCDE1234F` | (column dropped) |
| `investor_email` | `xx@xx.com` | full | (column dropped) |
| `consent_status` | filtered out | `WITHDRAWN_FOR_ANALYTICS` | (row excluded) |
| `model_score` | `0.847` (visible) | `0.847` | aggregated only |
| `shap_top_10` | visible | visible | not visible |

## CP-11 verification — three role-switches in sequence

```sql
-- As surveillance_analyst
SET ROLE surveillance_analyst;
SELECT alert_id, investor_pan FROM argus_${STUDENT_ID}_views.vw_alert_queue
WHERE planted_case_idx = 15;
-- investor_pan should show as 'XXXXX1234F'

-- As compliance_dpo (statutory bypass)
SET ROLE compliance_dpo;
SELECT alert_id, investor_pan FROM argus_${STUDENT_ID}_views.vw_surveillance_audit
WHERE planted_case_idx = 15;
-- investor_pan should show full 'ABCDE1234F'

-- As research_analyst (cases 15-19 filtered)
SET ROLE research_analyst;
SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_research_aggregates
WHERE planted_case_idx BETWEEN 15 AND 19;
-- Should return 0
```
