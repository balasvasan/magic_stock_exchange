# Module 3 — Temporal + Cross-Product Features + Rules

Day 5 · Closes **ARG-2 (Part 2)** · CP-07 / CP-08 / CP-09 (R-104 Jane Street)

```mermaid
flowchart TB
    classDef silver fill:#1e2535,stroke:#9ca3af,color:#e5e7eb
    classDef job    fill:#161b27,stroke:#f96302,color:#f96302
    classDef gold   fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px
    classDef rule   fill:#161b27,stroke:#f96302,color:#f96302
    classDef gate   fill:#1a1632,stroke:#6366f1,color:#6366f1,stroke-width:2px
    classDef cp     fill:#1a1632,stroke:#6366f1,color:#6366f1

    I["silver.order_events<br/>silver.executed_trades<br/>silver.instrument_master"]:::silver

    J7["JOB-07<br/>Feature engineering<br/>60 features in 6 groups"]:::job

    F1["gold.member_temporal_features<br/>cancel rate · time-to-cancel ·<br/>stack depth · order rate"]:::gold
    F2["gold.cross_product_features<br/>cash vs futures vs options<br/>delta imbalance per underlying"]:::gold

    J8[JOB-08 — fire 5 deterministic rules]:::job

    R101["R-101 SPOOFING<br/>large order held &gt;800ms<br/>then cancelled<br/>→ Case 0"]:::rule
    R102["R-102 LAYERING<br/>≥3 stacked non-bona-fide<br/>cancelled within 200ms<br/>→ Cases 1, 3, 5, 9"]:::rule
    R103["R-103 MOMENTUM IGN.<br/>order rate &gt;1000/sec<br/>sustained &gt;3 sec<br/>→ Case 4"]:::rule
    R104["⚡ R-104 CROSS-PRODUCT<br/>delta_imbalance &gt; 7.0<br/>F&O expiry day<br/>→ Case 2 CRITICAL"]:::gate
    R105["R-105 WASH TRADE<br/>same member firm<br/>cross at same price &lt;1s<br/>→ Case 5"]:::rule

    OUT["gold.alert_candidates<br/>60-feature payload per alert<br/>disposition=PENDING · model_score=NULL"]:::gold

    CP7["✓ CP-07<br/>distributions match category"]:::cp
    CP8["✓ CP-08<br/>all 10 cases (0-9) surface"]:::cp
    CP9["✓ CP-09<br/>Case 2 imbalance ≥ 7.0<br/>R-104 fires CRITICAL"]:::gate

    I --> J7 --> F1 & F2 --> J8
    J8 --> R101 & R102 & R103 & R104 & R105
    R101 & R102 & R103 & R104 & R105 --> OUT

    F1 --> CP7
    OUT --> CP8
    R104 --> CP9
```

## What this closes

The legacy CEP engine couldn't compute features that span more than one product (cash + F&O + options) on the same underlying — the literal pattern that defeated detection in the **Jane Street 2024 SEBI case**. Spark + Iceberg lets JOB-07 join across all three product types in a single batch and compute the imbalance metric that R-104 fires on.

After Module 3 every planted manipulation case (0-9) is detected. Modules 5+ then prioritize to remove the noise.

## The cross-product test

```sql
-- CP-09: verify Case 2 (Jane Street pattern) fires R-104
SELECT
    member_firm_id,
    underlying_code,
    cross_product_delta_imbalance,
    is_expiry_day
FROM argus_${STUDENT_ID}_gold.cross_product_features
WHERE planted_case_idx = 2
  AND cross_product_delta_imbalance >= 7.0
  AND is_expiry_day = TRUE;
-- Should show member_firm_id = 'BNXM-0231' with imbalance ≥ 7.0
```

## Bonus criterion (5%)

The bonus is awarded for replicating the Jane Street detection without leaning on R-104 — a SQL query against `cross_product_features` that flags Case 2 in its top 3 results using only behavioral features, no hardcoded identifier. Tests whether the student understands the *structural* pattern, not just the rule.
