# Module 5 — ML Alert Risk-Ranking

Day 7 · Closes **ARG-3** (92% false-positive flood) · CP-12 / CP-13 / CP-14

## Training pipeline with manual production gate

```mermaid
flowchart LR
    classDef data    fill:#1e2535,stroke:#9ca3af,color:#e5e7eb
    classDef step    fill:#161b27,stroke:#f96302,color:#f96302
    classDef gate    fill:#1a1632,stroke:#6366f1,color:#6366f1,stroke-width:2px
    classDef prod    fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px

    D["bronze.legacy_alerts<br/>~4.8M historical alerts<br/>label = is_confirmed_manipulation"]:::data

    HP["Hyperopt 50 trials<br/>5-fold time-series CV<br/>n_estimators · max_depth ·<br/>learning_rate · scale_pos_weight"]:::step

    FF["Final XGBoost fit<br/>+ holdout-30d evaluation<br/>Log to MLflow experiment<br/>argus_${SID}_alert_ranking_v1"]:::step

    GATE["⚠ MANUAL APPROVAL GATE<br/>DPO clicks 'Approve' in MLflow UI<br/>Staging → Production<br/>(SEBI requirement — never automated)"]:::gate

    P["JOB-09 batch_score<br/>Every 5 minutes<br/>Loads argus_${SID}_alert_ranker [Production]<br/>Writes back model_score + SHAP top-10"]:::prod

    D --> HP --> FF --> GATE --> P
```

## CP-13 pass criteria — the bar for production

| Metric | Threshold | What it tests |
|---|---|---|
| AUC-ROC | ≥ 0.82 | Overall ranking quality |
| Top-decile precision | ≥ 0.55 | Top 10% of alerts have ≥ 55% true positives |
| Planted-case separation | Cases 0/1/2 score ≥ 2× Cases 6/7 | Real cases rank above false positives |
| Calibration | Predicted vs actual within 5% per bin | Probabilities match reality |

If ANY of these fail → DPO does NOT click Approve, model stays in Staging, training iterates next week. **No automated promotion ever** — that's required by SEBI's draft AI/ML guidance.

## Production scoring loop

```mermaid
flowchart LR
    classDef data fill:#1e2535,stroke:#9ca3af,color:#e5e7eb
    classDef step fill:#161b27,stroke:#f96302,color:#f96302
    classDef out  fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px

    P["Pending alerts<br/>WHERE model_score IS NULL"]:::data
    M["Load Production model<br/>argus_${SID}_alert_ranker"]:::step
    S["Predict + SHAP<br/>top-10 features per alert"]:::step
    U["MERGE back into alert_candidates<br/>SET model_score, version, scored_at, shap"]:::out
    UI["Surveillance UI<br/>SELECT FROM vw_alert_queue<br/>ORDER BY model_score DESC LIMIT 50"]:::out

    P --> M --> S --> U --> UI
```

## What this closes

The legacy platform fired ~100K alerts/day; analysts closed 92% as no-action — *operationally indistinguishable from no surveillance* per the SEBI inspection. Module 5 keeps the deterministic rule layer (regulators won't accept "ML decided not to fire") but adds a probabilistic **ranking** on top. Top-decile precision ≥ 0.55 means the top 10K of those 100K alerts have ≥ 55% true positives — analysts work the top 10K, signal-to-noise jumps from 8% to ≥ 55%.

The manual gate isn't a UI nicety — it's the SEBI-defensible audit trail. Every promotion logs the DPO's click + the rationale typed into the dialog. The lab teaches this discipline so students carry it into production.
