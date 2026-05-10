# Module 5 — ML Alert Risk-Ranking

> 📊 **Visual reference**: [Module 5 ML training + production gate](../assets/diagrams/06_module5_cml_ml.md) ([SVG](../assets/diagrams/06_module5_cml_ml.svg))

> 👋 **New to MLflow, Hyperopt, XGBoost, or SHAP?** Read [`docs/module-5-primer.md`](module-5-primer.md) first. About 25 minutes.


> **Closes deficiency:** ARG-3 — 92% of fired alerts are closed as no-action; analysts buried in noise
> **Day:** 7
> **Checkpoints:** CP-12, CP-13, CP-14
> **Weight:** 20% of capstone

## What's broken

MSE's deterministic rules engine fires roughly 4,200 alerts per trading day. 92% are closed by analysts as "no manipulative intent" — bona-fide cancellations, market making, fat-finger errors, thin-volume false signals from low-liquidity small-caps. Three of the 14 SEBI-cited missed manipulation episodes had actually fired alerts in MSE's system, but the alerts were buried in the false-positive flood and not opened until SEBI inquired. The 28-person analyst team spends roughly 65% of its time triaging false positives — about 18 FTE-equivalents of wasted effort, costing ₹22 crore/year in fully-loaded analyst cost. The 8% of alerts that *are* real are diluted into a stream of noise, and median time-to-investigation is 11 hours when SEBI's circular expectation is "real-time review of high-priority alerts."

The deterministic rules can't be removed — regulators require alerts to be reproducible from rules for audit. But the rules can be *prioritized*. That's what this module builds.

## What you build

A supervised ML model — gradient-boosted trees (XGBoost) — that scores every fired alert with a probability of confirmed manipulation. The analyst queue is sorted by that score, so analysts work the top of the queue first instead of triaging chronologically through a 92% noise stream. The deterministic rules continue to fire every alert; the ML simply ranks them.

The training pipeline runs a 50-trial Hyperopt Bayesian search across XGBoost hyperparameters with 5-fold time-series cross-validation, then a final fit on the full training set. Performance is tracked in MLflow; models pass through Staging → Production via a manual approval gate (regulators require a human in the loop on model promotions). Every scored alert carries SHAP explanations of the top-10 contributing features so analysts can defend prioritization decisions to SEBI.

The production scorer (JOB-09) runs every 5 minutes against pending alerts. Scored alerts return to `argus_${STUDENT_ID}_gold.alert_candidates` with `model_score`, `model_version`, `scored_at`, and `shap_explanations` columns populated. The surveillance UI reads these columns to drive default sort order.

## CDP services used

- **Cloudera AI (CML)** — workbench for training; MLflow tracking server; Model Registry
- **Cloudera Data Engineering (CDE) / Spark** — JOB-09 production scorer
- **Apache Iceberg** — `MERGE INTO` updates `model_score` columns in place
- **Apache Airflow on CDE** — orchestrates the weekly retrain trigger
- **MLflow** — experiment tracking, hyperparameter logging, model registry, version promotion gates

## Source files

| File | Purpose |
|---|---|
| [`src/ml/feature_extraction.py`](../src/ml/feature_extraction.py) | Shared 60-feature numeric extraction used by both training and scoring |
| [`src/ml/train_alert_ranker.py`](../src/ml/train_alert_ranker.py) | Hyperopt + XGBoost + MLflow training (weekly cadence) |
| [`src/ml/batch_score.py`](../src/ml/batch_score.py) | JOB-09 production scorer (5-minute cadence) |

## Labs

| Lab | What it does | Checkpoint |
|---|---|---|
| [Lab 5.1 — MLflow Training](../labs/lab-5-1-mlflow-training.md) | Run Hyperopt search, log 50+ runs to MLflow | CP-12 |
| [Lab 5.2 — Performance Thresholds](../labs/lab-5-2-performance.md) | Final fit; verify AUC ≥ 0.82 and top-decile precision ≥ 0.55 | CP-13 |
| [Lab 5.3 — Production Scoring](../labs/lab-5-3-production-scoring.md) | Deploy JOB-09; verify model_score + SHAP write back every 5 minutes | CP-14 |

## Measurable outcome

By end of module:

- MLflow experiment `argus_${STUDENT_ID}_alert_ranking_v1` shows ≥ 50 runs from the Hyperopt search
- Final model achieves test-set AUC-ROC ≥ 0.82 on a 30-day held-out window
- Top-decile precision ≥ 0.55 — meaning of every 10 alerts the model ranks highest, ≥ 5.5 are true positives (versus the 0.8 you'd get from the unranked rules-only stream)
- Model registered in MLflow Model Registry; promoted Staging → Production via a manual approval step
- JOB-09 runs every 5 minutes; `argus_${STUDENT_ID}_gold.alert_candidates.model_score` populates within 5 minutes of new alerts
- Every scored alert has `shap_explanations` JSON containing top-10 contributing features

## What this fixes

Before ARGUS, an analyst opening their queue saw 4,200 alerts sorted by `fired_ts`. They worked through chronologically, closing the obvious noise, occasionally finding a real case. The 11-hour median time-to-investigation was a direct consequence of triaging 92% noise before reaching the 8% signal. After ARGUS, the same analyst opens their queue sorted by `model_score DESC` and the top of the queue is dominated by genuine manipulation candidates. Alerts from BNXM-0001 (the legitimate tier-1 market maker, planted Case 6) get scored low and sink to the bottom; alerts from BNXM-0042 (the case-0 layering manipulator) get scored high and surface immediately.

The 92% no-action rate doesn't go away — those alerts still fire deterministically, because the regulator requires it — but they no longer bury the real cases.

> 💡 **Tip:** If your AUC is below 0.82, the most common cause is feature leakage from the disposition itself. Check that the training set doesn't contain `disposition` or `disposition_rationale` as a feature — they're the label; using them as features makes the model trivially perfect on training and useless in production. The feature_extraction.py module deliberately excludes these.

> ⚠️ **Compliance gate:** Model promotion from Staging to Production requires a human approval. The training script will *register* a model that passes thresholds but won't auto-promote — that's a manual step in the MLflow UI. SEBI's stance on AI/ML in regulatory functions is that a human must be the last decision-maker on model deployment; the manual gate is what makes ARGUS defensible if challenged. Don't script around it.
