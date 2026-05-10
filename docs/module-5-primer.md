# Module 5 Primer — Read This Before Lab 5.1

> 📊 **Visual reference**: [Module 5 ML scoring pipeline](../assets/diagrams/06_module5_cml_ml.md) ([SVG](../assets/diagrams/06_module5_cml_ml.svg))

> 👋 **New to MLflow, Hyperopt, XGBoost, or SHAP?** This primer is for you. About 25 minutes — Module 5 has the most external concepts in the capstone.

This is a **primer**, not a procedure. The actual hands-on work is in Module 5's three labs. Read this first.

## The big picture in one paragraph

Module 5 turns the deterministic alert candidates from Module 3 into a **risk-ranked queue**. It's where ARG-3 (the 92% false-positive flood) gets fixed. Three labs: train an XGBoost model on the alert candidates with Hyperopt's Bayesian hyperparameter search and 5-fold cross-validation, all tracked in MLflow (Lab 5.1); verify it clears the AUC ≥ 0.82 and top-decile precision ≥ 0.55 thresholds and inspect SHAP feature importance for sanity (Lab 5.2); promote to MLflow Production stage via a *manual* approval click and deploy a 5-minute batch scoring job that writes `model_score` and `shap_explanations` back to `alert_candidates` (Lab 5.3). By the end of Module 5, the analyst queue is sorted by ML score and the legacy 92% no-action rate becomes a 30% no-action rate. **₹22 crore/year of recovered analyst capacity** is the operational impact ARGUS is being asked to deliver.

## Concepts you'll meet

### MLflow — three pieces

MLflow has three components ARGUS uses:

**1. Tracking server** — every model run gets logged. Hyperparameters, metrics, the trained model itself, plus arbitrary artifacts. Querying the tracking server in Lab 5.1's Step 4 is what gives you the audit trail SEBI demands.

**2. Model Registry** — registered models have versions and stages (`None`, `Staging`, `Production`, `Archived`). You promote v1 → v2 over time; you transition stages as the model matures. The Registry is what tells JOB-09 *which* model version to use in production.

**3. Models API** — load a model by name and stage: `mlflow.xgboost.load_model("models:/argus_${STUDENT_ID}_alert_ranker/Production")`. JOB-09 does this every 5 minutes; if you transition a new version to Production, JOB-09 picks it up on the next cycle (no restart).

### Hyperopt — Bayesian search over hyperparameters

XGBoost has ~20 hyperparameters (n_estimators, max_depth, learning_rate, etc.). Grid search is too slow; random search wastes compute. **Hyperopt's Tree-structured Parzen Estimator (TPE)** does Bayesian-flavored search: each trial uses information from previous trials to bias the next set of hyperparameters toward promising regions of the search space.

ARGUS configures 50 trials. Early trials are exploratory (the algorithm probes the space). Later trials concentrate near low-loss regions. Lab 5.1 Step 4 verifies the search trajectory shows decreasing loss over time — if it's flat, Hyperopt isn't actually exploiting prior signal.

### XGBoost — gradient-boosted trees for tabular

For tabular surveillance data with mixed feature types (continuous: cancel_rate; categorical: severity; binary: is_esm_flagged), XGBoost almost always beats deep nets. It's the right primitive.

Two XGBoost-specific knobs that matter for ARGUS:
- **`scale_pos_weight`** — positive-class weighting in the loss. With ~8% positive class, default behavior overweights the negative class. Setting `scale_pos_weight ≈ 11` (= negatives/positives) compensates. Lab 5.2 Common Failure Mode discusses tuning this when AUC plateaus.
- **`max_depth`** — tree complexity. Default 6 is fine for surveillance; pushing higher overfits.

### Cross-validation — 5-fold TIME-SERIES

A surveillance model trained on tomorrow's data can't predict yesterday's manipulation; that's leakage. **Time-series CV** sorts the dataset by event timestamp, splits into chronological folds, and trains on earlier folds + tests on later. It avoids the leakage that plain k-fold CV would introduce.

Lab 5.1 uses 5 time-series folds. Each Hyperopt trial does 5 fits + 5 evaluations + reports the mean AUC across folds. Total: 50 trials × 5 folds = 250 fits over the search.

### AUC ≥ 0.82 and top-decile precision ≥ 0.55 — the operational thresholds

Two thresholds. Both come from operational impact, not arbitrary academic targets.

**AUC ≥ 0.82** is set by SEBI's draft AI/ML guidance for surveillance systems. Below 0.82 the model is "no better than well-calibrated rules of thumb." Above 0.82 it's "a defensible AI-augmented decision aid."

**Top-decile precision ≥ 0.55** is the operational bar. The 28-person MSE analyst team can realistically work the top 10% of the daily alert queue in detail. If 55% of those top-10% are real, the analyst day is dominated by signal, not noise. This is the metric that converts ARG-3 from a paragraph in a PRD to ₹22cr/year of recovered analyst capacity.

If either threshold fails in Lab 5.2: don't promote. Re-run Lab 5.1 with adjusted hyperparameter ranges or more training data.

### SHAP — explainability

Every alert score that JOB-09 writes back has an associated SHAP explanation. Per-row, SHAP says: "feature X contributed +0.42 to the score; feature Y contributed -0.18." Total SHAP = score (after a constant baseline).

Why this matters: when an analyst challenges a low score on a real-looking alert, the SHAP explanation tells them which features drove the decision. When a regulator challenges a model decision, the SHAP explanation is the legal defense. Lab 5.3 verifies SHAP populates on every scored alert.

Global SHAP (mean absolute SHAP across the dataset) is a feature-importance summary. Lab 5.2 Step 3 inspects global SHAP to verify the model is using operationally meaningful features (cancel_rate, cross_product_delta_imbalance, etc.) and not exploiting trivial signal (hour_of_day).

### Manual promotion gate — the compliance sentinel

After Lab 5.2 verifies thresholds, the script registers the model and transitions it to **Staging**. **Production transition is a manual click in the MLflow UI.**

Why? SEBI's stance is: a human must be the last decision-maker on any model deployed in a regulatory function. The dialog text the human types when promoting ("Approved by Senior Analyst X on date Y; AUC=0.84, top-decile precision=0.58") is part of the compliance trail.

Don't automate this transition. The whole point is the human in the loop.

## What Module 5 closes — ARG-3

ARG-3 was the headline deficiency: 92% of legacy alerts produced no action. Module 5's ML re-ranking is what fixes it:
1. **No ML scoring of alerts** — closes in Lab 5.1 + 5.2 (CP-12, CP-13). 50-trial Hyperopt + thresholds.
2. **No production scoring back to the queue** — closes in Lab 5.3 (CP-14). JOB-09 writes scores every 5 min.
3. **No analyst-facing rank** — closes in Lab 5.3 + Module 4's `vw_alert_queue` ordered by `model_score`.

## Module 5's labs

| Lab | What you do | Checkpoint | Time |
|---|---|---|---|
| 5.1 — MLflow training | Run the 50-trial Hyperopt search; verify all runs land in MLflow | CP-12 | ~75 min (most spent waiting for Hyperopt to converge) |
| 5.2 — Performance thresholds | Verify AUC ≥ 0.82, top-decile prec ≥ 0.55, SHAP sanity, register to Staging | CP-13 | ~45 min |
| 5.3 — Production scoring | Manual promote to Production; deploy JOB-09 on 5-min schedule; verify scoring | CP-14 | ~60 min |

## Things confusing the first time

### "Hyperopt trials fail with NaN loss — what's wrong?"

Almost always feature-extraction returning NaN/-Inf that XGBoost handles but Hyperopt's TPE doesn't. The most common source: `log10(0)` produces `-inf`. Lab 5.1 Common Failure Mode covers diagnosis and the `_safe_log10` fix.

### "AUC plateaus at 0.75 — how do I get past?"

Three usual culprits. (1) `scale_pos_weight` upper bound too high — drop to 12. (2) Training data too small — bump `--scale` to 0.10 if compute allows. (3) Synthetic data planted patterns are too extreme — model learns the planted patterns but can't generalize. Lab 5.2 Common Failure Mode walks the fix.

### "Why does the model give legitimate market makers low scores?"

Because the training labels (legacy SMRITI alerts in `bronze.legacy_alerts`) encode that historical market-maker alerts were dispositioned no-action. The model learns this pattern. **This is what we want.** Case 6 (BNXM-0001, legitimate tier-1 MM) gets low scores in Lab 5.3; Case 0 (BNXM-0042, manipulator) gets high scores. The model uses *member context* (`member_historical_confirm_rate`) to separate them.

### "What's the difference between `model_score` and `severity`?"

`severity` is rule-determined (R-104 → CRITICAL). `model_score` is ML-determined and 0–1 continuous. Both go into the analyst queue. Severity is the floor (CRITICAL alerts always show); score is the priority within the floor.

### "Why doesn't JOB-09 just stream — why batch every 5 min?"

Because XGBoost inference is more efficient batched (vectorized). Latency from "alert fires" → "score is in the queue" is bounded at 5 minutes, which is fine for surveillance (the analyst won't act in <5 min anyway). Real-time inference would add infrastructure cost without operational benefit.

## Success at end of Module 5

- Run a 50-trial Hyperopt search over XGBoost and explain why TPE beats grid/random
- Verify training metrics against operational thresholds and know what to tune if they fail
- Inspect SHAP explanations for both global feature importance and per-row decisions
- Operate the MLflow Model Registry with stages and manual promotion
- Deploy a CDE Spark job on a schedule that loads a Production model and writes scores back

## What's NOT in Module 5

- Real-time streaming (Module 1 — already done)
- GenAI / RAG / STR drafting (Module 6)
- BI dashboards (covered partly in Module 4)
- Compliance erasure (Module 7)

If you find yourself wanting to "draft an STR from this alert" — that's Module 6.

---

When ready, head to [Lab 5.1 — MLflow Training](../labs/lab-5-1-mlflow-training.md). Allow ~75 minutes (most of it waiting for Hyperopt's 50 trials to finish).
