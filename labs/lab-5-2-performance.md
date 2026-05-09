# Lab 5.2 — Performance Thresholds (CP-13)

> ℹ️ **Module:** 5 — ML Alert Risk-Ranking
> **Closes deficiency:** ARG-3 (the central FP-rate problem)
> **Source files:** [`src/ml/train_alert_ranker.py`](../src/ml/train_alert_ranker.py)

## Objectives

- Verify the final model achieves test-set AUC ≥ 0.82
- Verify top-decile precision ≥ 0.55
- Confirm the model correctly separates planted real cases (0, 1, 2) from planted negative cases (6, 7)
- Register and promote the model to MLflow Staging

## Why this matters

The two thresholds are not arbitrary. They map directly to operational impact at MSE:

- **AUC ≥ 0.82** is the bar set by SEBI's draft AI/ML guidance for surveillance systems. Below 0.82, the model is "no better than well-calibrated rules of thumb"; above, it's a defensible AI-augmented decision aid.
- **Top-decile precision ≥ 0.55** is the operational bar. The 28-person analyst team can realistically work the top 10% of the daily alert queue in detail. If 55% of those are real, the analyst day is dominated by signal, not noise — the inverse of the legacy 92% no-action rate. This is the metric that converts ARG-3 from a paragraph in a PRD to ₹22 crore/year of recovered analyst capacity.

## Procedure

### Step 1 — Confirm Lab 5.1 ran

```python
import mlflow
runs = mlflow.search_runs(experiment_names=["argus_${STUDENT_ID}_alert_ranking_v1"])
final = runs[runs["tags.mlflow.runName"] == "final_fit"]
assert not final.empty, "Run Lab 5.1 first"
print(final[["metrics.test_auc", "metrics.test_top_decile_precision",
             "metrics.test_average_precision"]].iloc[0])
```

### Step 2 — Verify thresholds

```python
final_metrics = final.iloc[0]
auc = final_metrics["metrics.test_auc"]
tdp = final_metrics["metrics.test_top_decile_precision"]
print(f"AUC:                 {auc:.4f}  (threshold 0.82)  {'PASS' if auc >= 0.82 else 'FAIL'}")
print(f"Top-decile precision: {tdp:.4f}  (threshold 0.55)  {'PASS' if tdp >= 0.55 else 'FAIL'}")
```

If either fails: do not proceed to registration. The fix path is in the failure-mode section below; revisit Lab 5.1 with adjusted hyperparameter ranges or training-data scope and re-run.

### Step 3 — Inspect SHAP feature importance (sanity check)

The most important features should align with intuition. In a CML notebook:

```python
import mlflow
import shap
import pandas as pd
from src.ml.feature_extraction import (
    extract_feature_matrix, get_feature_names,
)

# Pull the final model from MLflow
final_run_id = final.iloc[0].run_id
model = mlflow.xgboost.load_model(f"runs:/{final_run_id}/model")

# Score a sample and compute global SHAP importance
from src.ml.train_alert_ranker import load_training_data
spark = SparkSession.builder.getOrCreate()
sample = load_training_data(spark).sample(n=2000, random_state=42)
X = extract_feature_matrix(sample)
explainer = shap.TreeExplainer(model)
sv = explainer.shap_values(X)

importance = pd.DataFrame({
    "feature": get_feature_names(),
    "mean_abs_shap": abs(sv).mean(axis=0)
}).sort_values("mean_abs_shap", ascending=False)
print(importance.head(15))
```

**Expected output**: the top 10 features should include several from this list (in any order):

- `pct_cancelled_under_50ms`
- `cross_product_delta_imbalance`
- `layered_stack_count`
- `member_historical_confirm_rate`
- `cancel_rate`
- `severity_critical`
- `is_esm_flagged`

If the top features are dominated by `hour_of_session`, `day_of_week`, or `notional_traded_log10` alone, the model is exploiting trivial temporal/scale patterns instead of learning the surveillance signal. This usually indicates training-data leakage or insufficient labeled positives — re-check the legacy_alerts disposition distribution.

### Step 4 — Verify planted-case separation

The capstone's most important behavioral test is whether the model ranks the real planted cases (0, 1, 2 — confirmed manipulation) above the negative planted cases (6, 7 — legitimate behavior that fires the rules but isn't manipulation):

```python
# Score the alert_candidates table (current production candidates)
candidates = spark.table("argus_${STUDENT_ID}_gold.alert_candidates").toPandas()

# Only the rows linked to planted cases via member_firm_id
planted_real_members = ["BNXM-0042", "BNXM-0117", "BNXM-0231"]   # cases 0, 1, 2
planted_fake_members = ["BNXM-0001", "BNXM-0156"]                # cases 6, 7

X_all = extract_feature_matrix(candidates)
candidates["score"] = model.predict_proba(X_all)[:, 1]

real_scores = candidates[candidates.member_firm_id.isin(planted_real_members)]["score"]
fake_scores = candidates[candidates.member_firm_id.isin(planted_fake_members)]["score"]

print(f"Real-case  median score: {real_scores.median():.4f}  (n={len(real_scores)})")
print(f"Fake-case  median score: {fake_scores.median():.4f}  (n={len(fake_scores)})")
```

**Expected output**: real-case median score should be **substantially higher** than fake-case median — typically 2–4× higher (e.g. 0.78 vs 0.22). This is the qualitative proof that the model isn't just chasing AUC — it correctly separates the operationally important cases.

### Step 5 — Register and promote to Staging

When all checks pass, re-run the training script with `--register`:

```bash
python src/ml/train_alert_ranker.py --max-trials 50 --register
```

This run registers the model in the MLflow Model Registry under name `argus_${STUDENT_ID}_alert_ranker` and transitions it to Staging if both thresholds pass.

In the MLflow UI: Models → `argus_${STUDENT_ID}_alert_ranker` → Latest version, confirm `Stage = Staging`.

> ⚠️ **Compliance gate:** Promotion to Production is **manual**. SEBI's stance is that a human must be the last decision-maker on any model deployed in a regulatory function. Don't script the Staging → Production transition. The lab shows a senior analyst clicking "Transition to Production" in the MLflow UI; that click is itself part of the compliance evidence trail.

## Checkpoint CP-13 — Performance thresholds met

### Pass condition

All four checks pass.

### Check 1 — AUC ≥ 0.82

`metrics.test_auc` for the final fit run is at least 0.82.

### Check 2 — Top-decile precision ≥ 0.55

`metrics.test_top_decile_precision` is at least 0.55.

### Check 3 — Top SHAP features are operationally meaningful

The top-15 features by mean-absolute-SHAP include at least 4 of: `pct_cancelled_under_50ms`, `cross_product_delta_imbalance`, `layered_stack_count`, `member_historical_confirm_rate`, `cancel_rate`, `severity_critical`, `is_esm_flagged`.

### Check 4 — Real-case median score >> fake-case median score

Real-case median (Cases 0, 1, 2 by `member_firm_id`) is at least 2× the fake-case median (Cases 6, 7).

---

## Common failure mode — AUC plateaus around 0.75

**Symptom**: AUC reproducibly lands in the 0.72–0.78 range; can't break 0.82 even after multiple Hyperopt searches.

**Diagnosis**: usually one of three issues:

1. **Class imbalance handling is too aggressive.** If `scale_pos_weight` upper bound is set too high (>20), the model overpredicts the positive class and AUC suffers because of false-positive inflation in the upper score region.
2. **Training data is too small.** At `--scale 0.05` you have ~25,000 alerts with ~2,000 positives. XGBoost wants ≥10,000 positives for stable feature interactions on tabular surveillance data. If you can't expand `--scale`, the AUC ceiling is what it is.
3. **Synthetic data is too uniform.** If the planted cases are all "obvious" (Case 0 layering with extreme cancel rates, Case 1 spoofing with extreme order sizes), the model learns the planted patterns but can't generalize. The fix is to soften the planted patterns in `data/generate_data.py` — but that breaks CP-08, so don't.

**Fix path**:

1. First, lower `scale_pos_weight` upper bound to 12 in `SEARCH_SPACE`.
2. If still failing, regenerate at `--scale 0.10` (twice the data).
3. If still failing, broaden the search by increasing `max_trials` to 100 — Hyperopt sometimes needs more exploration to find the hyperparameter sweet spot for a difficult dataset.

---

## Pass condition for CP-13

All four checks pass. The model is calibrated, defensible, and separates real manipulation from legitimate-but-similar-looking activity. The 92% false-positive flood is, in principle, fixable.
