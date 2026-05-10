# Lab 5.2 — Performance Thresholds (CP-13)

> 👋 **Module 5 first-timer?** Read [`docs/module-5-primer.md`](../docs/module-5-primer.md) first. About 25 minutes.

> ℹ️ **Module:** 5 — ML Alert Risk-Ranking
> **Closes deficiency:** ARG-3 (the central FP-rate problem)
> **Time:** ~45 minutes if both thresholds pass first try; up to 4 hours if AUC plateaus and retraining is needed.
> **Source files:** [`src/ml/train_alert_ranker.py`](../src/ml/train_alert_ranker.py)

## What you're going to do

1. **Confirm Lab 5.1 ran** — `final_fit` exists with metrics. (~3 min)
2. **Verify thresholds:** AUC ≥ 0.82, top-decile precision ≥ 0.55. (~5 min)
3. **Inspect global SHAP feature importance** — sanity-check the model uses operational features. (~10 min)
4. **Verify planted-case separation** — real cases score higher than fake cases. (~10 min)
5. **Register and promote to Staging** — re-run training with `--register`. (~10 min)
6. **Verify CP-13 pass conditions** — four checks. (~7 min)

Total: ~45 minutes if all passes. Up to 4 hours if AUC fails and you need to retrain.

## Before you begin — prerequisite checklist

- [ ] [Lab 5.1](lab-5-1-mlflow-training.md) is complete; `argus_${STUDENT_ID}_alert_ranking_v1` experiment has ≥50 runs including `final_fit`
- [ ] You can `import mlflow, shap, xgboost` in the workbench
- [ ] `gold.alert_candidates` populated (Module 3) for the planted-case test

## Why these two thresholds matter — read this before Step 2

**AUC ≥ 0.82** — set by SEBI's draft AI/ML guidance for surveillance systems.
- Below 0.82: "no better than well-calibrated rules of thumb." The model adds nothing over the rule engine.
- Above 0.82: "a defensible AI-augmented decision aid." Defensible means SEBI accepts model decisions as evidence.

**Top-decile precision ≥ 0.55** — the operational bar.
- The 28-person MSE analyst team can realistically work the top 10% of the daily alert queue in detail.
- If 55% of those top-10% alerts are real manipulation, the analyst day is dominated by signal, not noise.
- This is the metric that converts ARG-3 from a paragraph in a PRD to **₹22 crore/year of recovered analyst capacity**.

If either fails, **do not register**. The fix paths are in the failure-mode section.

## Step 1 — Confirm Lab 5.1 ran

```python
import mlflow
runs = mlflow.search_runs(experiment_names=["argus_${STUDENT_ID}_alert_ranking_v1"])
final = runs[runs["tags.mlflow.runName"] == "final_fit"]
assert not final.empty, "Run Lab 5.1 first"
print(final[["metrics.test_auc", "metrics.test_top_decile_precision",
             "metrics.test_average_precision"]].iloc[0])
```

**Expected:** prints three metric values. If `final` is empty, Lab 5.1 didn't complete the final fit — go back to Lab 5.1.

## Step 2 — Verify thresholds

```python
final_metrics = final.iloc[0]
auc = final_metrics["metrics.test_auc"]
tdp = final_metrics["metrics.test_top_decile_precision"]
print(f"AUC:                  {auc:.4f}  (threshold 0.82)  {'PASS' if auc >= 0.82 else 'FAIL'}")
print(f"Top-decile precision: {tdp:.4f}  (threshold 0.55)  {'PASS' if tdp >= 0.55 else 'FAIL'}")
```

**Expected:** both PASS.

If either FAILs: **do not proceed past this step.** See Common Failure Mode #1 below for the diagnosis + retraining path. Revisit Lab 5.1 with adjusted hyperparameter ranges or training-data scope, then come back here.

## Step 3 — Inspect SHAP feature importance (sanity check)

The most important features should align with intuition. The model should be using the surveillance-meaningful features (cancel rates, cross-product imbalance) — not exploiting trivial signal (hour of day, instrument size).

```python
import mlflow
import shap
import pandas as pd
from pyspark.sql import SparkSession
from src.ml.feature_extraction import (
    extract_feature_matrix, get_feature_names,
)
from src.ml.train_alert_ranker import load_training_data

# Pull the final model from MLflow
final_run_id = final.iloc[0].run_id
model = mlflow.xgboost.load_model(f"runs:/{final_run_id}/model")

# Score a sample and compute global SHAP importance
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

**Expected output:** the top 10 features should include several from this list (in any order):

- `pct_cancelled_under_50ms`
- `cross_product_delta_imbalance`
- `layered_stack_count`
- `member_historical_confirm_rate`
- `cancel_rate`
- `severity_critical`
- `is_esm_flagged`

> 💡 **What "global SHAP importance" tells you:** for each feature, the average absolute SHAP value across the sample is a measure of how much that feature contributes to model decisions on average. The top-15 features should make operational sense. If the top features are dominated by `hour_of_session`, `day_of_week`, or `notional_traded_log10` *alone*, the model is exploiting trivial temporal/scale patterns instead of learning the surveillance signal — usually indicates training-data leakage or insufficient labeled positives.

If the top features look wrong, see Common Failure Mode #2.

## Step 4 — Verify planted-case separation

The capstone's most important behavioral test: the model must rank the **real planted cases (0, 1, 2)** above the **negative planted cases (6, 7)**.

```python
# Score the alert_candidates table (current production candidates)
candidates = spark.table("argus_${STUDENT_ID}_gold.alert_candidates").toPandas()

# Cases linked to planted_real_members (confirmed manipulation)
planted_real_members = ["BNXM-0042", "BNXM-0117", "BNXM-0231"]   # cases 0, 1, 2
# Cases linked to planted_fake_members (legitimate but rules fired)
planted_fake_members = ["BNXM-0001", "BNXM-0156"]                # cases 6, 7

X_all = extract_feature_matrix(candidates)
candidates["score"] = model.predict_proba(X_all)[:, 1]

real_scores = candidates[candidates.member_firm_id.isin(planted_real_members)]["score"]
fake_scores = candidates[candidates.member_firm_id.isin(planted_fake_members)]["score"]

print(f"Real-case  median score: {real_scores.median():.4f}  (n={len(real_scores)})")
print(f"Fake-case  median score: {fake_scores.median():.4f}  (n={len(fake_scores)})")
print(f"Ratio (real/fake): {real_scores.median() / fake_scores.median():.2f}")
```

**Expected:** real-case median score is **substantially higher** than fake-case median — typically 2–4× higher (e.g. 0.78 vs 0.22).

> 💡 **Why this test matters more than AUC:** AUC measures global ranking quality, but it's possible to get high AUC while failing the planted-case test (the model could rank well on average but get the operationally-important cases wrong). The planted-case test is the *qualitative* proof that the model isn't just chasing a number — it correctly separates the cases that matter.

If real/fake ratio < 1.5×, see Common Failure Mode #3.

## Step 5 — Register and promote to Staging

When all checks pass, re-run the training script with `--register`:

```bash
python src/ml/train_alert_ranker.py --max-trials 50 --register
```

> 💡 **Why re-run instead of just registering the existing run?** `--register` flag wires the run to the MLflow Model Registry (creates a registered model entry, transitions to Staging). The simpler thing is to re-fit; the run logs cleanly. Some teams write a one-line script that promotes an existing run to the registry; either approach works for the capstone.

The script registers the model as `argus_${STUDENT_ID}_alert_ranker` and transitions to **Staging** if both thresholds pass.

In the MLflow UI: **Models** → `argus_${STUDENT_ID}_alert_ranker` → Latest version → confirm `Stage = Staging`.

> ⚠️ **Compliance gate:** **Promotion to Production is manual.** SEBI's stance: a human must be the last decision-maker on any model deployed in a regulatory function. Don't script the Staging → Production transition. Lab 5.3 walks the human-click. The click itself is part of the compliance evidence trail.

## Step 6 — Verify CP-13 pass conditions

CP-13 has **four checks**.

### Check 1 — AUC ≥ 0.82

`metrics.test_auc` for the `final_fit` run is at least 0.82. **Pass if:** yes. **Fail if:** below.

### Check 2 — Top-decile precision ≥ 0.55

`metrics.test_top_decile_precision` is at least 0.55. **Pass if:** yes. **Fail if:** below.

### Check 3 — Top SHAP features are operationally meaningful

The top-15 features by mean-absolute-SHAP include at least 4 of: `pct_cancelled_under_50ms`, `cross_product_delta_imbalance`, `layered_stack_count`, `member_historical_confirm_rate`, `cancel_rate`, `severity_critical`, `is_esm_flagged`. **Pass if:** ≥ 4 from list. **Fail if:** fewer.

### Check 4 — Real-case median score >> fake-case median score

Step 4's ratio (real/fake median) ≥ 2.0. **Pass if:** yes. **Fail if:** below.

---

## Common failure mode #1 — AUC plateaus around 0.75

**Symptom:** AUC reproducibly lands in 0.72–0.78 range; can't break 0.82 even after multiple Hyperopt searches.

**Cause** (in decreasing likelihood):
1. **Class imbalance handling too aggressive.** `scale_pos_weight` upper bound is too high (>20); model overpredicts positive class, AUC suffers from FP inflation in upper score region.
2. **Training data too small.** At `--scale 0.05` you have ~25K alerts with ~2K positives. XGBoost wants ≥10K positives for stable feature interactions on tabular data. AUC ceiling is what it is.
3. **Synthetic data too uniform.** Planted cases are extreme (Case 0 with extreme cancel rates, etc.); model learns planted patterns specifically but can't generalize. Fix would soften planted patterns — but that breaks CP-08, so don't.

**Fix path (in order):**
1. Lower `scale_pos_weight` upper bound to **12** in `SEARCH_SPACE` of `train_alert_ranker.py`.
2. Re-run Lab 5.1 with the narrower `scale_pos_weight`. Check if AUC improves.
3. If still failing, regenerate at `--scale 0.10` (twice the data). Re-run Module 1 oneshot, Module 3, then Lab 5.1.
4. If still failing, broaden the search by `--max-trials 100`. Hyperopt sometimes needs more exploration.

## Common failure mode #2 — Top SHAP features are wrong

**Symptom:** Top features dominated by `hour_of_session`, `day_of_week`, or single scale features (`notional_traded_log10`).

**Cause:** model is exploiting trivial temporal/scale patterns instead of surveillance signal.

**Diagnosis:**
```sql
-- Check the disposition distribution in legacy_alerts by hour and weekday
SELECT
    EXTRACT(HOUR FROM fired_ts) AS hr,
    AVG(CAST(is_confirmed_manipulation AS INT)) AS pos_rate
FROM argus_${STUDENT_ID}_bronze.legacy_alerts
GROUP BY EXTRACT(HOUR FROM fired_ts)
ORDER BY hr;
```
If pos_rate varies wildly by hour (e.g., 0.05 at 9am, 0.30 at 3pm), the synthetic generator has temporal leakage. Fix it in `data/generate_data.py` or accept the noisier model.

**Fix:** if synthetic temporal leakage exists, regenerate with `--seed 42`. The canonical seed has flat temporal distribution.

## Common failure mode #3 — Real-case median score not >> fake-case

**Symptom:** Step 4 shows ratio < 1.5×; real and fake cases score similarly.

**Cause:** model relies on features that don't distinguish manipulation from legitimate market making (cancel_rate alone). It needs the *member context* features (`member_historical_confirm_rate`, `member_firm_category` one-hots) to separate.

**Diagnosis:**
```python
# Check whether member_historical_confirm_rate is in the feature set
print('member_historical_confirm_rate' in get_feature_names())
```
If False, the feature isn't being extracted. Most likely `bronze.legacy_alerts` doesn't have enough history per member.

**Fix:** verify `bronze.legacy_alerts` has multiple alerts per member firm (`SELECT member_firm_id, COUNT(*) FROM bronze.legacy_alerts GROUP BY member_firm_id`). If not, regenerate synthetic data — the synthetic generator should produce ~50 alerts per member per year.

---

## Pass condition for CP-13

All four checks pass:
- ✅ AUC ≥ 0.82
- ✅ Top-decile precision ≥ 0.55
- ✅ Top SHAP features include ≥ 4 operational features
- ✅ Real-case median score ≥ 2× fake-case median

When all four pass, the model is **calibrated, defensible, and operationally meaningful**. The 92% false-positive flood is, in principle, fixable — Lab 5.3 makes it operational.

## Wrap-up — what you can now do that you couldn't before

You can verify a trained ML model against operational thresholds and know what to tune if they fail. You can compute and interpret global SHAP feature importance to sanity-check what the model is learning. You can run a planted-case separation test to validate qualitative correctness. You can register a model to MLflow's Model Registry and transition it to Staging.

Lab 5.3 deploys the model in production: manual click to Production, JOB-09 on a 5-min schedule, scores writing back to `alert_candidates`. Allow ~60 minutes.
