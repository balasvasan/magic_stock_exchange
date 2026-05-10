# Lab 5.1 — MLflow Training (CP-12)

> 👋 **Module 5 first-timer?** Read [`docs/module-5-primer.md`](../docs/module-5-primer.md) first. About 25 minutes — explains MLflow, Hyperopt, XGBoost, SHAP, the manual promotion gate.

> ℹ️ **Module:** 5 — ML Alert Risk-Ranking
> **Closes deficiency:** ARG-3 (false-positive flood) — Lab 5.1 produces the trial population
> **Time:** ~75 minutes if Hyperopt runs cleanly first try (most time is waiting for 50 trials × 5 folds = 250 model fits); up to 3 hours if NaN loss debugging is needed.
> **Source files:** [`src/ml/train_alert_ranker.py`](../src/ml/train_alert_ranker.py), [`src/ml/feature_extraction.py`](../src/ml/feature_extraction.py)

## What you're going to do

1. **Confirm prerequisites** — `bronze.legacy_alerts` populated with labels, ~8% positive class. (~3 min)
2. **Launch the 50-trial Hyperopt search** in a CML workbench session. (~50 min — Hyperopt running)
3. **Open MLflow UI** and verify all runs landed. (~5 min)
4. **Inspect the search trajectory** — confirm decreasing loss over trials (Hyperopt converging). (~5 min)
5. **Inspect the final-fit run** — verify the model artifact is logged. (~5 min)
6. **Verify CP-12 pass conditions** — four named checks. (~7 min)

Total: ~75 minutes.

## Before you begin — prerequisite checklist

- [ ] Module 3 is complete — `gold.alert_candidates` has rows
- [ ] `bronze.legacy_alerts` is populated with labels — quick check: `SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.legacy_alerts` should return ~25,000 at scale 0.05; if 0, the SMRITI batch load from Lab 1.2 wasn't run
- [ ] You have a **CML workbench** with PySpark, xgboost, hyperopt, mlflow available — quick check: in a Python kernel, `import xgboost, hyperopt, mlflow` should succeed
- [ ] MLflow tracking URI is configured — quick check: `import mlflow; mlflow.get_tracking_uri()` should return your CML MLflow endpoint, not `file:///...`

## Why MLflow tracking matters — read this before Step 2

Surveillance in financial services lives or dies on **defensibility**. When SEBI asks "why did your model deprioritize this alert?" the answer cannot be "we tried a few things and this one worked best."

It has to be: "we ran a 50-trial Bayesian search across a defined hyperparameter space, with 5-fold time-series cross-validation; here are all 50 runs in MLflow with their AUCs; here are the SHAP explanations of the top-10 features; here is the manual promotion record from Staging to Production by Senior Director X on date Y."

**Lab 5.1 establishes the trail.** The model itself comes out in Lab 5.2's verification step. Today's work is the substrate of regulatory defensibility.

## Step 1 — Confirm prerequisites

```sql
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN is_confirmed_manipulation = TRUE THEN 1 ELSE 0 END) AS positives,
    AVG(CAST(is_confirmed_manipulation AS INT)) AS positive_rate
FROM argus_${STUDENT_ID}_bronze.legacy_alerts;
```

**Expected output:**

| total | positives | positive_rate |
|---:|---:|---:|
| ~25,000 (lab) — ~4,800,000 (full) | ~2,000 (lab) — ~400,000 (full) | 0.07–0.09 |

> 💡 **Why ~8% positive class?** That's the historical rate at MSE — about 1 in 12 alerts dispositioned by analysts as confirmed manipulation. Production data has the same rate. The synthetic generator preserves it. The 8% rate is what makes XGBoost's `scale_pos_weight` matter (Lab 5.2 covers this).

If `total` is 0, the SMRITI batch load from Lab 1.2 wasn't run — go back and execute it. If `positive_rate` is wildly different (e.g. 0.50), the synthetic generator was run with non-default seed; regenerate with `--seed 42`.

## Step 2 — Launch the Hyperopt search

In a CML workbench session:

```bash
python src/ml/train_alert_ranker.py --max-trials 50
```

> 💡 **Don't pass `--register` yet.** Lab 5.1's job is to produce the trial population in MLflow. Lab 5.2 verifies thresholds before promoting. If you pass `--register` here and the thresholds fail, you've registered a bad model — registry pollution that's annoying to clean up.

**Expected output** (logs to stdout, takes 30–60 minutes):

```
==> Loading training data from Bronze + Gold
    25,000 labeled alerts; positive class = 8.124%
    train: 23,500 (8.05% pos);  test: 1,500 (8.97% pos)
==> Running Hyperopt with 50 trials (5-fold time-series CV)
100%|████████████████████████████| 50/50 [12:34<00:00, 15.09s/trial]
==> Final fit on full training set with best hyperparameters
==> Test AUC: 0.8540 (threshold 0.82) — PASS
==> Top-decile precision: 0.6233 (threshold 0.55) — PASS
==> Average precision: 0.4180
==> MLflow run id: 7c3e8a92...
```

> 💡 **What `5-fold time-series CV` does for you:** the dataset is sorted by alert `fired_ts`, then split into 5 chronological folds. Train on folds 1–4, test on fold 5; train on 1–3 + 5, test on 4; etc. This avoids the leakage you'd get from plain k-fold (where future data could inform predictions about past). For surveillance models, **time-series CV is non-optional.**

If trials report `NaN` losses, see Common Failure Mode #1.
If the final test AUC is below 0.82, that's expected at this stage — Lab 5.2 has the formal verification + retraining path.

## Step 3 — Open the MLflow UI

In CML, navigate to **MLflow** tab → **Experiments** → `argus_${STUDENT_ID}_alert_ranking_v1`.

**Expected:** 51 runs total (50 Hyperopt trials + one `final_fit`).

> 💡 **The experiment name format `argus_${STUDENT_ID}_alert_ranking_v1`** is fixed by ARGUS naming convention. The `_v1` suffix is for future model versions if the architecture changes (e.g. switching to a sequence model). Stick with v1 for the capstone.

## Step 4 — Inspect the search trajectory

In the MLflow UI:
1. Sort runs by `start_time DESC`
2. Add a column for the `loss` metric (it's negative mean AUC across folds, so **lower = better**)
3. Add columns for `n_estimators`, `max_depth`, `learning_rate`, `scale_pos_weight`

**Expected:** the trajectory should show **decreasing loss over the trial sequence**.

Hyperopt biases toward exploration early, exploitation late. Early trials probe the search space (loss values scattered widely). Late trials cluster near the best region (loss values concentrated lower).

If the trajectory is **flat or random**, Hyperopt isn't converging. Most likely cause: search space is too narrow — every trial gets near-identical hyperparameters because the `quniform` discretization is too coarse. Less common: training data is too small to differentiate hyperparameters (signal lost in noise).

> 💡 **Reading the trajectory:** the *first 10 trials* will have noisy loss because Hyperopt is just sampling uniformly. Look at trials 11–50; you should see the worst losses dropping out of the picture and the best losses tightening. If trial 50's best is essentially equal to trial 10's median, no learning happened.

## Step 5 — Inspect the final-fit run

Click into the `final_fit` run. Verify:

- **Parameters tab:** shows the 7 best hyperparameters from the search (the `best` dict returned by Hyperopt) — `n_estimators`, `max_depth`, `learning_rate`, `subsample`, `colsample_bytree`, `min_child_weight`, `scale_pos_weight`.
- **Metrics tab:** shows `test_auc`, `test_average_precision`, `test_top_decile_precision`, and `n_trials`.
- **Artifacts tab:** shows the serialized XGBoost model under `model/` — that's what Lab 5.2 will load to compute SHAP and what JOB-09 will pull from the registry to score alerts.

> 💡 **Why is the `final_fit` run separate from the Hyperopt trials?** Each Hyperopt trial trains 5 CV folds (each on 80% of the data) — the model from a trial is only ever fit on partial data. After Hyperopt finds the best hyperparameters, the script does **one final fit on the full training set** (90% of data) using those hyperparameters. That's the model that goes to the registry. The Hyperopt trial models are throwaway after the search ends.

## Step 6 — Verify CP-12 pass conditions

CP-12 has **four checks**.

### Check 1 — Experiment exists with ≥ 50 runs

```python
import mlflow
mlflow.set_experiment("argus_${STUDENT_ID}_alert_ranking_v1")
runs = mlflow.search_runs(experiment_names=["argus_${STUDENT_ID}_alert_ranking_v1"])
print(f"Total runs: {len(runs)}")
```
**Pass if:** ≥ 50. **Fail if:** < 50 — Hyperopt didn't complete all trials (likely crashed; check logs).

### Check 2 — All trials log a `loss` metric

```python
print(f"Runs with loss metric: {runs['metrics.loss'].notna().sum()}")
```
**Pass if:** ≥ 50. **Fail if:** smaller — some trials returned NaN; see Common Failure Mode #1.

### Check 3 — Search converges (best 5 trials < median trial)

```python
sorted_loss = runs["metrics.loss"].dropna().sort_values()
median = sorted_loss.median()
top5_mean = sorted_loss.head(5).mean()
print(f"Median trial loss: {median:.4f};  Best 5 mean: {top5_mean:.4f}")
print(f"Separation: {median - top5_mean:.4f}")
```
**Pass if:** `median - top5_mean > 0.005` (clear separation). **Fail if:** essentially equal — Hyperopt didn't improve. See Common Failure Mode #2.

### Check 4 — `final_fit` run carries the model artifact

```python
final_run = runs[runs["tags.mlflow.runName"] == "final_fit"].iloc[0]
artifacts = mlflow.artifacts.list_artifacts(run_id=final_run.run_id)
artifact_paths = [a.path for a in artifacts]
print(f"Final-fit artifacts: {artifact_paths}")
```
**Pass if:** `'model'` is in the list. **Fail if:** missing — the `mlflow.xgboost.log_model` call didn't run (script crashed before the final fit completed, or MLflow lost the artifact upload).

---

## Common failure mode #1 — Hyperopt trials fail with `NaN` loss

**Symptom:** most or all trials report `STATUS_FAIL`, MLflow loss column is NaN.

**Cause:** feature extraction returning NaN/-Inf. XGBoost handles NaN; Hyperopt's TPE doesn't (NaN poisons the next-trial computation). **The most common source: `notional_traded_log10` when `notional_traded` is 0.** `log10(0)` is `-inf`, and a single `-inf` in the feature matrix poisons every fold.

**Diagnosis:**
```python
from src.ml.feature_extraction import extract_feature_matrix
import numpy as np
spark = SparkSession.builder.getOrCreate()
sample = spark.table("argus_${STUDENT_ID}_gold.alert_candidates").limit(2000).toPandas()
X = extract_feature_matrix(sample)
print(f"NaN cells: {np.isnan(X).sum()}")
print(f"Inf cells: {np.isinf(X).sum()}")
```
If either is non-zero, you have the bug.

**Fix:** the `_safe_log10` helper in `feature_extraction.py` clips its input to ≥ 1.0 to avoid `-inf`. If you've modified it, restore the clip:
```python
def _safe_log10(x: float) -> float:
    return float(np.log10(max(x, 1.0)))   # clip to avoid -inf
```
Re-run the search.

## Common failure mode #2 — Hyperopt doesn't converge

**Symptom:** Step 4 shows trial loss flat or randomly scattered. Best 5 trials are essentially as bad as the median.

**Cause** (in decreasing likelihood):
1. **Search space too narrow.** All trials get near-identical hyperparameters; no signal differentiates them.
2. **Training data too small.** At `--scale 0.05` with ~25K alerts, signal is sometimes weak.
3. **Discretization too coarse.** `quniform(0.01, 0.30, 0.05)` only gives 6 distinct values — most trials get the same.

**Diagnosis:** in MLflow UI, sort runs by `learning_rate`. If you see only 3–5 distinct values, the discretization is too coarse.

**Fix:**
1. Open `src/ml/train_alert_ranker.py`, find `SEARCH_SPACE`, replace `quniform(0.01, 0.30, 0.05)` with `loguniform(np.log(0.005), np.log(0.30))` for `learning_rate`.
2. Bump `--scale 0.10` if compute allows (twice the data).
3. Re-run.

## Common failure mode #3 — MLflow tracking server unreachable

**Symptom:** script fails with `mlflow.exceptions.MlflowException: API request to ... failed with code 502`.

**Cause:** MLflow tracking server in CML is restarting or misconfigured.

**Fix:**
```python
import mlflow
print(mlflow.get_tracking_uri())
# Should be your CML MLflow endpoint (https://...), not file:///
```
If it shows `file:///`, the env var isn't set:
```bash
export MLFLOW_TRACKING_URI=https://${CML_MLFLOW_ENDPOINT}
```
Re-run script.

## Common failure mode #4 — OOM during Hyperopt trials

**Symptom:** trials around 30–40 fail with `MemoryError`.

**Cause:** XGBoost holds all 5 CV folds' data + tree structures in memory. At full scale, default workbench memory (8GB) isn't enough.

**Fix:** request a larger workbench (16GB or 32GB), restart the Python kernel, re-run.

---

## Pass condition for CP-12

All four checks pass:
- ✅ Experiment exists with ≥ 50 runs
- ✅ All trials log a `loss` metric
- ✅ Search converges (best 5 trials < median trial loss)
- ✅ `final_fit` run carries the model artifact

When all four pass, the model has the audit trail SEBI will require. **The model itself isn't yet verified or registered** — that's Lab 5.2's job.

## Wrap-up — what you can now do that you couldn't before

You can launch a Bayesian hyperparameter search via Hyperopt and track every trial in MLflow. You can read a search trajectory and tell whether the algorithm converged. You can produce a registered model artifact for downstream registration. You understand why **time-series cross-validation is non-optional for surveillance ML**.

Lab 5.2 verifies the trained model meets operational thresholds and registers it to MLflow Staging. Allow ~45 minutes.
