# Lab 5.1 — MLflow Training (CP-12)

> ℹ️ **Module:** 5 — ML Alert Risk-Ranking
> **Closes deficiency:** ARG-3 (false-positive flood)
> **Source files:** [`src/ml/train_alert_ranker.py`](../src/ml/train_alert_ranker.py), [`src/ml/feature_extraction.py`](../src/ml/feature_extraction.py)

## Objectives

- Launch the Hyperopt 50-trial Bayesian search over XGBoost hyperparameters
- Verify all runs land in MLflow experiment `argus_${STUDENT_ID}_alert_ranking_v1`
- Confirm hyperparameter and metric tracking is working end-to-end before committing to the final fit in Lab 5.2

## Why this matters

Surveillance in financial services lives or dies on defensibility. When SEBI asks "why did your model deprioritize this alert?", the answer cannot be "we tried a few things and this one worked best." It has to be "we ran a 50-trial Bayesian search across a defined hyperparameter space, with 5-fold time-series cross-validation; here are all 50 runs in MLflow with their AUCs, here are the SHAP explanations of the top-10 features, here is the manual promotion record from Staging to Production by Senior Director X on date Y." Lab 5.1 establishes the trail.

## Procedure

### Step 1 — Confirm prerequisites

```sql
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN is_confirmed_manipulation = TRUE THEN 1 ELSE 0 END) AS positives,
    AVG(CAST(is_confirmed_manipulation AS INT)) AS positive_rate
FROM argus_${STUDENT_ID}_bronze.legacy_alerts;
```

**Expected output**:

| total | positives | positive_rate |
|---:|---:|---:|
| ~25,000 (lab) — ~4,800,000 (full) | ~2,000 (lab) — ~400,000 (full) | 0.07–0.09 |

If `total` is 0, the SMRITI batch load from Lab 1.2 wasn't run — go back and execute it. If `positive_rate` is wildly different from the 8% expected, the synthetic generator was run with a non-default seed; re-generate with `--seed 42`.

### Step 2 — Launch the Hyperopt search

```bash
# In a CML workbench session with PySpark + xgboost + hyperopt + mlflow available
python src/ml/train_alert_ranker.py --max-trials 50
```

> 💡 **Tip:** Don't pass `--register` yet. Lab 5.1 is about producing the trial population in MLflow; Lab 5.2 verifies the final model passes thresholds before promoting.

**What you should see**: the script logs progress as Hyperopt explores the search space. Each trial trains 5 cross-validation folds and reports a mean AUC.

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

(Numbers vary; the run id will be different.)

### Step 3 — Open the MLflow UI

In CML, navigate to the MLflow tab and select the `argus_${STUDENT_ID}_alert_ranking_v1` experiment. You should see 51 runs total: 50 Hyperopt trials plus one `final_fit` run.

### Step 4 — Inspect the search trajectory

In the MLflow UI:

1. Sort runs by `start_time DESC`
2. Add a column for the `loss` metric (it's the negative mean AUC across folds, so lower = better)
3. Add columns for `n_estimators`, `max_depth`, `learning_rate`, `scale_pos_weight`

The trajectory should show **decreasing loss over the trial sequence** — Hyperopt is biased toward exploration early and exploitation late, so you'll see the loss values get more concentrated in the lower portion of the range as trials progress. If the trajectory is flat or random, Hyperopt isn't actually converging — most likely cause is the search space is too narrow (every trial gets the same hyperparameters because the `quniform` discretization is too coarse).

### Step 5 — Inspect the final-fit run

Click into the `final_fit` run. Verify:

- **Parameters tab**: shows the 7 best hyperparameters from the search (the `best` dict returned by Hyperopt)
- **Metrics tab**: shows `test_auc`, `test_average_precision`, `test_top_decile_precision`, and `n_trials`
- **Artifacts tab**: shows the serialized XGBoost model under `model/`

## Checkpoint CP-12 — MLflow tracking and Hyperopt search complete

### Pass condition

All four checks pass.

### Check 1 — Experiment exists with ≥ 50 runs

```python
import mlflow
mlflow.set_experiment("argus_${STUDENT_ID}_alert_ranking_v1")
runs = mlflow.search_runs(experiment_names=["argus_${STUDENT_ID}_alert_ranking_v1"])
print(f"Total runs: {len(runs)}")
# expect: >= 50 (50 Hyperopt + final fit)
```

### Check 2 — All trials log a `loss` metric

```python
print(f"Runs with loss metric: {runs['metrics.loss'].notna().sum()}")
# expect: >= 50
```

### Check 3 — Search converges — best 5 trials have loss < median trial loss

```python
sorted_loss = runs["metrics.loss"].dropna().sort_values()
median = sorted_loss.median()
top5_mean = sorted_loss.head(5).mean()
print(f"Median trial loss: {median:.4f};  Best 5 mean: {top5_mean:.4f}")
# expect: top5_mean < median - 0.005 (clear separation)
```

If `top5_mean` is essentially equal to the median, Hyperopt didn't find any improvement during the search — the search space might be miscalibrated (most likely too narrow `learning_rate` range), or training data is too small to differentiate. At lab scale 0.05 with ~25,000 alerts, the signal is sometimes weak; bump to scale 0.1 if you can spare the wall time.

### Check 4 — `final_fit` run carries the model artifact

```python
final_run = runs[runs["tags.mlflow.runName"] == "final_fit"].iloc[0]
artifacts = mlflow.artifacts.list_artifacts(run_id=final_run.run_id)
artifact_paths = [a.path for a in artifacts]
print(f"Final-fit artifacts: {artifact_paths}")
# expect: includes 'model'
```

---

## Common failure mode — Hyperopt trials fail with "loss is NaN"

**Symptom**: most or all trials report `STATUS_FAIL` and the MLflow loss column is NaN.

**Diagnosis**: this is almost always feature-extraction returning NaN values that XGBoost can usually handle, but Hyperopt's TPE algorithm cannot when computing the next trial. The most common source is `notional_traded_log10` when `notional_traded` is 0 — `log10(0)` is `-inf`, and a single `-inf` in the feature matrix poisons every fold.

**Fix**: the `_safe_log10` helper in `feature_extraction.py` clips its input to ≥ 1.0 specifically to avoid this. If you've modified that helper, restore the clip:

```python
def _safe_log10(x: float) -> float:
    return float(np.log10(max(x, 1.0)))   # clip to avoid -inf
```

Re-run the search; trials should complete cleanly.

---

## Pass condition for CP-12

All four checks pass. With 50+ tracked trials and a final-fit model in MLflow, the model has the audit trail SEBI will require.
