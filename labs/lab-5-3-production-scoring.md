# Lab 5.3 — Production Scoring (CP-14)

> 👋 **Module 5 first-timer?** Read [`docs/module-5-primer.md`](../docs/module-5-primer.md) first.

> ℹ️ **Module:** 5 — ML Alert Risk-Ranking
> **Closes deficiency:** ARG-3 (production-ready scoring) — this is where the analyst queue actually re-ranks
> **Time:** ~60 minutes if Production promotion + JOB-09 deploy work first try; up to 2.5 hours if scoring fails partway through.
> **Source files:** [`src/ml/batch_score.py`](../src/ml/batch_score.py)

## What you're going to do

1. **Manually promote the Staging model to Production** in MLflow UI — the compliance gate. (~10 min)
2. **Deploy JOB-09 (`batch_score.py`)** as a CDE Spark job on a 5-minute schedule. (~10 min)
3. **Watch the first scoring cycle execute** — verify scores landing. (~5 min)
4. **Verify `model_score` populates** on `alert_candidates` for new alerts. (~5 min)
5. **Verify SHAP explanations populate** alongside scores. (~5 min)
6. **Verify the analyst queue is re-ranked** — Cases 0/1/2 in top, Case 6 not. (~10 min)
7. **Verify CP-14 pass conditions** — four checks. (~5 min)

Total: ~60 minutes.

## Before you begin — prerequisite checklist

- [ ] [Lab 5.2](lab-5-2-performance.md) is complete and CP-13 passed; model is in MLflow Staging
- [ ] You have access to the **MLflow UI** with permission to promote model versions
- [ ] You have CDE access to deploy a scheduled Spark job
- [ ] [Module 4 / Lab 4.1](lab-4-1-governed-views.md) is complete (`vw_alert_queue` exists)

## Why production scoring matters — read this before Step 1

Lab 5.1 produced a tracked training run. Lab 5.2 verified thresholds and registered to Staging. **Neither matters operationally until JOB-09 is consuming the candidate stream and writing scores back.**

CP-14 is what makes the analyst queue actually re-rank in real time. **The moment the surveillance team's day starts to look different from the legacy stack.** A junior analyst who used to wade through 100 alerts a day in chronological order now sees 10 ML-prioritized alerts at the top — alerts ordered by the probability they're actual manipulation. That's the operational delivery of ARG-3.

## Step 1 — Manual promotion to Production (compliance gate)

In the MLflow UI:

1. **Models** → `argus_${STUDENT_ID}_alert_ranker`
2. Find the latest **Staging** version (from Lab 5.2)
3. Click the version row → **Transition to Production**
4. **In the dialog, fill out the description** (this is the compliance trail):
   ```
   Approved by [your name], date [today].
   AUC=[value from Lab 5.2], top-decile precision=[value from Lab 5.2].
   CP-13 passed.
   ```
5. Click **Confirm**.

> ⚠️ **Compliance gate:** **This is a manual step. Do not write a script that auto-promotes from Staging to Production.** The whole point is the human in the loop. SEBI's draft AI/ML guidance for surveillance systems requires a documented human approval for every model promotion; the dialog text is what gets cited if the model decision is challenged.

After approval, verify:

```python
import mlflow
client = mlflow.tracking.MlflowClient()
latest_prod = client.get_latest_versions("argus_${STUDENT_ID}_alert_ranker", stages=["Production"])
print(f"Production version: v{latest_prod[0].version}")
```
**Expected:** prints a version number (e.g., `v3`). Empty list = promotion didn't take.

## Step 2 — Deploy JOB-09 to CDE

```bash
cde job create --name "argus-${STUDENT_ID}-job_09_batch_score" \
    --type spark \
    --application-file src/ml/batch_score.py \
    --py-files src/ml/feature_extraction.py \
    --executor-memory 4g --executor-cores 2 --num-executors 2 \
    --schedule "*/5 * * * *"
```

> 💡 **Why every 5 minutes?** Latency from "alert fires" → "score is in the queue" is bounded at 5 minutes. The analyst won't act in <5 min anyway, so 5-min batch is fine. Real-time inference would add infrastructure cost without operational benefit. XGBoost inference is also more efficient batched (vectorized).

Confirm the job is running:
```bash
cde job list | grep "argus-${STUDENT_ID}-job_09_batch_score"
```
**Expected:** status `RUNNING` (or recently completed if between cycles).

## Step 3 — Watch the first cycle execute

In the CDE UI, navigate to the job and watch the first cycle's logs:

```
==> Loading argus_${STUDENT_ID}_alert_ranker v3 (Production)
==> Scoring 247 pending alerts
==> Scored: 247  |  HIGH (>=0.9): 18  MED (>=0.5): 41  LOW (<0.5): 188
==> Top decile threshold: 0.7233
```

(Numbers vary with how many pending alerts exist when the cycle runs.)

> 💡 **What `Top decile threshold` means:** the 90th percentile of model_score across the scored batch. JOB-09 logs this so analysts know what's "top 10%" today. The threshold drifts day-to-day with the alert mix; rather than hard-coding 0.7, the queue-rendering logic uses this dynamic threshold.

If the first cycle fails, see Common Failure Mode #1.

## Step 4 — Verify `model_score` populated on `alert_candidates`

```sql
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN model_score IS NOT NULL THEN 1 ELSE 0 END) AS scored,
    SUM(CASE WHEN model_score >= 0.9 THEN 1 ELSE 0 END) AS high_priority,
    AVG(model_score) AS avg_score
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE trade_date >= CURRENT_DATE - 1;
```

**Expected:** `scored / total >= 0.90` (most alerts scored within their 5-minute window). `avg_score` typically lands in 0.10–0.25 because most alerts (~92%) are eventually no-action; the score distribution is right-skewed.

If `scored / total < 0.50` after JOB-09 has run for an hour, see Common Failure Mode #1.

## Step 5 — Verify SHAP explanations are populated

```sql
SELECT
    alert_id,
    rule_id,
    severity,
    model_score,
    SUBSTR(shap_explanations, 1, 300) AS shap_preview
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE model_score IS NOT NULL
  AND trade_date >= CURRENT_DATE - 1
ORDER BY model_score DESC
LIMIT 5;
```

**Expected:** 5 rows; `shap_preview` shows JSON arrays of feature contributions, e.g.:

```json
[{"feature":"pct_cancelled_under_50ms","shap_value":1.842305},
 {"feature":"cross_product_delta_imbalance","shap_value":0.913221},
 {"feature":"member_historical_confirm_rate","shap_value":0.504876},
 ...]
```

> 💡 **What's in `shap_explanations`:** for every scored alert, JOB-09 computes per-row SHAP values for all features and stores the top-10 by absolute SHAP value as JSON. This is the per-alert evidence trail. When an analyst challenges a low score, the SHAP shows which features pulled the score down. When SEBI challenges a model decision, the SHAP is the legal defense.

Each row's top-10 features should include at least 2 features with absolute SHAP values > 0.5. If SHAP values are all near zero (like 0.001), the model has very little discriminative power on those alerts — likely fine if they're genuinely benign cases.

## Step 6 — Verify the analyst queue is re-ranked

Re-query `vw_alert_queue` from Module 4, sorted by `model_score DESC`:

```sql
SELECT
    alert_id,
    rule_id,
    pattern_type,
    severity,
    member_firm_id,
    member_firm_name,
    instrument_code,
    model_score
FROM argus_${STUDENT_ID}_views.vw_alert_queue
WHERE model_score IS NOT NULL
ORDER BY model_score DESC, severity DESC, fired_ts DESC
LIMIT 20;
```

**Expected output:** the top 20 should be dominated by:
- Alerts from member firms in the planted real-case set (BNXM-0042, BNXM-0117, BNXM-0231, BNXM-0089, BNXM-0276)
- High `severity` values (CRITICAL, HIGH)
- Rules R-104 (cross-product) and R-102 (layering) appearing more often than R-103 (momentum) or R-105 (wash)

**Critical:** alerts from BNXM-0001 (Case 6 — legitimate tier-1 market maker) should have low `model_score` and **NOT appear in the top 20**.

> 💡 **This is the entire point of the module.** The legacy platform showed Case 6 alerts to the analyst with equal priority — analysts wasted time on them. ARGUS doesn't. The 92% no-action rate becomes ~30% because the no-action alerts no longer dominate the top of the queue.

If Case 6 alerts appear in the top 20, see Common Failure Mode #2.

## Step 7 — Verify CP-14 pass conditions

CP-14 has **four checks**.

### Check 1 — Production model registered

```python
client = mlflow.tracking.MlflowClient()
prod = client.get_latest_versions("argus_${STUDENT_ID}_alert_ranker", stages=["Production"])
assert len(prod) > 0, "No Production version"
print(f"Production: v{prod[0].version}")
```
**Pass if:** non-empty list. **Fail if:** empty — Step 1 didn't take.

### Check 2 — JOB-09 running on schedule

```bash
cde job describe --name "argus-${STUDENT_ID}-job_09_batch_score" | grep -E '(schedule|status)'
```
**Pass if:** schedule = `*/5 * * * *` AND at least 3 successful runs in the past 30 minutes. **Fail if:** stopped or fewer runs.

### Check 3 — Score coverage ≥ 90%

The Step 4 query: `scored / total >= 0.90`. **Pass if:** yes. **Fail if:** below.

### Check 4 — Cases 0/1/2 in top 20; Case 6 not

The Step 6 query's top 20 includes at least one alert from BNXM-0042, BNXM-0117, or BNXM-0231 (planted real cases). **Critically, it does NOT include any alert from BNXM-0001** (planted Case 6, legitimate tier-1 MM). **Pass if:** both. **Fail if:** Case 6 in top 20 — model isn't separating legitimate MM from manipulation. Lab 5.2 retraining problem; not a Lab 5.3 deployment problem.

---

## Common failure mode #1 — JOB-09 scores some alerts but not others

**Symptom:** after JOB-09 has run for an hour, `scored / total` is around 0.60 instead of 0.90+. New alerts keep firing faster than JOB-09 can score them.

**Cause:** at lab scale this shouldn't happen. JOB-09 is configured to score 50,000 pending alerts per cycle (far above lab data volume). If you see this, JOB-09 is probably failing partway through (e.g., OOM on the SHAP computation) and rolling back.

**Diagnosis:** check the most recent JOB-09 run logs in CDE for stack traces. Look for:
- `MemoryError` in `compute_shap_explanations` → OOM
- `mlflow.exceptions.MlflowException: No model version found in stage Production` → Step 1 not done
- `ModuleNotFoundError: shap` → workbench env doesn't have SHAP

**Fix sequence:**
1. **OOM:** increase `--executor-memory` to 8g, restart job.
2. **No Production model:** go back to Step 1, do the manual promotion.
3. **SHAP missing:** ensure `--py-files src/ml/feature_extraction.py` is set on the job; verify SHAP is in the workbench requirements.txt.
4. **Throughput pressure (very rare at lab scale):** drop the per-cycle batch limit from 50,000 to 10,000 in `batch_score.py`'s `fetch_pending_alerts` query.

## Common failure mode #2 — Case 6 (legitimate MM) appears in top 20

**Symptom:** Step 6's top 20 includes BNXM-0001 alerts.

**Cause:** model isn't separating legitimate market making from manipulation. This is a **Lab 5.2 retraining problem**, not a Lab 5.3 deployment problem.

**Diagnosis:**
```python
# Check Case 6 alert scores
import mlflow
import pandas as pd
case6_df = spark.sql(
    "SELECT alert_id, member_firm_id, model_score, severity "
    "FROM argus_${STUDENT_ID}_gold.alert_candidates "
    "WHERE member_firm_id = 'BNXM-0001' "
    "AND model_score IS NOT NULL"
).toPandas()
print(f"Case 6 score median: {case6_df.model_score.median():.4f}")
print(f"Case 6 score 90th pct: {case6_df.model_score.quantile(0.90):.4f}")
```

If the 90th percentile is > 0.5, the model is treating BNXM-0001 alerts as suspicious — but they should be < 0.3.

**Fix:** retrain with broader hyperparameter ranges OR more training data. Go back to Lab 5.1 / 5.2:
1. Increase `scale_pos_weight` upper bound in SEARCH_SPACE
2. Add `--scale 0.10` for more training data
3. Re-run Lab 5.1 + Lab 5.2 + Lab 5.3 Step 1 (re-promote)

## Common failure mode #3 — JOB-09 fails immediately on first cycle

**Symptom:** first JOB-09 cycle errors out with `ModuleNotFoundError: feature_extraction`.

**Cause:** `--py-files src/ml/feature_extraction.py` was not set or not packaged correctly.

**Fix:**
```bash
cde job update --name "argus-${STUDENT_ID}-job_09_batch_score" \
    --py-files src/ml/feature_extraction.py
cde job restart --name "argus-${STUDENT_ID}-job_09_batch_score"
```

## Common failure mode #4 — `model_score` is non-NULL but the queue is unsorted

**Symptom:** Step 4 shows scores populated, but Step 6 shows alerts in chronological order (not score order).

**Cause:** the BI tool / analyst client is sorting by `fired_ts` instead of `model_score`. The view itself doesn't enforce sort order; the consumer does.

**Fix:** ensure analyst-facing applications/dashboards explicitly `ORDER BY model_score DESC, severity DESC, fired_ts DESC`. The view definition can include this for default sort.

---

## Pass condition for CP-14

All four checks pass:
- ✅ Production model registered
- ✅ JOB-09 running on schedule
- ✅ Score coverage ≥ 90%
- ✅ Cases 0/1/2 in top 20; Case 6 not in top 20

When all four pass, the analyst queue re-ranks in near-real-time, the SHAP explanations are available for any alert challenge, and **ARG-3 — the 92% noise problem that defined the legacy stack — is operationally closed**.

## Wrap-up — what you can now do that you couldn't before

You can promote a model from Staging to Production via the MLflow Model Registry with proper compliance documentation. You can deploy a CDE Spark job that loads a Production model and writes scores back to a Gold table on a schedule. You understand why the manual promotion gate exists. You can interpret an analyst queue ordered by ML score and verify the model is separating real manipulation from legitimate market activity.

**Module 5 is complete.** Module 6 next builds the GenAI / RAG pipeline that drafts STR (Suspicious Transaction Report) narratives for analysts. Allow about 4 hours for Module 6.
