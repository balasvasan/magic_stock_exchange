# Lab 5.3 — Production Scoring (CP-14)

> ℹ️ **Module:** 5 — ML Alert Risk-Ranking
> **Closes deficiency:** ARG-3 (production-ready scoring)
> **Source files:** [`src/ml/batch_score.py`](../src/ml/batch_score.py)

## Objectives

- Promote the Staging model to Production via MLflow Model Registry
- Deploy JOB-09 (`batch_score.py`) as a CDE Spark job on a 5-minute schedule
- Verify `model_score` and `shap_explanations` populate on `argus_${STUDENT_ID}_gold.alert_candidates` for new alerts within 5 minutes of firing

## Why this matters

Lab 5.1 produced a tracked training run; Lab 5.2 verified thresholds and registered to Staging. Neither matters operationally until JOB-09 is consuming the candidate stream and writing scores back. CP-14 is what makes the analyst queue actually re-rank in real time — the moment the surveillance team's day starts to look different from the legacy stack.

## Procedure

### Step 1 — Manual promotion to Production (compliance gate)

In the MLflow UI:

1. Models → `argus_${STUDENT_ID}_alert_ranker`
2. Find the latest Staging version (from Lab 5.2)
3. Click the version row → "Transition to Production"
4. **In the dialog, fill out the description**: "Approved by [analyst name], date [today]. AUC=[value], top-decile precision=[value]. CP-13 passed."

The text in the description is the compliance trail. SEBI's draft AI/ML guidance for surveillance systems requires a documented human approval for every model promotion; the dialog text is what gets cited if challenged.

> ⚠️ **Compliance gate:** This is a manual step. Do not write a script that auto-promotes from Staging to Production. The whole point is the human in the loop.

After approval, verify:

```python
import mlflow
client = mlflow.tracking.MlflowClient()
latest_prod = client.get_latest_versions("argus_${STUDENT_ID}_alert_ranker", stages=["Production"])
print(f"Production version: v{latest_prod[0].version}")
# expect: a version number, not empty
```

### Step 2 — Deploy JOB-09 to CDE

```bash
cde job create --name argus-job_09_batch_score \
    --type spark \
    --application-file src/ml/batch_score.py \
    --py-files src/ml/feature_extraction.py \
    --executor-memory 4g --executor-cores 2 --num-executors 2 \
    --schedule "*/5 * * * *"   # every 5 minutes
```

Confirm the job is running on schedule:

```bash
cde job list | grep argus-job_09_batch_score
# expect: status=RUNNING (or recently completed if between cycles)
```

### Step 3 — Watch the first cycle execute

In the CDE UI, navigate to the job and watch the first cycle's logs:

```
==> Loading argus_${STUDENT_ID}_alert_ranker v3 (Production)
==> Scoring 247 pending alerts
==> Scored: 247  |  HIGH (>=0.9): 18  MED (>=0.5): 41  LOW (<0.5): 188
==> Top decile threshold: 0.7233
```

(Numbers vary with how many pending alerts exist at the moment.)

### Step 4 — Verify model_score populated on alert_candidates

```sql
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN model_score IS NOT NULL THEN 1 ELSE 0 END) AS scored,
    SUM(CASE WHEN model_score >= 0.9 THEN 1 ELSE 0 END) AS high_priority,
    AVG(model_score) AS avg_score
FROM argus_${STUDENT_ID}_gold.alert_candidates
WHERE trade_date >= CURRENT_DATE - 1;
```

**Expected output**: `scored / total >= 0.90` (most alerts scored within their 5-minute window). `avg_score` typically lands in 0.10–0.25 because most alerts (~92%) are eventually no-action; the score distribution is right-skewed.

### Step 5 — Verify SHAP explanations are populated

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

**Expected output**: 5 rows; the `shap_preview` column shows JSON arrays of feature contributions, e.g.:

```json
[{"feature":"pct_cancelled_under_50ms","shap_value":1.842305},
 {"feature":"cross_product_delta_imbalance","shap_value":0.913221},
 {"feature":"member_historical_confirm_rate","shap_value":0.504876},
 ...]
```

Each row's top-10 features should include at least 2 features with absolute SHAP values > 0.5. If SHAP values are all near zero (like 0.001), the model has very little discriminative power on those alerts — likely fine if they're genuinely benign cases.

### Step 6 — Verify the analyst queue is now sorted by score

Re-query `vw_alert_queue` from Module 4, but this time sort by `model_score DESC`:

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

**Expected output**: the top of the queue should be dominated by:

- Alerts from member firms in the planted real-case set (BNXM-0042, BNXM-0117, BNXM-0231, BNXM-0089, BNXM-0276)
- High `severity` values (CRITICAL, HIGH)
- Rules R-104 (cross-product) and R-102 (layering) appearing more often than R-103 (momentum) or R-105 (wash)

Alerts from BNXM-0001 (Case 6 — the legitimate tier-1 market maker) should have low `model_score` and **not** appear in the top 20. That's the entire point of the module: the legacy platform showed those alerts to the analyst with equal priority; ARGUS doesn't.

## Checkpoint CP-14 — Production scoring writes back continuously

### Pass condition

All four checks pass.

### Check 1 — Production model registered

`mlflow.tracking.MlflowClient().get_latest_versions("argus_${STUDENT_ID}_alert_ranker", stages=["Production"])` returns a non-empty list.

### Check 2 — JOB-09 is running on schedule

CDE shows `argus-job_09_batch_score` as scheduled `*/5 * * * *` and at least 3 successful runs in the past 30 minutes.

### Check 3 — Score coverage ≥ 90%

The Step 4 query reports `scored / total >= 0.90`.

### Check 4 — Cases 0/1/2 surface in the top 20 of the queue; Case 6 does not

The Step 6 query's top 20 includes at least one alert from BNXM-0042, BNXM-0117, or BNXM-0231 (planted real cases). It does **not** include any alert from BNXM-0001 (planted Case 6 — legitimate tier-1 MM).

If Case 6 alerts appear in the top 20: the model isn't separating legitimate market making from manipulation. That's a Lab 5.2 retraining problem, not a Lab 5.3 deployment problem — go back and refit with broader hyperparameter ranges or more training data.

---

## Common failure mode — JOB-09 scores some alerts but not others

**Symptom**: after JOB-09 runs for an hour, `scored / total` is around 0.60 instead of 0.90+. New alerts keep firing faster than JOB-09 can score them.

**Diagnosis**: at lab scale this shouldn't happen — JOB-09 is configured to score 50,000 pending alerts per cycle, which is far above the lab data volume. If you see this, JOB-09 is probably failing partway through (e.g. OOM on the SHAP computation) and rolling back. Symptom looks the same as throughput pressure.

**Fix sequence**:

1. Check the most recent JOB-09 run logs in CDE for stack traces.
2. If you see `MemoryError` in `compute_shap_explanations`, increase `--executor-memory` to 8g.
3. If you see `mlflow.exceptions.MlflowException: No model version found in stage Production`, you skipped Step 1 — go promote the model.
4. If neither, drop the per-cycle batch limit from 50,000 to 10,000 in `batch_score.py`'s `fetch_pending_alerts` query — smaller batches commit faster.

---

## Pass condition for CP-14

All four checks pass. With JOB-09 running every 5 minutes and writing back to `alert_candidates`, the analyst queue re-ranks in near-real-time, the SHAP explanations are available for any alert challenge, and ARG-3 — the 92% noise problem that defined the legacy stack — is operationally closed.
