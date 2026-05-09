#!/usr/bin/env python3
"""
JOB-09 — batch_score (production scoring of pending alerts)
============================================================
Loads the current Production-stage XGBoost model from MLflow Model Registry,
scores every PENDING alert in gold.alert_candidates, and writes back
model_score, model_version, scored_at, and shap_explanations.

PRD reference: §7 (JOB-09); closes ARG-3 in production.
Schedule: every 5 minutes.
Source:   <gold>.alert_candidates WHERE model_score IS NULL
Sink:     <gold>.alert_candidates (UPDATE in place via MERGE)
Model:    'argus_${SID}_alert_ranker' from MLflow registry, stage='Production'

This job is intentionally simple: load model, score, write back. All the
complexity (feature definitions, hyperparameter tuning, model selection)
lives in train_alert_ranker.py. The split is deliberate — training is a
weekly batch, scoring is a 5-minute cycle, and they have very different
operational concerns.

Resource names (model registry, alert table, app name) resolved from
src.common.naming using ${STUDENT_ID}.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import mlflow
import numpy as np
import pandas as pd
import shap

from feature_extraction import extract_feature_matrix, get_feature_names

from src.common.naming import fqtn, mlflow_model, cde_job

PRODUCTION_STAGE = "Production"
TOP_K_SHAP = 10  # SHAP explanations: top-10 features per alert


def load_production_model():
    """Pull the current Production-stage model from MLflow Model Registry.
    Falls back to Staging if no Production version exists (lab convenience)."""
    registry_name = mlflow_model("alert_ranker")
    client = mlflow.tracking.MlflowClient()
    for stage in (PRODUCTION_STAGE, "Staging"):
        versions = client.get_latest_versions(registry_name, stages=[stage])
        if versions:
            v = versions[0]
            print(f"==> Loading {registry_name} v{v.version} ({stage})")
            return mlflow.xgboost.load_model(f"models:/{registry_name}/{stage}"), v.version
    raise RuntimeError(f"No model registered as {registry_name}; train one first.")


def fetch_pending_alerts(spark) -> pd.DataFrame:
    """Get alert_candidates rows that haven't been scored yet."""
    df = spark.sql(f"""
        SELECT alert_id, fired_ts, rule_id, severity, pattern_type,
               member_firm_id, trader_id, instrument_code, underlying_code,
               window_start_ts, window_end_ts, features, trade_date
        FROM {fqtn("gold", "alert_candidates")}
        WHERE model_score IS NULL
          AND disposition = 'PENDING'
        LIMIT 50000
    """).toPandas()
    return df


def compute_shap_explanations(model, X: np.ndarray) -> list[str]:
    """Per-alert top-K SHAP feature contributions, serialized as JSON."""
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    feature_names = get_feature_names()

    out: list[str] = []
    for i in range(X.shape[0]):
        contributions = list(zip(feature_names, shap_values[i].tolist()))
        # Sort by absolute contribution; keep top K
        top = sorted(contributions, key=lambda t: abs(t[1]), reverse=True)[:TOP_K_SHAP]
        out.append(json.dumps([
            {"feature": name, "shap_value": round(val, 6)}
            for name, val in top
        ]))
    return out


def main() -> int:
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.appName(cde_job("ml.batch_score")).getOrCreate()

    model, model_version = load_production_model()
    pending = fetch_pending_alerts(spark)

    if pending.empty:
        print("==> No pending alerts to score.")
        spark.stop()
        return 0

    print(f"==> Scoring {len(pending):,} pending alerts")
    X = extract_feature_matrix(pending)
    proba = model.predict_proba(X)[:, 1]
    pending["model_score"] = proba.round(6)
    pending["model_version"] = f"v{model_version}"
    pending["scored_at"] = pd.Timestamp.utcnow()
    pending["shap_explanations"] = compute_shap_explanations(model, X)

    # Write back via MERGE so we update only the rows we scored
    update_df = spark.createDataFrame(
        pending[["alert_id", "model_score", "model_version", "scored_at",
                 "shap_explanations"]]
    )
    update_df.createOrReplaceTempView("scored_alerts")
    spark.sql(f"""
        MERGE INTO {fqtn("gold", "alert_candidates")} t
        USING scored_alerts s ON t.alert_id = s.alert_id
        WHEN MATCHED THEN UPDATE SET
            t.model_score       = s.model_score,
            t.model_version     = s.model_version,
            t.scored_at         = s.scored_at,
            t.shap_explanations = s.shap_explanations
    """)

    # Quick distribution summary for ops dashboard
    high = (pending["model_score"] >= 0.9).sum()
    med  = ((pending["model_score"] >= 0.5) & (pending["model_score"] < 0.9)).sum()
    low  = (pending["model_score"] < 0.5).sum()
    print(f"==> Scored: {len(pending):,}  |  HIGH (>=0.9): {high:,}  "
          f"MED (>=0.5): {med:,}  LOW (<0.5): {low:,}")
    print(f"==> Top decile threshold: {pending['model_score'].quantile(0.90):.4f}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
