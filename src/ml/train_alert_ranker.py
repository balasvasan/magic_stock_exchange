#!/usr/bin/env python3
"""
train_alert_ranker — Module 5 ML training (closes ARG-3)
=========================================================
Trains an XGBoost binary classifier that ranks deterministic-rule alerts
by probability of confirmed manipulation. Output is consumed by
batch_score.py (JOB-09) every 5 minutes in production.

PRD reference: §8.
Algorithm:        XGBoost (gradient-boosted trees)
Target:           is_confirmed_manipulation (binary)
Training data:    bronze.legacy_alerts joined to gold.confirmed_manipulation_cases
Features:         60 features in 6 groups, see feature_extraction.py
Hyperparams:      Hyperopt 50-trial Bayesian search, 5-fold time-series CV
Performance:      AUC-ROC >= 0.82, top-decile precision >= 0.55 (CP-13)
Tracking:         MLflow experiment 'argus_${SID}_alert_ranking_v1'
Registry:         MLflow model 'argus_${SID}_alert_ranker'
Promotion:        Manual approval gate Staging -> Production (regulator
                  requires human in the loop on model promotions)

Resource names (MLflow experiment + model + table refs) resolved from
src.common.naming using ${STUDENT_ID}.

Usage:
    spark-submit --py-files src/ml/feature_extraction.py \\
        src/ml/train_alert_ranker.py --max-trials 50 --register
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make feature_extraction importable when running standalone, and project root
# importable for src.common.naming
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import xgboost as xgb
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from sklearn.metrics import (
    average_precision_score, precision_score, roc_auc_score
)
from sklearn.model_selection import TimeSeriesSplit

from feature_extraction import (
    N_FEATURES, extract_feature_matrix, get_feature_names,
)

from src.common.naming import (
    fqtn, mlflow_experiment, mlflow_model, cde_job,
)

MIN_AUC = 0.82                  # CP-13 pass threshold
MIN_TOP_DECILE_PRECISION = 0.55 # CP-13 pass threshold
SEARCH_SPACE = {
    "n_estimators":     hp.quniform("n_estimators", 200, 800, 50),
    "max_depth":        hp.quniform("max_depth", 4, 10, 1),
    "learning_rate":    hp.loguniform("learning_rate", np.log(0.01), np.log(0.2)),
    "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
    "subsample":        hp.uniform("subsample", 0.6, 1.0),
    "colsample_bytree": hp.uniform("colsample_bytree", 0.6, 1.0),
    "scale_pos_weight": hp.quniform("scale_pos_weight", 8, 15, 1),
}


def load_training_data(spark) -> pd.DataFrame:
    """Pull labeled alerts from Bronze + Gold; return single pandas DataFrame.

    The label is is_confirmed_manipulation. Training set is the 4.8M legacy
    alerts; the held-out test set is the most recent 30 days (no leakage).
    """
    sql = f"""
        SELECT
            l.alert_id, l.fired_ts, l.rule_id, l.severity, l.pattern_type,
            l.member_firm_id, l.trader_id, l.instrument_code,
            l.disposition, l.is_confirmed_manipulation,
            l.disposition_date,
            COALESCE(l.disposition_rationale, '')                     AS rationale,
            -- Reconstruct a synthetic features payload from the historical record.
            -- In production this would already be a JSON column, but the legacy
            -- vendor stored individual fields, so we reassemble.
            CAST(NULL AS STRING)                                       AS features
        FROM {fqtn("bronze", "legacy_alerts")} l
        WHERE l.disposition IN ('NO_ACTION', 'ESCALATED', 'CONFIRMED_MANIPULATION')
    """
    df = spark.sql(sql).toPandas()
    return df


def time_series_train_test_split(df: pd.DataFrame, holdout_days: int = 30):
    """Split chronologically — last 30 days = test, prior = train."""
    df = df.sort_values("fired_ts").reset_index(drop=True)
    cutoff = pd.to_datetime(df["fired_ts"]).max() - pd.Timedelta(days=holdout_days)
    train_mask = pd.to_datetime(df["fired_ts"]) < cutoff
    return df.loc[train_mask], df.loc[~train_mask]


def top_decile_precision(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Precision among the top 10% of predicted scores."""
    n = len(y_true)
    k = max(1, n // 10)
    top_idx = np.argsort(-y_score)[:k]
    return float(np.mean(y_true[top_idx]))


def objective(params: dict, X_tr, y_tr, cv) -> dict:
    """Hyperopt objective: minimize negative mean AUC across time-series CV folds."""
    params = {k: int(v) if k in ("n_estimators", "max_depth", "min_child_weight",
                                 "scale_pos_weight") else v
              for k, v in params.items()}
    aucs = []
    for tr_idx, va_idx in cv.split(X_tr):
        clf = xgb.XGBClassifier(
            objective="binary:logistic", eval_metric="auc",
            tree_method="hist", random_state=42, n_jobs=-1, **params,
        )
        clf.fit(X_tr[tr_idx], y_tr[tr_idx],
                eval_set=[(X_tr[va_idx], y_tr[va_idx])], verbose=False)
        aucs.append(roc_auc_score(y_tr[va_idx], clf.predict_proba(X_tr[va_idx])[:, 1]))
    return {"loss": -float(np.mean(aucs)), "status": STATUS_OK,
            "auc_mean": float(np.mean(aucs)), "auc_std": float(np.std(aucs))}


def main(max_trials: int = 50, register: bool = False) -> int:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(cde_job("ml.train_alert_ranker")).getOrCreate()

    # Resolve names once at startup (they need STUDENT_ID)
    experiment_name = mlflow_experiment("alert_ranking_v1")
    model_name = mlflow_model("alert_ranker")

    print("==> Loading training data from Bronze + Gold")
    df = load_training_data(spark)
    print(f"    {len(df):,} labeled alerts; positive class = {df['is_confirmed_manipulation'].mean():.3%}")

    train_df, test_df = time_series_train_test_split(df, holdout_days=30)
    X_tr = extract_feature_matrix(train_df)
    y_tr = train_df["is_confirmed_manipulation"].astype(int).to_numpy()
    X_te = extract_feature_matrix(test_df)
    y_te = test_df["is_confirmed_manipulation"].astype(int).to_numpy()
    print(f"    train: {len(y_tr):,} ({y_tr.mean():.3%} pos);  test: {len(y_te):,} ({y_te.mean():.3%} pos)")

    mlflow.set_experiment(experiment_name)
    cv = TimeSeriesSplit(n_splits=5)

    print(f"==> Running Hyperopt with {max_trials} trials (5-fold time-series CV)")
    trials = Trials()
    best = fmin(fn=lambda p: objective(p, X_tr, y_tr, cv),
                space=SEARCH_SPACE, algo=tpe.suggest, max_evals=max_trials,
                trials=trials, rstate=np.random.default_rng(42))
    best_params = {k: int(v) if k in ("n_estimators", "max_depth", "min_child_weight",
                                       "scale_pos_weight") else float(v)
                   for k, v in best.items()}

    print("==> Final fit on full training set with best hyperparameters")
    with mlflow.start_run(run_name="final_fit") as run:
        mlflow.log_params(best_params)
        clf = xgb.XGBClassifier(
            objective="binary:logistic", eval_metric="auc",
            tree_method="hist", random_state=42, n_jobs=-1, **best_params,
        )
        clf.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)

        proba = clf.predict_proba(X_te)[:, 1]
        auc = roc_auc_score(y_te, proba)
        ap = average_precision_score(y_te, proba)
        td_prec = top_decile_precision(y_te, proba)

        mlflow.log_metrics({
            "test_auc": auc,
            "test_average_precision": ap,
            "test_top_decile_precision": td_prec,
            "n_trials": float(max_trials),
        })
        mlflow.xgboost.log_model(clf, artifact_path="model",
                                 registered_model_name=model_name
                                 if register else None)

        passes_auc  = auc >= MIN_AUC
        passes_tdp  = td_prec >= MIN_TOP_DECILE_PRECISION
        print(f"==> Test AUC: {auc:.4f} (threshold {MIN_AUC}) — {'PASS' if passes_auc else 'FAIL'}")
        print(f"==> Top-decile precision: {td_prec:.4f} (threshold {MIN_TOP_DECILE_PRECISION}) — {'PASS' if passes_tdp else 'FAIL'}")
        print(f"==> Average precision: {ap:.4f}")
        print(f"==> MLflow run id: {run.info.run_id}")

        if register and passes_auc and passes_tdp:
            client = mlflow.tracking.MlflowClient()
            latest = client.get_latest_versions(model_name, stages=["None"])[0]
            client.transition_model_version_stage(
                model_name, latest.version, stage="Staging",
                archive_existing_versions=False,
            )
            print(f"==> Promoted {model_name} v{latest.version} to Staging "
                  f"(awaiting manual approval to Production)")

    spark.stop()
    return 0 if (passes_auc and passes_tdp) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-trials", type=int, default=50,
                        help="Hyperopt max evaluations (default 50)")
    parser.add_argument("--register", action="store_true",
                        help="Register the model in MLflow Model Registry "
                             "and promote to Staging if thresholds pass")
    args = parser.parse_args()
    sys.exit(main(args.max_trials, args.register))
