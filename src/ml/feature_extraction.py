#!/usr/bin/env python3
"""
feature_extraction — JSON payload → XGBoost-ready numeric matrix
================================================================
Shared by train_alert_ranker.py (Module 5 training) and batch_score.py
(Module 5 production scoring). Lives in src/ml/ so both jobs import it.

Why this is its own module: the schema of the feature JSON evolves over
time (new features added, old ones renamed). Centralizing the extraction
into one function means we only update one place when the schema changes,
and training + scoring stay aligned by definition.

PRD reference: §8 (ML model — 60 features in 6 groups).
"""

from __future__ import annotations

import json
from typing import Iterable

import numpy as np
import pandas as pd


# The 60 features in 6 groups (PRD §8). Order matters — XGBoost expects
# the same column order at training and scoring time, or the model will
# silently use wrong values.
FEATURE_NAMES: list[str] = [
    # -- Order-flow features (10) --
    "cancel_rate",
    "median_time_to_cancel_ms",
    "p95_time_to_cancel_ms",
    "pct_cancelled_under_50ms",
    "max_simultaneous_levels",
    "layered_stack_depth",
    "layered_stack_count",
    "order_to_trade_ratio_1m",
    "order_to_trade_ratio_5m",
    "order_to_trade_ratio_30m",
    # -- Cross-product features (8) --
    "cash_net_position",
    "futures_net_position",
    "options_net_delta_exposure",
    "cross_product_delta_imbalance",
    "directional_consistency_flag",
    "pre_close_concentration_pct",
    "morning_pump_ratio",
    "afternoon_dump_ratio",
    # -- Member context features (8) --
    "member_risk_score",
    "member_days_since_sebi_action",
    "member_historical_confirm_rate",
    "member_alert_volume_today",
    "trader_tenure_days",
    "is_tier1_market_maker",
    "is_prop_trader",
    "is_retail_broker",
    # -- Instrument context features (10) --
    "is_esm_flagged",
    "is_asm_flagged",
    "market_cap_large",
    "market_cap_mid",
    "market_cap_small",
    "market_cap_micro",
    "avg_daily_volume_log10",
    "days_to_nearest_expiry",
    "is_expiry_day",
    "circuit_band_pct",
    # -- Temporal context features (8) --
    "hour_of_session",
    "is_pre_market_close",
    "is_post_market_open",
    "is_lunch_window",
    "session_minute",
    "surrounding_news_density",
    "day_of_week",
    "is_friday_pre_expiry",
    # -- Rule context features (8) --
    "rule_R101_fired",
    "rule_R102_fired",
    "rule_R103_fired",
    "rule_R104_fired",
    "rule_R105_fired",
    "rule_historical_precision",
    "rule_firing_frequency_today",
    "severity_critical",
    # -- Notional + counts (8) --
    "notional_traded",
    "notional_traded_log10",
    "orders_placed",
    "orders_cancelled",
    "trades_executed",
    "buy_sell_ratio",
    "self_trade_count",
    "wash_trade_score",
]
N_FEATURES = len(FEATURE_NAMES)


def _safe_get(payload: dict, key: str, default: float = 0.0) -> float:
    """Pull a numeric value, defaulting if missing or non-numeric."""
    v = payload.get(key, default)
    if v is None:
        return default
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    try:
        f = float(v)
        return f if not np.isnan(f) else default
    except (TypeError, ValueError):
        return default


def _safe_log10(x: float) -> float:
    return float(np.log10(max(x, 1.0)))


def extract_features(row: pd.Series) -> np.ndarray:
    """Turn one alert_candidates row into a length-60 numeric vector.

    Works on a pandas Series-like object with at least these fields:
        features (str): JSON payload from JOB-08
        rule_id, severity, member_firm_id, instrument_code, fired_ts, etc.
    """
    payload = json.loads(row.get("features") or "{}")

    # Lookup rule firing flags
    rule_id = (row.get("rule_id") or "")
    severity = (row.get("severity") or "")

    # Hour of session: 09:15 IST = 0; 15:30 = 375 minutes later
    fired = pd.to_datetime(row.get("fired_ts"), utc=True, errors="coerce")
    if pd.isna(fired):
        hour = 0.0
        session_minute = 0.0
        dow = 0.0
    else:
        # Convert to IST naive for session math
        ist = fired.tz_convert("Asia/Kolkata") if fired.tzinfo else fired
        hour = float(ist.hour)
        session_minute = float(max(0, (ist.hour - 9) * 60 + ist.minute - 15))
        dow = float(ist.dayofweek)

    notional = _safe_get(payload, "notional_traded")

    return np.array([
        # Order-flow
        _safe_get(payload, "cancel_rate"),
        _safe_get(payload, "median_time_to_cancel_ms"),
        _safe_get(payload, "p95_time_to_cancel_ms"),
        _safe_get(payload, "pct_cancelled_under_50ms"),
        _safe_get(payload, "max_simultaneous_levels"),
        _safe_get(payload, "layered_stack_depth"),
        _safe_get(payload, "layered_stack_count"),
        _safe_get(payload, "order_to_trade_ratio_1m"),
        _safe_get(payload, "order_to_trade_ratio_5m"),
        _safe_get(payload, "order_to_trade_ratio_30m"),
        # Cross-product
        _safe_get(payload, "cash_net_position"),
        _safe_get(payload, "futures_net_position"),
        _safe_get(payload, "options_net_delta_exposure"),
        _safe_get(payload, "cross_product_delta_imbalance"),
        _safe_get(payload, "directional_consistency_flag"),
        _safe_get(payload, "pre_close_concentration_pct"),
        _safe_get(payload, "morning_pump_ratio"),
        _safe_get(payload, "afternoon_dump_ratio"),
        # Member context
        _safe_get(payload, "member_risk_score"),
        _safe_get(payload, "member_days_since_sebi_action", 999),
        _safe_get(payload, "member_historical_confirm_rate"),
        _safe_get(payload, "member_alert_volume_today"),
        _safe_get(payload, "trader_tenure_days"),
        1.0 if payload.get("member_firm_category") == "TIER1_MM" else 0.0,
        1.0 if payload.get("member_firm_category") == "PROP_TRADER" else 0.0,
        1.0 if payload.get("member_firm_category") == "RETAIL_BROKER" else 0.0,
        # Instrument context
        1.0 if payload.get("esm_flag") == "Y" else 0.0,
        1.0 if payload.get("asm_flag") == "Y" else 0.0,
        1.0 if payload.get("market_cap_bucket") == "LARGE" else 0.0,
        1.0 if payload.get("market_cap_bucket") == "MID" else 0.0,
        1.0 if payload.get("market_cap_bucket") == "SMALL" else 0.0,
        1.0 if payload.get("market_cap_bucket") == "MICRO" else 0.0,
        _safe_log10(_safe_get(payload, "avg_daily_volume", 1)),
        _safe_get(payload, "days_to_nearest_expiry", 30),
        _safe_get(payload, "is_expiry_day"),
        _safe_get(payload, "circuit_band_pct", 10.0),
        # Temporal context
        hour,
        1.0 if hour >= 15 else 0.0,           # is_pre_market_close
        1.0 if hour < 10 else 0.0,            # is_post_market_open
        1.0 if 12 <= hour < 13 else 0.0,      # is_lunch_window
        session_minute,
        _safe_get(payload, "surrounding_news_density"),
        dow,
        1.0 if dow == 4 else 0.0,             # Friday (post-Thursday-expiry next-day)
        # Rule context
        1.0 if rule_id == "R-101" else 0.0,
        1.0 if rule_id == "R-102" else 0.0,
        1.0 if rule_id == "R-103" else 0.0,
        1.0 if rule_id == "R-104" else 0.0,
        1.0 if rule_id == "R-105" else 0.0,
        _safe_get(payload, "rule_historical_precision", 0.08),
        _safe_get(payload, "rule_firing_frequency_today"),
        1.0 if severity == "CRITICAL" else 0.0,
        # Notional + counts
        notional,
        _safe_log10(notional),
        _safe_get(payload, "orders_placed"),
        _safe_get(payload, "orders_cancelled"),
        _safe_get(payload, "trades_executed"),
        _safe_get(payload, "buy_sell_ratio", 1.0),
        _safe_get(payload, "self_trade_count"),
        _safe_get(payload, "wash_trade_score"),
    ], dtype=np.float32)


def extract_feature_matrix(df: pd.DataFrame) -> np.ndarray:
    """Vectorize extract_features across a DataFrame; return shape (N, 60)."""
    return np.vstack([extract_features(row) for _, row in df.iterrows()])


def get_feature_names() -> list[str]:
    """Return the canonical feature-name list. Used by SHAP at scoring time
    so the explanations carry meaningful column labels."""
    return list(FEATURE_NAMES)
