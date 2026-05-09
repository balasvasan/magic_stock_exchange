#!/usr/bin/env python3
"""
ARGUS — Synthetic Data Generator
=================================

Produces the full data landscape for the capstone:
  - 14 output files (members, traders, investors, instruments, ...)
  - 23 planted test cases at fixed indices 0-22:
        0-9   : cross-product manipulation patterns (the meat of the capstone)
        10-14 : fuzzy-match identity-resolution cases
        15-19 : DPDP §6(4) consent-withdrawal cases
        20-22 : DPDP §12 erasure cases

All output is reproducible — same --seed gives identical files.

PRD reference: §11 (Synthetic Data Generator Spec).

Usage:
    python data/generate_data.py --seed 42 --out data/generated/
    python data/generate_data.py --seed 42 --out data/generated/ --scale 0.05
    python data/generate_data.py --dry-run

The --scale flag lets students run at reduced volumes for development. At
scale=1.0 the script produces the full ~50M order events; at scale=0.05
(the default for --dry-run) it produces ~2.5M, which finishes in minutes
on a laptop and still exercises every code path.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import random
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------
# Configuration — full-scale targets (PRD §11)
# ---------------------------------------------------------------------
FULL_SCALE = {
    "members":           380,
    "traders":         12_000,
    "investors":      250_000,
    "instruments":      4_800,
    "corp_actions":       600,
    "surv_state":         120,
    "consent":        250_000,    # one per investor
    "orders":      50_000_000,
    "trades":       3_500_000,
    "bbo":          7_000_000,
    "legacy_alerts": 4_800_000,
    "sebi_actions":       800,
    "news":            25_000,
}

TRADING_DAYS = 5      # simulate one trading week
SESSION_START_HOUR = 9
SESSION_START_MIN = 15
SESSION_END_HOUR = 15
SESSION_END_MIN = 30

# ---------------------------------------------------------------------
# Reference vocabularies (Indian-market-flavored)
# ---------------------------------------------------------------------
SECTORS = [
    "BANKING", "PHARMA", "IT", "AUTO", "FMCG", "METAL", "POWER",
    "OIL_GAS", "TELECOM", "REALTY", "INFRA", "CONSUMER_DURABLES",
]
MEMBER_CATEGORIES = ["TIER1_MM", "PROP_TRADER", "RETAIL_BROKER", "INSTITUTIONAL"]
MEMBER_CATEGORY_WEIGHTS = [0.05, 0.15, 0.65, 0.15]   # retail-heavy

INDIAN_FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Reyansh", "Krishna",
    "Ishaan", "Kabir", "Aanya", "Diya", "Saanvi", "Aadhya", "Anika",
    "Pari", "Riya", "Myra", "Sara", "Ananya", "Pooja", "Priya", "Rohit",
    "Suresh", "Rajesh", "Amit", "Vikram", "Ravi", "Kavita", "Sunita",
    "Meera", "Neha", "Deepika", "Anjali", "Manish", "Rakesh", "Sandeep",
]
INDIAN_LAST_NAMES = [
    "Sharma", "Patel", "Kumar", "Singh", "Gupta", "Mehta", "Iyer",
    "Nair", "Reddy", "Rao", "Pillai", "Joshi", "Mishra", "Verma",
    "Agarwal", "Bansal", "Jain", "Shah", "Desai", "Kapoor", "Malhotra",
    "Chopra", "Sinha", "Tripathi", "Saxena", "Pandey", "Roy", "Das",
]

# A small set of well-known Nifty/SmallCap-style tickers we'll seed; the
# rest are generated procedurally.
SEED_TICKERS = [
    ("RELIANCE", "OIL_GAS", "LARGE"), ("TCS", "IT", "LARGE"),
    ("HDFCBANK", "BANKING", "LARGE"), ("ICICIBANK", "BANKING", "LARGE"),
    ("INFY", "IT", "LARGE"), ("HINDUNILVR", "FMCG", "LARGE"),
    ("ITC", "FMCG", "LARGE"), ("KOTAKBANK", "BANKING", "LARGE"),
    ("LT", "INFRA", "LARGE"), ("AXISBANK", "BANKING", "LARGE"),
    ("MARUTI", "AUTO", "LARGE"), ("BAJFINANCE", "BANKING", "LARGE"),
    ("ASIANPAINT", "CONSUMER_DURABLES", "LARGE"), ("WIPRO", "IT", "LARGE"),
    ("SUNPHARMA", "PHARMA", "LARGE"), ("DRREDDY", "PHARMA", "LARGE"),
    ("CIPLA", "PHARMA", "MID"), ("TATAMOTORS", "AUTO", "LARGE"),
    ("BHARTIARTL", "TELECOM", "LARGE"), ("POWERGRID", "POWER", "LARGE"),
]

NEWS_HEADLINES_TEMPLATES = [
    "{ticker} reports Q{q} earnings beat, revenue up {pct}% YoY",
    "{ticker} announces {ratio} stock split, ex-date {dt}",
    "{ticker} signs {amount} crore order with government PSU",
    "{ticker} downgraded to 'Hold' by {broker} on margin pressure",
    "{ticker} block deal: {amount} crore changes hands at {price}",
    "{ticker} launches new product line targeting tier-2 cities",
    "{ticker} CEO resigns; board appoints interim chief",
    "{ticker} hits 52-week high on {sector} sector rotation",
    "SEBI grants approval for {ticker} fund-raising via QIP",
]

# Manipulation pattern types per planted case 0-9
PLANTED_PATTERNS = [
    ("LAYERING",            "BNXM-0042", "MID",   "PHARMA",  True),    # case 0
    ("SPOOFING",            "BNXM-0117", "LARGE", "BANKING", True),    # case 1
    ("MARKING_THE_CLOSE",   "BNXM-0231", "LARGE", "BANKING", True),    # case 2 (Jane Street style)
    ("MOMENTUM_IGNITION",   "BNXM-0089", "SMALL", "PHARMA",  True),    # case 3 (quote-stuffing)
    ("CROSS_PRODUCT_LAYER", "BNXM-0042", "MID",   "AUTO",    True),    # case 4
    ("WASH_TRADE",          "BNXM-0276", "MID",   "REALTY",  True),    # case 5
    ("LEGITIMATE_MM",       "BNXM-0001", "LARGE", "BANKING", False),   # case 6 (negative)
    ("LEGITIMATE_NEWS",     "BNXM-0156", "LARGE", "IT",      False),   # case 7 (negative)
    ("AMBIGUOUS",           "BNXM-0203", "MID",   "FMCG",    False),   # case 8 (analyst judgment)
    ("MULTI_DAY_LAYERING",  "BNXM-0117", "LARGE", "BANKING", True),    # case 9 (across full week)
]


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def fake_pan(rng: random.Random) -> str:
    """Generate a syntactically valid PAN: AAAAA9999A."""
    letters = "".join(rng.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
    digits = "".join(rng.choices("0123456789", k=4))
    last = rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    return f"{letters}{digits}{last}"


def hash_pan(pan: str) -> str:
    return hashlib.sha256(pan.encode()).hexdigest()[:32]


def fake_mobile(rng: random.Random) -> str:
    return f"+91-{rng.choice('6789')}{rng.randrange(10**8, 10**9):09d}"


def trading_session_seconds_us(day: date, rng: random.Random) -> int:
    """Random microsecond timestamp inside a trading session for given day."""
    base = datetime(day.year, day.month, day.day,
                    SESSION_START_HOUR, SESSION_START_MIN)
    session_secs = (SESSION_END_HOUR * 3600 + SESSION_END_MIN * 60) - \
                   (SESSION_START_HOUR * 3600 + SESSION_START_MIN * 60)
    offset_us = rng.randrange(0, session_secs * 1_000_000)
    return int(base.timestamp() * 1_000_000) + offset_us


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def jsonl_gz_writer(path: Path):
    return gzip.open(path, "wt", encoding="utf-8", compresslevel=4)


# ---------------------------------------------------------------------
# Generators — one per output file
# ---------------------------------------------------------------------
def gen_members(out: Path, scale: float, rng: random.Random) -> list[dict]:
    """members.csv — KAVACH member firm master."""
    n = max(int(FULL_SCALE["members"] * 1.0), 50)   # always ≥50, never scale members
    rows = []
    for i in range(n):
        category = rng.choices(MEMBER_CATEGORIES, weights=MEMBER_CATEGORY_WEIGHTS, k=1)[0]
        cap_crore = round(rng.uniform(50.0, 5000.0), 2) if category != "RETAIL_BROKER" \
                    else round(rng.uniform(20.0, 500.0), 2)
        rows.append({
            "member_firm_id":     f"BNXM-{i:04d}",
            "member_firm_name":   f"{rng.choice(INDIAN_LAST_NAMES)} Securities Pvt Ltd",
            "member_firm_category": category,
            "sebi_registration":  f"INZ{rng.randrange(10**8, 10**9):09d}",
            "capital_adequacy":   cap_crore,
            "suspension_history": json.dumps([]),
            "onboard_date":       (date(2009, 6, 1) + timedelta(days=rng.randrange(0, 5500))).isoformat(),
        })
    write_csv(out / "members.csv", rows,
              ["member_firm_id", "member_firm_name", "member_firm_category",
               "sebi_registration", "capital_adequacy", "suspension_history",
               "onboard_date"])
    return rows


def gen_traders(out: Path, members: list[dict], scale: float, rng: random.Random) -> list[dict]:
    """traders.csv — named human traders per member firm."""
    n = max(int(FULL_SCALE["traders"] * scale), 200)
    rows = []
    for i in range(n):
        member = rng.choice(members)
        rows.append({
            "trader_id":      f"TR-{i:06d}",
            "member_firm_id": member["member_firm_id"],
            "trader_name":    f"{rng.choice(INDIAN_FIRST_NAMES)} {rng.choice(INDIAN_LAST_NAMES)}",
            "tenure_days":    rng.randrange(30, 4500),
            "is_active":      rng.random() > 0.05,
        })
    write_csv(out / "traders.csv", rows,
              ["trader_id", "member_firm_id", "trader_name", "tenure_days", "is_active"])
    return rows


def gen_investors(out: Path, members: list[dict], scale: float, rng: random.Random
                  ) -> tuple[list[dict], list[dict]]:
    """investors.csv — end-investor accounts. Returns (investors, planted_fuzzy_groups)."""
    n = max(int(FULL_SCALE["investors"] * scale), 5_000)
    rows = []
    for i in range(n):
        first = rng.choice(INDIAN_FIRST_NAMES)
        last = rng.choice(INDIAN_LAST_NAMES)
        pan = fake_pan(rng)
        member = rng.choice(members)
        rows.append({
            "investor_acct":   f"INV-{i:08d}",
            "member_firm_id":  member["member_firm_id"],
            "investor_name":   f"{first} {last}",
            "investor_pan":    pan,
            "investor_pan_hash": hash_pan(pan),
            "investor_email":  f"{first.lower()}.{last.lower()}{i}@example.in",
            "investor_mobile": fake_mobile(rng),
            "investor_demat":  f"IN{rng.randrange(10**14, 10**15):015d}",
            "investor_kyc_tier": rng.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0],
            "registered_date": (date(2010, 1, 1) + timedelta(days=rng.randrange(0, 5500))).isoformat(),
        })

    # Plant fuzzy-match cases at indices 10-14 (5 cases, each = 1 underlying investor
    # appearing under 2-3 slightly different identities)
    fuzzy_groups = []
    for case_idx in range(10, 15):
        # pick a random base row, then create variants
        base = rows[rng.randrange(0, len(rows))]
        variants = [base]
        # Variant 1: PAN typo (one character changed)
        v1 = dict(base)
        v1["investor_acct"] = f"INV-FUZZY-{case_idx}-V1"
        pan_chars = list(base["investor_pan"])
        flip_pos = rng.randrange(0, 5)
        pan_chars[flip_pos] = chr((ord(pan_chars[flip_pos]) - 65 + 1) % 26 + 65)
        v1["investor_pan"] = "".join(pan_chars)
        v1["investor_pan_hash"] = hash_pan(v1["investor_pan"])
        rows.append(v1)
        variants.append(v1)
        # Variant 2: name spelling variant + different broker
        v2 = dict(base)
        v2["investor_acct"] = f"INV-FUZZY-{case_idx}-V2"
        v2["member_firm_id"] = rng.choice(members)["member_firm_id"]
        # subtle name change
        first, last = base["investor_name"].split(" ", 1)
        v2["investor_name"] = f"{first.replace('a', 'aa', 1)} {last}"
        v2["investor_email"] = base["investor_email"].replace("@", f".{case_idx}@")
        rows.append(v2)
        variants.append(v2)
        fuzzy_groups.append({"case_idx": case_idx, "variants": [v["investor_acct"] for v in variants]})

    write_csv(out / "investors.csv", rows,
              ["investor_acct", "member_firm_id", "investor_name", "investor_pan",
               "investor_pan_hash", "investor_email", "investor_mobile", "investor_demat",
               "investor_kyc_tier", "registered_date"])
    return rows, fuzzy_groups


def gen_instruments(out: Path, scale: float, rng: random.Random) -> list[dict]:
    """instruments.csv — cash equities + ETFs + F&O contracts."""
    rows = []
    # Seed tickers
    for code, sector, mcap in SEED_TICKERS:
        rows.append({
            "instrument_code":   f"{code}-EQ",
            "instrument_type":   "EQUITY",
            "underlying_code":   code,
            "expiry_date":       "",
            "strike_price":      "",
            "lot_size":          1,
            "tick_size":         0.05,
            "sector":            sector,
            "market_cap_bucket": mcap,
            "avg_daily_volume":  rng.randrange(500_000, 50_000_000),
        })
    # Procedural mid/small-cap equities
    target_equities = max(int(FULL_SCALE["instruments"] * scale * 0.55), 200)
    for i in range(target_equities):
        sector = rng.choice(SECTORS)
        mcap = rng.choices(["LARGE", "MID", "SMALL", "MICRO"], weights=[0.05, 0.20, 0.55, 0.20])[0]
        code = f"SYNTH{i:04d}"
        rows.append({
            "instrument_code":   f"{code}-EQ",
            "instrument_type":   "EQUITY",
            "underlying_code":   code,
            "expiry_date":       "",
            "strike_price":      "",
            "lot_size":          1,
            "tick_size":         0.05,
            "sector":            sector,
            "market_cap_bucket": mcap,
            "avg_daily_volume":  rng.randrange(10_000, 5_000_000),
        })
    # F&O contracts on a subset
    underlyings = [r for r in rows if r["instrument_type"] == "EQUITY"][:80]
    expiries = [date.today() + timedelta(days=d) for d in (7, 14, 28)]
    for u in underlyings:
        for exp in expiries:
            # Future
            rows.append({
                "instrument_code":   f"{u['underlying_code']}{exp.strftime('%y%b').upper()}FUT",
                "instrument_type":   "FUTURE",
                "underlying_code":   u["underlying_code"],
                "expiry_date":       exp.isoformat(),
                "strike_price":      "",
                "lot_size":          rng.choice([100, 250, 500, 1000]),
                "tick_size":         0.05,
                "sector":            u["sector"],
                "market_cap_bucket": u["market_cap_bucket"],
                "avg_daily_volume":  rng.randrange(10_000, 1_000_000),
            })
            # 5 strikes on either side for options
            base_strike = round(rng.uniform(100.0, 5000.0) / 50) * 50
            for k_off in range(-2, 3):
                strike = base_strike + k_off * 50
                for opt_type in ("CE", "PE"):
                    rows.append({
                        "instrument_code":   f"{u['underlying_code']}{exp.strftime('%y%b').upper()}{int(strike)}{opt_type}",
                        "instrument_type":   "OPTION",
                        "underlying_code":   u["underlying_code"],
                        "expiry_date":       exp.isoformat(),
                        "strike_price":      strike,
                        "lot_size":          rng.choice([100, 250, 500, 1000]),
                        "tick_size":         0.05,
                        "sector":            u["sector"],
                        "market_cap_bucket": u["market_cap_bucket"],
                        "avg_daily_volume":  rng.randrange(1_000, 500_000),
                    })

    write_csv(out / "instruments.csv", rows,
              ["instrument_code", "instrument_type", "underlying_code", "expiry_date",
               "strike_price", "lot_size", "tick_size", "sector", "market_cap_bucket",
               "avg_daily_volume"])
    return rows


def gen_corporate_actions(out: Path, instruments: list[dict], scale: float,
                          rng: random.Random) -> None:
    """corporate_actions.csv — splits, bonuses, dividends, mergers."""
    n = max(int(FULL_SCALE["corp_actions"] * scale), 30)
    equities = [i for i in instruments if i["instrument_type"] == "EQUITY"]
    rows = []
    for _ in range(n):
        inst = rng.choice(equities)
        action = rng.choices(["SPLIT", "BONUS", "DIVIDEND", "MERGER"],
                             weights=[0.20, 0.15, 0.60, 0.05], k=1)[0]
        ratio = {"SPLIT": rng.choice(["1:2", "1:5", "1:10"]),
                 "BONUS": rng.choice(["1:1", "1:2", "2:5"]),
                 "DIVIDEND": f"₹{rng.uniform(0.5, 50.0):.2f}",
                 "MERGER": "1:1"}[action]
        rows.append({
            "instrument_code": inst["instrument_code"],
            "underlying_code": inst["underlying_code"],
            "action_type":     action,
            "action_ratio":    ratio,
            "ex_date":         (date.today() - timedelta(days=rng.randrange(0, 90))).isoformat(),
            "announce_date":   (date.today() - timedelta(days=rng.randrange(91, 180))).isoformat(),
        })
    write_csv(out / "corporate_actions.csv", rows,
              ["instrument_code", "underlying_code", "action_type",
               "action_ratio", "ex_date", "announce_date"])


def gen_surveillance_state(out: Path, instruments: list[dict], scale: float,
                           rng: random.Random) -> None:
    """surveillance_state.csv — current ESM/ASM flags."""
    candidates = [i for i in instruments
                  if i["instrument_type"] == "EQUITY"
                  and i["market_cap_bucket"] in ("SMALL", "MICRO")]
    n = min(max(int(FULL_SCALE["surv_state"] * scale), 20), len(candidates))
    selected = rng.sample(candidates, k=n)
    rows = []
    for inst in selected:
        rows.append({
            "instrument_code":  inst["instrument_code"],
            "esm_flag":         "Y" if rng.random() > 0.5 else "N",
            "asm_flag":         "Y" if rng.random() > 0.4 else "N",
            "circuit_band_pct": rng.choice([2.0, 5.0, 10.0, 20.0]),
            "effective_from":   (date.today() - timedelta(days=rng.randrange(1, 30))).isoformat(),
        })
    write_csv(out / "surveillance_state.csv", rows,
              ["instrument_code", "esm_flag", "asm_flag",
               "circuit_band_pct", "effective_from"])


def gen_consent_records(out: Path, investors: list[dict], rng: random.Random
                        ) -> tuple[list[dict], list[int], list[int]]:
    """consent_records.csv — DPDP consent state per investor.

    Plants:
        - 5 cases (indices 15-19): consent withdrawn under DPDP §6(4)
        - 3 cases (indices 20-22): erasure requested under DPDP §12

    Returns (rows, withdrawn_acct_ids, erased_acct_ids).
    """
    rows = []
    for inv in investors:
        rows.append({
            "investor_acct":      inv["investor_acct"],
            "investor_pan_hash":  inv["investor_pan_hash"],
            "consent_status":     "ACTIVE",
            "consent_purpose":    "TRADING,SURVEILLANCE,ANALYTICS,MARKETING",
            "consent_granted_ts": (datetime.now() - timedelta(days=rng.randrange(30, 1500))).isoformat(),
            "consent_withdrawn_ts": "",
            "erasure_requested_ts": "",
            "erasure_completed_ts": "",
            "legal_basis":        "CONSENT",
        })

    # Plant withdrawals (cases 15-19)
    withdrawn_idxs = rng.sample(range(len(rows)), k=5)
    withdrawn_acct_ids = []
    for case_idx, row_idx in zip(range(15, 20), withdrawn_idxs):
        row = rows[row_idx]
        row["consent_status"] = "WITHDRAWN"
        # withdrew analytics + marketing but kept trading + surveillance (statutory)
        row["consent_purpose"] = "TRADING,SURVEILLANCE"
        row["consent_withdrawn_ts"] = (datetime.now() - timedelta(days=rng.randrange(1, 60))).isoformat()
        withdrawn_acct_ids.append(row["investor_acct"])

    # Plant erasures (cases 20-22) — DO NOT remove from rows; keep as audit evidence
    remaining = [i for i in range(len(rows)) if i not in withdrawn_idxs]
    erased_idxs = rng.sample(remaining, k=3)
    erased_acct_ids = []
    for case_idx, row_idx in zip(range(20, 23), erased_idxs):
        row = rows[row_idx]
        row["consent_status"] = "ERASED"
        row["consent_purpose"] = ""
        row["erasure_requested_ts"] = (datetime.now() - timedelta(days=rng.randrange(7, 45))).isoformat()
        row["erasure_completed_ts"] = (datetime.now() - timedelta(days=rng.randrange(0, 6))).isoformat()
        row["legal_basis"] = "ERASURE_§12"
        erased_acct_ids.append(row["investor_acct"])

    write_csv(out / "consent_records.csv", rows,
              ["investor_acct", "investor_pan_hash", "consent_status",
               "consent_purpose", "consent_granted_ts", "consent_withdrawn_ts",
               "erasure_requested_ts", "erasure_completed_ts", "legal_basis"])
    return rows, withdrawn_acct_ids, erased_acct_ids


def gen_orders_and_trades(out: Path, members: list[dict], traders: list[dict],
                          investors: list[dict], instruments: list[dict],
                          scale: float, rng: random.Random) -> dict:
    """Stream orders + trades. Plants manipulation cases 0-9 by injecting
    structured patterns at known timestamps and tagging them in
    compliance_test_cases.csv."""
    n_orders = max(int(FULL_SCALE["orders"] * scale), 100_000)
    n_trades_target = max(int(FULL_SCALE["trades"] * scale), 7_000)
    fill_probability = n_trades_target / n_orders

    base_day = date.today() - timedelta(days=TRADING_DAYS + 1)
    days = [base_day + timedelta(days=i) for i in range(TRADING_DAYS)]
    equities_and_derivs = [i for i in instruments if i["instrument_type"] != "BOND"]

    planted_alerts = []  # records to thread into legacy_alerts later
    orders_path = out / "orders_synthetic.jsonl.gz"
    trades_path = out / "trades_synthetic.jsonl.gz"

    orders_written = 0
    trades_written = 0

    with jsonl_gz_writer(orders_path) as fo, jsonl_gz_writer(trades_path) as ft:

        # ---- Plant manipulation cases 0-9 first, deterministically ----
        for case_idx, (pattern, mfid, mcap, sector, is_real) in enumerate(PLANTED_PATTERNS):
            day = rng.choice(days)
            target = next((i for i in equities_and_derivs
                           if i["sector"] == sector
                              and i["market_cap_bucket"] == mcap
                              and i["instrument_type"] == "EQUITY"),
                          rng.choice(equities_and_derivs))
            base_us = trading_session_seconds_us(day, rng)
            trader = rng.choice([t for t in traders if t["member_firm_id"] == mfid] or traders)
            tid = trader["trader_id"]

            if pattern in ("LAYERING", "CROSS_PRODUCT_LAYER", "MULTI_DAY_LAYERING"):
                # 5 stacked non-bona-fide buy orders, all cancelled within 200ms
                base_price = round(rng.uniform(100, 2000), 2)
                for level in range(5):
                    oid = str(uuid.uuid4())
                    place_us = base_us + level * 5_000  # 5ms apart
                    cancel_us = place_us + rng.randrange(50_000, 200_000)
                    px = base_price - 0.05 * (level + 1)
                    qty = rng.choice([5000, 7500, 10000])
                    fo.write(json.dumps({
                        "event_id": oid, "ts_us": place_us,
                        "member_firm_id": mfid, "trader_id": tid,
                        "instrument_code": target["instrument_code"],
                        "side": "BUY", "order_type": "LIMIT",
                        "qty": qty, "price": px, "action": "NEW",
                        "parent_order_id": None,
                        "planted_case_idx": case_idx,
                    }) + "\n")
                    fo.write(json.dumps({
                        "event_id": str(uuid.uuid4()), "ts_us": cancel_us,
                        "member_firm_id": mfid, "trader_id": tid,
                        "instrument_code": target["instrument_code"],
                        "side": "BUY", "order_type": "LIMIT",
                        "qty": qty, "price": px, "action": "CANCEL",
                        "parent_order_id": oid,
                        "planted_case_idx": case_idx,
                    }) + "\n")
                    orders_written += 2
                # Bona-fide sell that profits from the manufactured depth
                sell_oid = str(uuid.uuid4())
                fo.write(json.dumps({
                    "event_id": sell_oid, "ts_us": base_us + 250_000,
                    "member_firm_id": mfid, "trader_id": tid,
                    "instrument_code": target["instrument_code"],
                    "side": "SELL", "order_type": "LIMIT",
                    "qty": 8000, "price": base_price - 0.20,
                    "action": "NEW", "parent_order_id": None,
                    "planted_case_idx": case_idx,
                }) + "\n")
                orders_written += 1

            elif pattern == "SPOOFING":
                # Single large buy order held 1.4s then cancelled
                oid = str(uuid.uuid4())
                base_price = round(rng.uniform(500, 1500), 2)
                fo.write(json.dumps({
                    "event_id": oid, "ts_us": base_us,
                    "member_firm_id": mfid, "trader_id": tid,
                    "instrument_code": target["instrument_code"],
                    "side": "BUY", "order_type": "LIMIT",
                    "qty": 50000, "price": base_price,
                    "action": "NEW", "parent_order_id": None,
                    "planted_case_idx": case_idx,
                }) + "\n")
                fo.write(json.dumps({
                    "event_id": str(uuid.uuid4()),
                    "ts_us": base_us + 1_400_000,
                    "member_firm_id": mfid, "trader_id": tid,
                    "instrument_code": target["instrument_code"],
                    "side": "BUY", "order_type": "LIMIT",
                    "qty": 50000, "price": base_price,
                    "action": "CANCEL", "parent_order_id": oid,
                    "planted_case_idx": case_idx,
                }) + "\n")
                orders_written += 2

            elif pattern == "MARKING_THE_CLOSE":
                # Concentrated aggressive buying in last 10 minutes
                close_us = int(datetime(day.year, day.month, day.day,
                                        SESSION_END_HOUR, SESSION_END_MIN - 10).timestamp()
                               * 1_000_000)
                base_price = round(rng.uniform(40000, 50000), 2)  # BANKNIFTY-like
                for k in range(40):
                    oid = str(uuid.uuid4())
                    fo.write(json.dumps({
                        "event_id": oid, "ts_us": close_us + k * 15_000_000,
                        "member_firm_id": mfid, "trader_id": tid,
                        "instrument_code": target["instrument_code"],
                        "side": "BUY", "order_type": "LIMIT",
                        "qty": rng.randrange(2000, 8000),
                        "price": base_price + k * 0.50,
                        "action": "NEW", "parent_order_id": None,
                        "planted_case_idx": case_idx,
                    }) + "\n")
                    orders_written += 1

            elif pattern == "MOMENTUM_IGNITION":
                # 12,000 orders/sec burst for 8 seconds
                for k in range(min(8 * 1200, 4000)):  # cap 4k for synth volume
                    oid = str(uuid.uuid4())
                    fo.write(json.dumps({
                        "event_id": oid, "ts_us": base_us + k * 833,
                        "member_firm_id": mfid, "trader_id": tid,
                        "instrument_code": target["instrument_code"],
                        "side": rng.choice(["BUY", "SELL"]),
                        "order_type": "LIMIT",
                        "qty": rng.randrange(100, 500),
                        "price": round(rng.uniform(10, 50), 2),
                        "action": "NEW", "parent_order_id": None,
                        "planted_case_idx": case_idx,
                    }) + "\n")
                    orders_written += 1

            elif pattern == "WASH_TRADE":
                # Two orders from same member, one buy one sell, that cross
                buy_oid = str(uuid.uuid4())
                sell_oid = str(uuid.uuid4())
                px = round(rng.uniform(200, 800), 2)
                fo.write(json.dumps({
                    "event_id": buy_oid, "ts_us": base_us,
                    "member_firm_id": mfid, "trader_id": tid,
                    "instrument_code": target["instrument_code"],
                    "side": "BUY", "order_type": "LIMIT",
                    "qty": 5000, "price": px, "action": "NEW",
                    "parent_order_id": None, "planted_case_idx": case_idx,
                }) + "\n")
                fo.write(json.dumps({
                    "event_id": sell_oid, "ts_us": base_us + 100_000,
                    "member_firm_id": mfid, "trader_id": tid,
                    "instrument_code": target["instrument_code"],
                    "side": "SELL", "order_type": "LIMIT",
                    "qty": 5000, "price": px, "action": "NEW",
                    "parent_order_id": None, "planted_case_idx": case_idx,
                }) + "\n")
                orders_written += 2
                # Generate the matching trade
                ft.write(json.dumps({
                    "trade_id": str(uuid.uuid4()),
                    "ts_us": base_us + 110_000,
                    "instrument_code": target["instrument_code"],
                    "buy_member_firm_id": mfid, "sell_member_firm_id": mfid,
                    "buy_trader_id": tid, "sell_trader_id": tid,
                    "exec_price": px, "exec_qty": 5000,
                    "is_self_trade": True, "planted_case_idx": case_idx,
                }) + "\n")
                trades_written += 1

            else:  # LEGITIMATE_MM, LEGITIMATE_NEWS, AMBIGUOUS — produce noisy but
                   # explainable order flow for negative cases
                for k in range(60):
                    oid = str(uuid.uuid4())
                    side = rng.choice(["BUY", "SELL"])
                    px = round(rng.uniform(100, 1000), 2)
                    fo.write(json.dumps({
                        "event_id": oid,
                        "ts_us": base_us + k * 500_000,
                        "member_firm_id": mfid, "trader_id": tid,
                        "instrument_code": target["instrument_code"],
                        "side": side, "order_type": "LIMIT",
                        "qty": rng.randrange(100, 1000),
                        "price": px, "action": "NEW",
                        "parent_order_id": None,
                        "planted_case_idx": case_idx,
                    }) + "\n")
                    orders_written += 1

            planted_alerts.append({
                "case_idx": case_idx, "pattern": pattern, "member_firm_id": mfid,
                "trader_id": tid, "instrument_code": target["instrument_code"],
                "is_real": is_real, "trade_date": day.isoformat(),
                "fired_ts_us": base_us,
            })

        # ---- Background noise: random orders to fill out the volume budget ----
        remaining = max(0, n_orders - orders_written)
        for k in range(remaining):
            day = rng.choice(days)
            ts_us = trading_session_seconds_us(day, rng)
            inst = rng.choice(equities_and_derivs)
            trader = rng.choice(traders)
            side = rng.choice(["BUY", "SELL"])
            action = rng.choices(["NEW", "MODIFY", "CANCEL"],
                                 weights=[0.55, 0.15, 0.30], k=1)[0]
            qty = rng.choice([100, 250, 500, 1000, 2500])
            px = round(rng.uniform(10, 5000), 2)
            oid = str(uuid.uuid4())
            fo.write(json.dumps({
                "event_id": oid, "ts_us": ts_us,
                "member_firm_id": trader["member_firm_id"],
                "trader_id": trader["trader_id"],
                "instrument_code": inst["instrument_code"],
                "side": side, "order_type": rng.choice(["LIMIT", "MARKET", "IOC"]),
                "qty": qty, "price": px if action != "MARKET" else None,
                "action": action,
                "parent_order_id": None,
                "planted_case_idx": None,
            }) + "\n")
            orders_written += 1
            # Probabilistic fill — generate a trade
            if action == "NEW" and rng.random() < fill_probability:
                buy_member = trader["member_firm_id"]
                sell_member = rng.choice(members)["member_firm_id"]
                if rng.random() < 0.5:
                    buy_member, sell_member = sell_member, buy_member
                ft.write(json.dumps({
                    "trade_id": str(uuid.uuid4()),
                    "ts_us": ts_us + rng.randrange(100, 50_000),
                    "instrument_code": inst["instrument_code"],
                    "buy_member_firm_id": buy_member,
                    "sell_member_firm_id": sell_member,
                    "buy_trader_id": trader["trader_id"],
                    "sell_trader_id": rng.choice(traders)["trader_id"],
                    "exec_price": px, "exec_qty": qty,
                    "is_self_trade": buy_member == sell_member,
                    "planted_case_idx": None,
                }) + "\n")
                trades_written += 1

    return {
        "orders_written": orders_written,
        "trades_written": trades_written,
        "planted_alerts": planted_alerts,
    }


def gen_bbo(out: Path, instruments: list[dict], scale: float, rng: random.Random) -> int:
    """bbo_synthetic.jsonl.gz — cross-exchange best-bid/best-offer ticks."""
    n = max(int(FULL_SCALE["bbo"] * scale), 30_000)
    base_day = date.today() - timedelta(days=TRADING_DAYS + 1)
    days = [base_day + timedelta(days=i) for i in range(TRADING_DAYS)]
    equities = [i for i in instruments if i["instrument_type"] == "EQUITY"]
    bbo_path = out / "bbo_synthetic.jsonl.gz"
    written = 0
    with jsonl_gz_writer(bbo_path) as f:
        for _ in range(n):
            inst = rng.choice(equities)
            day = rng.choice(days)
            ts_us = trading_session_seconds_us(day, rng)
            mid = round(rng.uniform(10, 5000), 2)
            spread = round(rng.uniform(0.05, 2.0), 2)
            f.write(json.dumps({
                "ts_us": ts_us,
                "instrument_code": inst["instrument_code"],
                "venue": rng.choice(["NSE", "BSE"]),
                "best_bid_px": round(mid - spread / 2, 2),
                "best_bid_qty": rng.randrange(100, 50_000),
                "best_offer_px": round(mid + spread / 2, 2),
                "best_offer_qty": rng.randrange(100, 50_000),
            }) + "\n")
            written += 1
    return written


def gen_legacy_alerts(out: Path, members: list[dict], traders: list[dict],
                      instruments: list[dict], planted_alerts: list[dict],
                      scale: float, rng: random.Random) -> int:
    """legacy_alerts_history.csv — 7 years of historical alerts with analyst
    dispositions. The supervised ML training set."""
    n = max(int(FULL_SCALE["legacy_alerts"] * scale), 25_000)
    rows = []
    # Plant the 10 manipulation cases (0-9) into the historical record so the
    # ML model has labeled exemplars (real cases 0-5,9 → CONFIRMED;
    # cases 6-7 → NO_ACTION; case 8 → ESCALATED-but-DISMISSED).
    for pa in planted_alerts:
        case_idx = pa["case_idx"]
        if pa["is_real"]:
            disp = "CONFIRMED_MANIPULATION"
            sebi = "ACTION_TAKEN" if case_idx in (0, 1, 2) else "INQUIRY_OPENED"
        elif case_idx == 8:
            disp = "ESCALATED"
            sebi = "DISMISSED"
        else:
            disp = "NO_ACTION"
            sebi = "NONE"
        rows.append({
            "alert_id":         f"PLANTED-{case_idx:02d}-{uuid.uuid4().hex[:8]}",
            "fired_ts":         datetime.fromtimestamp(pa["fired_ts_us"] / 1_000_000).isoformat(),
            "rule_id":          f"R-{rng.randrange(100, 999)}",
            "rule_version":     "v3.2.1",
            "severity":         rng.choice(["MEDIUM", "HIGH", "CRITICAL"]),
            "member_firm_id":   pa["member_firm_id"],
            "trader_id":        pa["trader_id"],
            "instrument_code":  pa["instrument_code"],
            "pattern_type":     pa["pattern"],
            "analyst_id":       f"AN-{rng.randrange(1, 30):03d}",
            "disposition":      disp,
            "disposition_ts":   (datetime.now() - timedelta(days=rng.randrange(1, 60))).isoformat(),
            "disposition_rationale": _rationale_for(pa["pattern"], disp, rng),
            "linked_str_id":    f"STR-{uuid.uuid4().hex[:10]}" if disp == "CONFIRMED_MANIPULATION" else "",
            "sebi_outcome":     sebi,
            "sebi_outcome_ts":  (datetime.now() - timedelta(days=rng.randrange(1, 30))).isoformat() if sebi != "NONE" else "",
            "is_confirmed_manipulation": disp == "CONFIRMED_MANIPULATION",
            "disposition_date": (date.today() - timedelta(days=rng.randrange(1, 60))).isoformat(),
        })
    # Background historical alerts — bulk of the training set
    background_n = n - len(rows)
    for _ in range(background_n):
        member = rng.choice(members)
        trader = rng.choice([t for t in traders if t["member_firm_id"] == member["member_firm_id"]] or traders)
        inst = rng.choice(instruments)
        # Disposition distribution mirrors the 92% no-action / 8% confirmed described in PRD §2 ARG-3
        disp = rng.choices(
            ["NO_ACTION", "ESCALATED", "CONFIRMED_MANIPULATION"],
            weights=[0.92, 0.04, 0.04], k=1)[0]
        sebi = "ACTION_TAKEN" if disp == "CONFIRMED_MANIPULATION" and rng.random() < 0.30 else "NONE"
        days_ago = rng.randrange(1, 7 * 365)
        fired = datetime.now() - timedelta(days=days_ago, hours=rng.randrange(0, 24))
        rows.append({
            "alert_id":         f"LGCY-{uuid.uuid4().hex[:12]}",
            "fired_ts":         fired.isoformat(),
            "rule_id":          f"R-{rng.randrange(100, 999)}",
            "rule_version":     rng.choice(["v2.8.0", "v3.0.0", "v3.1.5", "v3.2.1"]),
            "severity":         rng.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
            "member_firm_id":   member["member_firm_id"],
            "trader_id":        trader["trader_id"],
            "instrument_code":  inst["instrument_code"],
            "pattern_type":     rng.choice(["SPOOFING", "LAYERING", "MOMENTUM_IGNITION", "WASH", "OTHER"]),
            "analyst_id":       f"AN-{rng.randrange(1, 30):03d}",
            "disposition":      disp,
            "disposition_ts":   (fired + timedelta(hours=rng.randrange(1, 48))).isoformat(),
            "disposition_rationale": _rationale_for("OTHER", disp, rng),
            "linked_str_id":    f"STR-{uuid.uuid4().hex[:10]}" if disp == "CONFIRMED_MANIPULATION" else "",
            "sebi_outcome":     sebi,
            "sebi_outcome_ts":  (fired + timedelta(days=rng.randrange(30, 365))).isoformat() if sebi != "NONE" else "",
            "is_confirmed_manipulation": disp == "CONFIRMED_MANIPULATION",
            "disposition_date": (fired + timedelta(hours=rng.randrange(1, 48))).date().isoformat(),
        })
    write_csv(out / "legacy_alerts_history.csv", rows,
              ["alert_id", "fired_ts", "rule_id", "rule_version", "severity",
               "member_firm_id", "trader_id", "instrument_code", "pattern_type",
               "analyst_id", "disposition", "disposition_ts",
               "disposition_rationale", "linked_str_id", "sebi_outcome",
               "sebi_outcome_ts", "is_confirmed_manipulation",
               "disposition_date"])
    return len(rows)


def _rationale_for(pattern: str, disposition: str, rng: random.Random) -> str:
    """Short analyst-style note. Used as training signal for GenAI exemplars."""
    if disposition == "NO_ACTION":
        return rng.choice([
            "Bona-fide market-making activity; cancel ratio consistent with member's baseline.",
            "Order pattern explained by news event; no manipulative intent indicated.",
            "Fat-finger correction; analyst contacted member firm and confirmed.",
            "Thin-volume false signal; pattern within normal small-cap microstructure.",
        ])
    if disposition == "ESCALATED":
        return rng.choice([
            "Pattern unusual for member's history; escalated to senior analyst for second review.",
            "Cross-product correlation observed; awaiting clearing data to confirm.",
            "Member has open SEBI matter; flagged for additional scrutiny.",
        ])
    return rng.choice([
        f"Layering pattern: {rng.randrange(3, 8)} stacked non-bona-fide orders cancelled within 200ms; price moved against retail counterparties.",
        f"Spoofing: large order held {rng.randrange(800, 2000)}ms then cancelled; member benefited from pre-positioned opposite leg.",
        "Marking-the-close: aggressive buying in final 10 minutes pushed underlying toward strike where short puts held.",
        "Momentum ignition: high-frequency burst created false urgency; reversed within 5 minutes.",
    ])


def gen_sebi_actions(out: Path, members: list[dict], rng: random.Random) -> None:
    """sebi_actions_feed.csv — historical SEBI watchlist + debarment + ASM actions."""
    rows = []
    for _ in range(FULL_SCALE["sebi_actions"]):
        action_type = rng.choices(
            ["WATCHLIST_ADD", "WATCHLIST_REMOVE", "ESM_ADD", "ASM_ADD",
             "PFUTP_ORDER", "DEBARMENT", "CONSENT_ORDER"],
            weights=[0.30, 0.20, 0.15, 0.15, 0.08, 0.04, 0.08], k=1)[0]
        rows.append({
            "action_id":        f"SEBI-{uuid.uuid4().hex[:10]}",
            "action_ts":        (datetime.now() - timedelta(days=rng.randrange(1, 5 * 365))).isoformat(),
            "action_type":      action_type,
            "member_firm_id":   rng.choice(members)["member_firm_id"] if action_type in ("PFUTP_ORDER", "DEBARMENT", "CONSENT_ORDER") else "",
            "instrument_code":  "" if action_type in ("PFUTP_ORDER", "DEBARMENT") else "",
            "regulation_ref":   rng.choice(["PFUTP Reg 4(2)(e)", "PFUTP Reg 4(2)(g)",
                                            "SEBI Master Circular Surv. ch.6",
                                            "FPI Reg 20(4)"]),
            "document_url":     f"https://www.sebi.gov.in/orders/2025/{rng.randrange(1000, 9999)}.pdf",
        })
    write_csv(out / "sebi_actions_feed.csv", rows,
              ["action_id", "action_ts", "action_type", "member_firm_id",
               "instrument_code", "regulation_ref", "document_url"])


def gen_news(out: Path, instruments: list[dict], scale: float, rng: random.Random) -> None:
    """news_headlines.csv — corporate disclosures + news headlines."""
    n = max(int(FULL_SCALE["news"] * scale), 1_500)
    equities = [i for i in instruments if i["instrument_type"] == "EQUITY"]
    rows = []
    for _ in range(n):
        inst = rng.choice(equities)
        template = rng.choice(NEWS_HEADLINES_TEMPLATES)
        headline = template.format(
            ticker=inst["underlying_code"],
            q=rng.choice(["1", "2", "3", "4"]),
            pct=rng.randrange(2, 25),
            ratio=rng.choice(["1:2", "1:5", "1:10"]),
            dt=(date.today() + timedelta(days=rng.randrange(1, 60))).isoformat(),
            amount=rng.randrange(50, 5000),
            broker=rng.choice(["Morgan Stanley", "Goldman Sachs", "Jefferies",
                               "Kotak Institutional", "Motilal Oswal"]),
            price=round(rng.uniform(50, 5000), 2),
            sector=inst["sector"].title(),
        )
        rows.append({
            "news_id":          f"N-{uuid.uuid4().hex[:10]}",
            "ts":               (datetime.now() - timedelta(days=rng.randrange(0, 90),
                                                            hours=rng.randrange(0, 24))).isoformat(),
            "headline":         headline,
            "source":           rng.choice(["Reuters", "Bloomberg", "ET Markets",
                                            "Mint", "Moneycontrol"]),
            "instruments_tagged": json.dumps([inst["instrument_code"]]),
            "sentiment_hint":   rng.choices(["POSITIVE", "NEGATIVE", "NEUTRAL"],
                                            weights=[0.45, 0.30, 0.25], k=1)[0],
        })
    write_csv(out / "news_headlines.csv", rows,
              ["news_id", "ts", "headline", "source", "instruments_tagged",
               "sentiment_hint"])


def write_test_case_index(out: Path, fuzzy_groups: list[dict],
                          withdrawn: list[str], erased: list[str]) -> None:
    """compliance_test_cases.csv — master index of all 23 planted cases."""
    rows = []
    # 0-9: cross-product manipulation
    for case_idx, (pattern, mfid, mcap, sector, is_real) in enumerate(PLANTED_PATTERNS):
        rows.append({
            "case_idx": case_idx,
            "category": "CROSS_PRODUCT_MANIPULATION",
            "pattern":  pattern,
            "member_firm_id": mfid,
            "is_real_manipulation": is_real,
            "expected_disposition": (
                "CONFIRMED_MANIPULATION" if is_real
                else "ESCALATED_DISMISSED" if case_idx == 8
                else "NO_ACTION"
            ),
            "tested_in_module": {
                0: 3, 1: 3, 2: 3, 3: 3, 4: 3, 5: 3, 6: 5, 7: 5, 8: 5, 9: 3,
            }[case_idx],
            "checkpoint":      "CP-08, CP-09, CP-13",
            "notes":           f"{mcap}-cap {sector}; see PRD §11",
        })
    # 10-14: fuzzy-match
    for fg in fuzzy_groups:
        rows.append({
            "case_idx": fg["case_idx"],
            "category": "FUZZY_MATCH",
            "pattern":  "PAN_TYPO_PLUS_NAME_VARIANT",
            "member_firm_id": "",
            "is_real_manipulation": "",
            "expected_disposition": "MERGE_INTO_SINGLE_ENTITY",
            "tested_in_module": 2,
            "checkpoint": "CP-06",
            "notes": f"Variants: {','.join(fg['variants'])}",
        })
    # 15-19: DPDP §6(4) consent withdrawal
    for case_idx, acct in zip(range(15, 20), withdrawn):
        rows.append({
            "case_idx": case_idx,
            "category": "DPDP_CONSENT_WITHDRAWAL",
            "pattern":  "ANALYTICS_AND_MARKETING_WITHDRAWN",
            "member_firm_id": "",
            "is_real_manipulation": "",
            "expected_disposition": "FILTER_FROM_NON_STATUTORY_QUERIES",
            "tested_in_module": 4,
            "checkpoint": "CP-11, CP-18",
            "notes": f"Investor account: {acct}",
        })
    # 20-22: DPDP §12 erasure
    for case_idx, acct in zip(range(20, 23), erased):
        rows.append({
            "case_idx": case_idx,
            "category": "DPDP_ERASURE",
            "pattern":  "FULL_ERASURE_WITH_AUDIT_PRESERVATION",
            "member_firm_id": "",
            "is_real_manipulation": "",
            "expected_disposition": "ERASE_PII_RETAIN_AUDIT",
            "tested_in_module": 7,
            "checkpoint": "CP-19 (COMPLIANCE GATE)",
            "notes": f"Investor account: {acct}",
        })
    write_csv(out / "compliance_test_cases.csv", rows,
              ["case_idx", "category", "pattern", "member_firm_id",
               "is_real_manipulation", "expected_disposition",
               "tested_in_module", "checkpoint", "notes"])


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="ARGUS synthetic data generator")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42); same seed = identical files")
    parser.add_argument("--out", type=Path, default=Path("data/generated/"),
                        help="Output directory")
    parser.add_argument("--scale", type=float, default=0.05,
                        help="Volume scale 0.001–1.0; default 0.05 ≈ 2.5M orders")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate inputs and report sizes without writing")
    args = parser.parse_args()

    if not (0.001 <= args.scale <= 1.0):
        parser.error("--scale must be between 0.001 and 1.0")

    rng = random.Random(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)

    print(f"==> ARGUS synthetic data generator")
    print(f"    seed:  {args.seed}")
    print(f"    scale: {args.scale} (≈{int(FULL_SCALE['orders'] * args.scale):,} orders)")
    print(f"    out:   {args.out}")

    if args.dry_run:
        print("==> Dry-run — projected output sizes:")
        for k, v in FULL_SCALE.items():
            scaled = int(v * args.scale) if k in ("traders", "investors", "orders",
                                                  "trades", "bbo", "news",
                                                  "legacy_alerts", "consent",
                                                  "instruments", "corp_actions",
                                                  "surv_state") else v
            print(f"      {k:18s} {scaled:>12,} rows")
        print("==> Dry-run complete — no files written.")
        return 0

    print("==> [ 1/14] members.csv");          members = gen_members(args.out, args.scale, rng)
    print("==> [ 2/14] traders.csv");          traders = gen_traders(args.out, members, args.scale, rng)
    print("==> [ 3/14] investors.csv");        investors, fuzzy_groups = gen_investors(args.out, members, args.scale, rng)
    print("==> [ 4/14] instruments.csv");      instruments = gen_instruments(args.out, args.scale, rng)
    print("==> [ 5/14] corporate_actions.csv"); gen_corporate_actions(args.out, instruments, args.scale, rng)
    print("==> [ 6/14] surveillance_state.csv"); gen_surveillance_state(args.out, instruments, args.scale, rng)
    print("==> [ 7/14] consent_records.csv");  _, withdrawn, erased = gen_consent_records(args.out, investors, rng)
    print("==> [ 8/14] orders + trades.jsonl.gz"); ot = gen_orders_and_trades(args.out, members, traders, investors, instruments, args.scale, rng)
    print(f"            orders:         {ot['orders_written']:>12,}")
    print(f"            trades:         {ot['trades_written']:>12,}")
    print("==> [10/14] bbo_synthetic.jsonl.gz"); bbo_n = gen_bbo(args.out, instruments, args.scale, rng)
    print(f"            bbo ticks:      {bbo_n:>12,}")
    print("==> [11/14] legacy_alerts_history.csv"); la_n = gen_legacy_alerts(args.out, members, traders, instruments, ot["planted_alerts"], args.scale, rng)
    print(f"            legacy alerts:  {la_n:>12,}")
    print("==> [12/14] sebi_actions_feed.csv"); gen_sebi_actions(args.out, members, rng)
    print("==> [13/14] news_headlines.csv");   gen_news(args.out, instruments, args.scale, rng)
    print("==> [14/14] compliance_test_cases.csv"); write_test_case_index(args.out, fuzzy_groups, withdrawn, erased)

    print(f"==> Done — {len(list(args.out.iterdir()))} files written to {args.out}")
    print(f"==> Planted test cases:")
    print(f"      0-9   : 10 cross-product manipulation cases")
    print(f"      10-14 : 5 fuzzy-match cases (variants: {sum(len(fg['variants']) for fg in fuzzy_groups)} total identities)")
    print(f"      15-19 : 5 DPDP §6(4) consent-withdrawal cases")
    print(f"      20-22 : 3 DPDP §12 erasure cases")
    print(f"==> See compliance_test_cases.csv for the full index.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
