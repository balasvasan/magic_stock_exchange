#!/usr/bin/env python3
"""
ccpa_optout_enforcement — DPDP §6(4) consent withdrawal enforcement
====================================================================
NOTE on filename: framework convention is ccpa_optout_enforcement.py.
The actual regulation implemented is India's DPDP Act 2023 §6(4)
(consent withdrawal). DPDP §6(4) and CCPA §1798.120 are functionally
analogous — both are opt-outs from non-statutory data processing.

PRD reference: §10. Closes the consent enforcement piece of ARG-5.
This is the implementation behind CP-18.

Workflow per PRD §10:
    1. Investor withdraws consent for specific processing purposes
       (commonly: ANALYTICS, MARKETING) via Consent Manager or DPO.
       Statutory purposes (TRADING, SURVEILLANCE) cannot be withdrawn —
       those are required for SEBI compliance and covered by DPDP §7.
    2. Withdrawal recorded in gold.consent_audit (event_type=
       CONSENT_WITHDRAWN), capturing pre/post Iceberg snapshots
    3. silver.member_master.consent_status / consent_purpose
       updated for the affected investor
    4. The Ranger row-level filter dpdp_consent_filter (Module 4)
       picks up the new consent_status at next query time — no
       application change needed
    5. Statutory tables tagged SEBI_AUDIT_TRAIL keep showing the
       investor under DPDP §7 legitimate-use exception

Resource names (schemas, app name) resolved from src.common.naming
using ${STUDENT_ID}.

Usage:
    python src/governance/ccpa_optout_enforcement.py \\
        --investor-acct INV-12345678 \\
        --withdraw-purposes ANALYTICS,MARKETING \\
        --request-id REQ-20260315-007

Run by an authorized DPO user (Ranger role: compliance_dpo).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.naming import fqtn, schema as schema_for, cde_job

LOG = logging.getLogger("argus.governance.consent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# DPDP §7 legitimate-use purposes — CANNOT be withdrawn
STATUTORY_PURPOSES = {"TRADING", "SURVEILLANCE"}
# Optional purposes — CAN be withdrawn under DPDP §6(4)
OPTIONAL_PURPOSES = {"ANALYTICS", "MARKETING", "RESEARCH"}


def current_snapshot_id(spark, layer: str, table: str) -> str | None:
    physical = fqtn(layer, table)
    df = spark.sql(
        f"SELECT snapshot_id FROM {physical}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1")
    rows = df.collect()
    return str(rows[0]["snapshot_id"]) if rows else None


def fetch_current_consent(spark, investor_acct: str) -> dict | None:
    df = spark.sql(f"""
        SELECT investor_acct, investor_pan_hash, consent_status, consent_purpose
        FROM {fqtn("silver", "member_master")}
        WHERE investor_acct = '{investor_acct}' AND is_current
        LIMIT 1
    """).toPandas()
    return df.iloc[0].to_dict() if len(df) else None


def normalize_purposes(raw: str) -> list[str]:
    return [p.strip().upper() for p in raw.split(",") if p.strip()]


def compute_remaining_purposes(current: list[str], to_withdraw: list[str]
                               ) -> tuple[list[str], list[str]]:
    """Returns (remaining_after_withdrawal, statutory_kept_anyway)."""
    current_set = {p.strip().upper() for p in current}
    withdraw_set = set(to_withdraw)

    statutory_kept = []
    for p in withdraw_set:
        if p in STATUTORY_PURPOSES:
            statutory_kept.append(p)

    optional_to_remove = withdraw_set & OPTIONAL_PURPOSES
    remaining = sorted(current_set - optional_to_remove)
    return remaining, statutory_kept


def write_audit_row(spark, audit_id: str, event_type: str,
                    investor_acct: str, investor_pan_hash: str,
                    purpose: str, request_id: str,
                    requestor_channel: str, actioned_by: str,
                    pre_snap: str, post_snap: str, notes: str) -> None:
    silver_master_physical = fqtn("silver", "member_master")
    silver_schema_only = silver_master_physical.rsplit(".", 1)[0]
    spark.sql(f"""
        INSERT INTO {fqtn("gold", "consent_audit")} VALUES (
            '{audit_id}',
            CAST('{datetime.now(timezone.utc).isoformat()}' AS TIMESTAMP),
            '{event_type}',
            '{investor_pan_hash}',
            '{investor_acct}',
            '{purpose}',
            'CONSENT_§6(4)',
            '{requestor_channel}',
            '{request_id}',
            '{actioned_by}',
            '{json.dumps([{"schema": silver_schema_only, "table":"member_master"}]).replace("'", "''")}',
            1,
            '{pre_snap}',
            '{post_snap}',
            '{notes.replace("'", "''")}',
            CAST(CURRENT_DATE() AS DATE)
        )
    """)


def update_consent_in_master(spark, investor_acct: str,
                             new_purposes: list[str], status: str) -> None:
    purpose_csv = ",".join(new_purposes)
    spark.sql(f"""
        UPDATE {fqtn("silver", "member_master")}
        SET consent_status  = '{status}',
            consent_purpose = '{purpose_csv}',
            effective_from  = CURRENT_TIMESTAMP()
        WHERE investor_acct = '{investor_acct}' AND is_current
    """)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--investor-acct", required=True,
                        help="Investor account ID (PII_LOW)")
    parser.add_argument("--withdraw-purposes", required=True,
                        help="Comma-separated list (ANALYTICS,MARKETING,...)")
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--requestor-channel",
                        choices=["CONSENT_MANAGER", "DIRECT_DPO", "AUTOMATIC"],
                        default="DIRECT_DPO")
    parser.add_argument("--actioned-by", default="argus_consent_workflow")
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(cde_job("governance.consent_withdrawal")).getOrCreate()

    LOG.info("Consent withdrawal request %s for investor %s",
             args.request_id, args.investor_acct)

    current = fetch_current_consent(spark, args.investor_acct)
    if current is None:
        LOG.error("Investor %s not found in %s",
                  args.investor_acct, fqtn("silver", "member_master"))
        return 1

    current_purposes = normalize_purposes(current.get("consent_purpose") or "")
    requested_withdrawal = normalize_purposes(args.withdraw_purposes)

    remaining, statutory_kept = compute_remaining_purposes(
        current_purposes, requested_withdrawal)
    if statutory_kept:
        LOG.warning("Cannot withdraw statutory purposes %s — retained under DPDP §7. "
                    "Withdrawal proceeds for the optional purposes only.",
                    sorted(statutory_kept))

    # If the investor has any remaining purposes, they're WITHDRAWN-FOR-SOME;
    # if they tried to withdraw everything (all optional purposes), they remain
    # ACTIVE for statutory purposes and WITHDRAWN for optional.
    new_status = "WITHDRAWN" if (set(current_purposes) - set(remaining)) else "ACTIVE"

    pre = current_snapshot_id(spark, "silver", "member_master") or "n/a"
    update_consent_in_master(spark, args.investor_acct, remaining, new_status)
    post = current_snapshot_id(spark, "silver", "member_master") or "n/a"

    audit_id = f"AUDIT-{uuid.uuid4().hex[:12].upper()}"
    write_audit_row(
        spark, audit_id, "CONSENT_WITHDRAWN",
        args.investor_acct, current.get("investor_pan_hash") or "",
        ",".join(sorted(set(requested_withdrawal) & OPTIONAL_PURPOSES)),
        args.request_id, args.requestor_channel, args.actioned_by,
        pre, post,
        f"Withdrew {requested_withdrawal}; retained {remaining}. "
        f"Statutory purposes kept under DPDP §7: {sorted(statutory_kept) if statutory_kept else 'none'}.",
    )
    LOG.info("==> Consent updated. Audit row: %s", audit_id)
    LOG.info("    Status:    %s", new_status)
    LOG.info("    Purposes:  %s", ",".join(remaining))
    LOG.info("    Snapshots: pre=%s post=%s", pre[:12], post[:12])
    LOG.info("    Ranger row-filter dpdp_consent_filter applies on next query.")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
