#!/usr/bin/env python3
"""
apply_atlas_tags — Module 7 governance setup
=============================================
Applies per-student Atlas classifications to the columns listed in
atlas_classifications.json.

Per the cohort design, each student creates their OWN copies of the 6
PRD-locked tags so they can apply / remove without affecting peers:

    Logical name (in JSON)    →  Per-student physical name in Atlas
    -----------------------------------------------------------------
    PII_HIGH                  →  PII_HIGH_${STUDENT_ID}
    PII_LOW                   →  PII_LOW_${STUDENT_ID}
    FINANCIAL_SENSITIVE       →  FINANCIAL_SENSITIVE_${STUDENT_ID}
    SURVEILLANCE_RESTRICTED   →  SURVEILLANCE_RESTRICTED_${STUDENT_ID}
    DPDP_CONSENT_REQUIRED     →  DPDP_CONSENT_REQUIRED_${STUDENT_ID}
    SEBI_AUDIT_TRAIL          →  SEBI_AUDIT_TRAIL_${STUDENT_ID}

Schema names in `applied_to` are similarly transformed:
    argus_bronze → argus_${STUDENT_ID}_bronze, etc.

PRD reference: §10. Closes the lineage + classification piece of ARG-5.

Two-phase operation:
    1. Ensure each per-student classification type is registered in Atlas
       (idempotent — skips if the type already exists).
    2. For each (table × column) target, attach the classification via the
       Atlas Hive entity GUID lookup + classifications POST.

Usage:
    python src/governance/apply_atlas_tags.py \\
        --atlas-url https://atlas.argus.local:31443 \\
        --classifications src/governance/atlas_classifications.json \\
        [--dry-run]

In CDP the Atlas service typically requires Knox SSO; this script
delegates auth to the requests session via env vars (ATLAS_USER /
ATLAS_PASSWORD or ATLAS_TOKEN). For air-gapped deployments, the
classifications can be applied via the Atlas UI's bulk-import path
using the same JSON file.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests

from src.common.naming import atlas_tag, schema as schema_for, get_student_id

LOG = logging.getLogger("argus.governance.atlas")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _physical_schema(logical_schema: str) -> str:
    """Translate a logical schema name from the JSON ('argus_bronze',
    'argus_silver', etc.) to the per-student physical schema."""
    if logical_schema == "argus_bronze":
        return schema_for("bronze")
    if logical_schema == "argus_silver":
        return schema_for("silver")
    if logical_schema == "argus_gold":
        return schema_for("gold")
    if logical_schema == "argus_views":
        return schema_for("views")
    # Unknown — pass through (lets future schemas land here without a code change,
    # but they won't be auto-namespaced unless you add them here).
    LOG.warning("Schema %r not in known logical set; passing through unchanged.",
                logical_schema)
    return logical_schema


def auth_session() -> requests.Session:
    """Build a requests session with auth from env vars."""
    s = requests.Session()
    if os.environ.get("ATLAS_TOKEN"):
        s.headers["Authorization"] = f"Bearer {os.environ['ATLAS_TOKEN']}"
    elif os.environ.get("ATLAS_USER") and os.environ.get("ATLAS_PASSWORD"):
        s.auth = (os.environ["ATLAS_USER"], os.environ["ATLAS_PASSWORD"])
    else:
        LOG.warning("No ATLAS_TOKEN or ATLAS_USER/PASSWORD set — calls will be unauthenticated")
    s.headers["Content-Type"] = "application/json"
    return s


def ensure_classification_type(atlas_url: str, sess: requests.Session,
                               clazz: dict, dry_run: bool) -> None:
    """Create the per-student classification type in Atlas if it doesn't exist."""
    physical_name = atlas_tag(clazz["name"])
    url = f"{atlas_url}/api/atlas/v2/types/typedef/name/{physical_name}"
    r = sess.get(url, timeout=15)
    if r.status_code == 200:
        LOG.info("  [exists]  type %s — skipping", physical_name)
        return
    if r.status_code != 404:
        r.raise_for_status()

    payload = {
        "classificationDefs": [{
            "name": physical_name,
            "description": clazz["description"],
            "superTypes": [],
            "attributeDefs": [
                {"name": a["name"], "typeName": "string", "isOptional": True,
                 "cardinality": "SINGLE", "defaultValue": str(a["value"])}
                for a in clazz.get("attributes", [])
            ],
            "entityTypes": ["hive_column", "hive_table"],
        }]
    }
    LOG.info("  [create]  type %s", physical_name)
    if dry_run:
        return
    create_url = f"{atlas_url}/api/atlas/v2/types/typedefs"
    r = sess.post(create_url, json=payload, timeout=30)
    r.raise_for_status()


def hive_column_qualified_name(schema: str, table: str, column: str,
                               cluster: str = "default") -> str:
    """Atlas qualifiedName convention for Hive columns: db.table.column@cluster."""
    return f"{schema}.{table}.{column}@{cluster}"


def hive_table_qualified_name(schema: str, table: str,
                              cluster: str = "default") -> str:
    return f"{schema}.{table}@{cluster}"


def lookup_entity_guid(atlas_url: str, sess: requests.Session,
                       type_name: str, qualified_name: str) -> str | None:
    """Resolve a Hive table/column qualified name → Atlas entity GUID."""
    url = (f"{atlas_url}/api/atlas/v2/entity/uniqueAttribute/type/{type_name}"
           f"?attr:qualifiedName={quote(qualified_name)}")
    r = sess.get(url, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("entity", {}).get("guid")


def attach_classification(atlas_url: str, sess: requests.Session,
                          guid: str, clazz_name: str, attrs: dict,
                          dry_run: bool) -> None:
    """POST a classification to an entity GUID. Idempotent — Atlas dedupes."""
    payload = [{
        "typeName": clazz_name,
        "attributes": attrs,
        "propagate": True,
        "removePropagationsOnEntityDelete": False,
    }]
    if dry_run:
        return
    url = f"{atlas_url}/api/atlas/v2/entity/guid/{guid}/classifications"
    r = sess.post(url, json=payload, timeout=15)
    if r.status_code in (204, 200):
        return
    if r.status_code == 409:  # already present — fine
        return
    r.raise_for_status()


def apply_classification_to_targets(atlas_url: str, sess: requests.Session,
                                    clazz: dict, dry_run: bool) -> tuple[int, int]:
    """For one classification, walk its applied_to list and attach to each entity.
    Returns (n_attached, n_missing).

    Both the tag name and the schema names are translated to their per-student
    physical forms.
    """
    physical_name = atlas_tag(clazz["name"])
    attrs = {a["name"]: str(a["value"]) for a in clazz.get("attributes", [])}
    attached, missing = 0, 0

    for target in clazz.get("applied_to", []):
        physical_schema = _physical_schema(target["schema"])
        table, columns = target["table"], target["columns"]

        # Wildcard '*' → tag the table itself (whole-row classification).
        if columns == ["*"]:
            qn = hive_table_qualified_name(physical_schema, table)
            guid = lookup_entity_guid(atlas_url, sess, "hive_table", qn)
            if not guid:
                LOG.warning("    [miss]  table %s — no Atlas GUID found", qn)
                missing += 1
                continue
            attach_classification(atlas_url, sess, guid, physical_name, attrs, dry_run)
            LOG.info("    [tag ]  %s → table %s.%s", physical_name, physical_schema, table)
            attached += 1
            continue

        for col in columns:
            qn = hive_column_qualified_name(physical_schema, table, col)
            guid = lookup_entity_guid(atlas_url, sess, "hive_column", qn)
            if not guid:
                LOG.warning("    [miss]  column %s.%s.%s — no Atlas GUID",
                            physical_schema, table, col)
                missing += 1
                continue
            attach_classification(atlas_url, sess, guid, physical_name, attrs, dry_run)
            LOG.info("    [tag ]  %s → %s.%s.%s",
                     physical_name, physical_schema, table, col)
            attached += 1
    return attached, missing


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--atlas-url", required=True,
                        help="Atlas REST URL e.g. https://atlas.argus.local:31443")
    parser.add_argument("--classifications", type=Path,
                        default=Path("src/governance/atlas_classifications.json"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = json.loads(args.classifications.read_text())
    classifications = config["classifications"]
    sid = get_student_id()
    LOG.info("Loaded %d classifications from %s", len(classifications), args.classifications)
    LOG.info("Student ID: %s — tags will be created as <NAME>_%s", sid, sid)
    if len(classifications) != 6:
        LOG.error("Expected 6 classifications per PRD §10; got %d", len(classifications))
        return 1

    sess = auth_session()

    LOG.info("Phase 1 — ensuring classification types exist in Atlas")
    for clazz in classifications:
        ensure_classification_type(args.atlas_url, sess, clazz, args.dry_run)

    LOG.info("Phase 2 — attaching classifications to columns/tables")
    grand_attached, grand_missing = 0, 0
    for clazz in classifications:
        LOG.info("  %s", clazz["name"])
        a, m = apply_classification_to_targets(args.atlas_url, sess, clazz, args.dry_run)
        grand_attached += a
        grand_missing += m

    LOG.info("==> Done — attached %d, missing %d (entities not yet known to Atlas)",
             grand_attached, grand_missing)
    if grand_missing > 0:
        LOG.warning("    'Missing' usually means the table hasn't been written to yet, "
                    "so Atlas has no metadata. Run JOB-01..09 first, then re-apply.")
    return 0 if grand_attached >= 6 else 1


if __name__ == "__main__":
    sys.exit(main())
