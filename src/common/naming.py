"""
naming — single source of truth for all per-student resource names
====================================================================
Every shared resource on the cohort's CDP cluster — Kafka topics, Iceberg
schemas, MLflow experiments, CDE jobs, Atlas tags, etc. — must embed the
${STUDENT_ID} so students can't collide with each other.

This module is the ONLY place that knows the naming convention. Every
piece of student code reads from these helpers; tests and instructor
scripts can override the convention by passing student_id explicitly.

Why a module instead of inline f-strings?

1. Single point of change. If the cohort decides tomorrow that schemas
   should be `argus.s001.bronze` instead of `argus_s001_bronze`, the
   change happens here and propagates to every consumer automatically.
2. Fail fast. Forgetting `export STUDENT_ID=...` produces a clear error
   ("STUDENT_ID is not set") instead of silently writing to the wrong
   place or, worse, silently colliding with another student.
3. Removes infra concerns from surveillance code. Job code reads
   `topic("orders.v1")` — what the topic IS conceptually — not the
   namespaced string, which is a deployment detail.

Usage examples (in Spark/Python jobs):

    from src.common.naming import topic, schema, mlflow_experiment

    df = spark.readStream.format("kafka") \\
              .option("subscribe", topic("orders.v1")) \\
              .load()

    df.writeTo(f"{schema('bronze')}.orders_raw").append()

    mlflow.set_experiment(mlflow_experiment("alert_ranking_v1"))
"""

from __future__ import annotations

import os
import re

# ---------------------------------------------------------------------------
# Validation — student IDs that don't conform produce illegal Kafka topic /
# Iceberg schema / Atlas tag names. Catch them before they cause obscure
# downstream errors.
# ---------------------------------------------------------------------------
_VALID_SID = re.compile(r"^[a-z][a-z0-9]{2,15}$")


def get_student_id() -> str:
    """Read STUDENT_ID from the environment.

    Raises a clear, instructional error if it's missing or malformed.
    Cohort instructors should ensure students set this in their .bashrc
    or workbench profile before any lab work begins.
    """
    sid = os.environ.get("STUDENT_ID", "").strip()
    if not sid:
        raise RuntimeError(
            "STUDENT_ID environment variable is not set. "
            "Run:\n    export STUDENT_ID=<your-assigned-id>\n"
            "Then re-run the command. See docs/03_day1_primer.md for context."
        )
    if not _VALID_SID.match(sid):
        raise ValueError(
            f"STUDENT_ID={sid!r} is invalid. Must be lowercase letters and "
            f"digits only, starting with a letter, 3-16 characters total. "
            f"Examples of valid IDs: 's001', 'priya23', 'bv01'."
        )
    return sid


# ---------------------------------------------------------------------------
# Per-resource resolvers
# ---------------------------------------------------------------------------

def topic(logical_name: str, *, student_id: str | None = None) -> str:
    """Resolve a logical Kafka topic name to its per-student physical name.

    Examples:
        topic("orders.v1")  -> "argus.s001.orders.v1"
        topic("trades.dlq") -> "argus.s001.trades.dlq"
    """
    sid = student_id or get_student_id()
    return f"argus.{sid}.{logical_name}"


def schema(layer: str, *, student_id: str | None = None) -> str:
    """Resolve an Iceberg layer name to its per-student schema.

    Examples:
        schema("bronze") -> "argus_s001_bronze"
        schema("views")  -> "argus_s001_views"

    Valid layers: bronze, silver, gold, views.
    """
    sid = student_id or get_student_id()
    if layer not in {"bronze", "silver", "gold", "views"}:
        raise ValueError(
            f"Unknown schema layer {layer!r}. Valid layers: bronze, silver, gold, views."
        )
    return f"argus_{sid}_{layer}"


def fqtn(layer: str, table: str, *, student_id: str | None = None) -> str:
    """Fully-qualified table name: schema(layer) + '.' + table.

    Convenience for `f"{schema(layer)}.{table}"`. Reads cleanly inside
    SQL string-building code:
        spark.sql(f"SELECT * FROM {fqtn('gold', 'alert_candidates')}")
    """
    return f"{schema(layer, student_id=student_id)}.{table}"


def mlflow_experiment(logical_name: str, *, student_id: str | None = None) -> str:
    """Resolve an MLflow experiment name.

    Example:
        mlflow_experiment("alert_ranking_v1") -> "argus_s001_alert_ranking_v1"
    """
    sid = student_id or get_student_id()
    return f"argus_{sid}_{logical_name}"


def mlflow_model(logical_name: str, *, student_id: str | None = None) -> str:
    """Resolve an MLflow registered model name.

    Example:
        mlflow_model("alert_ranker") -> "argus_s001_alert_ranker"
    """
    sid = student_id or get_student_id()
    return f"argus_{sid}_{logical_name}"


def cde_job(logical_name: str, *, student_id: str | None = None) -> str:
    """Resolve a CDE Spark job application name.

    Example:
        cde_job("bronze.orders_ingest") -> "argus.s001.bronze.orders_ingest"
    """
    sid = student_id or get_student_id()
    return f"argus.{sid}.{logical_name}"


def consumer_group(logical_name: str, *, student_id: str | None = None) -> str:
    """Resolve a Kafka consumer-group ID.

    Example:
        consumer_group("bronze.orders") -> "argus.s001.bronze.orders"
    """
    sid = student_id or get_student_id()
    return f"argus.{sid}.{logical_name}"


def milvus_collection(logical_name: str, *, student_id: str | None = None) -> str:
    """Resolve a Milvus collection name.

    Example:
        milvus_collection("str_corpus") -> "argus_s001_str_corpus"
    """
    sid = student_id or get_student_id()
    return f"argus_{sid}_{logical_name}"


def atlas_tag(logical_tag: str, *, student_id: str | None = None) -> str:
    """Resolve an Atlas classification tag name to its per-student variant.

    Per the cohort design, each student creates their own copies of the 6
    PRD-locked tags so they can apply/remove without affecting peers.

    Example:
        atlas_tag("PII_HIGH") -> "PII_HIGH_s001"
    """
    sid = student_id or get_student_id()
    return f"{logical_tag}_{sid}"


def s3_bucket(*, student_id: str | None = None) -> str:
    """Resolve the per-student S3 bucket name.

    Reads the BUCKET_NAME environment variable directly because the
    instructor pre-provisions buckets with arbitrary naming schemes that
    don't necessarily follow the `argus-${SID}-mum` pattern. If
    BUCKET_NAME isn't set, falls back to the conventional pattern.
    """
    explicit = os.environ.get("BUCKET_NAME", "").strip()
    if explicit:
        return explicit
    sid = student_id or get_student_id()
    return f"argus-{sid}-mum"


# ---------------------------------------------------------------------------
# Inventory helpers — used by provisioning + verification scripts
# ---------------------------------------------------------------------------

# 8 production topics + 2 DLQ topics = 10 total to create per student
LOGICAL_TOPICS_WITH_PARTITIONS: list[tuple[str, int]] = [
    ("orders.v1",             48),
    ("trades.v1",             24),
    ("bbo.v1",                12),
    ("member.cdc.v1",          6),
    ("instrument.cdc.v1",      3),
    ("surveillance.state.v1",  6),
    ("regulator.feed.v1",      3),
    ("news.v1",                6),
    ("orders.dlq",             3),
    ("trades.dlq",             3),
]

LOGICAL_SCHEMAS = ["bronze", "silver", "gold", "views"]

LOGICAL_ATLAS_TAGS = [
    "PII_HIGH",
    "PII_LOW",
    "FINANCIAL_SENSITIVE",
    "SURVEILLANCE_RESTRICTED",
    "DPDP_CONSENT_REQUIRED",
    "SEBI_AUDIT_TRAIL",
]


def all_topics(*, student_id: str | None = None) -> list[tuple[str, int]]:
    """List of (physical_topic_name, partitions) tuples for this student.
    Used by provision_environment.sh and verification queries."""
    return [
        (topic(name, student_id=student_id), partitions)
        for name, partitions in LOGICAL_TOPICS_WITH_PARTITIONS
    ]


def all_schemas(*, student_id: str | None = None) -> list[str]:
    """List of physical schema names for this student."""
    return [schema(layer, student_id=student_id) for layer in LOGICAL_SCHEMAS]


def all_atlas_tags(*, student_id: str | None = None) -> list[str]:
    """List of physical Atlas classification names for this student."""
    return [atlas_tag(t, student_id=student_id) for t in LOGICAL_ATLAS_TAGS]
