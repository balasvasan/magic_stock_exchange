#!/bin/bash
#
# ARGUS — Day 1 Environment Provisioning (verification + topic creation)
# =======================================================================
# The cohort runs on a SHARED CDP cluster (16 students + 4 instructors).
# Resources that students could collide on are namespaced per-student via
# ${STUDENT_ID}. This script:
#
#   1. Verifies the instructor-provisioned S3 bucket is reachable and
#      has the seven expected prefixes.
#   2. Creates 10 per-student Kafka topics (8 production + 2 DLQ) with
#      partition counts from PRD §5.
#
# It does NOT create the S3 bucket itself — that's instructor-provisioned.
# Each student's bucket name is given to them by the instructor and may
# follow any naming scheme (so we read BUCKET_NAME directly from the env
# rather than constructing it).
#
# Usage:
#   export STUDENT_ID=<your-student-id>            # e.g. 's001' or 'priya23'
#   export BUCKET_NAME=<full-bucket-name>          # given by your instructor
#   export AWS_REGION=ap-south-1                   # Mumbai for DPDP residency
#   export KAFKA_BROKERS=<host:9092,host:9092>     # CDF cluster brokers
#   bash sql/provision_environment.sh
#
# Idempotent — safe to re-run; topics that already exist are skipped.

set -euo pipefail

# ---------- 1. Required environment variables ----------
: "${STUDENT_ID:?Set STUDENT_ID=<your-student-id> before running}"
: "${BUCKET_NAME:?Set BUCKET_NAME=<full-bucket-name-from-instructor> before running}"
: "${AWS_REGION:?Set AWS_REGION=ap-south-1 before running}"
: "${KAFKA_BROKERS:?Set KAFKA_BROKERS=<host:port,host:port> before running}"

# Validate STUDENT_ID format — must match the convention enforced in
# src/common/naming.py so all references resolve consistently.
if ! [[ "${STUDENT_ID}" =~ ^[a-z][a-z0-9]{2,15}$ ]]; then
    echo "ERROR: STUDENT_ID='${STUDENT_ID}' is invalid."
    echo "       Must be lowercase letters and digits, starting with a letter,"
    echo "       3-16 characters total. Examples: s001, priya23, bv01."
    exit 1
fi

echo "==> ARGUS provisioning"
echo "    Student ID: ${STUDENT_ID}"
echo "    Bucket:     ${BUCKET_NAME}"
echo "    Region:     ${AWS_REGION}"
echo ""

# ---------- 2. Verify S3 bucket (instructor-provisioned) ----------
echo "==> [1/2] Verifying S3 bucket: s3://${BUCKET_NAME}/"

if ! aws s3api head-bucket --bucket "${BUCKET_NAME}" 2>/dev/null; then
    echo "    ✗ ERROR: bucket '${BUCKET_NAME}' is not reachable."
    echo "      Check that:"
    echo "        - BUCKET_NAME matches what the instructor gave you exactly"
    echo "        - your AWS credentials are valid:  aws sts get-caller-identity"
    echo "        - your IAM permissions allow s3:HeadBucket on this bucket"
    exit 1
fi
echo "    ✓ bucket is reachable"

# Verify the seven required prefixes exist (instructor pre-creates them).
missing_prefixes=()
for prefix in bronze silver gold landing checkpoints mlflow milvus; do
    if ! aws s3api list-objects-v2 \
            --bucket "${BUCKET_NAME}" \
            --prefix "${prefix}/" \
            --max-keys 1 \
            --query 'KeyCount' --output text 2>/dev/null | grep -q -E "^[01]$"; then
        missing_prefixes+=("${prefix}/")
    else
        echo "    ✓ prefix found: ${prefix}/"
    fi
done

if [ ${#missing_prefixes[@]} -gt 0 ]; then
    echo ""
    echo "    ⚠  WARNING: missing prefixes: ${missing_prefixes[*]}"
    echo "       Tell your instructor — these should be pre-created."
    echo "       (Continuing anyway; Iceberg / Spark will create them on first write,"
    echo "        but that's a sign of an incomplete bucket setup.)"
fi

# ---------- 3. Per-student Kafka topics ----------
echo ""
echo "==> [2/2] Creating per-student Kafka topics"
echo "    Naming convention: argus.${STUDENT_ID}.<topic>"
echo ""

# Per PRD §5 — 8 production topics + 2 DLQ topics = 10 per student × 16 students = 160 topics on the cluster.
# Cluster admins should size the broker accordingly (each topic is small in
# storage; what matters is total partition count).
#
# Format: <logical-name>  <partitions>  <replication>  [extra-config]
declare -a TOPICS=(
    "orders.v1             48 3"
    "trades.v1             24 3"
    "bbo.v1                12 3"
    "member.cdc.v1          6 3 cleanup.policy=compact"
    "instrument.cdc.v1      3 3 cleanup.policy=compact"
    "surveillance.state.v1  6 3"
    "regulator.feed.v1      3 3"
    "news.v1                6 3"
    "realtime_alerts.v1    12 3"
    "orders.dlq             3 3"
    "trades.dlq             3 3"
)

created=0
skipped=0
for topic_spec in "${TOPICS[@]}"; do
    # shellcheck disable=SC2086
    set -- ${topic_spec}
    logical_name="$1"
    partitions="$2"
    replication="$3"
    extra_config="${4:-}"

    physical_name="argus.${STUDENT_ID}.${logical_name}"

    if kafka-topics --bootstrap-server "${KAFKA_BROKERS}" --list 2>/dev/null \
            | grep -qx "${physical_name}"; then
        echo "    [skip] ${physical_name} (exists)"
        skipped=$((skipped+1))
        continue
    fi

    config_args=()
    if [[ -n "${extra_config}" ]]; then
        config_args=(--config "${extra_config}")
    fi

    kafka-topics --bootstrap-server "${KAFKA_BROKERS}" \
        --create \
        --topic "${physical_name}" \
        --partitions "${partitions}" \
        --replication-factor "${replication}" \
        "${config_args[@]}" \
        >/dev/null
    echo "    [ ok ] ${physical_name} (${partitions}p${extra_config:+, ${extra_config}})"
    created=$((created+1))
done

echo ""
echo "==> Done."
echo "    Bucket:  s3://${BUCKET_NAME}/  (verified)"
echo "    Topics:  ${created} created, ${skipped} already existed"
echo ""
echo "Next step — create Iceberg tables. The DDL files are templates that"
echo "use \${STUDENT_ID}, so run them through envsubst:"
echo ""
echo "    envsubst < sql/bronze_ddl.sql | hive -f -"
echo "    envsubst < sql/silver_ddl.sql | hive -f -"
echo "    envsubst < sql/gold_ddl.sql   | hive -f -"
echo ""
echo "If your environment uses Impala instead of Hive, swap 'hive -f -'"
echo "for 'impala-shell -f -'."
