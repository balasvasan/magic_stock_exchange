# Day 1 — Environment Setup

> 📊 **Visual reference**: [Day 1 setup activity diagram](../assets/diagrams/01_day1_setup.md) ([SVG](../assets/diagrams/01_day1_setup.svg))

> 👋 **New to Cloudera, Iceberg, Kafka, or Spark?** Read [`docs/03_day1_primer.md`](03_day1_primer.md) first. It explains what each piece of the stack *is* before you start using it.
>
> 🚶 **For the step-by-step procedure with inline teaching**, the actual lab is at [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md). This file (`03_day1_setup.md`) is a **reference** — concise summary for instructors and returning students who already know the material.

> ℹ️ **Note:** This is the foundation everything else depends on. If Day 1 isn't clean, every subsequent module produces confusing errors. Take the time to validate each step before moving on.

By end of day you will have:

- An S3 bucket holding all Bronze/Silver/Gold data and MLflow/Milvus artifacts
- 11 Kafka topics (9 production + 2 DLQ) provisioned with correct partition counts
- All 19 Iceberg tables created (6 Bronze · 4 Silver · 9 Gold)
- A full synthetic data set generated locally with all 23 planted test cases
- Bulk-loaded Bronze layer via FLOW-SIM oneshot mode

The capstone uses a fictional Indian stock exchange — Magic Street Exchange — with five fictional internal source systems (TARANG, KAVACH, NIPATAN, PRATEEK, SMRITI) and three external feeds (SEBI, cross-exchange BBO, news). Day 1 is purely environment provisioning; the actual streaming work begins in Module 1.

## 0. Prerequisites checklist

Before starting, confirm you have:

- A CDP environment with at least 6 worker nodes and 1 GPU node
- AWS CLI configured for the `ap-south-1` (Mumbai) region — chosen for DPDP data-residency
- An assigned `STUDENT_ID` from your instructor (e.g. `sxx21`, `jpatel`)
- Kafka broker addresses from your CDF cluster
- Hive / Impala access for DDL execution
- Python 3.10+ on your workstation

```bash
export STUDENT_ID=<your-student-id>
export AWS_REGION=ap-south-1
export KAFKA_BROKERS=<broker:9092,broker:9092,broker:9092>
```

## 1. Provision S3 + Kafka — `sql/provision_environment.sh`

The provisioning script is idempotent: re-running is safe, anything already present is skipped.

```bash
bash sql/provision_environment.sh
```

**What this does**: verifies the instructor-provisioned bucket `${BUCKET_NAME}` is reachable and has the seven required prefixes (`bronze/`, `silver/`, `gold/`, `landing/`, `checkpoints/`, `mlflow/`, `milvus/`); then creates the 10 per-student Kafka topics (8 production + 2 DLQ) from the PRD with their full partition counts:

| Topic | Partitions | Notes |
|---|---:|---|
| `argus.${STUDENT_ID}.orders.v1` | 48 | TARANG firehose — biggest topic |
| `argus.${STUDENT_ID}.trades.v1` | 24 | NIPATAN executed trades |
| `argus.${STUDENT_ID}.bbo.v1` | 12 | Cross-exchange BBO |
| `argus.${STUDENT_ID}.member.cdc.v1` | 6 | Compacted (KAVACH CDC) |
| `argus.${STUDENT_ID}.instrument.cdc.v1` | 3 | Compacted (PRATEEK reference) |
| `argus.${STUDENT_ID}.surveillance.state.v1` | 6 | ESM/ASM/circuit state |
| `argus.${STUDENT_ID}.regulator.feed.v1` | 3 | SEBI feed |
| `argus.${STUDENT_ID}.news.v1` | 6 | News + corporate disclosures |
| `argus.${STUDENT_ID}.orders.dlq` | 3 | Dead-letter queue (orders) |
| `argus.${STUDENT_ID}.trades.dlq` | 3 | Dead-letter queue (trades) |

**Verify**:

```bash
aws s3 ls s3://${BUCKET_NAME}/
kafka-topics --bootstrap-server ${KAFKA_BROKERS} --list | grep "argus.${STUDENT_ID}."
```

You should see 7 prefixes in S3 and 10 topics under `argus.${STUDENT_ID}.` in Kafka. SMM (Streams Messaging Manager) on the CDF UI gives a richer view; navigate to your cluster and confirm partition counts match the table above.

## 2. Create Iceberg DDL — three SQL files in order

The DDL files are templates that contain `${STUDENT_ID}` placeholders, so pipe them through `envsubst` to substitute your actual ID. Run them in dependency order — Bronze first (Silver doesn't depend on Bronze tables existing, but it's a good habit to follow the layer order):

```bash
envsubst < sql/bronze_ddl.sql | hive -f -      # creates argus_${STUDENT_ID}_bronze (6 tables)
envsubst < sql/silver_ddl.sql | hive -f -      # creates argus_${STUDENT_ID}_silver (4 tables)
envsubst < sql/gold_ddl.sql   | hive -f -      # creates argus_${STUDENT_ID}_gold (8 tables)
```

> 💡 **Tip:** If you're using Impala instead of Hive, swap `hive -f` for `impala-shell -f`. The DDL is portable across both.

**What this creates**:

- **Bronze (6, MOR/ORC)**: `orders_raw`, `trades_raw`, `member_cdc`, `instrument_cdc`, `external_feeds`, `legacy_alerts`
- **Silver (4, COW/Parquet)**: `order_events`, `executed_trades`, `member_master`, `instrument_master`
- **Gold (8, COW/Parquet)**: `order_book_snapshots`, `member_temporal_features`, `cross_product_features`, `alert_candidates`, `confirmed_manipulation_cases`, `member_risk_scores`, `consent_audit`, `surveillance_kpis`

> ⚠️ **Compliance gate:** `argus_${STUDENT_ID}_gold.consent_audit` is created with `'history.expire.enabled' = 'false'`. Do **not** remove this property anywhere in the capstone — it is what makes the consent + erasure audit trail evidentiary under DPDP §12. Module 7 verifies this in CP-19, the COMPLIANCE GATE checkpoint.

**Verify**:

```sql
SHOW TABLES IN argus_${STUDENT_ID}_bronze;   -- expect 6 tables
SHOW TABLES IN argus_${STUDENT_ID}_silver;   -- expect 4
SHOW TABLES IN argus_${STUDENT_ID}_gold;     -- expect 8

-- Confirm consent_audit's mandatory property
DESCRIBE FORMATTED argus_${STUDENT_ID}_gold.consent_audit;
-- Look for:  history.expire.enabled    false
```

See [`sql/bronze_ddl.sql`](../sql/bronze_ddl.sql), [`sql/silver_ddl.sql`](../sql/silver_ddl.sql), and [`sql/gold_ddl.sql`](../sql/gold_ddl.sql) for the full DDL with column-level comments and Atlas-classification hints.

## 3. Generate the synthetic data — `data/generate_data.py`

The generator produces 14 files containing the full data landscape: members, traders, investors, instruments, corporate actions, surveillance state, consent records, ~50M order events, ~3.5M executed trades, ~7M BBO ticks, 4.8M historical alerts, SEBI actions, news headlines, and the compliance test-case index.

For Day 1, generate at the default reduced scale so it finishes in minutes rather than hours:

```bash
python data/generate_data.py --seed 42 --scale 0.05 --out data/generated/
```

**Why `--scale 0.05`**: At 5% scale you get ~2.5M order events — enough to exercise every code path, big enough to feel real, small enough to fit in your CDP environment without ops overhead. For the full 50M-event firehose, switch to `--scale 1.0` near the end of the course; Module 1's CP-03 (150K events/sec sustained) actually needs the full scale to be meaningful.

> ℹ️ **Note:** Same `--seed` produces identical files. Planted test cases land at fixed indices 0–22 regardless of scale. You can re-run the generator at any time without breaking downstream state — the script writes to `data/generated/` which is in `.gitignore`.

**Verify**:

```bash
ls data/generated/
# Expect 14 files. The big ones are *.jsonl.gz; the rest are CSV.

# Confirm the test case index has all 23 planted cases:
wc -l data/generated/compliance_test_cases.csv
# Expect 24 (1 header + 23 cases)

cat data/generated/compliance_test_cases.csv | head -5
```

The 23 planted cases break down as:

- **0–9**: cross-product manipulation patterns (LAYERING, SPOOFING, MARKING_THE_CLOSE, MOMENTUM_IGNITION, CROSS_PRODUCT_LAYER, WASH_TRADE, plus 3 negative/edge cases at 6, 7, 8, plus a multi-day case at 9). These are the meat of Modules 2 and 3.
- **10–14**: fuzzy-match identity-resolution cases. Tested in Module 2 (CP-06).
- **15–19**: DPDP §6(4) consent-withdrawal cases. Tested in Modules 4 (CP-11) and 7 (CP-18).
- **20–22**: DPDP §12 erasure cases. Tested in Module 7's COMPLIANCE GATE checkpoint (CP-19).

## 4. Bulk-load Bronze with FLOW-SIM oneshot

FLOW-SIM is a Python script (`src/ingest/replay_simulator.py`) that takes the synthetic JSONL files and produces them to Kafka in two modes — `oneshot` for initial bulk-load and `continuous` for live replay. The continuous mode lives in Module 1 (Lab 1.3); for Day 1 we only need oneshot.

```bash
python src/ingest/replay_simulator.py --mode oneshot \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS}
```

> 💡 **Tip:** Oneshot mode produces all events as fast as Kafka will accept. On a healthy CDF cluster this takes 5–15 minutes for 2.5M events.

**Verify**: in SMM, navigate to each topic and confirm that the partition message counts roughly match expectations (orders > trades > BBO > everything else). All 8 topics should have at least some traffic except `argus.${STUDENT_ID}.regulator.feed.v1` and `argus.${STUDENT_ID}.news.v1` which start sparse and fill up over time.

## 5. Checkpoint CP-00 — environment ready

See [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md) for the full CP-00 verification — what to check, what the expected outputs look like, and one common failure mode you'll probably hit at least once.

---

## Day 1 file map

| File | Purpose |
|---|---|
| [`sql/provision_environment.sh`](../sql/provision_environment.sh) | Bash: S3 bucket + 11 Kafka topics |
| [`sql/bronze_ddl.sql`](../sql/bronze_ddl.sql) | DDL for 6 Bronze tables (MOR/ORC) |
| [`sql/silver_ddl.sql`](../sql/silver_ddl.sql) | DDL for 4 Silver tables (COW/Parquet, SCD2 masters) |
| [`sql/gold_ddl.sql`](../sql/gold_ddl.sql) | DDL for 8 Gold tables — `consent_audit` has `history.expire.enabled=false` |
| [`data/generate_data.py`](../data/generate_data.py) | Synthetic data generator with 23 planted test cases |
| [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md) | CP-00 verification checklist |
