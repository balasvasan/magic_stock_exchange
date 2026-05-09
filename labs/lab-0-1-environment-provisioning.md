# Lab 0.1 — Environment Provisioning (CP-00 + CP-01)

> 👋 **Day 1 first-timer?** Read [`docs/03_day1_primer.md`](../docs/03_day1_primer.md) before starting this lab. It explains what Cloudera, Iceberg, Kafka, Spark, and the four-layer architecture *are*. About 15 minutes — well worth the time.

> ℹ️ **Module:** Day 1 — Setup
> **Closes:** No deficiency yet. Day 1 is the foundation.
> **Time:** ~90 minutes if everything works first try; up to 3 hours if you hit configuration issues.
> **Source files:** [`sql/provision_environment.sh`](../sql/provision_environment.sh), [`sql/bronze_ddl.sql`](../sql/bronze_ddl.sql), [`sql/silver_ddl.sql`](../sql/silver_ddl.sql), [`sql/gold_ddl.sql`](../sql/gold_ddl.sql), [`data/generate_data.py`](../data/generate_data.py), [`src/ingest/replay_simulator.py`](../src/ingest/replay_simulator.py)

## What you're going to do

In order:

1. **Set up environment variables** so the rest of the lab knows which student you are
2. **Create the S3 bucket** that will hold all your data — runs in seconds
3. **Create the 11 Kafka topics** — runs in seconds
4. **Create the 19 Iceberg tables** (empty) — runs in 1–2 minutes
5. **Generate the synthetic data** — runs in 5–10 minutes
6. **Bulk-load Bronze with FLOW-SIM** — runs in 5–15 minutes
7. **Verify everything via CP-00 and CP-01 checks**

Total: about 25–40 minutes of actual command execution, plus reading and verification time.

## Before you begin — prerequisite checklist

Confirm each of these is true. If any aren't, ask your instructor before starting.

- [ ] You have access to a CDP Public Cloud environment (your instructor will have given you a workbench URL)
- [ ] You have an assigned `STUDENT_ID` from your instructor (4–8 character code, e.g. `s001`, `bv01`, `priya23`)
- [ ] Your instructor has given you the **full S3 bucket name** for your work — note it down, you'll need it in Step 1 (sample format: `argus-cohort7-s001`, but yours may look different)
- [ ] You can open a terminal — either on the CDP workbench, on a CDF gateway node, or on your local laptop with AWS CLI configured
- [ ] You know your Kafka broker addresses (your instructor will share these — they look like `kafka1.argus.local:9092,kafka2.argus.local:9092,kafka3.argus.local:9092`)
- [ ] You can run `aws s3 ls` without a credentials error (test it now if you're unsure)
- [ ] You can connect to Hive or Impala — your instructor will give you a JDBC URL or web UI link
- [ ] You have Python 3.10 or newer (`python3 --version`)
- [ ] `envsubst` is installed (it ships with `gettext`; on Ubuntu/Debian: `sudo apt install gettext-base` if missing)
- [ ] You've cloned the capstone repo and are in the `argus-capstone/` directory

If any of those are missing or unclear, sort them out first. The lab will not work otherwise.

---

## Step 1 — Set up environment variables

Throughout this lab you'll see commands that reference `${STUDENT_ID}` and `${KAFKA_BROKERS}`. These are shell variables; the shell substitutes your real values at runtime so you don't have to type them every time.

Set them now, in the terminal session you'll be working in:

```bash
export STUDENT_ID=s001                   # ← REPLACE s001 WITH YOUR ASSIGNED ID
export BUCKET_NAME=argus-cohort7-s001    # ← REPLACE with the FULL bucket name your instructor gave you
export AWS_REGION=ap-south-1             # Mumbai region for DPDP data residency
export KAFKA_BROKERS=kafka1.argus.local:9092,kafka2.argus.local:9092,kafka3.argus.local:9092
                                          # ← REPLACE with the brokers your instructor gave you
```

> ℹ️ **Note about the bucket name:** The instructor pre-provisioned an S3 bucket for you. They'll have given you the *full* bucket name (it might look like `argus-cohort7-s001`, `mse-training-priya23`, or any other naming the instructor chose). Use that exact name — don't try to construct one yourself. It's the value of `BUCKET_NAME` everywhere from here on.

> 💡 **Tip:** If you open a new terminal window, you'll need to set these again — `export` only lasts for the current session. To make them permanent, add the same `export` lines to your `~/.bashrc` or `~/.zshrc` file.

**Verify they're set:**

```bash
echo "STUDENT_ID  = $STUDENT_ID"
echo "BUCKET_NAME = $BUCKET_NAME"
echo "AWS_REGION  = $AWS_REGION"
echo "KAFKA_BROKERS = $KAFKA_BROKERS"
```

You should see your values printed back. **If any line shows `=` with nothing after it, the variable wasn't set** — re-run the `export` commands and retry.

---

## Step 2 — Create the S3 bucket

The provisioning script is `sql/provision_environment.sh`. It does two things: creates the S3 bucket with the right configuration, then creates the 11 Kafka topics. We'll run it once and let it do both.

But first, **let's understand what it's about to do** so the output makes sense.

Your instructor pre-provisioned an S3 bucket for you with the seven required prefixes already created. The provisioning script's job today is to **verify** the bucket is reachable and then **create your 10 per-student Kafka topics**. It does NOT create the bucket itself.

The seven prefixes inside your bucket are like top-level folders:

| Prefix | Purpose |
|---|---|
| `bronze/` | Raw Iceberg tables — data exactly as it came in |
| `silver/` | Cleaned + joined Iceberg tables |
| `gold/` | Analytics-ready Iceberg tables |
| `landing/` | Files dropped here before ingestion (used by JOB-03 and the seed script) |
| `checkpoints/` | Spark Structured Streaming checkpoint state |
| `mlflow/` | ML experiment artifacts (Modules 5+) |
| `milvus/` | Vector store data (Module 6) |

The script also creates 10 per-student Kafka topics — 8 production topics plus 2 dead-letter queue topics. Per-student naming (`argus.${STUDENT_ID}.orders.v1` rather than just `argus.orders.v1`) means 16 students can share one Kafka cluster without colliding.

**Now run the script:**

```bash
bash sql/provision_environment.sh
```

**Expected output**: a verification block showing the bucket is reachable and the 7 prefixes are found, then 10 lines for the topics being created (or skipped if you're re-running). The whole thing takes 20–40 seconds.

**Common errors and fixes:**

- **`bucket '<name>' is not reachable`** → either `BUCKET_NAME` is wrong (verify against what the instructor gave you — exact match including any prefix/suffix) or your AWS credentials aren't valid. Run `aws sts get-caller-identity` to confirm credentials. If that errors, run `aws sso login` (or whichever auth flow your CDP uses).
- **`STUDENT_ID='XX' is invalid`** → IDs must be 3–16 lowercase letters/digits, starting with a letter. Examples: `s001`, `priya23`, `bv01`. If your instructor gave you something else, ask for one matching this format.
- **Topic creation fails with `Topic '<name>' already exists`** → harmless; the script catches that and shows `[skip]`. Real errors (broker unreachable, permission denied) bubble up clearly.
- **`Unable to locate credentials`** → AWS isn't authenticated. Run `aws sts get-caller-identity` to confirm. If that errors, run `aws sso login` and retry.

---

## Step 3 — Verify the S3 bucket and Kafka topics (CP-00, Checks 1 and 2)

The provisioning script does most of this verification inline, but here are the manual checks for CP-00.

### Check 1 — S3 bucket exists with correct config

```bash
# Check 1a — does the bucket exist?
aws s3api head-bucket --bucket ${BUCKET_NAME} 2>&1
```

**Expected output**: nothing (success). If you see anything other than no output, the bucket isn't there — most likely `BUCKET_NAME` doesn't match what the instructor provisioned.

```bash
# Check 1b — list its contents (the seven prefixes)
aws s3 ls s3://${BUCKET_NAME}/
```

**Expected output**: seven lines, each starting with `PRE` (meaning "prefix" — i.e. folder):

```
                           PRE bronze/
                           PRE checkpoints/
                           PRE gold/
                           PRE landing/
                           PRE milvus/
                           PRE mlflow/
                           PRE silver/
```

The order may differ; that's fine. **Count them — there should be exactly seven.** If any are missing, tell your instructor — they should be pre-created.

```bash
# Check 1c — confirm encryption is enabled (instructor sets this)
aws s3api get-bucket-encryption --bucket ${BUCKET_NAME}
```

**Expected output**: a JSON response containing `"SSEAlgorithm": "AES256"`. If you see `ServerSideEncryptionConfigurationNotFoundError`, the bucket isn't encrypted — flag it to your instructor; this is a setup issue on their side, not yours.

### Check 2 — Kafka topics with correct partition counts

```bash
kafka-topics --bootstrap-server ${KAFKA_BROKERS} --describe \
    --topic argus.${STUDENT_ID}.orders.v1,argus.${STUDENT_ID}.trades.v1,argus.${STUDENT_ID}.bbo.v1,argus.${STUDENT_ID}.member.cdc.v1,argus.${STUDENT_ID}.instrument.cdc.v1,argus.${STUDENT_ID}.surveillance.state.v1,argus.${STUDENT_ID}.regulator.feed.v1,argus.${STUDENT_ID}.news.v1 \
    | grep -E 'PartitionCount|Topic:'
```

**Expected output**: 16 lines — one `Topic: argus.<your-id>.X.vN` line and one `PartitionCount: N` line per topic. Compare against the table:

| Topic | Expected `PartitionCount` | Why this number |
|---|---:|---|
| `argus.${STUDENT_ID}.orders.v1` | 48 | TARANG firehose — highest volume, needs the most parallelism |
| `argus.${STUDENT_ID}.trades.v1` | 24 | NIPATAN executed trades — high but lower than orders |
| `argus.${STUDENT_ID}.bbo.v1` | 12 | Cross-exchange BBO — modest volume |
| `argus.${STUDENT_ID}.member.cdc.v1` | 6 | KAVACH CDC — low volume but compacted |
| `argus.${STUDENT_ID}.instrument.cdc.v1` | 3 | PRATEEK reference — very low volume, compacted |
| `argus.${STUDENT_ID}.surveillance.state.v1` | 6 | ESM/ASM/circuit state — modest, compacted |
| `argus.${STUDENT_ID}.regulator.feed.v1` | 3 | SEBI feed — sparse |
| `argus.${STUDENT_ID}.news.v1` | 6 | News + corporate disclosures |

Total partitions: **108**. If any topic shows a wrong partition count, you can grow it but not shrink it:

```bash
# To grow a topic from N to M partitions (M > N):
kafka-topics --bootstrap-server ${KAFKA_BROKERS} \
    --alter --topic <topic_name> --partitions <new_count>
```

If you need to shrink a topic, you have to delete and recreate it — easier to just re-run `provision_environment.sh` after manually deleting the offending topic.

> 💡 **Tip:** If `kafka-topics` isn't found on your machine, it's because the Kafka CLI lives on CDF gateway nodes, not on your laptop. Easier alternative: open the **Streams Messaging Manager (SMM)** web UI — your instructor will have given you the URL — and inspect topics visually there. The same partition counts should appear.

---

## Step 4 — Create the Iceberg tables (CP-00, Checks 3 and 4)

The DDL files declare three schemas (`argus_${STUDENT_ID}_bronze`, `argus_${STUDENT_ID}_silver`, `argus_${STUDENT_ID}_gold`) and the 19 tables inside them. **DDL** stands for "Data Definition Language" — SQL that describes table shapes, not SQL that queries data.

You'll run the three DDL files in a specific order. The DDL files are **templates** that contain `${STUDENT_ID}` placeholders, so you pipe them through `envsubst` first to substitute your actual student ID at the moment they execute:

```bash
envsubst < sql/bronze_ddl.sql | hive -f -      # creates argus_${STUDENT_ID}_bronze schema + 6 tables
envsubst < sql/silver_ddl.sql | hive -f -      # creates argus_${STUDENT_ID}_silver schema + 4 tables
envsubst < sql/gold_ddl.sql   | hive -f -      # creates argus_${STUDENT_ID}_gold schema + 8 tables
```

The `envsubst` command reads the DDL on stdin, replaces every `${STUDENT_ID}` it finds with whatever you exported, and writes the result to stdout — which then pipes into Hive's `-f -` flag (read SQL from stdin).

**Expected output**: each command prints `OK` after each `CREATE TABLE` statement, totaling 6 + 4 + 8 = 18 OKs across the three runs. Any error means a DDL statement failed — read the error message; it usually points at the line.

> 💡 **Tip:** If your environment uses Impala instead of Hive (it usually does in CDP 7.3+), swap `hive -f` for `impala-shell -f`. The DDL is portable.

> 💡 **Tip:** The DDL files start with `DROP TABLE IF EXISTS` for every table they create. This means re-running them is safe — they'll wipe and recreate, not fail with "table already exists." If you've made any tweaks to the DDL and want to start fresh, just re-run.

### Check 3 — All 18 tables present

Connect to your SQL engine (Hue, Beeline, or `impala-shell`) and run:

```sql
SELECT 'bronze' AS layer, table_name FROM information_schema.tables
  WHERE table_schema = 'argus_${STUDENT_ID}_bronze'
UNION ALL
SELECT 'silver', table_name FROM information_schema.tables
  WHERE table_schema = 'argus_${STUDENT_ID}_silver'
UNION ALL
SELECT 'gold',   table_name FROM information_schema.tables
  WHERE table_schema = 'argus_${STUDENT_ID}_gold'
ORDER BY layer, table_name;
```

**Expected output**: exactly 18 rows. The table names should be:

- **bronze** (6): `external_feeds`, `instrument_cdc`, `legacy_alerts`, `member_cdc`, `orders_raw`, `trades_raw`
- **silver** (4): `executed_trades`, `instrument_master`, `member_master`, `order_events`
- **gold** (8): `alert_candidates`, `confirmed_manipulation_cases`, `consent_audit`, `cross_product_features`, `member_risk_scores`, `member_temporal_features`, `order_book_snapshots`, `surveillance_kpis`

If you see fewer tables in any layer, re-run the failing DDL: `envsubst < sql/<layer>_ddl.sql | hive -f -`.

### Check 4 — `consent_audit` has `history.expire.enabled = false`

This is the single most important Day 1 check. If you skip it, the COMPLIANCE GATE in Module 7 (CP-19) will fail and you fail the capstone overall — no recovery possible at that late stage.

```sql
DESCRIBE FORMATTED argus_${STUDENT_ID}_gold.consent_audit;
```

Scroll through the output until you find the **Table Parameters** section. Inside it, look for:

```
history.expire.enabled    false
```

> ⚠️ **CRITICAL — DO NOT SKIP:** If you don't see this property, **stop and fix it before doing anything else**. Re-run `envsubst < sql/gold_ddl.sql | hive -f -`. The gold DDL sets this property at the bottom of the `consent_audit` definition.
>
> The reason: Module 7's CP-19 checkpoint will run a query against this table from a past snapshot ID — `SELECT * FROM consent_audit FOR SYSTEM_VERSION AS OF <past_id>`. If Iceberg has been allowed to garbage-collect old metadata, the past snapshot is gone and the query fails. CP-19 is the COMPLIANCE GATE for the whole capstone. Don't risk it.

If the property *is* set, you've cleared the most important Day 1 hurdle. Take a moment to feel good about that.

---

## Step 5 — Generate the synthetic data

Now you generate the dataset that the rest of the capstone uses. The generator script is `data/generate_data.py`. Read its module docstring if you're curious — it lays out exactly what gets generated and the planted-case strategy.

```bash
python data/generate_data.py --seed 42 --scale 0.05 --out data/generated/
```

**What the arguments mean:**

- `--seed 42` — the random seed. Same seed = identical output every run, across every student. Don't change this without a reason.
- `--scale 0.05` — generate at 5% of full production scale. Full scale is ~50M order events; 5% is ~2.5M, which is plenty to exercise every code path while finishing in minutes rather than hours. (Module 1's CP-03 throughput test wants full scale; you can regenerate then.)
- `--out data/generated/` — where to write the files. The directory is in `.gitignore`, so the data won't accidentally get committed to your repo.

**Expected runtime**: 5–10 minutes on a normal machine. The generator prints progress as it goes — file names, row counts, planted-case confirmations.

**Expected output**: 14 files in `data/generated/`. Three of them are gzipped JSONL (`*.jsonl.gz` — these are the high-volume event streams), the other 11 are plain CSV.

```bash
ls -la data/generated/
```

You should see roughly:

```
-rw-r--r--  members.csv                       (~50 KB)
-rw-r--r--  traders.csv                       (~120 KB)
-rw-r--r--  investors.csv                     (~3 MB)
-rw-r--r--  instruments.csv                   (~80 KB)
-rw-r--r--  corporate_actions.csv             (~30 KB)
-rw-r--r--  surveillance_state.csv            (~5 KB)
-rw-r--r--  consent_records.csv               (~2 MB)
-rw-r--r--  legacy_alerts_history.csv         (~9 MB)
-rw-r--r--  sebi_actions.csv                  (~10 KB)
-rw-r--r--  news_headlines.csv                (~150 KB)
-rw-r--r--  compliance_test_cases.csv         (~5 KB)
-rw-r--r--  orders_synthetic.jsonl.gz         (~6 MB)
-rw-r--r--  trades_synthetic.jsonl.gz         (~500 KB)
-rw-r--r--  bbo_synthetic.jsonl.gz            (~1 MB)
```

Sizes vary slightly with the seed; what matters is that all 14 files exist.

### Quick sanity check — confirm the planted cases are there

```bash
wc -l data/generated/compliance_test_cases.csv
```

**Expected**: `24 data/generated/compliance_test_cases.csv` (1 header line + 23 planted cases).

Look at a few:

```bash
head -5 data/generated/compliance_test_cases.csv
```

You'll see the indices, pattern types (LAYERING, SPOOFING, MARKING_THE_CLOSE, etc.), and the member firm IDs the cases use. This file is the "answer key" for downstream verification — if a Module 3 test says "Case 0 should fire an R-102 LAYERING alert by BNXM-0042," it's reading this file.

Confirm a planted manipulation event landed in the order stream:

```bash
zcat data/generated/orders_synthetic.jsonl.gz | grep '"planted_case_idx": 0' | head -3
```

**Expected**: at least 3 lines of order events tagged with `"planted_case_idx": 0` and `"member_firm_id": "BNXM-0042"`. These are the layered orders for Case 0 — Module 3 will test that the rules engine fires on these.

---

## Step 6 — Bulk-load Bronze with FLOW-SIM (CP-01)

Now you'll feed the synthetic events into Kafka. The script is `src/ingest/replay_simulator.py` — nicknamed FLOW-SIM. It runs in two modes; today you only need `oneshot`.

```bash
python src/ingest/replay_simulator.py --mode oneshot \
    --data-dir data/generated/ \
    --bootstrap-servers ${KAFKA_BROKERS}
```

**What happens**: FLOW-SIM reads the three JSONL streaming files (`orders_synthetic.jsonl.gz`, `trades_synthetic.jsonl.gz`, `bbo_synthetic.jsonl.gz`) and produces every event as a Kafka message to the matching topic (`argus.${STUDENT_ID}.orders.v1`, `argus.${STUDENT_ID}.trades.v1`, `argus.${STUDENT_ID}.bbo.v1`). It's "as fast as Kafka will accept" — typically 30K–60K events per second on a healthy CDF cluster.

**Expected runtime**: 5–15 minutes. You'll see progress lines like `[oneshot] orders_synthetic.jsonl.gz → argus.${STUDENT_ID}.orders.v1` followed by row counts.

**Expected output at the end**: a summary line showing total events, similar to:

```
==> oneshot complete: 3,025,000 events in 75.3s (40,200 ev/s)
```

(Numbers will differ for you; the order of magnitude is what matters.)

### Verify the load (CP-01 check)

In SMM, navigate to your CDF cluster. For each of the three streaming topics, check the cumulative message count:

```bash
# CLI alternative if you can't access SMM:
for topic in argus.${STUDENT_ID}.orders.v1 argus.${STUDENT_ID}.trades.v1 argus.${STUDENT_ID}.bbo.v1; do
    n=$(kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server ${KAFKA_BROKERS} --topic "$topic" --time -1 \
        | awk -F: '{sum += $3} END {print sum}')
    echo "$topic: $n messages"
done
```

**Expected output**:

```
argus.${STUDENT_ID}.orders.v1: ~2,500,000 messages
argus.${STUDENT_ID}.trades.v1: ~175,000 messages
argus.${STUDENT_ID}.bbo.v1: ~350,000 messages
```

(Within ±10% — the synthetic generator's exact counts vary slightly with the seed.)

**The other 5 topics will be empty** — that's correct. Topics like `argus.${STUDENT_ID}.member.cdc.v1` and `argus.${STUDENT_ID}.regulator.feed.v1` get populated by NiFi flows in Module 1, not by the Day 1 bulk-load. Empty topics in SMM today is **expected**, not a problem.

---

## Checkpoint pass conditions

### CP-00 — Environment ready

All four checks above pass:
- ✅ Check 1 — S3 bucket with seven prefixes and AES256 encryption
- ✅ Check 2 — All 11 Kafka topics with correct partition counts
- ✅ Check 3 — All 19 Iceberg tables across the three schemas
- ✅ Check 4 — `consent_audit` has `history.expire.enabled = false` ⚠️ CRITICAL

### CP-01 — Bulk data loaded

- ✅ The three streaming topics each have non-zero message counts within ~10% of expected
- ✅ Partition distribution in SMM is roughly even across each topic's partitions (max-to-min ratio under 10×)
- ✅ The compliance-test CSV has 24 lines (header + 23 cases)

If both pass, **Day 1 is complete**. You're ready for [Module 1](../docs/module-1-streaming-ingest.md).

---

## Common failure mode — "AWS credentials expired" mid-lab

**Symptom**: an early step works, then a later step (often DDL execution or the bulk-load) fails with `An error occurred (ExpiredToken)` or `Unable to locate credentials`.

**Diagnosis**: AWS sessions in CDP environments often expire after 1–4 hours. If you started Day 1 in the morning and got distracted by lunch, your session likely lapsed.

**Fix**:

```bash
# Confirm the session is alive:
aws sts get-caller-identity
# Should print your AWS account + ARN.

# If it errors, refresh:
aws sso login          # or whichever auth flow your CDP uses
```

After refreshing, re-run whichever step was failing. Everything is idempotent — partial state is safe.

---

## Common failure mode — "Hive can't see my Iceberg tables"

**Symptom**: `SHOW TABLES IN argus_${STUDENT_ID}_bronze` returns "Database does not exist" even though you ran `envsubst < sql/bronze_ddl.sql | hive -f -` and saw `OK` outputs.

**Diagnosis**: Hive and Impala maintain separate metadata caches in CDP. If you created the tables via Hive but are querying via Impala, Impala may not have refreshed its catalog yet.

**Fix**: in `impala-shell` or Hue, run:

```sql
INVALIDATE METADATA;
```

Then retry your query. This forces Impala to re-read the catalog from Hive Metastore.

---

## Wrap-up

When you've cleared all the checks, you've set up the platform's plumbing. No surveillance logic runs yet — that starts in [Module 1 — CDF Streaming Ingest at Scale](../docs/module-1-streaming-ingest.md). Day 2 is where things get interesting: NiFi flows handling source-system messiness, Spark Structured Streaming jobs writing Bronze tables in real time, and the throughput test that proves the platform survives F&O expiry day.

Take a break before starting Module 1. You earned it.
