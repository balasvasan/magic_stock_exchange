# Day 1 Primer — Read This Before You Start

> 📊 **Visual reference**: [Day 1 setup activity diagram](../assets/diagrams/01_day1_setup.md) ([SVG](../assets/diagrams/01_day1_setup.svg))

> 👋 **New to Cloudera, Iceberg, Kafka, or Spark?** This primer is for you. It explains what each piece of technology *is*, why it exists in the ARGUS architecture, and what Day 1 sets up. Reading this first will save you confusion later. About 15 minutes.

This is a **primer**, not a procedure. There's nothing to type or run on this page. The actual setup steps live in [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md). Read this first; do that next.

## The big picture in one paragraph

Magic Street Exchange (MSE) needs to detect stock-market manipulation in real time. To do that, the platform has to ingest billions of trading events per day, transform them into structured features, train a machine-learning model that ranks suspicious cases, generate written reports for regulators, and prove to the privacy regulator that personal data is handled correctly. The Cloudera Data Platform — usually written CDP — is a bundle of open-source tools that together do all of those things on a single cluster. ARGUS is the surveillance system you'll build on top of it. **Day 1's job is to set up a clean kitchen before any cooking happens**: create cloud storage, declare empty database tables, set up a synthetic dataset to play with. None of the surveillance logic runs today. That starts in Module 1.

## The technologies you'll meet today

You'll see references to seven things on Day 1. Here's what each one is, in plain English. Don't try to memorize — you'll meet them again in context.

### Apache Kafka

A **message bus**. Imagine a conveyor belt that carries small data items (called "events" or "messages") from senders to receivers. A "sender" might be the trading system emitting one event for every stock order placed; a "receiver" is a job that reads those events and writes them somewhere durable. Kafka organizes events into named streams called **topics** — `argus.${STUDENT_ID}.orders.v1` is a topic, so is `argus.${STUDENT_ID}.trades.v1`. Each topic is split into **partitions** (think of partitions as parallel lanes on the conveyor belt) so multiple readers can consume in parallel without stepping on each other.

You'll create 11 Kafka topics (9 production + 2 DLQ) today with specific partition counts. The counts matter — `argus.${STUDENT_ID}.orders.v1` has 48 partitions because it carries the highest-volume stream and needs the most parallelism.

### Apache Iceberg

A **table format**. Iceberg lets you treat files in cloud storage (like S3) as if they were database tables. You write SQL like `SELECT * FROM argus_${STUDENT_ID}_bronze.orders_raw` and Iceberg figures out which files to open. It also gives you superpowers a normal database doesn't: **time-travel** (query the table as it existed at a past moment), **schema evolution** (rename columns without rewriting all your data), and **ACID transactions** (consistent reads while writes happen).

You'll create 19 Iceberg tables today, all empty. Three "layers" — Bronze, Silver, Gold — each with a different purpose:

- **Bronze**: raw data exactly as it arrived, no cleanup. (6 tables today)
- **Silver**: cleaned, joined, normalized. (4 tables today)
- **Gold**: analytics-ready tables built for specific questions. (8 tables today)

This Bronze→Silver→Gold pattern is industry-standard in data engineering and you'll see it everywhere.

### Apache Spark

A **distributed compute engine**. When you have so much data that a single computer can't process it, Spark splits the work across many computers (called "executors") and aggregates the results. You write Python or SQL code that *looks* like it's processing one DataFrame, and Spark transparently runs it in parallel across the cluster. In ARGUS, every transformation from Bronze→Silver and Silver→Gold is a Spark job (JOB-01 through JOB-09).

You won't run Spark on Day 1. You'll meet it on Day 2.

### Apache NiFi (and the broader CDF — Cloudera DataFlow)

A **visual flow tool** for data ingestion. NiFi specializes in the "messy world" outside the data platform: tapping a multicast feed, polling an SFTP server every minute, calling a REST API, validating that incoming records have the right structure, routing failures to a dead-letter queue. You build flows visually by dragging boxes called "processors" onto a canvas and connecting them with arrows.

You won't touch NiFi on Day 1. Module 1 starts with NiFi.

### Apache Impala (and the broader CDW — Cloudera Data Warehouse)

A **fast SQL query engine**. Once you have data in Iceberg tables, you query it with Impala. It's tuned for interactive analytics — you type a query, get results in seconds. Day 1 uses Impala (or Hive — they're similar enough that the same DDL works in both) just to create empty tables. The interesting Impala work — defining views, applying access policies — happens in Module 4.

### Apache Atlas + Apache Ranger (together: SDX — Shared Data Experience)

The **governance layer**. Atlas tracks metadata: which columns hold personal data, where each Gold column came from (lineage), what classification tags apply. Ranger enforces access policies: who can read which rows, who sees which columns redacted, who's allowed to elevate during an active investigation. They work together — Atlas decides "what kind of data is this?", Ranger decides "what's allowed for this user given that classification?".

You won't touch Atlas or Ranger on Day 1. Module 7 is dedicated to them.

### AWS S3

**Cloud object storage**. Files live in buckets; buckets live in regions. Everything Iceberg writes — the actual Parquet/ORC data files plus the metadata files that organize them — lives in your S3 bucket. Same with Spark's checkpoint state for streaming jobs, MLflow's model artifacts, and Milvus's vector indexes. S3 is the single backing store under everything.

Your instructor pre-provisioned a bucket for you with the seven required prefixes (`bronze/`, `silver/`, `gold/`, `landing/`, `checkpoints/`, `mlflow/`, `milvus/`) already inside. They'll give you the full bucket name on Day 1 — you just `export BUCKET_NAME=...` and use it.

## The four-layer architecture, briefly

You'll see this diagram referenced everywhere. It's worth 30 seconds to understand top-down:

1. **Govern** — Atlas + Ranger. Cuts horizontally across all the other layers; nothing in those layers can dodge governance.
2. **Serve** — what humans interact with. Impala for SQL, the surveillance UI, the ML model serving scored alerts.
3. **Process** — Spark batch + streaming jobs. The data engineering work happens here.
4. **Ingest** — NiFi + Kafka. The boundary between the messy outside world and the tidy inside.

Data flows **upward**: events arrive at Ingest, get processed and transformed in Process, get served to humans at the Serve layer. Govern watches all three.

The full architecture diagram is in [`docs/01_architecture.md`](01_architecture.md). On Day 1 you're touching the *plumbing* of layers 1 and 2 — the topics in Ingest, the empty tables in Process — but no actual data flow yet.

## What Day 1 sets up, and why

Day 1 has five steps. Here's why each step exists:

### 1. Cloud storage (S3 bucket)

Without a bucket, there's nowhere for Iceberg to write files. The bucket and its seven prefixes (`bronze/`, `silver/`, `gold/`, `landing/`, `checkpoints/`, `mlflow/`, `milvus/`) are pre-provisioned by your instructor for the cohort, so you start Day 1 by *verifying* the bucket is reachable rather than creating it. The `landing/` prefix is for files dropped by the synthetic generator before they're ingested; `checkpoints/` is where Spark Streaming saves "I've processed up to here" state so jobs can resume after a restart.

The bucket is yours alone — your work doesn't share storage with the rest of the cohort. The region is `ap-south-1` (Mumbai) because the scenario assumes data residency in India under DPDP Act 2023.

### 1a. Per-student namespacing on the shared CDP cluster

Your cohort runs on a **shared CDP cluster** (16 students + 4 instructors = 20 users), so resources that students would otherwise collide on are namespaced per-student. Specifically:

- **Kafka topics** named `argus.${STUDENT_ID}.orders.v1` instead of just `argus.orders.v1`
- **Iceberg schemas** named `argus_${STUDENT_ID}_bronze`, `argus_${STUDENT_ID}_silver`, `argus_${STUDENT_ID}_gold`, `argus_${STUDENT_ID}_views`
- **MLflow experiment** named `argus_${STUDENT_ID}_alert_ranking_v1`
- **MLflow registered model** named `argus_${STUDENT_ID}_alert_ranker`
- **Atlas classification tags** named `PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, etc.
- **Milvus vector collection** named `argus_${STUDENT_ID}_str_corpus`

Every code file in the repo reads the names through a single helper (`src/common/naming.py`) — you never type the namespaced names by hand. Just `export STUDENT_ID=<your-id>` once at the start of your session and everything resolves correctly.

### 2. Kafka topics

Each topic is the destination for one source-system stream. Creating them upfront with the right partition counts is critical because **partition counts can't be reduced later** — only increased. If you create `argus.${STUDENT_ID}.orders.v1` with 6 partitions today and need 48 in Module 1, you can grow it; but if you create it with 48 and only need 6, that's a structural over-allocation. The PRD has done this sizing for you. Just trust the table.

### 3. Iceberg DDL — empty tables

The DDL files (`bronze_ddl.sql`, `silver_ddl.sql`, `gold_ddl.sql`) declare the *shape* of the tables — column names, types, partitioning strategies, table format properties — but don't put any data in them. Think of it like designing the spreadsheet headers before anyone has typed a row.

One DDL property matters more than all the others on Day 1: **`history.expire.enabled = false` on `argus_${STUDENT_ID}_gold.consent_audit`**. This tells Iceberg "never garbage-collect old metadata for this table." Why? Because Module 7's compliance gate checkpoint depends on being able to query the table at past points in time, years from now. If you let history expire, the audit trail evaporates and the regulator can't verify a past erasure happened. We'll come back to this — Day 1's job is just to make sure the property is set.

### 4. Synthetic data generation

The capstone uses fictional data, generated locally by a Python script. It produces 14 files representing a realistic mix of the eight source systems: members, traders, investors, instruments, ~2.5M order events, ~175K executed trades, etc. Critically, the generator plants **23 specific test cases at fixed indices 0–22** that downstream modules check for. For example, planted Case 0 is a layering manipulation by member firm `BNXM-0042`; Module 3's tests verify that JOB-08's rules engine fires an alert on it. Without the planted cases, you can't tell whether your pipeline is detecting things correctly — you'd be guessing.

The generator uses a fixed random seed (`--seed 42`) so everyone in the cohort gets identical data. Same seed, same output, every time.

### 5. Bulk-load into Kafka

The synthetic generator writes files to disk. To make them flow through the platform, you need to feed those files into Kafka. The `replay_simulator.py` script (nicknamed FLOW-SIM) does this. Today you'll run it in **`oneshot`** mode, which fires every event into Kafka as fast as Kafka will accept — perfect for getting data into the platform once. There's a second mode (`continuous`) that we'll use in Module 1 to simulate live traffic.

## Success at end of Day 1

When Day 1 is done you'll have:

- ✅ Verified your assigned S3 bucket (instructor-provisioned, with seven prefixes already inside)
- ✅ 10 per-student Kafka topics totaling 108+ partitions, all empty
- ✅ 19 Iceberg tables in 4 per-student schemas (`argus_${STUDENT_ID}_bronze`, `_silver`, `_gold`, `_views`), all empty (9 in Gold including the new `realtime_alert_stream`)
- ✅ 14 synthetic data files in `data/generated/`
- ✅ Kafka topics now full of synthetic events (after the bulk-load)

The lab procedure runs through these in order, with **two checkpoints** (CP-00 verifying the platform setup, CP-01 verifying the bulk-load). Each checkpoint has explicit pass conditions and an expected output for every step.

## Things you'll find confusing the first time, and what to do about them

A heads-up on the four most common stumbles:

**"I don't know what my STUDENT_ID is."** Your instructor assigned one at the start of the cohort, usually a 4-character code like `s001` or your initials + a number like `bv01`. Ask if you don't have one. The capstone uses it in bucket names and S3 paths to keep your work isolated from your classmates.

**"The AWS CLI is asking for credentials."** If `aws s3 ls` fails with a credentials error, your AWS session has expired or wasn't set up. The CDP environment your instructor provisioned has built-in AWS credentials — usually you log into a workstation that has them pre-set, or you run `aws sso login` once at the start of the day. Ask your instructor; the answer is environment-specific.

**"The Kafka commands aren't found."** The `kafka-topics` CLI lives on the CDF cluster, not on your laptop. Either SSH into a CDF gateway node first, or use Streams Messaging Manager (SMM) — the web UI — instead. Both work; SMM is more visual and may be easier the first time.

**"I don't know what `${STUDENT_ID}` means in code blocks."** That's a shell variable. Lines like `kafka-topics --topic argus.${STUDENT_ID}.orders.v1` substitute your actual student ID at runtime. To make it work, set the variable once per terminal session: `export STUDENT_ID=s001` (using your real value). The labs remind you to do this. The bucket name works the same way via `${BUCKET_NAME}` — your instructor gives you the full name and you `export BUCKET_NAME=...` once.

## What's NOT on Day 1

To set expectations: there's no Spark, no NiFi, no machine learning, no Atlas, no Ranger today. Those all show up later. Day 1 is just the foundation. If you finish in 90 minutes feeling like "wait, that was easy" — that's the right reaction. Day 2 starts the real work.

---

When you're ready, head to [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md) for the actual procedure.
