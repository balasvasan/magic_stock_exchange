# Module 1 Primer — Read This Before Lab 1.1

> 📊 **Visual reference**: [Module 1 streaming + real-time detection pipeline](../assets/diagrams/02_module1_streaming.md) ([SVG](../assets/diagrams/02_module1_streaming.svg))

> 👋 **New to streaming systems, NiFi, or Flink?** This primer is for you. It explains what each piece of Module 1's technology *is*, why it exists in ARGUS, and how the three streaming engines work together. About 20 minutes — well worth the time before you start Lab 1.1.

This is a **primer**, not a procedure. Nothing on this page asks you to type or run anything. The actual hands-on work is in `labs/lab-1-1-flow-sim.md` through `labs/lab-1-5-throughput-comparison.md`. Read this first; do those next.

If a section already feels familiar, skim the headings and skip ahead. Nothing here is required reading if you've used streaming systems before — but if you've only ever written batch SQL, please read the whole thing.

## The big picture in one paragraph

Module 1 is where ARGUS becomes real-time. Day 1 prepared the kitchen — empty tables, ready Kafka topics, synthetic data on disk. Today, **events start flowing**. NiFi taps the source systems and pushes events into Kafka. Three different engines read those Kafka topics at the same time: Spark Structured Streaming writes the canonical record to Iceberg Bronze tables, PyFlink CEP detects manipulation patterns in under a second, and SQL Stream Builder lets analysts write streaming SQL through a UI. By the end of Module 1, the platform sustains 150,000 events per second peak load (the F&O expiry-day worst case), planted spoofing cases get flagged within 800 milliseconds, and ARG-1 — the deficiency that triggered SEBI's Show Cause Notice — is provably closed. **Module 1 doesn't do any feature engineering or ML scoring**; that starts in Module 2. Today's job is just to ingest cleanly and detect patterns fast.

## The technologies you'll meet in Module 1

Six things matter today. Here's what each is, in plain terms.

### Apache NiFi

A **data-flow engine**. Imagine NiFi as a wiring diagram for moving data from messy outside sources into clean inside topics. You drag boxes (called *processors*) onto a canvas and connect them with arrows. Each processor does one thing — read from an SFTP server, parse a CSV row, validate JSON against a schema, route bad records to a different output, push to Kafka. NiFi handles the operational hard parts that custom code has to reinvent every time: backpressure (slow down upstream if downstream gets clogged), retry-on-failure, prioritized queues, and provenance tracking ("which exact source byte became which output record?").

In Module 1 you'll import four pre-built NiFi flows from `src/ingest/nifi_flows/`. You won't author them from scratch — designing NiFi flows is a multi-day topic on its own. Your job is to **import them, configure them with your `STUDENT_ID`, and start them**. Once started, they sit there pulling events from MSE's source systems (which are simulated by the synthetic data generator) and pushing them into Kafka topics.

The four flows match the four ingest paths:
- `flow_01_orders` — taps the TARANG match-engine multicast feed → `argus.${STUDENT_ID}.orders.v1`
- `flow_02_trades` — pulls executed trades from NIPATAN → `argus.${STUDENT_ID}.trades.v1`
- `flow_03_kavach_cdc` — consumes CDC events from the KAVACH KYC database → `argus.${STUDENT_ID}.member.cdc.v1` (PII-bearing, encrypted)
- `flow_04_external_feeds` — merges BBO, SEBI, and news feeds → multiple topics

### Apache Kafka (refresher)

You met Kafka on Day 1: the **message bus**, conveyor belt, topics with partitions. In Module 1 you'll start *using* it as a real-time pipeline, not just as a place to bulk-load test data. Two things become important:

- **Partitions = parallelism**. If a topic has 48 partitions, up to 48 consumers can read in parallel. Spark, Flink, and SSB each run multiple parallel readers; the partition count caps how many can usefully run at once.
- **Consumer groups** are how multiple jobs share a topic without stepping on each other. Each job declares a unique consumer-group ID (e.g., `argus.${STUDENT_ID}.spark_bronze_orders`). Kafka tracks the read position separately per group, so Spark and Flink can both read the same topic without interfering.

You don't have to configure consumer groups manually — the source code in `src/ingest/` does it for you. But you'll see them in monitoring tools and need to recognize them.

### Apache Spark Structured Streaming

Spark you met on Day 1 (sort of — through the synthetic data generator). In Module 1 you'll meet **Spark Structured Streaming**, which is Spark's answer to "what if I want this query to run continuously instead of once?"

The mental model: write a query as if the input table were finite. Spark transparently re-runs it every N seconds (or every N events) against new arrivals only, and appends results to the output. The trigger interval — `processingTime='10 seconds'` in our jobs — sets how often Spark wakes up to process the next micro-batch.

Why micro-batch instead of true event-by-event? Because Spark is a JVM batch engine that learned to do streams. The micro-batch overhead is ~5–15 seconds of latency, which is fine when "canonical persistence" is the goal but **too slow for real-time pattern detection**. That's why ARGUS doesn't use Spark for the real-time alerts — it uses Spark for the durable record, and Flink for the fast detection.

In Module 1 you deploy four Spark Streaming jobs (`JOB-01` through `JOB-04`) that each consume one Kafka topic and write to one Bronze Iceberg table. They're small (~50 lines each) because the actual work — schema validation, partition routing, exactly-once writes — is mostly handled by Spark and Iceberg under the hood.

### Apache Flink + PyFlink CEP

Flink is a **streaming engine that's natively streaming**, not a batch engine adapted to streams. That's not a religious distinction — it has practical consequences:

- Flink processes events one at a time (or in small windows), not in 10-second batches. Latency from event arrival to job output can be < 100ms.
- Flink keeps **state** per key (e.g., per `instrument_code`) in memory + RocksDB, with periodic checkpoints to S3 for fault recovery. State lookups are sub-millisecond.
- Flink's **CEP library** (Complex Event Processing) lets you express patterns like "5+ orders placed at the same price level within 200ms, then all cancelled within 200ms more, on the same side" — declaratively, in a few lines of Python (PyFlink) or Java code.

In Module 1 you'll deploy `JOB-10` (PyFlink CEP) which encodes two manipulation patterns:
- **R-101 SPOOFING**: large order placed and cancelled before any execution, repeatedly
- **R-102 LAYERING**: multiple stacked orders at successive price levels, all cancelled when an opposite-side fill happens

When Flink detects either pattern, it emits an alert event to a dedicated Kafka topic `argus.${STUDENT_ID}.realtime_alerts.v1`. **Latency from the manipulative order arriving to the alert being emitted is sub-second** — typically 200–500 ms, with p99 < 800ms (that's the CP-02b pass condition).

Why does sub-second latency matter? Because if a spoofer's strategy completes in 800ms, an analyst notified at minute 15 has nothing to act on. Sub-second alerts mean the surveillance team can intervene while the manipulation is still in progress: pull the trader's quote, halt the instrument, freeze the firm's order entry. That intervention capability — which the legacy platform never had — is half of why ARG-1 was named in the Show Cause Notice.

### SQL Stream Builder (SSB)

SSB is a **UI for writing streaming SQL**. Under the hood it generates Flink jobs, but you don't see Flink — you see a SQL editor in a web browser, hit "Deploy", and your SQL becomes a running streaming job.

Why does this exist alongside PyFlink? Because **the surveillance analysts who write detection rules are not Java engineers**. They know SQL. They know what manipulation looks like. They should not have to file a ticket with the platform team and wait three weeks every time they want to try a new pattern.

In Module 1 you'll deploy `JOB-11` (SSB SQL) which encodes one pattern:
- **R-104 CROSS-PRODUCT IMBALANCE**: large directional volume in single-stock futures while the cash market shows the opposite imbalance — the Jane Street pattern

The SQL is ~80 lines. It uses HOP windows (60-second sliding window, slid every 10 seconds) and joins against a reference table (`instrument_master_${SID}`) to map futures contracts to their underlying cash equity. Latency is ~30 seconds — slower than PyFlink because SSB SQL works in window batches, not event-by-event. But 30 seconds is still vastly faster than the 30-minute batch of Module 3.

The architectural point: **the right engine for the job depends on who writes the rule and how fast it has to fire**. Engineering writes PyFlink for sub-second deterministic patterns. Analysts write SSB SQL for slower, iterative patterns they want to deploy themselves. Both are right for their use case.

### Iceberg Bronze tables (refresher)

You created the empty Bronze tables on Day 1. In Module 1 they start filling up. Each Spark Streaming job writes to one Bronze table, partitioned by `ingest_date`, in MOR (merge-on-read) ORC format.

Two things to know that the Day 1 primer didn't cover:

- **MOR is good for ingest, slower for reads.** New rows append as small data files plus delete files. Reads have to merge those at query time. That's fine for Bronze where reads are infrequent. The downstream Silver/Gold tables use COW (copy-on-write) Parquet because they're read many times.
- **Partitioning matters for downstream cost.** Bronze partitions by `ingest_date` (a daily column auto-derived from the event arrival timestamp), so a query that says `WHERE ingest_date = '2026-05-09'` only scans that day's files. Without partitioning, every query reads the whole history. Iceberg handles partitioning declaratively — you wrote the `PARTITIONED BY (ingest_date)` clause on Day 1; Spark and Iceberg cooperate to route incoming rows to the right partition file automatically.

## The three-engine architecture, briefly

This is the single most important concept in Module 1, and it confuses everyone the first time.

The legacy MSE platform had **one** detection engine (the vendor-supplied CEP system). It was overloaded on F&O expiry days — 150K events/sec exceeded its 40K design capacity — and any rule the surveillance team wanted to add became a ticket to engineering, weeks of waiting, and a fragile deployment. ARGUS replaces that with **three** engines, each specialized:

| Engine | Job ID | Latency | Owns | Who writes the rules |
|---|---|---|---|---|
| Spark Structured Streaming | JOB-01..04, JOB-12 | ~10 s trigger | Canonical persistence (the durable record of what happened) | Engineering (jobs change rarely) |
| **PyFlink CEP** | JOB-10 | **< 800 ms p99** | Sub-second pattern detection (engineered patterns) | Engineering (high-quality, code-reviewed) |
| **SQL Stream Builder** | JOB-11 | ~30 s | Analyst-driven streaming SQL | Surveillance analysts (deploy via UI, no engineering) |

All three read the same Kafka topics. They don't compete; they specialize. A planted spoofing event lands in:
- **Bronze table** (Spark, durable record) within 10 seconds — this is what ML training and regulators audit
- **`realtime_alerts.v1` Kafka topic + `realtime_alert_stream` Iceberg table** (Flink, sub-second alert) within 800 ms — this is what triggers the analyst notification
- **Same `realtime_alert_stream` table** but a different `source_engine` row (SSB, ~30 seconds) — for the Jane Street cross-product pattern

The take-away: **batch versus streaming is a false dichotomy**. The right answer is *multiple* specialized consumers reading a shared event log. That's what a modern data platform looks like, and that's what ARGUS demonstrates.

## What Module 1 closes — ARG-1

ARG-1 has two halves:

1. **Throughput**: legacy platform saturated at 40K ev/s, peak demand is 150K ev/s. Module 1 demonstrates the platform sustains 150K for 10 minutes (CP-03).
2. **Detection latency**: legacy platform's pattern detection ran in batches every ~30 minutes, useless for real-time intervention. Module 1 demonstrates planted Case 0 (spoofing) is detected by PyFlink CEP in p99 < 800 ms (CP-02b), and planted Case 2 (Jane Street cross-product) is detected by SSB SQL within 60 seconds (CP-04b).

Both halves close in Module 1 by Day 4 morning. Module 2 starts Day 4 afternoon and tackles ARG-2 (temporal feature engineering).

## What Module 1 sets up — the lab-by-lab map

Five labs, in order:

| Lab | What you do | Checkpoint | Time budget |
|---|---|---|---|
| 1.1 — FLOW-SIM bulk-load | Push synthetic data into Kafka topics with the replay simulator | CP-02 (NiFi/Kafka ingest healthy) | ~30 min |
| 1.2 — Bronze ingest deployment | Import 4 NiFi flows; deploy 4 Spark Streaming jobs; watch Bronze fill up | CP-04 (Bronze populated) | ~60 min |
| 1.3 — Throughput at peak | Run FLOW-SIM continuous at 150K ev/s for 10 min; confirm no DLQ growth | CP-03 (150K sustained) | ~45 min |
| 1.4 — PyFlink CEP | Deploy JOB-10 to Flink; verify Case 0 detected p99 < 800 ms | CP-02b (Flink real-time) | ~75 min |
| 1.5 — SSB SQL + comparison | Deploy JOB-11 via SSB UI; verify Case 2 detected < 60 s; run 3-engine latency comparison | CP-04b (SSB real-time) | ~75 min |

Total Module 1 wall-clock: about 5–6 hours of hands-on work, spread across Days 2–4 morning. Reading time, troubleshooting, and instructor Q&A doubles that.

## Things you'll find confusing the first time, and what to do about them

### "What's the difference between Kafka topic partitions and Spark partitions?"

They're different concepts that share a name. **Kafka partitions** are how a topic is sharded for parallel writes/reads — set at topic creation time, fixed cardinality (48, 24, etc). **Spark partitions** are how Spark splits a job's work across executors — dynamic, based on input size and configuration. When a Spark Streaming job reads a Kafka topic, by default Spark creates one Spark partition per Kafka partition (so a 48-partition topic produces a 48-task Spark job). You can repartition after reading, but that costs a shuffle.

### "Why is the Spark Streaming job lagging by 30 seconds when the trigger is 10 seconds?"

Lag is **end-to-end latency**, trigger interval is **how often Spark wakes up**. Even with a 10-second trigger, the work to read the batch + parse + validate + write to Iceberg takes time. If batch processing time exceeds trigger interval, Spark's queue lengthens — that's lag. You'll watch this in Lab 1.3 and learn how to scale executors to keep lag bounded.

### "PyFlink CEP looks like Java but it's Python — what's actually running?"

PyFlink translates your Python code into Flink's native JVM job graph. The Python is glue; the actual streaming runtime is Java/Scala. That's why some operations have weird names (e.g., `KeyedStream`) and why error messages sometimes show Java stack traces. It's normal. The important Python is just the pattern definition (`Pattern.begin(...).next(...).times(N)`); everything else is mechanical.

### "Why do I have to deploy SSB through a UI? Can't I script it?"

You can — SSB has a REST API. But the *point* of SSB is that analysts can deploy without scripts. The UI is the canonical interface. Lab 1.5 walks you through the UI; if you'd rather use the API, the SSB README in `src/ingest/ssb/` covers that path too.

### "I'm seeing both `argus.${STUDENT_ID}.orders.v1` AND `argus.s001.orders.v1` in monitoring — which is mine?"

The literal string `${STUDENT_ID}` is a placeholder. In every command you actually run, that gets substituted with your student ID. So if your `STUDENT_ID=s001`, the topic is `argus.s001.orders.v1`. Your instructor will tell you your specific ID. The placeholder form (`${STUDENT_ID}`) appears in docs because docs serve every student.

### "Are NiFi flows committed to the repo, or do I need to draw them?"

The four flows live as JSON files in `src/ingest/nifi_flows/`. They're stubs — small enough to read in 5 minutes, complete enough to be importable into NiFi. You import them in Lab 1.2 Step 1; you don't draw anything. Some instructors provide a more complete production-style flow bundle separately; if so, use those instead.

## Success at end of Module 1

By the time you finish Lab 1.5, you should be able to:

- Explain (in your own words) why ARGUS uses three streaming engines instead of one
- Read any Spark Structured Streaming job in `src/ingest/job_0?_*.py` and predict what it does without running it
- Read the PyFlink CEP pattern in `src/ingest/flink_cep/job_10_*.py` and explain what events would match
- Deploy a new SSB SQL job from scratch (using the UI you learned in Lab 1.5)
- Diagnose a Bronze ingest failure by checking, in order: NiFi flow status → Kafka topic offsets → Spark job logs → Iceberg snapshot history
- Sustain 150K events/sec on the platform for ≥ 10 minutes with DLQ growth < 0.01%

If any of those feel impossible right now, that's expected — that's why we have the labs. By Day 4 morning all six should feel routine.

## What's NOT in Module 1

Module 1 is ingest + real-time detection only. **It does not do**:

- Feature engineering on the events (that's Module 3)
- Identity resolution across brokers (Module 2)
- Order book reconstruction (Module 2)
- ML scoring of alerts (Module 5)
- Suspicious Transaction Report drafts (Module 6)
- Atlas tagging or Ranger policy enforcement (Module 7)

If you find yourself thinking "but how do I rank this alert?" or "but what about the consent withdrawal case?" — write it down for later. Today, just focus on landing events cleanly and detecting patterns fast.

---

When you're ready, head to [Lab 1.1 — FLOW-SIM Bulk Load](../labs/lab-1-1-flow-sim.md). Allow about 30 minutes if everything works first try.
