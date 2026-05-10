# Module 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection

> 📊 **Visual reference**: [Module 1 streaming + real-time detection pipeline](../assets/diagrams/02_module1_streaming.md) ([SVG](../assets/diagrams/02_module1_streaming.svg))

> 👋 **New to NiFi, Spark Streaming, PyFlink CEP, or SSB SQL?** Read [`docs/module-1-primer.md`](module-1-primer.md) first. It explains what each piece of Module 1's technology *is* before you start using it. About 20 minutes — well worth the time.

> **Closes deficiency:** ARG-1 — peak ingest throughput AND peak detection latency
> **Day:** 2 (full) + Day 3 (full) + Day 4 morning = 3.5 days
> **Checkpoints:** CP-02, CP-02b, CP-03, CP-04, CP-04b
> **Weight:** 15% of capstone

## What's broken

The legacy MSE surveillance platform is a single-node CEP engine sized in 2017 for ~40,000 events per second. On F&O expiry Thursdays the matching engine emits ~150,000 events per second across cash equities, single-stock futures, and index options. The legacy engine pulls events serially from a JMS queue; when its input queue depth exceeds ~2 million events, it silently drops messages.

Worse, even when the legacy engine *does* keep up, its detection runs as a 30-minute batch — by the time a spoofing alert fires, the trader has already disconnected and the manipulator has cycled through three more attacks. SEBI's Q2 2025 inspection report cited 14 manipulative episodes that fell into these gaps — the firm responsible was eventually disgorged ₹47 crore in unlawful gains, none of it discovered by MSE.

The first thing the new platform must do is keep up. The second thing it must do is **detect within the time window where intervention matters**.

## What you build

A three-engine streaming architecture that addresses both halves of ARG-1:

1. **NiFi flows** route source events into 11 Kafka topics with schema validation and DLQ. Same pattern as a traditional pipeline — this is the boring, reliable part.

2. **Spark Structured Streaming** (JOB-01..04) handles the canonical persistence path: Kafka → Iceberg Bronze tables, exactly-once semantics, schema enforcement. This is what every later module reads from.

3. **PyFlink CEP** (JOB-10) detects R-101 SPOOFING and R-102 LAYERING patterns on the live order stream with **<800ms p99** latency. Same patterns that JOB-08 implements as 30-min batch in Module 3, but as event-driven CEP for sub-second detection.

4. **SQL Stream Builder** (JOB-11) implements R-104 cross-product imbalance (the Jane Street pattern) as **declarative streaming SQL**. No Python, no submit script — surveillance analysts deploy it themselves through SSB's web UI.

5. **Spark Structured Streaming** (JOB-12) persists real-time alerts from JOB-10 + JOB-11 back to `gold.realtime_alert_stream` so they're queryable from CDW alongside the batch alerts.

The architectural insight: **all three engines run in parallel on the same Kafka backbone**. They don't compete; they specialize. Spark owns "land + persist" because it's exactly-once and Iceberg-native. Flink CEP owns "detect with sub-second latency" because pattern matching with state is what Flink is built for. SSB owns "let analysts iterate without engineering" because declarative SQL on streams is what SSB exists to provide.

## Architecture

```
                              ┌──► JOB-01..04 ──► Bronze tables (canonical persistence)
                              │     CDE Spark
                              │     Structured Streaming
                              │
TARANG ──► NiFi (FLOW-01) ──► │
NIPATAN ─► NiFi (FLOW-02) ──► │
KAVACH ──► NiFi (FLOW-03) ──► │  Kafka  ──► JOB-10 ──► realtime_alerts.v1 ──► JOB-12 ──► gold.realtime_alert_stream
externals► NiFi (FLOW-04) ──► │  topics      PyFlink                          Spark
                              │              CEP                              Structured
                              │              (R-101, R-102)                   Streaming
                              │
                              └──► JOB-11 ──► realtime_alerts.v1 ──┘
                                    SSB SQL
                                    (R-104)
```

Three consumers reading the same source events, three different jobs:

- **Spark Structured Streaming** owns Bronze ingestion (existing JOB-01..04)
- **PyFlink CEP** owns sub-second pattern detection (JOB-10)
- **SSB SQL** owns analyst-driven declarative streaming (JOB-11)

Both real-time engines write to the same `argus.${STUDENT_ID}.realtime_alerts.v1` topic with `source_engine` discriminating; JOB-12 lands them into `gold.realtime_alert_stream` for cross-checking against the batch detections from JOB-08 in Module 3.

## CDP services used

- **Cloudera DataFlow (CDF) / Apache NiFi** — visual flow orchestration for the four ingest flows
- **Apache Kafka** — event backbone, 9 production + 2 DLQ topics, 126 partitions
- **Cloudera Data Engineering (CDE) / Apache Spark** — Structured Streaming jobs for Bronze ingest + JOB-12 alert persistence
- **Cloudera Streaming Analytics / Apache Flink (PyFlink)** — JOB-10 sub-second CEP pattern detection
- **SQL Stream Builder (SSB)** — JOB-11 declarative streaming SQL for analyst-driven patterns
- **Apache Iceberg** — Bronze + Gold realtime_alert_stream table format
- **Streams Messaging Manager (SMM)** — observability for Kafka topics + partition lag

## When to use which engine

| Concern | CDE/Spark batch | PyFlink CEP | SSB SQL |
|---|---|---|---|
| Detection latency | 30 min (JOB-08) | <800ms p99 | ~30s |
| Authored by | Engineering | Engineering | Surveillance analyst |
| Iterable without engineering involvement | No | No | **Yes** |
| Supports complex temporal patterns | Yes (full SQL) | Yes (CEP DSL) | Limited (window SQL only) |
| Catches multi-hour patterns (e.g. wash rings) | Yes | No (state TTL) | No (state TTL) |
| Replay-able from Iceberg snapshots | **Yes — auditable** | Stateful checkpoints | Stateful checkpoints |
| Used as canonical record for ML scoring | **Yes** | No | No |

This is not "newer = better." Each engine wins for specific concerns; production exchanges deploy all three. The lab teaches when to choose which.

## Source files

| File | Purpose | Lines |
|---|---|---:|
| [`src/ingest/replay_simulator.py`](../src/ingest/replay_simulator.py) | FLOW-SIM — oneshot bulk-load + continuous rate-limited replay | 265 |
| [`src/ingest/job_01_bronze_orders.py`](../src/ingest/job_01_bronze_orders.py) | TARANG → `argus_${STUDENT_ID}_bronze.orders_raw` | 103 |
| [`src/ingest/job_02_bronze_trades.py`](../src/ingest/job_02_bronze_trades.py) | NIPATAN → `argus_${STUDENT_ID}_bronze.trades_raw` | 100 |
| [`src/ingest/job_03_bronze_member_cdc.py`](../src/ingest/job_03_bronze_member_cdc.py) | KAVACH CDC → `argus_${STUDENT_ID}_bronze.member_cdc` (PII-bearing) | 95 |
| [`src/ingest/job_04_bronze_external_feeds.py`](../src/ingest/job_04_bronze_external_feeds.py) | SEBI + BBO + news + PRATEEK → 2 Bronze tables | 125 |
| [`src/ingest/nifi_flows/flow_*.json`](../src/ingest/nifi_flows/) | NiFi flow exports — multicast tap, Debezium, REST/SFTP polls | — |
| [`src/ingest/flink_cep/job_10_realtime_spoofing_layering.py`](../src/ingest/flink_cep/job_10_realtime_spoofing_layering.py) | **JOB-10 — PyFlink CEP for R-101 + R-102** | ~180 |
| [`src/ingest/flink_cep/README.md`](../src/ingest/flink_cep/README.md) | Flink submission instructions | — |
| [`src/ingest/ssb/job_11_cross_product_imbalance.sql`](../src/ingest/ssb/job_11_cross_product_imbalance.sql) | **JOB-11 — SSB SQL for R-104** | ~80 |
| [`src/ingest/ssb/README.md`](../src/ingest/ssb/README.md) | SSB UI deployment instructions | — |
| [`src/ingest/job_12_realtime_alert_persistence.py`](../src/ingest/job_12_realtime_alert_persistence.py) | **JOB-12 — Spark persistence of realtime alerts** | ~50 |

## Labs

| Lab | Focus | Day | Checkpoint |
|---|---|---|---|
| [Lab 1.1 — FLOW-SIM](../labs/lab-1-1-flow-sim.md) | FLOW-SIM oneshot bulk load | Day 2 morning | CP-02 |
| [Lab 1.2 — Bronze ingest jobs](../labs/lab-1-2-bronze-ingest.md) | NiFi flows + JOB-01..04 Spark Structured Streaming | Day 2 afternoon | CP-04 |
| [Lab 1.3 — Throughput at peak](../labs/lab-1-3-throughput-test.md) | 150K ev/s sustained 10 min | Day 4 morning | CP-03 |
| [**Lab 1.4 — PyFlink CEP**](../labs/lab-1-4-flink-cep.md) | **JOB-10 sub-second R-101 + R-102 detection** | Day 3 morning | **CP-02b** |
| [**Lab 1.5 — SSB SQL + comparison**](../labs/lab-1-5-throughput-comparison.md) | **JOB-11 R-104 + 3-engine latency comparison** | Day 3 afternoon | **CP-04b** |

## Measurable outcomes

By end of module:

- All four Bronze ingest jobs run continuously without OOM, lag growth, or DLQ rate above 0.01%
- `argus.${STUDENT_ID}.orders.v1` Kafka topic sustains 150,000 events/sec for at least 10 minutes with all 3 engines (Spark + Flink + SSB) running simultaneously
- Median ingest-to-Bronze latency < 5 seconds; p99 latency < 30 seconds
- **PyFlink CEP detects planted Case 0 spoofing within p99 < 800ms** (CP-02b)
- **SSB SQL detects planted Case 2 cross-product imbalance, persisted to `gold.realtime_alert_stream` within 60s** (CP-04b)
- All 6 Bronze tables and the new `gold.realtime_alert_stream` have non-zero row counts after 30 min of replay

## What this fixes

Before ARGUS, the legacy CEP engine became blind on every expiry day — and even when it didn't, alerts fired 30 minutes after the fact. With this 3-engine architecture, ingest scales linearly with partitions, real-time detection happens in 800ms (Flink) or ~30s (SSB), and batch detection in Module 3 still runs every 30 min for the canonical record. The platform is no longer the bottleneck on either axis. ARG-1 closes; the gates that defeated MSE in 2024 won't defeat it again.

> 💡 **Tip:** If your Spark throughput plateaus before 150K ev/s, the most common cause is too few Spark executors on JOB-01. The job needs at least one executor per 4-8 Kafka partitions; with 48 partitions on `argus.${STUDENT_ID}.orders.v1`, that's 6–12 executors. Bump `spark.executor.instances` and re-run.

> 💡 **Tip:** If Flink CEP latency is high, check your TaskManager parallelism — should be ≥ 4 to keep up with 150K ev/s. The lab in 1.4 walks through this.

> ⚠️ **Compliance gate preview:** JOB-03 consumes the KAVACH CDC stream which carries investor PII. Module 7 will apply Atlas classifications (`PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`) on the columns of `argus_${STUDENT_ID}_bronze.member_cdc`. Do not re-route or copy this Bronze table to any other location during Module 1 — doing so leaks PII into untagged tables and breaks the lineage that CP-19 (the COMPLIANCE GATE) verifies. JOB-10 (Flink) only reads `orders.v1` and `trades.v1` which carry firm/instrument identifiers but not investor PII; that's deliberate.
