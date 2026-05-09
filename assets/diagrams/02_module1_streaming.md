# Module 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection

Days 2-4 (3.5 days) · Closes **ARG-1** · CP-02 / CP-02b / CP-03 / CP-04 / CP-04b

```mermaid
flowchart LR
    classDef src    fill:#1e2535,stroke:#9ca3af,color:#e5e7eb
    classDef nifi   fill:#161b27,stroke:#f96302,color:#f96302
    classDef kafka  fill:#161b27,stroke:#f96302,color:#e5e7eb
    classDef new    fill:#1a1632,stroke:#6366f1,color:#6366f1,stroke-width:2px
    classDef spark  fill:#161b27,stroke:#f96302,color:#f96302
    classDef sink   fill:#161b27,stroke:#f96302,color:#e5e7eb,stroke-width:2px
    classDef newSink fill:#1a1632,stroke:#6366f1,color:#e5e7eb,stroke-width:2px
    classDef cp     fill:#1a1632,stroke:#6366f1,color:#6366f1
    classDef dlq    fill:#1e2535,stroke:#ef4444,color:#ef4444

    %% Sources
    S1[TARANG match engine<br/>3.5B/d · 150K/s peak]:::src
    S2[NIPATAN clearing<br/>280M/d trades]:::src
    S3[KAVACH KYC + members<br/>Debezium CDC · PII]:::src
    S4[PRATEEK + BBO + SEBI + news<br/>4 external/reference]:::src

    %% NiFi flows
    F1[flow_01_orders<br/>multicast tap]:::nifi
    F2[flow_02_trades<br/>re-key by instrument]:::nifi
    F3[flow_03_kavach_cdc<br/>Atlas PII_HIGH attribution]:::nifi
    F4[flow_04_external_feeds<br/>multi-source merge]:::nifi

    %% Kafka topics — 9 prod + 2 DLQ
    K1[orders.v1 · 48p]:::kafka
    K2[trades.v1 · 24p]:::kafka
    K3[member.cdc.v1 · 6p compact]:::kafka
    K4[bbo + instrument.cdc + state<br/>+ regulator + news · 30p]:::kafka
    KRA["⚡ realtime_alerts.v1 · 12p<br/><b>NEW v1.2</b>"]:::new
    DLQ["orders.dlq · trades.dlq<br/>schema-failed events"]:::dlq

    %% Three engines on the same Kafka backbone
    E1[CDE / Spark Structured Streaming<br/>JOB-01..04 Bronze ingest<br/>JOB-12 realtime alert persistence]:::spark
    E2["⚡ CSA / PyFlink CEP<br/>JOB-10 R-101 SPOOFING + R-102 LAYERING<br/><b>p99 &lt; 800ms · NEW v1.2</b>"]:::new
    E3["⚡ SQL Stream Builder<br/>JOB-11 R-104 cross-product imbalance<br/><b>analyst-deployed · NEW v1.2</b>"]:::new

    %% Sinks
    BR[argus_$SID_bronze<br/>orders_raw · trades_raw · member_cdc<br/>+ instrument_cdc + external_feeds + legacy_alerts<br/>6 tables · MOR/ORC]:::sink
    GRA["⚡ argus_$SID_gold.realtime_alert_stream<br/>source_engine: FLINK or SSB<br/><b>NEW v1.2 · COW/Parquet · partitioned fired_date</b>"]:::newSink

    %% Checkpoints
    CP02[✓ CP-02<br/>NiFi flows healthy<br/>DLQ rate &lt; 0.01%]:::cp
    CP02b["✓ CP-02b ⚡<br/>Flink CEP detects<br/>Case 0 spoofing in p99 &lt;800ms"]:::new
    CP03[✓ CP-03<br/>150K ev/s sustained<br/>all 3 engines running]:::cp
    CP04[✓ CP-04<br/>Bronze row counts<br/>match expected ±0.1%]:::cp
    CP04b["✓ CP-04b ⚡<br/>SSB SQL detects Case 2<br/>persisted to Iceberg &lt;60s"]:::new

    %% Wiring: sources → NiFi → Kafka
    S1 --> F1 --> K1
    S2 --> F2 --> K2
    S3 --> F3 --> K3
    S4 --> F4 --> K4
    F1 -.schema fail.-> DLQ
    F2 -.schema fail.-> DLQ

    %% Kafka → engines (the parallel paths)
    K1 --> E1
    K2 --> E1
    K3 --> E1
    K4 --> E1
    K1 --> E2
    K2 --> E2
    K1 --> E3
    K2 --> E3
    K4 --> E3

    %% Engines → sinks
    E1 --> BR
    E2 --> KRA
    E3 --> KRA
    KRA --> E1
    E1 --> GRA

    %% Checkpoint wiring
    F1 --> CP02
    E2 --> CP02b
    E1 --> CP03
    BR --> CP04
    GRA --> CP04b
```

## Three engines, one Kafka backbone

The architectural insight of Module 1: all three engines read the same source events. They don't compete — they specialize.

| Engine | Job(s) | Owns | Latency | Authored by |
|---|---|---|---|---|
| **Spark Structured Streaming** | JOB-01..04, JOB-12 | Canonical persistence + realtime sink | ~10s trigger | Engineering |
| **PyFlink CEP** *(new in v1.2)* | JOB-10 | Sub-second pattern detection | **<800ms p99** | Engineering |
| **SSB SQL** *(new in v1.2)* | JOB-11 | Analyst-driven streaming SQL | ~30s | Surveillance analyst |

JOB-10 and JOB-11 do **not** replace JOB-08 batch (Module 3). They run in parallel — the same `event_id` from a planted spoofing case lands in both `gold.alert_candidates` (within 30 min, batch) and `gold.realtime_alert_stream` (within 800ms, streaming). When they disagree, that's a debugging signal. The lab in 1.5 covers this comparison explicitly.

## What this closes

ARG-1 has two halves: peak ingest throughput, and peak detection latency. The legacy CEP engine failed both. Spark + Kafka fixes the throughput half; PyFlink CEP + SSB SQL fix the latency half. By the end of Module 1 a planted Case 0 spoofing event triggers an alert in under one second — the analyst can intervene before the trader cycles to the next attempt.

## The latency comparison test

```sql
-- CP-02b + CP-04b verification: the 3-engine cross-check
SELECT
    pattern_type,
    source_engine,
    COUNT(*)                                 AS n_alerts,
    ROUND(PERCENTILE(detection_latency_ms, 0.5),  0) AS p50_ms,
    ROUND(PERCENTILE(detection_latency_ms, 0.99), 0) AS p99_ms
FROM argus_${STUDENT_ID}_gold.realtime_alert_stream
WHERE fired_date = CURRENT_DATE
GROUP BY pattern_type, source_engine
ORDER BY pattern_type, source_engine;
-- Expect: SPOOFING|FLINK p99 < 800ms · CROSS_PRODUCT_IMBALANCE|SSB p99 < 60000ms
```

When you re-run this query in Module 3 after JOB-08 batch has fired, the same `event_id` will appear in both this table and `gold.alert_candidates` — proving the streaming and batch paths are detecting the same patterns, just at different latencies.
