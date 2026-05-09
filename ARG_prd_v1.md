# Project ARGUS — PRD v1

**Capstone codename**: ARGUS
**Customer (fictional)**: Magic Street Exchange (MSE), Mumbai, India
**Course duration**: 7 modules · 10 days · Cloudera Data Platform Public/Private Cloud (hybrid)
**Delivery mode**: C — Both, repo-first, HTML published via GitHub Pages
**Repo**: `github.com/cloudera-training/argus-capstone`
**Pages URL**: `cloudera-training.github.io/argus-capstone/`
**License**: Apache 2.0
**Initial cohort tag**: `v1.0-spring2026`
**Visual identity**: accent `#f96302` (Cloudera orange), secondary `#6366f1` (indigo for compliance sections)
**Status**: Locked — no deviation during Phase 3 rendering without explicit user re-approval.

---

## 1. Executive Summary

**Magic Street Exchange (MSE)** is a fictional Indian stock exchange headquartered in Mumbai (Bandra Kurla Complex) with a technology center in Bangalore (Whitefield). MSE is the clear #3 stock exchange in India behind NSE and BSE, with a niche identity in mid-cap and small-cap derivatives where the larger venues underinvest.

| Attribute | Value |
|---|---|
| Founded | 2009 |
| Headquarters | Mumbai (BKC) + Bangalore tech center |
| Revenue (FY2025–26) | ₹3,400 crore (~$410M USD), +22% YoY |
| Employees | 920 (520 Mumbai, 340 Bangalore, 60 Delhi) |
| Listed securities | ~4,800 (3,200 equities, 1,400 ETFs/funds, 200 corporate bonds) |
| F&O contracts | ~280 underlying names |
| Investor accounts | ~24 million (via 380 trading member firms) |
| Order volume | 3.5B events/day steady-state; 9–12B on F&O expiry Thursdays |
| Surveillance team | 28 analysts (22 Mumbai, 6 Bangalore) |
| Market share | ~14% cash equities, ~9% equity derivatives |

**Triggering event**: In Q2 2025, SEBI issued a Show Cause Notice citing 14 manipulative episodes between January and March 2025 that MSE's surveillance system failed to detect during F&O expiry-day volume spikes. The proprietary trading firm responsible was eventually disgorged ₹47 crore in unlawful gains — none of it discovered by MSE. SEBI gave MSE 18 months to remediate or face restrictions on new product approvals. The board approved a ₹220 crore (~$26M) program — internally codenamed **ARGUS** — to rebuild the surveillance stack on Cloudera Data Platform.

**Why Cloudera**: MSE's CIO joined from a large Indian private bank in 2024 with a strong CDP preference. Indian data residency requirements (DPDP Act 2023 §16, RBI guidance, SEBI's own data localization expectations for market data) make Cloudera Private Cloud on MSE's existing Mumbai datacenter footprint the path of least resistance, with AWS Mumbai region presence available for non-sensitive analytics and DR.

**Mythological grounding**: ARGUS Panoptes — the hundred-eyed watchman of Greek myth, who in the relevant version of the legend kept some eyes open while others slept, never fully unwatched. The platform is named for the property it must achieve: every order at every price level on every symbol, watched simultaneously and continuously. The myth's epilogue — Hermes (the trickster, patron of merchants and thieves) eventually outwits Argus — is also apt: surveillance is an eternal cat-and-mouse, never a solved problem.

---

## 2. Business Problem — 5 Deficiencies

### ARG-1 — Surveillance system collapses under peak F&O expiry-day volume

**Symptom**: On F&O expiry Thursdays, when peak order rates exceed ~150,000 events per second across cash equities, single-stock futures, and index options, MSE's existing vendor surveillance platform falls behind the order stream by 8–45 minutes. Alerts that should fire in real-time fire hours later — frequently after the trading session has closed and the manipulator has already disconnected. SEBI's Q2 2025 inspection report explicitly cited that "alerts generated post-session are of substantially diminished investigative value and do not satisfy the real-time detection requirements of the Master Circular on Surveillance of Securities Market dated 09 July 2024."

**Root cause**: The legacy platform is a single-node CEP engine sized in 2017 for ~40,000 events per second, fed from a JMS queue and processed serially through a hand-coded rules engine. There is no streaming framework, no distributed state store, no horizontal scaling, and no ability to replay or backfill detection over historical order books. When input queue depth exceeds ~2 million events, the engine silently sheds messages — a fact MSE only discovered when SEBI's inspection team reconciled MSE's alert logs against the regulator's own market replay and identified gaps.

**Quantified business impact**: SEBI's Show Cause Notice cited 14 specific manipulative episodes that MSE failed to detect during expiry-day volume spikes in Q1 2025. The proprietary firm responsible was disgorged ₹47 crore in unlawful gains — none of it discovered by MSE. Worst-case regulatory exposure: ₹50–80 crore in penalties under SEBI Act §15 plus a freeze on new product approvals. Two major institutional listings withdrawn in the past year cited "concerns about market integrity and surveillance capability" — estimated ₹95 crore in foregone annual listing and member-firm revenue.

**Closed by**: Module 1 (CDF + PyFlink CEP + SQL Stream Builder streaming ingest & real-time detection).

### ARG-2 — Cannot reconstruct order-book state or compute the temporal features that spoofing/layering detection requires

**Symptom**: Surveillance analysts cannot answer questions of the form "what did the order book at price level 2 look like 50 milliseconds before the manipulator's cancel?" The current platform stores only firing alerts, not the underlying event sequences that produced them. When SEBI requested order-book reconstruction for the 14 missed episodes, MSE engineering took 11 weeks to reproduce — and the reproductions were partial because the upstream raw event archive had a 90-day retention. Analysts also cannot run cross-product correlation: they cannot identify cases where a member firm moves price in cash equities to profit on a position in single-stock futures or index options — the exact pattern at the heart of SEBI's July 2025 Jane Street order, which cited ₹4,843 crore in unlawful gains across 18 trading days through cross-product index manipulation.

**Root cause**: Order events are written to a relational OLTP database with no time-travel capability, no incremental state materialization, and no way to express sequential features (time-to-cancel distributions, order-to-trade ratios over rolling windows, layered-order pattern detection across the bid stack). Cross-product joining is impossible because cash, futures, and options events live in separate siloed databases owned by separate trading-tech teams, with no unified entity model linking the same member firm or instrument across venues. The data architecture was designed for trade reconciliation and clearing, not for behavioral analytics on order flow.

**Quantified business impact**: Delays in producing reconstructed order books for SEBI inquiries have already triggered two warning letters citing "inadequate evidentiary capability." Internal estimate: 60–70% of sophisticated layering activity on MSE's books goes undetected because the surveillance platform cannot express the features needed to find it. Engineering cost of producing ad-hoc reconstructions for regulatory and internal investigations: ~₹14 crore/year in burned engineering time.

**Closed by**: Modules 2–3 (CDE / Spark on Iceberg with time-travel for order-book reconstruction; cross-product temporal feature engineering).

### ARG-3 — 92% of fired alerts are closed as no-action; analysts are buried in noise while real manipulation slips through

**Symptom**: The current rules engine fires ~4,200 alerts per trading day. 92% are closed by surveillance analysts as "no manipulative intent" — bona-fide cancellations, legitimate market-making activity, fat-finger errors, or thin-volume false signals from low-liquidity small-caps. Analysts spend the bulk of their day clearing false positives, which means real cases sit in the queue for hours or days before being triaged. Three of the 14 SEBI-cited missed episodes had actually fired alerts in MSE's system — but the alerts were buried in the false-positive flood and not opened until SEBI inquired.

**Root cause**: The rules are deterministic and over-fit. They were tuned in 2017–2019 against a market structure that no longer exists (pre-options-boom retail volumes, fewer co-located members, different liquidity profiles in mid-caps). The rules cannot learn from analyst dispositions — every alert closed as "no-action" produces no feedback signal that improves future alert quality. There is no risk-ranking; every alert lands in the queue with equal priority, regardless of whether it's a clean institutional pattern or a suspicious sequence from a member firm with three open SEBI matters.

**Quantified business impact**: 28 surveillance analysts spend ~65% of their time triaging false positives — ~18 FTE-equivalents of wasted effort, costing ~₹22 crore/year in fully-loaded analyst cost. More damaging: the 8% of alerts that *are* real are diluted into a stream of noise, and median time-to-investigation is 11 hours when SEBI's circular expectation is "real-time review of high-priority alerts." Three confirmed manipulation episodes in 2024–25 were detected only after SEBI flagged them.

**Closed by**: Module 5 (CML supervised ML — XGBoost alert risk-ranking with explainability).

### ARG-4 — Suspicious Transaction Report narratives are entirely hand-written, taking 40+ minutes per case and creating a documentation backlog

**Symptom**: When a surveillance analyst confirms a suspicious pattern, they produce a written narrative — the Suspicious Transaction Report (STR) — describing the manipulation, the order-flow sequence, the price impact, the suspected intent, and the regulation alleged to have been violated. This narrative goes to MSE Compliance, then to SEBI. Each narrative currently takes 40–90 minutes to draft. The backlog of confirmed-but-undocumented cases stands at 340 reports, oldest 70 days old. SEBI's Master Circular requires reports filed "within reasonable time of detection" and has flagged MSE's documentation backlog as a separate finding.

**Root cause**: There is no template, no automation, no LLM-assisted drafting, and no integration between the surveillance platform and the report-writing tool. Analysts open Microsoft Word, copy-paste order-event tables from the surveillance UI, and write the entire narrative from scratch — including standard regulatory boilerplate identical across reports. The cognitive task is mostly translation: turning a structured event sequence into prose that a SEBI investigator can follow. It is exactly the kind of work an LLM can draft in seconds and a human can review and finalize in minutes.

**Quantified business impact**: 340-report backlog × ₹1.8 lakh average analyst-cost-per-report = ₹6.1 crore in stalled compliance throughput. SEBI has explicitly cited the documentation gap. The deeper cost is opportunity cost — every hour an analyst spends drafting narratives is an hour not spent on triage, perpetuating ARG-3.

**Closed by**: Module 6 (Cloudera AI Inference / GenAI RAG — auto-drafted STR narratives with human-in-the-loop review).

### ARG-5 — No data lineage, no consent governance, no DPDP-compliant erasure workflow on personal data

**Symptom**: MSE's surveillance platform processes personal data on millions of investors but cannot answer four questions a SEBI inspector or Data Protection Officer would ask: (1) Which downstream tables contain personal data of investor X? (2) When was investor X's consent obtained, and what was the stated purpose? (3) If investor X invokes their right to erasure under DPDP §12, can we demonstrate erasure across all systems? (4) When an analyst opens an investigation file, who else accessed the file, and was each access logged with a justified business purpose?

**Root cause**: Data lineage is not tracked — no Atlas, no equivalent metadata service, no automated capture of derivation chains. Access control is coarse-grained (table-level GRANTs in the legacy database) with no row-level or column-level masking. Personal data fields (PAN, Aadhaar reference numbers, registered email, mobile, demat account) are mixed into operational tables with no classification tagging. Consent records exist in member firm KYC systems but are not federated into MSE's surveillance data plane, so purpose limitation under DPDP §6(4) cannot be enforced at query time. Erasure requests are handled manually by a junior analyst running ad-hoc DELETE statements — a process that has produced two known referential-integrity incidents.

**Quantified business impact**: Under DPDP, the Data Protection Board can impose penalties up to ₹250 crore for failure to implement reasonable security safeguards. The DPDP Rules 2025 took effect in phased rollout from November 2025, putting MSE in active enforcement scope. SEBI separately requires audit-trail integrity for all surveillance data under the Master Circular. Independent estimate of regulatory exposure under combined DPDP + SEBI scrutiny: ₹150–300 crore worst case, plus loss of "Significant Data Fiduciary" trust which would compound business damage.

**Closed by**: Modules 4 + 7 (CDW governed views with Ranger policies + SDX/Atlas classification + DPDP §6(4) consent withdrawal + DPDP §12 erasure with Iceberg time-travel evidence).

### Deficiency-to-module mapping (locked)

| Deficiency | Closing module(s) | Primary CDP service(s) |
|---|---|---|
| ARG-1 | Module 1 | CDF / Apache NiFi + Kafka |
| ARG-2 | Modules 2–3 | CDE / Apache Spark + Iceberg time-travel |
| ARG-3 | Module 5 | Cloudera AI / CML (XGBoost + MLflow) |
| ARG-4 | Module 6 | Cloudera AI Inference (CAII) + Milvus + RAG |
| ARG-5 | Modules 4 + 7 | CDW (Impala) + SDX / Atlas + Ranger |

---

## 3. Source Systems

### Internal (5)

**INT-1 — TARANG (Matching Engine Telemetry)**
- **Vendor**: MSE in-house C++ matching engine
- **Data type**: Order-book event firehose (place / modify / cancel / partial-fill / full-fill) with µs-precision timestamps, member firm ID, trader ID, instrument code, side, order type, quantity, price, resulting book state at affected price level
- **Volume**: 3.5B events/day steady-state; 9–12B on F&O expiry Thursdays
- **Update frequency**: Real-time (target latency < 5ms from match engine to telemetry tap)
- **Ingestion method**: NiFi consuming from Kafka mirror of multicast feed (multicast cannot cross surveillance VLAN boundary)

**INT-2 — KAVACH (KYC & Member Master)**
- **Vendor**: MSE in-house, Oracle-backed
- **Data type**: Trading member firm reference (380 firms), trader-level reference (~12,000 named human traders), end-investor reference (~24M PAN, registered name, email, mobile, demat account, KYC tier). Personal-data-heavy.
- **Volume**: ~12M reference rows; ~50K daily change events
- **Update frequency**: Hourly delta CDC
- **Ingestion method**: Debezium → Kafka → NiFi → Bronze

**INT-3 — NIPATAN (Trade Clearing & Settlement Feed)**
- **Vendor**: MSE in-house, integrated with NSE Clearing Corporation
- **Data type**: Executed trades post-clearing — trade ID, both legs (buyer/seller member firms, end-investor accounts where available), instrument, executed price, executed quantity, trade timestamp, settlement date, clearing-member flag
- **Volume**: ~280M events/day steady-state
- **Update frequency**: Trade-by-trade, 50–200ms lag behind TARANG
- **Ingestion method**: Direct Kafka producer from clearing system → NiFi → Bronze

**INT-4 — PRATEEK (Instrument & Corporate Action Reference)**
- **Vendor**: MSE in-house, integrated with NSDL/CDSL depositories
- **Data type**: Instrument master (~4,800 securities + ~2,800 active F&O contracts), corporate actions (splits, bonus issues, dividends, mergers), circuit-breaker bands per symbol, ESM/ASM flag list
- **Volume**: ~600 corporate-action events / 90-day window; static reference ~7,600 rows
- **Update frequency**: End-of-day batch for static reference; real-time for circuit-breaker / ESM state changes
- **Ingestion method**: NiFi REST pull (static) + Kafka producer (state changes)

**INT-5 — SMRITI (Legacy Alert & Analyst Disposition History)**
- **Vendor**: 2017-vintage third-party surveillance platform (being decommissioned)
- **Data type**: Historical alerts (2018–present) with analyst disposition (no-action / escalated / confirmed), free-text rationale, case ID, linked STR ID, eventual SEBI outcome
- **Volume**: ~4.8M historical alert records
- **Update frequency**: End-of-day batch (append-only nightly export)
- **Ingestion method**: Nightly S3 sync → Bronze batch load (NOT a Kafka topic)

### External (3)

**EXT-1 — SEBI Watchlist & Action Feed**
- **Vendor**: SEBI Intermediary Portal (SI Portal)
- **Data type**: PFUTP orders, debarment lists, ASM/ESM additions/removals across all Indian exchanges, Show Cause Notices, consent orders
- **Volume**: ~800 actions/year; ~2,000 watchlist deltas/year
- **Update frequency**: Real-time push for actions; daily batch for watchlist deltas
- **Ingestion method**: NiFi pulling from SI Portal SFTP + webhook for urgent notifications

**EXT-2 — Cross-Exchange BBO (NSE/BSE Public Feed)**
- **Vendor**: Licensed market-data provider (Refinitiv-equivalent)
- **Data type**: Real-time best-bid / best-offer ticks on NSE and BSE for every symbol cross-listed on MSE
- **Volume**: ~500M BBO ticks/day
- **Update frequency**: Real-time
- **Ingestion method**: Vendor Kafka feed → NiFi

**EXT-3 — News & Corporate Disclosure Feed**
- **Vendor**: Wire vendor (Reuters/Bloomberg-equivalent) + SEBI/BSE/NSE corporate filings
- **Data type**: Earnings announcements, M&A disclosures, regulatory filings, news headlines tagged to instruments
- **Volume**: ~25,000 headlines/quarter
- **Update frequency**: Real-time
- **Ingestion method**: Vendor REST/Kafka feed → NiFi

---

## 4. Solution Architecture (4-Layer, v1.2 amendment)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              4. GOVERN (SDX)                                │
│  Atlas classification tags · Ranger row/column policies · DPDP audit log    │
│      Lineage capture · Time-travel erasure proofs · Access audit trail      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                3. SERVE                                     │
│  CDW (Impala): governed views over Gold tables for surveillance UI · BI     │
│  CML batch scoring: XGBoost alert risk model produces argus_gold.alert_     │
│  candidates.model_score every 5 min · CAII GenAI: STR narrative drafts      │
│  Cloudera Data Visualization: surveillance ops dashboard + KPI tracking     │
├─────────────────────────────────────────────────────────────────────────────┤
│                              2. PROCESS                                     │
│  ┌───────────────────────────────┬─────────────────────────────────────┐    │
│  │   BATCH PATH (CDE / Spark)    │  REAL-TIME PATH (Flink + SSB)       │    │
│  │  JOB-01..09 Bronze→Silver→    │  JOB-10 PyFlink CEP — sub-second    │    │
│  │  Gold · identity resolution · │  R-101 SPOOFING + R-102 LAYERING    │    │
│  │  book reconstruction · temp   │  detection on argus.orders.v1       │    │
│  │  features · alert candidates  │  JOB-11 SSB SQL — analyst-driven    │    │
│  │  · ML scoring · MLflow track  │  R-104 cross-product imbalance      │    │
│  │                               │  JOB-12 persists realtime alerts    │    │
│  └───────────────────────────────┴─────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────────────┤
│                               1. INGEST                                     │
│  CDF / Apache NiFi flows consuming 9 Kafka topics · CDC from KAVACH ·       │
│  REST/SFTP pulls from PRATEEK + SEBI SI Portal · Nightly batch from SMRITI  │
│  Schema validation · DLQ routing · Bronze landing in Iceberg (ORC/MOR)      │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Service-to-layer mapping**:

| CDP Service | Layer | Role |
|---|---|---|
| Apache NiFi (CDF) | 1. Ingest | Streaming + batch ingestion, schema validation, DLQ |
| Apache Kafka | 1. Ingest | Event backbone (9 production + 2 DLQ topics, 126 partitions) |
| Apache Spark (CDE) | 2. Process — batch | Bronze→Silver→Gold transforms, feature engineering, model scoring (JOB-01..09, JOB-12) |
| **Cloudera Flink (PyFlink)** | **2. Process — real-time** | **Sub-second pattern detection via CEP (JOB-10) — R-101 SPOOFING + R-102 LAYERING** |
| **SQL Stream Builder (SSB)** | **2. Process — real-time** | **Analyst-driven streaming SQL (JOB-11) — R-104 cross-product imbalance, declarative pattern logic without code** |
| Apache Iceberg | 2. Process | Open table format with time-travel + schema evolution |
| Apache Impala (CDW) | 3. Serve | SQL serving for surveillance UI, BI, governed views |
| Cloudera AI (CML) | 3. Serve | XGBoost training, MLflow tracking, batch scoring |
| Cloudera AI Inference (CAII) | 3. Serve | LLM hosting + Milvus vector store for RAG |
| Cloudera Data Visualization | 3. Serve | Surveillance ops dashboard |
| Apache Atlas (SDX) | 4. Govern | Classification tags, lineage, audit |
| Apache Ranger (SDX) | 4. Govern | Row/column policies, access enforcement |
| Apache Airflow on CDE | Cross-cutting | Job orchestration, ML retraining schedule |

**Real-time vs batch — when each wins**:

| Concern | Batch (CDE/Spark, 30 min) | Real-time (Flink/SSB, sub-second) |
|---|---|---|
| Detection latency | 30 minutes | < 800ms (Flink) / < 2s (SSB) |
| Historical context window | Full day; arbitrary backfill | Last 60s windowed only |
| Re-runnable on Iceberg time-travel | Yes — auditable | Stateful; checkpoint-based |
| Catches sub-second patterns | No (R-101 spoofing fires after the cancel) | Yes — fires while order is still resting |
| Catches multi-hour patterns | Yes (e.g. wash-trade rings) | No — state TTLs out |
| Defensible to SEBI auditors | Easy — replay batch from Iceberg | Harder — needs Flink savepoint preserved |
| **Used as canonical record** | **Yes — JOB-08 → alert_candidates → ML scoring** | No — JOB-10/11 → realtime_alert_stream → analyst notification |

JOB-08 (batch) and JOB-10/11 (real-time) **both fire on the same patterns**. The same `event_id` should land in both `gold.alert_candidates` (within 30 min) and `gold.realtime_alert_stream` (within 1 second). When they disagree, that's a debugging signal — almost always indicates a state-management bug in the streaming engine. The lab covers this comparison explicitly.

---

## 5. Kafka Topics (9 production + 2 DLQ = 11 — locked, v1.2 amendment)

| # | Topic | Source | Partitions | Key | Value schema (summary) |
|---:|---|---|---:|---|---|
| 1 | `argus.orders.v1` | INT-1 TARANG | 48 | `instrument_code` | `event_id, ts_us, member_firm_id, trader_id, instrument_code, side, order_type, qty, price, action, resulting_book_state` |
| 2 | `argus.trades.v1` | INT-3 NIPATAN | 24 | `instrument_code` | `trade_id, ts_us, instrument_code, buy_member_firm_id, sell_member_firm_id, buy_investor_acct, sell_investor_acct, exec_price, exec_qty, settlement_date` |
| 3 | `argus.bbo.v1` | EXT-2 | 12 | `instrument_code` | `ts_us, instrument_code, venue, best_bid_px, best_bid_qty, best_offer_px, best_offer_qty` |
| 4 | `argus.member.cdc.v1` | INT-2 KAVACH | 6 (compacted) | `member_firm_id` | `op, ts, member_firm, traders[], investor_accts[], pii_payload` |
| 5 | `argus.instrument.cdc.v1` | INT-4 PRATEEK | 3 (compacted) | `instrument_code` | `op, ts, instrument_code, lot_size, tick_size, expiry, strike, corporate_actions[]` |
| 6 | `argus.surveillance.state.v1` | INT-4 PRATEEK | 6 | `instrument_code` | `ts, instrument_code, esm_flag, asm_flag, circuit_band, action` |
| 7 | `argus.regulator.feed.v1` | EXT-1 SEBI | 3 | `action_type` | `ts, action_type, member_firm_id, instrument_code, regulation_ref, document_url` |
| 8 | `argus.news.v1` | EXT-3 | 6 | `instrument_code` | `ts, headline, body, source, instruments_tagged[], sentiment_hint` |
| 9 | `argus.realtime_alerts.v1` | JOB-10 (Flink) + JOB-11 (SSB) | 12 | `member_firm_id` | `alert_id, fired_ts, source_engine, rule_id, severity, pattern_type, member_firm_id, trader_id, instrument_code, window_start_ts, window_end_ts, evidence_json, detection_latency_ms` |
| DLQ-1 | `argus.orders.dlq` | NiFi flow_01 | 3 | n/a | malformed orders rejected by schema validation |
| DLQ-2 | `argus.trades.dlq` | NiFi flow_02 | 3 | n/a | malformed trades rejected by schema validation |

**Total**: 126 partitions across 11 topics. Sizing rationale: `argus.orders.v1` at 48 partitions allows ~3,000 events/sec/partition at the 150K events/sec peak — comfortable for downstream Spark Structured Streaming, **PyFlink CEP, SSB SQL** and NiFi consumers reading the same source events in parallel. CDC topics (#4, #5) are log-compacted; all others retain 7 days hot + 90 days cold (Iceberg-archived).

`argus.realtime_alerts.v1` (#9) is the new sub-second alert sink populated by JOB-10 (Flink CEP) and JOB-11 (SSB SQL) — both write to the same topic with `source_engine` discriminating between them. This topic feeds JOB-12 which lands real-time alerts into `gold.realtime_alert_stream` for cross-checking against batch detections from JOB-08.

INT-5 (SMRITI legacy alerts) is intentionally *not* a Kafka topic — it's a nightly batch S3 sync. This is a pedagogical contrast: streaming where it matters (live order events), batch where it's correct (end-of-day historical labels).

---

## 6. Iceberg Tables (19 — locked, v1.2 amendment)

Schema naming pattern: `argus_bronze.*` / `argus_silver.*` / `argus_gold.*`.

### Bronze (6 tables — MOR/ORC, partitioned by `ingest_date`)

| # | Table | Source | Key columns | Purpose |
|---:|---|---|---|---|
| 1 | `argus_bronze.orders_raw` | INT-1 / Topic 1 | `event_id`, `ts_us`, `instrument_code` | Raw TARANG events, schema-validated, DLQ-protected |
| 2 | `argus_bronze.trades_raw` | INT-3 / Topic 2 | `trade_id`, `ts_us`, `instrument_code` | Raw NIPATAN executed trades |
| 3 | `argus_bronze.member_cdc` | INT-2 / Topic 4 | `member_firm_id`, `cdc_op`, `cdc_ts` | KAVACH CDC events, append-only |
| 4 | `argus_bronze.instrument_cdc` | INT-4 / Topics 5+6 | `instrument_code`, `cdc_op`, `cdc_ts` | PRATEEK reference + corporate actions + surveillance state |
| 5 | `argus_bronze.external_feeds` | EXT-1/2/3 / Topics 3,7,8 | `source`, `ts`, `payload` | Union of SEBI feed + BBO + news, tagged by `source` |
| 6 | `argus_bronze.legacy_alerts` | INT-5 (batch) | `alert_id`, `disposition_date` | 4.8M historical alerts with analyst dispositions — ML training set |

All Bronze tables: `STORED AS ICEBERG TBLPROPERTIES ('write.format.default'='ORC', 'write.merge.mode'='merge-on-read')`. Bronze #6 partitioned by `disposition_date` rather than `ingest_date` (different upstream cadence).

### Silver (4 tables — COW/Parquet, partitioned by `trade_date`)

| # | Table | Derived from | Key columns | Purpose |
|---:|---|---|---|---|
| 1 | `argus_silver.order_events` | Bronze #1 + #3 + #4 | `event_id`, `ts_us`, `member_firm_id`, `trader_id`, `instrument_code` | Cleaned, deduplicated TARANG events with member + instrument enrichment joined |
| 2 | `argus_silver.executed_trades` | Bronze #2 + #3 + #4 | `trade_id`, `ts_us`, both legs | Cleaned NIPATAN with both-leg enrichment + clearing-member resolution |
| 3 | `argus_silver.member_master` | Bronze #3 | `member_firm_id`, `effective_from`, `effective_to` | SCD2 current-state of KAVACH; PII columns tagged with Atlas classifications |
| 4 | `argus_silver.instrument_master` | Bronze #4 | `instrument_code`, `effective_from`, `effective_to` | SCD2 current-state instrument + active surveillance flags + corporate-action-adjusted price history |

All Silver tables: `STORED AS ICEBERG TBLPROPERTIES ('write.format.default'='PARQUET', 'write.merge.mode'='copy-on-write')`.

### Gold (9 tables — COW/Parquet, partitioned by `trade_date` except where noted)

| # | Table | Derived from | Purpose |
|---:|---|---|---|
| 1 | `argus_gold.order_book_snapshots` | Silver #1 | Reconstructed book state at 1s/100ms/per-event intervals — answers ARG-2 |
| 2 | `argus_gold.member_temporal_features` | Silver #1+#2 | Order-to-trade ratios, time-to-cancel distributions, layered-order patterns over rolling windows per member×instrument×date |
| 3 | `argus_gold.cross_product_features` | Silver #1+#2+#4 | Cash-vs-derivatives delta exposure, directional consistency, options-vs-cash position-size ratio per member×underlying — answers Jane Street pattern |
| 4 | `argus_gold.alert_candidates` | Gold #1+#2+#3 | Candidate alerts from JOB-08 batch rules (rule-fired) with feature payload — consumed by ML scorer |
| 5 | `argus_gold.confirmed_manipulation_cases` | Gold #4 + Bronze #6 | Analyst-confirmed cases with disposition + STR linkage — truth table for ML + GenAI |
| 6 | `argus_gold.member_risk_scores` | Gold #5 + EXT-1 history | Daily rollup combining historical disposition, SEBI actions, current-day alert volume |
| 7 | `argus_gold.consent_audit` | All erasure / consent events | DPDP §12 + §6(4) audit log — `'history.expire.enabled'='false'` (consent history never expires) |
| 8 | `argus_gold.surveillance_kpis` | Gold #4 + #5 | Operational metrics for surveillance team's own dashboard (alert volume, FP rate, time-to-disposition, backlog age) |
| 9 | `argus_gold.realtime_alert_stream` | Topic 9 (`realtime_alerts.v1`) | Sub-second alerts from JOB-10 (Flink CEP) + JOB-11 (SSB SQL); `source_engine` discriminates between them; partitioned by `fired_date` |

All Gold tables: `STORED AS ICEBERG TBLPROPERTIES ('write.format.default'='PARQUET', 'write.merge.mode'='copy-on-write')`. Gold #7 additionally `'history.expire.enabled'='false'` per framework requirement (consent and audit history must never expire).

**Totals**: 6 Bronze + 4 Silver + 9 Gold = **19 tables** (exceeds framework minimum of 17).

---

## 7. Data Pipeline Jobs (JOB-01 through JOB-12 — locked, v1.2 amendment)

Jobs run on CDE (Spark), Cloudera Flink (PyFlink), and SQL Stream Builder. Orchestrated by Airflow on CDE for batch jobs; streaming jobs are long-running deployments. Schedules below are illustrative production cadence; lab exercises run them on demand.

| Job ID | Name | Schedule | Input → Output | CDP Service |
|---|---|---|---|---|
| JOB-01 | `bronze_orders_ingest` | Streaming (continuous) | Topic `argus.orders.v1` → `argus_bronze.orders_raw` | CDE / Spark Structured Streaming |
| JOB-02 | `bronze_trades_ingest` | Streaming (continuous) | Topic `argus.trades.v1` → `argus_bronze.trades_raw` | CDE / Spark Structured Streaming |
| JOB-03 | `bronze_member_cdc` | Streaming (continuous) | Topic `argus.member.cdc.v1` → `argus_bronze.member_cdc` | CDE / Spark Structured Streaming |
| JOB-04 | `bronze_external_feeds` | Streaming + nightly | Topics 3,5,6,7,8 → `argus_bronze.external_feeds` + `argus_bronze.instrument_cdc` | CDE / Spark Structured Streaming + nightly batch |
| JOB-05 | `silver_identity_resolution` | Hourly | `argus_bronze.member_cdc` → `argus_silver.member_master` (SCD2) + fuzzy-match resolution for cases 10–14 | CDE / Spark batch |
| JOB-06 | `silver_order_book_reconstruction` | Every 15 min | `argus_silver.order_events` → `argus_gold.order_book_snapshots` (also produces enriched Silver tables) | CDE / Spark batch |
| JOB-07 | `gold_temporal_features` | Every 30 min | `argus_silver.order_events` + `argus_silver.executed_trades` → `argus_gold.member_temporal_features` + `argus_gold.cross_product_features` | CDE / Spark batch |
| JOB-08 | `gold_alert_candidates` | Every 30 min | Gold #1+#2+#3 → `argus_gold.alert_candidates` (5 deterministic rules, batch firing) | CDE / Spark batch |
| JOB-09 | `gold_score_alerts` | Every 5 min | `argus_gold.alert_candidates` (unscored) → `argus_gold.alert_candidates` (with `model_score`) | CML batch scoring |
| JOB-10 | `realtime_spoofing_layering_cep` | Streaming (continuous) | Topic `argus.orders.v1` + `argus.trades.v1` → Topic `argus.realtime_alerts.v1` (R-101 SPOOFING + R-102 LAYERING patterns, sub-second detection) | **Cloudera Flink / PyFlink CEP** |
| JOB-11 | `realtime_cross_product_imbalance` | Streaming (continuous) | Topics `argus.orders.v1` + `argus.trades.v1` + `argus.instrument.cdc.v1` → Topic `argus.realtime_alerts.v1` (R-104 Jane Street pattern) | **SQL Stream Builder (SSB)** |
| JOB-12 | `realtime_alert_persistence` | Streaming (continuous) | Topic `argus.realtime_alerts.v1` → `argus_gold.realtime_alert_stream` | CDE / Spark Structured Streaming |

**GenAI service** (not a JOB-NN — deployed as a service): `argus_genai_str_drafter` — CAII REST endpoint invoked from the analyst review UI when an analyst confirms an alert and clicks "Draft STR." Asynchronous; result returned in 8–20 seconds.

**Real-time vs batch detection — architectural rationale**: JOB-10 and JOB-11 do NOT replace JOB-08. They run in parallel on the same Kafka topics. JOB-08 owns the batch detection of all 5 rules with full historical context every 30 min; JOB-10 owns sub-second detection of R-101/R-102 (the time-sensitive patterns where 30-min batch latency makes intervention impossible); JOB-11 demonstrates analyst-driven SSB SQL for R-104. Both real-time engines write to the same `realtime_alerts.v1` topic with `source_engine` discriminating; JOB-12 lands them into `gold.realtime_alert_stream` for cross-checking against batch detections from JOB-08 (the same `event_id` should appear in both within the latency window). When both engines fire on the same event, batch wins for canonical record-keeping and ML scoring; real-time wins for analyst notification and circuit-breaker triggers.

**Target line counts** (per framework):
- Bronze ingest jobs (JOB-01..04): ~50 lines each
- Identity resolution (JOB-05): ~120 lines
- Order book reconstruction (JOB-06): ~110 lines
- ML training (`train_alert_ranker.py`): ~110 lines
- GenAI engine (`genai_rag_engine.py`): ~130 lines
- **PyFlink CEP (JOB-10): ~180 lines**
- **SSB SQL (JOB-11): ~80 lines (declarative SQL, less ceremony)**
- **Real-time persistence (JOB-12): ~50 lines**

---

## 8. ML Models (1 model — closes ARG-3)

### Model: `argus_alert_ranker`

| Attribute | Value |
|---|---|
| **Algorithm** | XGBoost (gradient-boosted trees) |
| **Target** | Binary: `is_confirmed_manipulation` (1 = analyst escalated AND case filed as STR or led to SEBI action; 0 = analyst closed as no-action) |
| **Training data** | 4.8M labeled alerts from `argus_bronze.legacy_alerts` joined to `argus_gold.confirmed_manipulation_cases` |
| **Class balance** | ~8% positive; handled via `scale_pos_weight` (no oversampling) |
| **Feature count** | ~60 features in 6 groups (order-flow, cross-product, member context, instrument context, temporal context, rule context) |
| **Training cadence** | Weekly retrain on rolling 12-month window |
| **MLflow experiment** | `argus_alert_ranking_v1` |
| **Model registry stages** | Staging → Production (manual approval gate; regulator requires human-in-the-loop on model promotions) |
| **Monitoring** | Daily distribution-drift checks on production scoring stream |

**Hyperparameters** (Hyperopt search ranges — 50-trial Bayesian search with 5-fold time-series CV):

| Parameter | Range |
|---|---|
| `n_estimators` | 200–800 |
| `max_depth` | 4–10 |
| `learning_rate` | 0.01–0.2 |
| `min_child_weight` | 1–10 |
| `subsample` | 0.6–1.0 |
| `colsample_bytree` | 0.6–1.0 |
| `scale_pos_weight` | 8–15 |

**Performance thresholds (minimum to pass CP-13)**:
- AUC-ROC ≥ **0.82** on held-out test set (last 30 days, no leakage)
- Top-decile precision ≥ **0.55** (among top 10% scored alerts, ≥55% must be true positives)

**Production scoring**: Batch every 5 minutes via JOB-09. Scored alerts return to `argus_gold.alert_candidates.model_score`; analyst UI sorts queue by `model_score DESC`.

**Explainability**: SHAP values computed at scoring time for the top-10-scoring features per alert, written to `argus_gold.alert_candidates.shap_explanations` (struct column). Required for regulator defense — every elevated alert must be explainable.

---

## 9. GenAI Component (closes ARG-4)

### `argus_genai_str_drafter` — RAG pipeline

**Context source (vector store contents)**:
1. **Regulatory corpus**: 14 chapters of SEBI Master Circular on Surveillance of Securities Market (09-Jul-2024) + PFUTP Regulations 2003, paragraph-chunked (~1,800 chunks)
2. **MSE STR style guide + ~200 historical exemplar STRs** approved by Compliance (paragraph-chunked, ~600 chunks)
3. **ESM/ASM rule definitions** (~150 chunks)
4. **News headlines for relevant instrument over ±24h alert window** (from `argus_silver.news_enriched`, real-time fetched, ~10–30 chunks per request)

Static corpus 1–3 totals ~2,550 chunks; refreshed monthly.

**Vector store**: **Milvus** running on Cloudera AI Inference cluster.

**Embedding model**: Locally-hosted multilingual BGE variant on CAII (data sovereignty — DPDP requires India-resident processing).

**LLM**: Locally-hosted Llama 3.1 70B (or Mistral Large) on CAII GPU pool. Lab provides pre-configured CAII endpoint.

**Pipeline**:
```
Confirmed alert
  → fetch alert structured payload from argus_gold.alert_candidates
  → retrieve top-k relevant SEBI/PFUTP citations (k=5)
  → retrieve top-3 most similar historical exemplar STRs
  → fetch news headlines for instrument over ±24h window
  → assemble grounded prompt
  → call CAII LLM endpoint
  → parse response into structured JSON
  → write to analyst review queue (human-in-the-loop)
```

**Output format (structured JSON, 5 required sections)**:
```json
{
  "executive_summary": "<2 sentences>",
  "order_flow_narrative": "<200-400 word prose>",
  "quantified_market_impact": {
    "price_move_pct": "<float>",
    "volume_during_window": "<int>",
    "retail_account_exposure_inr": "<int>"
  },
  "suspected_violation_citation": "<specific PFUTP regulation, e.g., 'Reg 4(2)(e)'>",
  "recommended_next_steps": "<max 100 words>"
}
```

**Fallback behavior**:
- Malformed JSON → retry once with stricter prompt
- Still malformed → deterministic template-fill skeleton with `narrative_generation_failed=true` flag; analyst hand-writes prose
- System never silently fails; every confirmed alert gets *something* in the queue

**System prompt constraints (locked)**:
1. LLM **MUST NOT** fabricate prices, volumes, member firm names, or trader identifiers — these come only from the structured alert payload, passed in literally
2. LLM **MUST** cite the specific PFUTP regulation it alleges was violated; the regulation list is in the retrieval context
3. LLM **MUST NOT** assert intent — it can describe behavior consistent with manipulative intent but cannot conclude intent (only SEBI can find intent)
4. LLM **MUST NOT** compare the alert to specific historical SEBI orders or named cases (avoids prejudicial language)
5. Output language **MUST** be English (SEBI's working language) regardless of input language in news/context

**Human-in-the-loop**: Every generated narrative is reviewed and edited by an analyst before submission to Compliance. The system never auto-files an STR. Success metric is analyst time saved per report (target: 60min → 8min, an 87% reduction), not autonomy.

**Quality measurement**: Analysts rate each draft 1–5 during review. Average rolling rating tracked; <3.0 triggers prompt/retrieval pipeline review.

---

## 10. Compliance

### Regulatory regime

| Regulation | Jurisdiction | Article/Section | Workflow |
|---|---|---|---|
| **SEBI Master Circular on Surveillance of Securities Market** (SEBI/HO/ISD/ISD-PoD-2/P/CIR/2024/99 dated 09-Jul-2024) | India / SEBI | Multiple chapters | Real-time alert detection + STR filing + audit-trail integrity |
| **SEBI (PFUTP) Regulations 2003** | India / SEBI | Reg 4(2)(e), 4(2)(g) | Specific prohibitions on spoofing, layering, momentum ignition |
| **DPDP Act 2023 §6(4)** | India / DPDPA / Data Protection Board | Consent withdrawal | Cease processing on withdrawal; legitimate-use exception for statutory surveillance under §7 |
| **DPDP Act 2023 §12** | India / DPDPA / Data Protection Board | Right to erasure | Erase personal data on request unless retention required by other law |
| **DPDP Act 2023 §8(7)(a)** | India / DPDPA / Data Protection Board | Automatic erasure | Erase when specified purpose no longer served |
| **DPDP Rules 2025** | India / MeitY | Phased rollout from Nov-2025 | Operational mechanics of consent + erasure |

### DPDP §12 erasure workflow (analogous to GDPR Art. 17)

1. Erasure request submitted via Consent Manager (DPDP-registered) or directly to MSE DPO
2. Identity verification via DigiLocker / Aadhaar OTP
3. Workflow service (`gdpr_erasure_workflow.py` — kept this filename for framework compliance, despite the regulation actually being DPDP §12) writes erasure record to `argus_gold.consent_audit`
4. Spark job propagates erasure to all Bronze/Silver/Gold tables containing investor PAN
5. Vector store (Milvus) sweeps for embeddings derived from investor's data; deletes
6. Iceberg time-travel proof: `SELECT ... FROM argus_silver.member_master FOR SYSTEM_TIME AS OF '<pre-erasure ts>' WHERE pan = '<hashed_pan>'` returns rows; same query AS OF current returns 0 rows. Audit log preserved (consent_audit has `history.expire.enabled=false`).
7. **Statutory exception**: Surveillance order/trade data retained for SEBI's required retention period (currently 8 years per SEBI archival requirements) under DPDP §7 legitimate-use exception. The investor's *identity* is severed from the surveillance data via cryptographic hashing + key destruction (k-anonymized retention).

### DPDP §6(4) consent withdrawal enforcement (analogous to CCPA §1798.120 opt-out)

1. Investor withdraws consent for specific processing purposes (e.g., "marketing analytics," "third-party analytics")
2. Withdrawal recorded in `argus_gold.consent_audit`
3. Ranger row-level filter policy `dpdp_consent_filter` enforces that any query against personal-data tables filters out withdrawn investors *for non-statutory purposes*
4. Statutory surveillance queries (tagged by Atlas classification `SEBI_AUDIT_TRAIL`) bypass the filter under §7 legitimate-use exception

### Atlas classification tags (6 — locked)

| Tag | Applied to | Purpose |
|---|---|---|
| `PII_HIGH` | PAN, Aadhaar reference, full name, registered email, mobile | Strictest masking; column-level Ranger redaction for non-DPO roles |
| `PII_LOW` | Demat account number, member firm ID, trader ID | Hashed display for non-investigation roles |
| `FINANCIAL_SENSITIVE` | Order quantities, prices, P&L, position sizes | Restricted to surveillance + compliance + finance roles |
| `SURVEILLANCE_RESTRICTED` | Alert details, case files, STR drafts | Restricted to surveillance team + compliance; analyst access logged |
| `DPDP_CONSENT_REQUIRED` | All tables with investor PII | Triggers row-level consent filter at query time |
| `SEBI_AUDIT_TRAIL` | All surveillance + alert + disposition + STR data | Subject to 8-year retention; bypasses DPDP filter under §7 |

### Ranger policies (3 — locked)

| Policy | Type | Effect |
|---|---|---|
| `dpdp_consent_filter` | Row-level filter | Filters out investors with active DPDP §6(4) withdrawal from non-statutory queries |
| `pii_column_mask` | Column masking | Hashes/redacts PAN, Aadhaar, email, mobile for non-DPO roles |
| `surveillance_time_bound_access` | Time-bound row access | Analysts get access to a specific case's data only during the active investigation window; access auto-revoked on case close |

---

## 11. Synthetic Data Generator Spec

**File**: `data/generate_data.py` — Python 3, pandas + Faker + numpy.
**Reproducibility**: All randomness seeded via `--seed` (default 42); planted test cases land at fixed indices on every run.
**Output**: 14 files in `data/generated/` (gitignored).

### Generated files

| File | Rows | Format | Purpose |
|---|---:|---|---|
| `members.csv` | 380 | CSV | KAVACH member firm master |
| `traders.csv` | ~12,000 | CSV | Named human traders linked to member firms |
| `investors.csv` | 250,000 | CSV | End-investor accounts (sampled subset of 24M universe) |
| `instruments.csv` | 4,800 | CSV | Equities + ETFs + F&O contracts |
| `corporate_actions.csv` | 600 | CSV | Splits, bonuses, dividends, mergers (90-day window) |
| `surveillance_state.csv` | 120 | CSV | ESM/ASM flags + circuit-breaker bands |
| `consent_records.csv` | 250,000 | CSV | DPDP consent records per investor |
| `orders_synthetic.jsonl.gz` | ~50M | JSONL gzip | TARANG order events over 5 simulated trading days |
| `trades_synthetic.jsonl.gz` | ~3.5M | JSONL gzip | NIPATAN executed trades aligned with orders |
| `bbo_synthetic.jsonl.gz` | ~7M | JSONL gzip | Cross-exchange BBO ticks |
| `legacy_alerts_history.csv` | 4.8M | CSV | 7 years of alerts with analyst dispositions (ML training set) |
| `sebi_actions_feed.csv` | 800 | CSV | SEBI watchlist + debarment + ASM/ESM history |
| `news_headlines.csv` | 25,000 | CSV | Corporate announcements + news (90-day window) |
| `compliance_test_cases.csv` | 23 | CSV | Master index of planted test cases at indices 0–22 |

### Planted test cases (locked at indices 0–22)

**Indices 0–9 — Cross-product manipulation cases** (the framework's "multi-system customers" slot, repurposed for cross-product orchestration):

| # | Pattern | Member firm | Detection difficulty |
|---:|---|---|---|
| 0 | Layering in mid-cap pharma — 5 stacked non-bona-fide buy orders, all cancelled <200ms after bona-fide sell fills | BNXM-0042 | Medium |
| 1 | Spoofing in single-stock futures — 50K-lot buy held 1.4s, cancelled when cash follows; profit on pre-positioned cash short | BNXM-0117 | Medium |
| 2 | Marking-the-close in index option — concentrated aggressive buying in closing 10 min to push underlying toward strike where short puts held (Jane Street pattern) | BNXM-0231 | Hard |
| 3 | Quote-stuffing + momentum ignition in small-cap — 12,000 orders/sec burst for 8s | BNXM-0089 | Easy |
| 4 | Cross-product layering — fake depth in cash, real position in single-stock futures | BNXM-0042 | Hard |
| 5 | Wash trading between two related accounts at same member | BNXM-0276 | Medium |
| 6 | **Negative case**: legitimate market-maker high-cancel pattern that rules engine fires on — ML must deprioritize | BNXM-0001 (Tier-1 MM) | Critical for FP reduction |
| 7 | **Negative case**: legitimate news-driven momentum (real earnings reaction) | BNXM-0156 | Critical for FP reduction |
| 8 | **Edge case**: ambiguous pattern, clean member history — analyst judgment | BNXM-0203 | Hard |
| 9 | Sophisticated multi-day layering across full expiry week — only visible joining 5 sessions | BNXM-0117 | Hardest |

**Indices 10–14 — Fuzzy-match cases**: Five end-investors who exist in KAVACH under slightly different identifiers (PAN typo, name variant, two demat accounts at different brokers). Module 2 identity resolution must merge them. Required to detect coordinated manipulation across the multiple identities.

**Indices 15–19 — DPDP §6(4) consent withdrawal cases**: Five investors who have withdrawn consent for non-statutory processing purposes. Platform must filter them out of analytics/ML feature stores while *retaining* surveillance data under §7 legitimate-use exception.

**Indices 20–22 — DPDP §12 erasure cases**: Three investors with formal account closure who have invoked right to erasure. Platform must produce time-travel evidence of erasure across Bronze/Silver/Gold + vector store + audit log preservation.

---

## 12. Learning Objectives (one per non-setup module)

**Module 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection** (3.5 days, v1.2 amendment)
*What's broken*: ARG-1 — single-node CEP collapses at 150K events/sec; legacy detection latency of 30+ minutes makes intervention impossible for sub-second manipulation patterns.
*What you build*: A three-engine streaming architecture on the same Kafka backbone:
  - **NiFi flows** route source events into 9 Kafka topics with schema validation + DLQ
  - **Spark Structured Streaming** (JOB-01..04) lands events into Iceberg Bronze (the canonical persistence path)
  - **PyFlink CEP** (JOB-10) detects R-101 SPOOFING and R-102 LAYERING in sub-second windows over the live order stream and writes to `argus.realtime_alerts.v1`
  - **SSB SQL** (JOB-11) implements R-104 cross-product imbalance as a declarative streaming SQL pattern — analyst-driven, no Java/Python required
  - **Spark Structured Streaming** (JOB-12) persists the real-time alerts back to `gold.realtime_alert_stream` for cross-checking against batch detections from JOB-08
*Measurable outcome*: (a) Sustain 150K events/sec end-to-end with <30s p99 ingest-to-Bronze latency (CP-03); (b) PyFlink CEP detects planted Case 0 spoofing pattern in <800ms p99 (**CP-02b** — new); (c) SSB SQL pattern detects Case 2 cross-product imbalance and persists to `realtime_alert_stream` (**CP-04b** — new); (d) Same `event_id` from a planted spoofing case appears in BOTH `gold.alert_candidates` (Module 3 batch) AND `gold.realtime_alert_stream` (Module 1 streaming) within their respective latency budgets.

**Module 2 — Identity Resolution & Order Book Reconstruction**
*What's broken*: ARG-2 (part 1) — cannot reconstruct book state; cannot resolve same investor across identifiers.
*What you build*: SCD2 member master with fuzzy-match resolution; per-event order book reconstruction using Spark with Iceberg.
*Measurable outcome*: Reproduce book state at any historical microsecond timestamp with `FOR SYSTEM_TIME AS OF` query; resolve 5 fuzzy-match cases (10–14) into unified entities (CP-05, CP-06).

**Module 3 — Temporal & Cross-Product Feature Engineering**
*What's broken*: ARG-2 (part 2) — cannot express features for spoofing/layering/cross-product detection.
*What you build*: Sequential feature pipeline computing order-to-trade ratios, time-to-cancel distributions, layered-order-stack features, cross-product cash-vs-derivatives delta features over rolling windows.
*Measurable outcome*: All 10 manipulation cases (0–9) appear in `argus_gold.alert_candidates` with feature payloads; cases 6–7 ranked as low-priority by deterministic rules (CP-08, CP-09).

**Module 4 — Governed Views in CDW**
*What's broken*: ARG-5 (part 1) — coarse-grained access; no row/column governance on PII.
*What you build*: Impala governed views over Gold tables; Ranger row-level filter for DPDP consent; column masking for PAN/Aadhaar.
*Measurable outcome*: Surveillance role can query alert details with PAN masked; DPO role can see PAN; consent-withdrawn investors filtered from non-statutory queries (CP-10, CP-11).

**Module 5 — ML Alert Risk-Ranking**
*What's broken*: ARG-3 — 92% false-positive flood drowns analysts.
*What you build*: XGBoost training on 4.8M labeled alerts with Hyperopt search, MLflow tracking, batch scoring deployment.
*Measurable outcome*: Held-out AUC ≥ 0.82, top-decile precision ≥ 0.55; production scoring writes `model_score` to alert candidates every 5 min (CP-13, CP-14).

**Module 6 — GenAI STR Narrative Engine**
*What's broken*: ARG-4 — 40+ min hand-written STRs; 340-report backlog.
*What you build*: Milvus vector store with regulatory corpus + exemplar STRs; CAII LLM endpoint; RAG pipeline producing structured JSON STR drafts.
*Measurable outcome*: 5 confirmed alerts produce valid JSON drafts with all 5 required sections, no fabricated prices/names, correct PFUTP citation (CP-16).

**Module 7 — SDX Governance & DPDP Compliance**
*What's broken*: ARG-5 (part 2) — no lineage; no consent governance; no erasure workflow.
*What you build*: Atlas classification tags (6) on all PII columns; lineage capture across Bronze→Silver→Gold; DPDP §6(4) consent filter in Ranger; DPDP §12 erasure workflow with Iceberg time-travel proof.
*Measurable outcome*: All 6 Atlas tags applied; lineage graph visible end-to-end; cases 15–19 filtered correctly; cases 20–22 erased with time-travel proof and consent_audit preserved (CP-19 — **COMPLIANCE GATE**).

---

## 13. 10-Day Schedule (v1.2 amendment — Module 1 expanded to 3.5 days, Module 2 absorbs the 0.5-day squeeze)

| Day | Module(s) | Focus | Checkpoints |
|---:|---|---|---|
| 1 | Setup | S3 buckets, 11 Kafka topics (incl. `realtime_alerts.v1` + 2 DLQ), all 19 Iceberg DDLs, `generate_data.py`, FLOW-SIM oneshot bulk load | CP-00, CP-01 |
| 2 | Module 1 (Day 1 of 3.5) | NiFi flows + Spark Structured Streaming Bronze ingest (JOB-01..04); FLOW-SIM continuous mode | CP-02, CP-04 |
| 3 (full) | Module 1 (Day 2 of 3.5) | **PyFlink CEP fundamentals (morning)** + JOB-10 R-101 SPOOFING + R-102 LAYERING streaming detection. **SSB UI walkthrough (afternoon)** + JOB-11 R-104 cross-product imbalance SQL pattern | **CP-02b, CP-04b** (new) |
| 4 morning | Module 1 (Day 3 of 3.5 — wrap) | Combined-engine throughput test: 150K ev/s with NiFi + Spark + Flink + SSB all running simultaneously; batch-vs-stream latency comparison lab | CP-03 |
| 4 afternoon | Module 2 (Day 1 of 1.5) | Identity resolution kickoff (JOB-05 SCD2 + fuzzy-match) | — |
| 5 | Module 2 (Day 2 of 1.5) → Module 3 | Order book reconstruction (JOB-06); start temporal features (JOB-07) | CP-05, CP-06 |
| 6 | Module 3 | Cross-product features + alert candidate generation (JOB-08 batch rules — same patterns as JOB-10/11 but full-context) | CP-07, CP-08, CP-09 |
| 7 | Modules 4 + 5 (start) | CDW governed views + Ranger; XGBoost training kickoff | CP-10, CP-11, CP-12 |
| 8 | Module 5 (finish) + Module 6 | MLflow training complete + JOB-09 batch scoring; Milvus vector store + STR draft engine | CP-13, CP-14, CP-15, CP-16 |
| 9 | Module 7 | Atlas tags, DPDP §6(4) consent filter, DPDP §12 erasure workflow with time-travel proof | CP-17, CP-18, **CP-19 (COMPLIANCE GATE)** |
| 10 | Capstone | End-to-end integration test, presentation, final assessment | CP-20 |

**Schedule note**: Module 7 retains its full Day 9 (no squeeze on the COMPLIANCE GATE). Module 2 absorbs the 0.5-day shift caused by Module 1 expansion — Module 2's identity resolution + book reconstruction are technically substantial but failure isn't catastrophic to the overall capstone, so this is the safer slack absorber.

---

## 14. Assessment Rubric

Total: 100% + 5% bonus.

| Component | Weight | Minimum passing |
|---|---:|---|
| Module 1 — Streaming ingest + real-time detection (CP-02 / **CP-02b** / CP-03 / CP-04 / **CP-04b**) | 15% | 150K events/sec sustained; DLQ working; <30s p99 ingest-to-Bronze latency; PyFlink CEP detects Case 0 with p99 < 800ms; SSB SQL detects Case 2 within 60s |
| Modules 2–3 — Feature engineering (CP-05 through CP-09) | 25% | All 10 manipulation cases in alert candidates with correct features; book reconstruction passes time-travel test |
| Module 4 — Governed views (CP-10, CP-11) | 10% | Role-based views return correct masked/unmasked data |
| Module 5 — ML model (CP-12, CP-13, CP-14) | 20% | AUC ≥ 0.82, top-decile precision ≥ 0.55, model registered to MLflow Production stage |
| Module 6 — GenAI STR drafter (CP-15, CP-16) | 15% | 5 valid JSON drafts; system prompt constraints honored; no fabrication |
| Module 7 — Compliance & governance (CP-17, CP-18, CP-19) | 15% | **CP-19 is COMPLIANCE GATE — non-negotiable**; all 6 Atlas tags + 3 Ranger policies + erasure with time-travel proof |
| **Bonus** — Cross-product Jane Street replication | +5% | Detect case 2 (marking-the-close) via cross-product features alone, without rule engine |

**Pass threshold**: 70% overall AND CP-19 must pass. Failing CP-19 = capstone fail regardless of overall score.

---

## 15. 20 Lab Checkpoints (CP-00 through CP-20)

| CP | Module | Verifies | Pass condition |
|---|---|---|---|
| CP-00 | Setup | Environment up | All 11 Kafka topics exist with correct partitions; all 19 Iceberg tables created; `generate_data.py --dry-run` succeeds |
| CP-01 | Setup | Bulk data loaded | FLOW-SIM oneshot loads 50M orders into `argus.orders.v1`; 280M trades into `argus.trades.v1`; SMM shows correct partition distribution |
| CP-02 | Module 1 | NiFi ingest healthy | All 4 Bronze ingest jobs running; <0.01% DLQ rate over 10-min sample |
| **CP-02b** | **Module 1** | **Flink CEP detects spoofing in real-time** | **JOB-10 fires on planted Case 0 spoofing within p99 800ms of the cancel event; same `event_id` later appears in JOB-08 batch alert_candidates within 30 min** |
| CP-03 | Module 1 | Combined-engine throughput at peak | FLOW-SIM continuous mode at 150K events/sec sustained 10 min with NiFi + Spark + Flink + SSB all running; p99 ingest-to-Bronze latency <30s |
| CP-04 | Module 1 | Bronze tables populated | `argus_bronze.orders_raw` row count matches expected ±0.1% |
| **CP-04b** | **Module 1** | **SSB SQL pattern fires** | **JOB-11 (SSB) detects planted Case 2 cross-product imbalance and writes to `argus.realtime_alerts.v1` with `source_engine='SSB'`; row visible in `gold.realtime_alert_stream` within 60s** |
| CP-05 | Module 2 | Order book reconstruction works | `FOR SYSTEM_TIME AS OF` query reproduces book state at planted timestamp matching ground truth from generator |
| CP-06 | Module 2 | Fuzzy-match identity resolution | All 5 cases (10–14) merged into unified `argus_silver.member_master` entities |
| CP-07 | Module 3 | Temporal features computed | `argus_gold.member_temporal_features` populated for all 380 members × 5 days |
| CP-08 | Module 3 | Manipulation cases surfaced | All 10 cases (0–9) appear in `argus_gold.alert_candidates` |
| CP-09 | Module 3 | Cross-product features working | Case 2 (Jane Street pattern) shows `cross_product_delta_imbalance` ≥ threshold |
| CP-10 | Module 4 | Governed views — masking | Surveillance role sees PAN as `XXXXX****X`; DPO role sees full PAN |
| CP-11 | Module 4 | Governed views — consent filter | Cases 15–19 filtered from `vw_member_analytics`; visible in `vw_surveillance_audit` |
| CP-12 | Module 5 | MLflow tracked training | `argus_alert_ranking_v1` experiment shows ≥50 runs from Hyperopt search |
| CP-13 | Module 5 | Model performance | Best model AUC ≥ 0.82, top-decile precision ≥ 0.55 |
| CP-14 | Module 5 | Production scoring | JOB-09 writes `model_score` to alert candidates every 5 min; SHAP values populated |
| CP-15 | Module 6 | RAG retrieval | Query for "layering in mid-cap pharma" returns case 0 + relevant SEBI/PFUTP citations |
| CP-16 | Module 6 | STR drafts valid | 5 confirmed alerts → 5 valid JSON drafts; no fabricated prices/names; correct PFUTP citation |
| CP-17 | Module 7 | Atlas tags applied | All 6 classification tags applied to expected columns; Atlas lineage shows full Bronze→Gold chain |
| CP-18 | Module 7 | Ranger policies active | Cases 15–19 filtered from non-statutory queries; statutory queries (tagged `SEBI_AUDIT_TRAIL`) bypass filter |
| CP-19 | Module 7 | **COMPLIANCE GATE** — DPDP §12 erasure | Cases 20–22 erased; `FOR SYSTEM_TIME AS OF` proves data existed before, gone after; `consent_audit` row preserved |
| CP-20 | Capstone | End-to-end integration | Full pipeline runs against fresh data; planted manipulation case detected, scored, narrated, governed — single thread from ingest to STR |

---

## 16. Delivery Artifacts

| Field | Value |
|---|---|
| **Mode** | C — Both, repo-first, HTML published via GitHub Pages |
| **Repo name** | `argus-capstone` |
| **Repo URL** | `github.com/cloudera-training/argus-capstone` |
| **Pages URL** | `cloudera-training.github.io/argus-capstone/` (Pages enabled, source = `main` branch root, entry = `capstone.html`) |
| **Branch strategy** | `main` = student-facing starter (skeleton implementations in `src/`); `solutions` = instructor reference branch in same repo, access-restricted via GitHub team membership |
| **License** | Apache 2.0 |
| **Cohort tagging** | `v1.0-spring2026` (initial); subsequent cohorts: `v1.0-summer2026`, `v1.1-fall2026`, with minor-version bumps for content changes between cohorts |
| **HTML file at repo root** | `capstone.html` (also keep `argus_combined.html` as alias) |
| **Asset handling** | SVG diagrams in `assets/`; HTML embeds via `<img src="assets/architecture.svg">` (relative paths only) |

---

## 17. Naming Convention — Per-Student Namespacing (v1.1 amendment)

The cohort runs on a **shared CDP cluster** (16 students + 4 instructors = 20 users). To prevent students from colliding on shared resources, every namespaceable resource embeds `${STUDENT_ID}` in its physical name. The full convention is locked here; it must not vary across implementations.

### Convention table

| Resource | Logical name | Physical name |
|---|---|---|
| Kafka topics | `argus.<topic>` | `argus.${STUDENT_ID}.<topic>` |
| Iceberg schemas | `argus_<layer>` | `argus_${STUDENT_ID}_<layer>` (layers: bronze/silver/gold/views) |
| MLflow experiment | `argus_alert_ranking_v1` | `argus_${STUDENT_ID}_alert_ranking_v1` |
| MLflow registered model | `argus_alert_ranker` | `argus_${STUDENT_ID}_alert_ranker` |
| CDE Spark application | `argus.<app>` | `argus.${STUDENT_ID}.<app>` |
| **Flink job name (JOB-10)** | `argus_realtime_cep` | `argus_${STUDENT_ID}_realtime_cep` |
| **SSB job name (JOB-11)** | `argus_cross_product_imbalance` | `argus_${STUDENT_ID}_cross_product_imbalance` |
| **Flink savepoint path** | n/a | `s3a://${BUCKET_NAME}/checkpoints/flink/${STUDENT_ID}/` |
| **Flink state TTL** | n/a | 60s for windowed CEP state (avoid runaway memory) |
| Kafka consumer group | `argus.<group>` | `argus.${STUDENT_ID}.<group>` |
| Milvus vector collection | `argus_str_corpus` | `argus_${STUDENT_ID}_str_corpus` |
| Atlas classification tag | `<TAG>` (e.g. `PII_HIGH`) | `<TAG>_${STUDENT_ID}` (suffix form) |
| S3 bucket | n/a — instructor-provisioned | passed via `BUCKET_NAME` env var |
| S3 checkpoint paths | `s3a://<bucket>/checkpoints/<job>/` | already namespaced via the per-student bucket |

### `STUDENT_ID` validation

Valid format: `^[a-z][a-z0-9]{2,15}$` — lowercase letters and digits, starting with a letter, 3–16 characters total. Examples that conform: `s001`, `priya23`, `bv01`. Examples that do not: `S001` (uppercase), `42student` (starts with digit), `ab` (too short), `priya-23` (hyphen disallowed).

### Atlas tag handling

Per the cohort design, each student creates **their own copies** of the 6 PRD-locked classification tags rather than sharing a global tag definition. The logical names remain as locked in §10:

- `PII_HIGH`, `PII_LOW`, `FINANCIAL_SENSITIVE`, `SURVEILLANCE_RESTRICTED`, `DPDP_CONSENT_REQUIRED`, `SEBI_AUDIT_TRAIL`

Each student applies them to their own per-student schema as `PII_HIGH_${STUDENT_ID}`, `PII_LOW_${STUDENT_ID}`, etc. Students never see or modify each other's tags. The trade-off is verbose tag names in the Atlas UI; the gain is true isolation.

### Implementation

A single source-of-truth helper module — `src/common/naming.py` — exposes resolver functions: `topic()`, `schema()`, `fqtn()`, `mlflow_experiment()`, `mlflow_model()`, `cde_job()`, `consumer_group()`, `milvus_collection()`, `atlas_tag()`, `s3_bucket()`. Every Spark job, ML pipeline, and governance script imports from this module rather than hardcoding names. SQL DDL and shell scripts use shell-variable expansion (`${STUDENT_ID}`) directly via `envsubst`. NiFi flow JSONs use `{{STUDENT_ID}}` placeholder substitution at flow-import time.

### Required environment variables (set once per session)

```
STUDENT_ID    — assigned by instructor, matches the regex above
BUCKET_NAME   — full S3 bucket name given by instructor (any naming scheme)
KAFKA_BROKERS — Kafka broker host:port list
AWS_REGION    — ap-south-1 (Mumbai) for DPDP residency
```

If `STUDENT_ID` is missing, every Python source file fails immediately with a clear error directing the student to set it. This fail-fast behavior prevents accidental writes to wrong namespaces.

### Cluster sizing implication

A 20-user cohort produces roughly:

- **200 Kafka topics** (10 topics × 20 students)
- **80 Iceberg schemas** (4 layers × 20 students)
- **20 MLflow experiments + 20 registered models**
- **120 Atlas classification tags** (6 logical × 20 students)
- **20 Milvus collections**

Cluster admins should size accordingly. Topic count is the most visible — plan for 500+ topics on the broker to leave headroom for v2 streams in future cohorts. None of the per-student resources individually consume substantial storage; what matters is total partition count and metadata-service throughput.

---



This PRD is the locked source of truth for Phase 3 rendering. All technical specifications above (table names, job IDs, Kafka topics, partition counts, ML hyperparameter ranges, performance thresholds, system prompt constraints, planted test case indices, Atlas tags, Ranger policies, schedule, assessment rubric, and 20 checkpoints) cannot be changed during rendering without explicit user re-approval and a PRD version bump to `ARG_prd_v2.md`.
