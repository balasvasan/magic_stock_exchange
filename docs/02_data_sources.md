# Data Sources

ARGUS pulls from eight source systems — five internal to MSE, three external. The five internal systems are named in Hindi and Sanskrit (TARANG, KAVACH, NIPATAN, PRATEEK, SMRITI), reflecting the convention at real Indian financial-services firms where internal systems frequently carry meaningful cultural names rather than English acronyms.

## Internal source systems

### INT-1 — TARANG (Matching Engine Telemetry)

The order-event firehose. TARANG ("wave" in Hindi) is MSE's in-house C++ matching engine emitting telemetry over a low-latency multicast feed: every order placed, modified, cancelled, partially filled, or fully filled, each event carrying microsecond-precision timestamps, member firm ID, trader ID, instrument code, side, order type, quantity, price, and the resulting book state at the affected price level.

| Attribute | Value |
|---|---|
| Vendor | MSE in-house |
| Data type | Order-book event firehose (NEW / MODIFY / CANCEL / PARTIAL_FILL / FULL_FILL) |
| Volume | 3.5B events/day steady-state; 9–12B on F&O expiry Thursdays |
| Update frequency | Real-time (target latency < 5ms from match engine to telemetry tap) |
| Ingestion method | NiFi consuming from a Kafka mirror of the multicast feed (multicast cannot cross the surveillance VLAN boundary) |
| Lands in | `argus_${STUDENT_ID}_bronze.orders_raw` via JOB-01 |

This single feed supplies most of the manipulation signal. ARG-1 (the peak-volume crisis that anchors Module 1) is fundamentally a TARANG-throughput problem.

### INT-2 — KAVACH (KYC & Member Master)

KAVACH ("armor" or "shield") is MSE's reference system for the 380 trading member firms (broker-dealers that are direct members of the exchange) and the ~24 million investor accounts they bring. Carries member-firm metadata (regulatory category, capital adequacy, suspension history, SEBI registration), trader-level data (each member firm has 1–500 named human traders with unique IDs that appear on every order), and end-investor reference (PAN, registered name, email, mobile, demat account, KYC tier).

| Attribute | Value |
|---|---|
| Vendor | MSE in-house, Oracle-backed |
| Data type | Member firm master + trader master + investor master (PII-heavy) |
| Volume | ~12M reference rows; ~50K daily change events |
| Update frequency | Hourly delta CDC |
| Ingestion method | Debezium → Kafka → NiFi → Bronze |
| Lands in | `argus_${STUDENT_ID}_bronze.member_cdc` via JOB-03 |

This is the personal-data system that drives most of ARG-5's compliance work. Module 7's Atlas classifications target columns of `argus_${STUDENT_ID}_bronze.member_cdc` and `argus_${STUDENT_ID}_silver.member_master` directly.

### INT-3 — NIPATAN (Trade Clearing & Settlement Feed)

NIPATAN ("settlement") is the post-match feed — every executed trade after it has been cleared and assigned to a clearing member. Carries trade ID, both legs (buyer member firm, seller member firm, end-investor account on each side if available), instrument, executed price, executed quantity, trade timestamp, settlement date, and the clearing-member flag.

| Attribute | Value |
|---|---|
| Vendor | MSE in-house, integrated with NSE Clearing Corporation |
| Data type | Executed trades post-clearing |
| Volume | ~280M events/day steady-state |
| Update frequency | Trade-by-trade, 50–200ms lag behind TARANG |
| Ingestion method | Direct Kafka producer from clearing system → NiFi → Bronze |
| Lands in | `argus_${STUDENT_ID}_bronze.trades_raw` via JOB-02 |

Critical for joining order-stream behavior to actual realized P&L — needed to compute "did the manipulator actually profit from this pattern?" The cross-product feature engineering in JOB-07 joins NIPATAN executed trades against TARANG order events on `member_firm_id` × `instrument_code` × time window.

### INT-4 — PRATEEK (Instrument & Corporate Action Reference)

PRATEEK ("symbol") is MSE's instrument master. Carries the full universe of listed equities, ETFs, F&O contracts (with expiry dates, strikes, lot sizes, tick sizes), corporate actions (splits, bonus issues, dividend dates, mergers), circuit-breaker bands per symbol, and the daily list of "Enhanced Surveillance Measure" (ESM) and "Additional Surveillance Measure" (ASM) symbols where SEBI has flagged elevated manipulation risk.

| Attribute | Value |
|---|---|
| Vendor | MSE in-house, integrated with NSDL/CDSL depositories |
| Data type | Instrument master + corporate actions + ESM/ASM state |
| Volume | ~600 corporate-action events / 90-day window; static reference ~7,600 rows |
| Update frequency | End-of-day batch for static reference; real-time for circuit-breaker / ESM state changes |
| Ingestion method | NiFi REST pull (static) + Kafka producer (state changes) |
| Lands in | `argus_${STUDENT_ID}_bronze.instrument_cdc` via JOB-04 |

PRATEEK joins to TARANG and NIPATAN to enrich every event with the instrument's surveillance status. Module 3's rule R-104 (cross-product) gates on `is_expiry_day` from PRATEEK; Module 4's `vw_alert_queue` joins to PRATEEK for sector and market-cap-bucket display.

### INT-5 — SMRITI (Legacy Alert & Analyst Disposition History)

SMRITI ("memory") is the historical archive from MSE's legacy vendor surveillance platform — every alert that fired between 2018 and the present, with the analyst's disposition (no-action, escalated, confirmed-manipulation, referred-to-SEBI), the analyst's free-text rationale, the case ID, the linked STR if one was filed, and the eventual SEBI outcome.

| Attribute | Value |
|---|---|
| Vendor | 2017-vintage third-party surveillance platform (being decommissioned) |
| Data type | Historical alerts (2018–present) with analyst dispositions and outcomes |
| Volume | ~4.8M historical alert records |
| Update frequency | End-of-day batch (append-only nightly export) |
| Ingestion method | Nightly S3 sync → Bronze batch load (NOT a Kafka topic) |
| Lands in | `argus_${STUDENT_ID}_bronze.legacy_alerts` via batch loader |

This is the labeled training data for Module 5's ML model — without these labels, supervised learning is impossible. The 92% no-action rate that defines ARG-3 is empirically observable in this table.

## External source systems

### EXT-1 — SEBI Watchlist & Action Feed

A controlled feed of regulatory actions and watchlist updates from SEBI: PFUTP orders, debarment lists, ASM/ESM additions and removals across all Indian exchanges (not just MSE), Show Cause Notices, and consent orders. Many of these are public, but MSE receives them through SEBI's regulator-to-exchange secure channel (SEBI Intermediary Portal — SI Portal) ahead of public release.

| Attribute | Value |
|---|---|
| Vendor | SEBI Intermediary Portal (SI Portal) |
| Data type | Regulatory actions + watchlist deltas across all Indian exchanges |
| Volume | ~800 actions/year; ~2,000 watchlist deltas/year |
| Update frequency | Real-time push for actions; daily batch for watchlist deltas |
| Ingestion method | NiFi pulling from SI Portal SFTP + webhook for urgent notifications |
| Lands in | `argus_${STUDENT_ID}_bronze.external_feeds` (source='SEBI') via JOB-04 |

Crucial for risk-ranking — a member firm with an active SEBI matter should have its alerts elevated. The `member_days_since_sebi_action` feature in Module 5's XGBoost ranker comes from this feed.

### EXT-2 — Cross-Exchange Reference & Best-Bid-Best-Offer Feed

A real-time feed of best bid and best offer prices on NSE and BSE for every symbol that's also listed on MSE. Used to detect manipulation patterns where a manipulator hits MSE because liquidity is thinner there, while the other venues continue to reflect "true" price.

| Attribute | Value |
|---|---|
| Vendor | Licensed market-data provider (Refinitiv-equivalent) |
| Data type | Real-time best-bid / best-offer ticks on NSE and BSE |
| Volume | ~500M BBO ticks/day |
| Update frequency | Real-time |
| Ingestion method | Vendor Kafka feed → NiFi |
| Lands in | `argus_${STUDENT_ID}_bronze.external_feeds` (source='BBO') via JOB-04 |

Cross-venue reference is also essential for spotting "spread sniping" and stale-quote arbitrage. A manipulator placing layered orders on MSE while the NSE BBO holds steady is a tell that the MSE depth is non-bona-fide.

### EXT-3 — News & Corporate Disclosure Feed

A real-time feed of corporate announcements (SEBI BSE/NSE filings, earnings, M&A disclosures) plus financial news headlines from a wire vendor. Used to differentiate manipulation from legitimate news-driven price moves: if a stock just moved 6% on an earnings beat at 3:24 PM, the order pattern around 3:24:30 PM is probably bona-fide momentum, not layering.

| Attribute | Value |
|---|---|
| Vendor | Wire vendor (Reuters/Bloomberg-equivalent) + SEBI/BSE/NSE corporate filings |
| Data type | Earnings announcements, M&A disclosures, regulatory filings, news headlines |
| Volume | ~25,000 headlines/quarter |
| Update frequency | Real-time |
| Ingestion method | Vendor REST/Kafka feed → NiFi |
| Lands in | `argus_${STUDENT_ID}_bronze.external_feeds` (source='NEWS') via JOB-04 |

EXT-3 also serves the GenAI module — the LLM uses news context when drafting STR narratives, helping it correctly characterize whether a price move was news-driven or manipulation-driven.
