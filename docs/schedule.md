# 10-Day Schedule

> 📊 **Visual reference**: [Master 10-day timeline](../assets/diagrams/00_master_overview.md) ([SVG](../assets/diagrams/00_master_overview.svg))

ARGUS runs for ten working days, mapped module-to-day with checkpoint gates that prevent students from advancing on a broken foundation. The schedule deliberately gives Module 1 (CDF/NiFi + PyFlink CEP + SSB SQL streaming ingest and real-time detection) 3.5 days because the three-engine architecture requires hands-on exposure to each engine; Module 2 absorbs a 0.5-day squeeze in exchange. Module 7 retains its full 1.5-day allocation — CP-19, the COMPLIANCE GATE, is non-negotiable and gets full breathing room.

## Daily plan

### Day 1 — Setup
**Module:** None — environment provisioning
**Checkpoints:** CP-00, CP-01

S3 bucket creation with `${STUDENT_ID}` namespacing. Eleven Kafka topics created with PRD partition counts (126 total — 9 production + 2 DLQ, including the new `realtime_alerts.v1` for v1.2 streaming-detection output). All 19 Iceberg tables created — 6 Bronze (MOR/ORC), 4 Silver (COW/Parquet), 9 Gold (COW/Parquet, including the new `realtime_alert_stream`). The Day 1 critical step is verifying `argus_${STUDENT_ID}_gold.consent_audit` has `history.expire.enabled=false` — this property is what makes Module 7's COMPLIANCE GATE provable. The synthetic data generator runs at `--scale 0.05` to produce 14 files including ~2.5M order events and 23 planted test cases at fixed indices 0–22. FLOW-SIM oneshot mode loads the data into Kafka.

### Day 2 — Module 1 (Day 1 of 3.5)
**Module:** 1 — CDF + Flink + SSB Streaming Ingest & Real-Time Detection (NiFi + Spark Bronze)
**Checkpoints:** CP-02, CP-04

NiFi flows imported and started — four flows handling TARANG, NIPATAN, KAVACH CDC, and the consolidated external feeds. Spark Structured Streaming jobs JOB-01 through JOB-04 deployed to CDE with 6 executors apiece. Bronze tables start populating. CP-02 verifies all four flows are healthy with DLQ rate <0.01%; CP-04 verifies Bronze row counts match Kafka offsets ±0.1%.

### Day 3 — Module 1 (Day 2 of 3.5)
**Module:** 1 — CDF + Flink + SSB (PyFlink CEP morning, SSB SQL afternoon)
**Checkpoints:** CP-02b, CP-04b

Morning: PyFlink CEP fundamentals, then deploy JOB-10 to detect R-101 SPOOFING and R-102 LAYERING patterns on the live order stream with sub-second latency. CP-02b verifies p99 detection latency <800ms on a planted Case 0 spoofing event. Afternoon: SQL Stream Builder UI walkthrough, then deploy JOB-11 — declarative SSB SQL implementing R-104 cross-product imbalance (the Jane Street pattern) without any Python or Java. CP-04b verifies the SSB job fires on Case 2 and the alert lands in `gold.realtime_alert_stream` within 60s.

### Day 4 — Module 1 wrap (morning) → Module 2 start (afternoon)
**Module:** 1 finishing + 2 starting
**Checkpoints:** CP-03

Morning: combined-engine throughput test. FLOW-SIM continuous mode at 150,000 events/sec for 10 minutes with NiFi + Spark + Flink CEP + SSB all running simultaneously. The latency probe verifies p99 ingest-to-Bronze latency stays under 30 seconds while the streaming engines continue producing real-time alerts. CP-03 — the proof that ARG-1's peak-volume crisis is solved on both axes (throughput AND detection latency). Afternoon: Module 2 begins — running the seed loader for `argus_${STUDENT_ID}_bronze.member_cdc` from the synthetic CSVs and starting JOB-05 (identity resolution).

### Day 5 — Module 2 (Day 2 of 1.5) → Module 3 start
**Module:** 2 — Identity Resolution & Order Book Reconstruction (finishing) + 3 starting
**Checkpoints:** CP-05, CP-06

JOB-05 completes populating `argus_${STUDENT_ID}_silver.member_master` with SCD2 effective windows, including the multi-signal fuzzy-match resolution that merges the 5 planted cases at indices 10–14 (CP-06). JOB-06 runs to produce per-second order-book snapshots in `argus_${STUDENT_ID}_gold.order_book_snapshots`. The CP-05 test uses an Iceberg `FOR SYSTEM_VERSION AS OF` time-travel query to prove the book can be reconstructed at any historical microsecond — the capability that took MSE 11 weeks pre-ARGUS. Late afternoon: start Module 3 (JOB-07 temporal feature engineering kicks off, runs into Day 6).

### Day 6 — Module 3
**Module:** 3 — Temporal & Cross-Product Feature Engineering
**Checkpoints:** CP-07, CP-08, CP-09

JOB-07 completes per-member temporal features (cancel rates, time-to-cancel distributions, layered-stack patterns) and per-member-per-underlying cross-product features (cash vs futures vs options imbalance — the Jane Street threshold). JOB-08 fires the deterministic rules engine, producing alert candidates for all 10 planted manipulation cases (0–9). The CP-09 test verifies Case 2 (the Jane Street pattern) shows `cross_product_delta_imbalance ≥ 7.0` and that R-104 fires a CRITICAL alert. Important sanity check this day: the same `event_id` from a Case 0 spoofing event should appear in BOTH `gold.alert_candidates` (from JOB-08 batch, latency 30 min) AND `gold.realtime_alert_stream` (from JOB-10 streaming, latency <800ms) — proving the streaming and batch detection paths agree on what's a manipulation pattern.

### Day 7 — Modules 4 + 5 (start)
**Module:** 4 — Governed Views in CDW + 5 — ML Alert Risk-Ranking (training kickoff)
**Checkpoints:** CP-10, CP-11, CP-12

7 governed views deployed to `argus_${STUDENT_ID}_views` schema. Three Ranger policy families applied: `dpdp_consent_filter` (DPDP §6(4) row filter), `pii_column_mask` (PAN/email/mobile masking), `surveillance_time_bound_access` (investigation-lead time-bounded row access). The CP-11 test switches roles between `surveillance_analyst` (sees masked PAN), `compliance_dpo` (sees full PAN, statutory bypass), and `research_analyst` (sees aggregates only, consent-withdrawn investors filtered). Late afternoon: kick off Hyperopt 50-trial Bayesian search over XGBoost hyperparameters logged to MLflow (CP-12 verifies the experiment registers with ≥50 runs).

### Day 8 — Module 5 (finish) + Module 6
**Module:** 5 finishing + 6 — GenAI STR Narrative Engine
**Checkpoints:** CP-13, CP-14, CP-15, CP-16

CP-13 verifies test-set AUC ≥ 0.82 and top-decile precision ≥ 0.55 — the operational thresholds for the platform to actually help analysts. Manual promotion from Staging to Production through the MLflow UI (compliance gate — never automated). JOB-09 deploys to score every pending alert every 5 minutes (CP-14). Mid-day: Module 6 begins. Milvus vector store built with the regulatory corpus (SEBI Master Circular + PFUTP), 200 exemplar STRs, and ESM/ASM rule definitions. The RAG engine generates STR drafts for the 5 real planted manipulation cases (0, 1, 2, 4, 5). CP-16 verifies all 5 drafts conform to the required JSON schema, contain no fabricated values, cite the correct PFUTP regulation per pattern type, and respect the 5 locked system-prompt constraints.

### Day 9 — Module 7
**Module:** 7 — SDX Governance & DPDP Compliance
**Checkpoints:** CP-17, CP-18, **CP-19 (COMPLIANCE GATE)**

All 6 Atlas classifications applied to their column targets; lineage graph visible Bronze→Silver→Gold. DPDP §6(4) consent withdrawal workflow runs for the 5 planted cases at indices 15–19; CP-18 verifies they're filtered for non-statutory roles and visible to the DPO via `vw_surveillance_audit`. The day's main event is **CP-19, the COMPLIANCE GATE** — the DPDP §12 erasure workflow runs for the 3 planted cases at indices 20–22, with Iceberg `FOR SYSTEM_VERSION AS OF` queries proving (a) the data existed pre-erasure, (b) is gone now, and (c) the audit trail is preserved. **CP-19 is non-negotiable.**

### Day 10 — Capstone
**Module:** Integration + assessment
**Checkpoints:** CP-20

End-to-end integration test: a fresh planted manipulation case is injected, flows through Bronze → Silver → Gold, fires an alert candidate, gets ML-scored, gets GenAI-drafted into an STR, and lands in the surveillance UI. Final assessment exam covers PRD knowledge, lab outcomes, and design rationales. Group presentations summarize the build for the rest of the cohort.

## Cumulative checkpoint progress

| End of day | Cumulative checkpoints passed | Cumulative deficiencies closed |
|---|---|---|
| Day 1 | CP-00, CP-01 | None — setup only |
| Day 2 | + CP-02, CP-04 | M1 partial — Bronze ingest live |
| Day 3 | + CP-02b, CP-04b | M1 partial — real-time detection live |
| Day 4 AM | + CP-03 | **ARG-1 closed** (peak throughput AND <800ms detection) |
| Day 5 | + CP-05, CP-06 | ARG-2 part 1 closed (book reconstruction + identity resolution) |
| Day 6 | + CP-07, CP-08, CP-09 | ARG-2 part 2 closed (temporal + cross-product features) |
| Day 7 | + CP-10, CP-11, CP-12 | ARG-5 part 1 closed (governance enforcement); M5 training kicked off |
| Day 8 | + CP-13, CP-14, CP-15, CP-16 | **ARG-3 closed** (false-positive flood) + **ARG-4 closed** (STR backlog) |
| Day 9 | + CP-17, CP-18, **CP-19** | ARG-5 part 2 closed; **all 5 deficiencies fully closed** |
| Day 10 | + CP-20 | Capstone complete |

## Prerequisites by day

- **Days 1–2** require an active CDP environment, `STUDENT_ID` assigned, `KAFKA_BROKERS` accessible, AWS CLI configured for `ap-south-1`
- **Day 3** requires CSA (Cloudera Streaming Analytics) and SSB (SQL Stream Builder) services available — instructor task
- **Day 4 PM** (Module 2 start) requires Day 4 morning Bronze tables to be non-empty (CP-04 must have passed)
- **Day 6** (Module 3) requires Day 5 Silver masters to be populated (CP-05 + CP-06 must have passed)
- **Day 8** (Module 5 finish) requires `argus_${STUDENT_ID}_bronze.legacy_alerts` to be batch-loaded (Module 1 / Lab 1.2)
- **Day 8** (Module 6) requires Milvus to be deployed on the CAI cluster (instructor sets up; not a student task)
- **Day 9** requires the Day 1 verification of `history.expire.enabled=false` on `consent_audit` to still hold

If a checkpoint fails on the day it's expected, the recommended path is to fix the failing checkpoint before advancing. Skipping forward results in compounding errors that take longer to debug than fixing the underlying issue.
