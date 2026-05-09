# Assessment

ARGUS is graded across 6 weighted components plus a 5% bonus. The pass threshold is **70% overall AND CP-19 must pass**. CP-19 is the COMPLIANCE GATE — a non-negotiable pass condition. A student scoring 95% overall but failing CP-19 fails the capstone, because a surveillance platform that can't prove DPDP §12 erasure cannot be deployed at any Indian financial-services customer.

## Component rubric

| Component | Weight | Pass criterion |
|---|---:|---|
| Module 1 — Streaming ingest + real-time detection (CP-02 / **CP-02b** / CP-03 / CP-04 / **CP-04b**) | 15% | 150K events/sec sustained for 10 minutes; DLQ rate < 0.01%; p99 ingest-to-Bronze latency < 30s; **PyFlink CEP detects planted Case 0 with p99 < 800ms; SSB SQL detects Case 2 within 60s** |
| Modules 2–3 — Feature engineering (CP-05 through CP-09) | 25% | All 10 manipulation cases (0–9) appear in `argus_${STUDENT_ID}_gold.alert_candidates`; book reconstruction passes Iceberg time-travel test; Case 2 shows `cross_product_delta_imbalance ≥ 7.0` |
| Module 4 — Governed views (CP-10, CP-11) | 10% | Role-based views return correct masked/unmasked data; cases 15–19 filtered for `research_analyst`, visible to `compliance_dpo` |
| Module 5 — ML model (CP-12, CP-13, CP-14) | 20% | AUC ≥ 0.82, top-decile precision ≥ 0.55; model registered to MLflow Production stage via manual promotion; JOB-09 writes `model_score` + SHAP every 5 minutes |
| Module 6 — GenAI drafter (CP-15, CP-16) | 15% | 5 valid JSON STR drafts; system prompt constraints honored; no fabrication; correct PFUTP citation per pattern type |
| Module 7 — Compliance & governance (CP-17, CP-18, CP-19) | 15% | All 6 Atlas tags applied; 3 Ranger policies enforced; **CP-19 COMPLIANCE GATE**: DPDP §12 erasure provable via `FOR SYSTEM_VERSION AS OF` |
| Bonus — Cross-product Jane Street replication | +5% | Detect Case 2 (marking-the-close) via cross-product features alone, without rule R-104 firing — purely behavioral detection |

**Total possible:** 100% + 5% bonus.

**Pass threshold:** ≥ 70% overall AND CP-19 passes. Failing CP-19 = capstone fail regardless of overall score.

## 21 checkpoints (CP-00 through CP-20) + 2 sub-checkpoints

Every checkpoint has explicit pass conditions documented in the relevant lab file. The table below summarizes what each checkpoint verifies and the file that documents the pass condition in detail. Two sub-checkpoints (CP-02b, CP-04b) were added in v1.2 to verify the new PyFlink CEP and SSB SQL real-time engines respectively; they roll up under CP-02 and CP-04 for grading purposes but have their own pass conditions.

| CP | Module | Verifies | Pass condition (summary) | Lab file |
|---|---|---|---|---|
| CP-00 | Setup | Environment up | All 11 Kafka topics (9 production + 2 DLQ) with correct partition counts; all 19 Iceberg tables created; `consent_audit` has `history.expire.enabled=false` | [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md) |
| CP-01 | Setup | Bulk data loaded | FLOW-SIM oneshot loads ~2.5M orders into `argus.${STUDENT_ID}.orders.v1`; SMM shows even partition distribution | [`labs/lab-0-1-environment-provisioning.md`](../labs/lab-0-1-environment-provisioning.md) |
| CP-02 | Module 1 | NiFi/Kafka ingest healthy | All 3 streaming topics have traffic; partition skew < 10×; planted Case 0 visible in stream; DLQ empty | [`labs/lab-1-1-flow-sim.md`](../labs/lab-1-1-flow-sim.md) |
| **CP-02b** | Module 1 | **PyFlink CEP real-time detection** | JOB-10 detects planted Case 0 (R-101 SPOOFING) with **p99 < 800ms** measured over 100 alerts; alert lands in `realtime_alerts.v1` with `source_engine='FLINK'` | [`labs/lab-1-4-flink-cep.md`](../labs/lab-1-4-flink-cep.md) |
| CP-03 | Module 1 | Throughput at peak | 150K events/sec sustained 10 minutes; p99 ingest-to-Bronze latency < 30s; DLQ rate < 0.01% | [`labs/lab-1-3-throughput-test.md`](../labs/lab-1-3-throughput-test.md) |
| CP-04 | Module 1 | Bronze tables populated | All 6 Bronze tables have non-zero rows; row count matches Kafka offset within 0.1% | [`labs/lab-1-2-bronze-ingest.md`](../labs/lab-1-2-bronze-ingest.md) |
| **CP-04b** | Module 1 | **SSB SQL real-time detection** | JOB-11 detects planted Case 2 (R-104 cross-product imbalance) within 60s of the event window closing; alert lands in `realtime_alert_stream` with `source_engine='SSB'` | [`labs/lab-1-5-throughput-comparison.md`](../labs/lab-1-5-throughput-comparison.md) |
| CP-05 | Module 2 | Order book reconstruction | `FOR SYSTEM_VERSION AS OF` query reproduces book state at planted Case 0 timestamp; layered orders show > 5× depth asymmetry | [`labs/lab-2-1-order-book-reconstruction.md`](../labs/lab-2-1-order-book-reconstruction.md) |
| CP-06 | Module 2 | Fuzzy-match identity resolution | All 5 fuzzy-match cases (10–14) merged into single canonical `entity_id`; `known_aliases` arrays have ≥ 2 elements | [`labs/lab-2-2-fuzzy-match.md`](../labs/lab-2-2-fuzzy-match.md) |
| CP-07 | Module 3 | Temporal features computed | Feature distributions match expected member-category pattern (TIER1_MM > PROP_TRADER > INSTITUTIONAL > RETAIL_BROKER on cancel rate) | [`labs/lab-3-1-temporal-features.md`](../labs/lab-3-1-temporal-features.md) |
| CP-08 | Module 3 | All 10 manipulation cases surface | All planted cases (0–9) appear in `argus_${STUDENT_ID}_gold.alert_candidates`; 5 rules (R-101..R-105) all fire ≥ 1 alert | [`labs/lab-3-2-alert-candidates.md`](../labs/lab-3-2-alert-candidates.md) |
| CP-09 | Module 3 | Cross-product features detect Jane Street pattern | Case 2 (BNXM-0231) shows `cross_product_delta_imbalance ≥ 7.0`; R-104 fires CRITICAL severity | [`labs/lab-3-3-cross-product.md`](../labs/lab-3-3-cross-product.md) |
| CP-10 | Module 4 | Governed views deployed | All 7 views in `argus_${STUDENT_ID}_views` schema return rows; `vw_alert_queue` JOIN to member/instrument masters succeeds | [`labs/lab-4-1-governed-views.md`](../labs/lab-4-1-governed-views.md) |
| CP-11 | Module 4 | Ranger policies enforced | `surveillance_analyst` sees masked PAN; `compliance_dpo` sees full PAN; `research_analyst` filtered from cases 15–19 | [`labs/lab-4-2-ranger-policies.md`](../labs/lab-4-2-ranger-policies.md) |
| CP-12 | Module 5 | MLflow tracked training | ≥ 50 Hyperopt runs in `argus_${STUDENT_ID}_alert_ranking_v1` experiment; Hyperopt converges (best 5 trials' loss < median) | [`labs/lab-5-1-mlflow-training.md`](../labs/lab-5-1-mlflow-training.md) |
| CP-13 | Module 5 | Model performance | AUC ≥ 0.82, top-decile precision ≥ 0.55; planted real cases (0, 1, 2) score ≥ 2× planted negative cases (6, 7) | [`labs/lab-5-2-performance.md`](../labs/lab-5-2-performance.md) |
| CP-14 | Module 5 | Production scoring | Model promoted to Production via manual gate; JOB-09 runs every 5 minutes; ≥ 90% of pending alerts scored within 5 min | [`labs/lab-5-3-production-scoring.md`](../labs/lab-5-3-production-scoring.md) |
| CP-15 | Module 6 | RAG retrieval works | Milvus collection has ≥ 2,000 chunks across 3 source types; retrieval for "layering" returns PFUTP 4(2)(e) in top 3 | [`labs/lab-6-1-vector-store.md`](../labs/lab-6-1-vector-store.md) |
| CP-16 | Module 6 | STR drafts valid | 5 confirmed alerts produce 5 valid JSON drafts; no fabricated prices/names; correct PFUTP citation per pattern; no constraint violations | [`labs/lab-6-2-str-generation.md`](../labs/lab-6-2-str-generation.md) |
| CP-17 | Module 7 | Atlas tags applied | All 6 PRD-locked classifications registered + applied; lineage Bronze→Silver→Gold visible in Atlas UI | [`labs/lab-7-1-atlas-classifications.md`](../labs/lab-7-1-atlas-classifications.md) |
| CP-18 | Module 7 | Consent filter | Cases 15–19 filtered from non-statutory queries; statutory queries (`vw_surveillance_audit`) bypass filter under DPDP §7 | [`labs/lab-7-2-consent-withdrawal.md`](../labs/lab-7-2-consent-withdrawal.md) |
| **CP-19** | **Module 7** | **COMPLIANCE GATE — DPDP §12 erasure** | **Cases 20–22 erased; pre-snapshot `FOR SYSTEM_VERSION AS OF` returns row, current returns 0; `consent_audit` audit row preserved** | [`labs/lab-7-3-erasure.md`](../labs/lab-7-3-erasure.md) |
| CP-20 | Capstone | End-to-end integration | Full pipeline runs against fresh data; planted manipulation case detected, scored, narrated, governed — single thread from ingest to STR | (Day 10 final exam) |

## Deficiency-to-checkpoint mapping

Every PRD deficiency is closed by specific checkpoints; this is the explicit mapping that the grader uses to confirm full coverage:

| Deficiency | Checkpoints that close it |
|---|---|
| ARG-1 — Peak volume + real-time detection latency | CP-02, **CP-02b**, CP-03, CP-04, **CP-04b** |
| ARG-2 — Temporal feature gap (incl. cross-product) | CP-05, CP-06, CP-07, CP-08, CP-09 |
| ARG-3 — 92% false positive rate | CP-12, CP-13, CP-14 |
| ARG-4 — STR documentation backlog | CP-15, CP-16 |
| ARG-5 — No lineage / consent / erasure | CP-10, CP-11, CP-17, CP-18, **CP-19** |

A student who passes all 21 checkpoints has provably closed all 5 deficiencies — that's the operational definition of a successful capstone.

## Bonus criterion (5%)

The bonus is awarded for replicating the SEBI Jane Street detection without leaning on the rules engine. Specifically: write a SQL query against `argus_${STUDENT_ID}_gold.cross_product_features` that flags member-firm-underlying-date triples where the imbalance pattern alone — without R-104 firing — predicts the Case 2 manipulation. The query must produce Case 2 in its top 3 results across the synthetic data window without using `member_firm_id = 'BNXM-0231'` or any hard-coded identifier. This tests whether the student understands the *structural* pattern Jane Street represented, not just the rule that fires on it.

Bonus marking is by instructor review of the SQL query. Award is binary (full 5% or 0%) based on whether the query correctly identifies Case 2 using only behavioral features.
