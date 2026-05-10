# Module 3 — Temporal & Cross-Product Feature Engineering

> 📊 **Visual reference**: [Module 3 features + 5 rules](../assets/diagrams/04_module3_cde_features.md) ([SVG](../assets/diagrams/04_module3_cde_features.svg))

> 👋 **New to feature engineering or windowed aggregations?** Read [`docs/module-3-primer.md`](module-3-primer.md) first. About 15 minutes.


> **Closes deficiency:** ARG-2 part 2 — cannot express features for spoofing/layering/cross-product detection
> **Day:** 5
> **Checkpoints:** CP-07, CP-08, CP-09
> **Weight:** part of the 25% allocated to Modules 2–3

## What's broken

Even if you can replay the order book, that's not enough. Detecting spoofing means computing a distribution: for each member firm × instrument × day, what fraction of orders were cancelled within 50 milliseconds? Detecting layering means tracking the maximum number of distinct price levels at which a single member had simultaneous open orders on the same side. And detecting the Jane Street pattern — the marking-the-close manipulation that produced ₹4,843 crore in unlawful gains across 18 trading days — requires correlating cash equity flow with single-stock futures and options exposure on the same underlying, then flagging cases where the options delta is ≥ 7× the cash + futures position.

None of these features can be expressed as a deterministic SQL query against the legacy platform's flat order log. The legacy schema has rows; surveillance needs sequences.

## What you build

Two CDE / Spark batch jobs that turn enriched Silver into the analytics-ready Gold tables.

`JOB-07 gold_temporal_features` produces two outputs. `argus_${STUDENT_ID}_gold.member_temporal_features` carries the per-member sequential features — order/cancel counts, cancel rate, median and p95 time-to-cancel, percentage of orders cancelled within 50 ms, max simultaneous price levels, layered-stack count, order-to-trade ratios over rolling 1m / 5m / 30m windows, percent of orders placed above/below last-traded price, and percent of book depth owned. `argus_${STUDENT_ID}_gold.cross_product_features` computes per-member × underlying delta-equivalent positions across cash, futures, and options, plus the cross-product imbalance ratio (the Jane Street threshold), pre-close concentration, and pnl correlation indicators.

`JOB-08 gold_alert_candidates` then runs a deterministic rule engine over the feature tables to produce `argus_${STUDENT_ID}_gold.alert_candidates`. Five rules fire alerts:

- **R-101 SPOOFING** — single large order held > 800ms then cancelled
- **R-102 LAYERING** — three or more stacked non-bona-fide orders cancelled within 200ms of an opposite-side fill
- **R-103 MOMENTUM_IGNITION** — order rate > 1000/sec sustained for > 3 seconds
- **R-104 CROSS_PRODUCT** — `cross_product_delta_imbalance` > 7.0 (the Jane Street threshold)
- **R-105 WASH** — buy and sell from the same member firm crossing at the same price within 1 second

Each fired alert carries a 60-feature payload that JOB-09 (Module 5) will use to assign a probability of confirmed manipulation.

> ℹ️ **Note:** Regulators do not accept "the ML model decided not to fire the alert" — alerts must be deterministic and reproducible from rules, defensible in a SEBI inquiry. ML scoring is allowed for *prioritization* only. That separation is the architectural reason JOB-08 (rules) is split from JOB-09 (ML scoring).

## CDP services used

- **Cloudera Data Engineering (CDE) / Apache Spark** — windowed aggregations, self-joins for temporal features
- **Apache Iceberg** — append-only writes to `member_temporal_features`, `cross_product_features`, `alert_candidates`
- **Apache Airflow on CDE** — orchestrates the JOB-06 → JOB-07 → JOB-08 chain

## Source files

| File | Purpose |
|---|---|
| [`src/transform/job_07_gold_temporal_features.py`](../src/transform/job_07_gold_temporal_features.py) | Temporal + cross-product features |
| [`src/transform/job_08_gold_alert_candidates.py`](../src/transform/job_08_gold_alert_candidates.py) | Five-rule deterministic alert generation |

## Labs

| Lab | What it does | Checkpoint |
|---|---|---|
| [Lab 3.1 — Temporal Features](../labs/lab-3-1-temporal-features.md) | Run JOB-07; verify per-member feature rows + sanity-check time-to-cancel distributions | CP-07 |
| [Lab 3.2 — Alert Candidates](../labs/lab-3-2-alert-candidates.md) | Run JOB-08; verify all 10 manipulation cases (0–9) appear in alert_candidates | CP-08 |
| [Lab 3.3 — Cross-Product Detection](../labs/lab-3-3-cross-product.md) | Verify Case 2 (Jane Street pattern) shows imbalance ≥ 7.0 | CP-09 |

## Measurable outcome

By end of module:

- `argus_${STUDENT_ID}_gold.member_temporal_features` populated for all 380 members × active instruments × 5 days
- `argus_${STUDENT_ID}_gold.cross_product_features` populated with non-zero `cross_product_delta_imbalance` for every member with positions on both sides of the cash/derivatives boundary
- All 10 planted manipulation cases (0–9) appear in `argus_${STUDENT_ID}_gold.alert_candidates` with feature payloads
- Negative cases (6 — legitimate market maker, 7 — legitimate news-driven move) appear but with deterministic-rule severity that the ML model in Module 5 will deprioritize correctly
- Case 2 (Jane Street marking-the-close pattern) shows `cross_product_delta_imbalance` ≥ 7.0 — a value that R-104 fires on as CRITICAL severity

## What this fixes

Before ARGUS, the question "is BNXM-0042 a spoofer?" was answered by an analyst eyeballing a chart. After ARGUS, the same question is `SELECT pct_cancelled_under_50ms, layered_stack_count FROM argus_${STUDENT_ID}_gold.member_temporal_features WHERE member_firm_id = 'BNXM-0042' AND trade_date >= current_date - 30`. And the question "did anyone run a Jane Street-style scheme today?" is now `SELECT * FROM argus_${STUDENT_ID}_gold.alert_candidates WHERE rule_id = 'R-104' AND severity = 'CRITICAL' AND fired_ts > current_timestamp - interval 1 hour`. The features are the platform; everything downstream is consumption.

> 💡 **Tip:** JOB-07's layering detection uses a per-minute approximation rather than full per-event book reconstruction. The approximation is intentional — exact detection requires JOB-06's per-event snapshots, which are 100× the storage and unnecessary for the candidate generator. JOB-08's R-102 rule fires on the approximation; if a candidate looks suspicious, the analyst drills into the per-event reconstruction in Module 4's surveillance UI for the precise picture.
