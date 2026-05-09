# Module 2 — Identity Resolution & Order Book Reconstruction

> 📊 **Visual reference**: [Module 2 identity + book reconstruction](../assets/diagrams/03_module2_cde_identity.md) ([SVG](../assets/diagrams/03_module2_cde_identity.svg))

> **Closes deficiency:** ARG-2 part 1 — cannot reconstruct order-book state, cannot resolve same investor across identifiers
> **Day:** 3–4
> **Checkpoints:** CP-05, CP-06
> **Weight:** part of the 25% allocated to Modules 2–3

## What's broken

Two related failures define ARG-2's first half. First, the legacy platform stores only firing alerts, never the underlying event sequences that produced them. When SEBI requested order-book reconstruction for the 14 missed manipulation episodes, MSE engineering took 11 weeks to reproduce and the reproductions were partial because the upstream raw event archive had a 90-day retention. Second, when a manipulator places coordinated orders through three brokers under three slightly different KYC records — a PAN typo here, a name spelling variant there, two demat accounts at different brokers — the legacy platform treats them as three independent participants. Sophisticated layering and circular trading patterns are simply invisible to a system that can't merge identities.

## What you build

Two CDE / Spark batch jobs that turn raw Bronze events into the Silver and Gold structures the rest of the platform consumes.

`JOB-05 silver_identity_resolution` builds `argus_${STUDENT_ID}_silver.member_master` as an SCD2 with effective-from / effective-to windows, and runs fuzzy-match resolution that merges investor records when at least two of three signals align: PAN edit-distance ≤ 1, normalized name similarity ≥ 0.85, or demat-account prefix match. Merged records carry a canonical `entity_id` plus an array of `known_aliases` so investigators can see all the identifiers a single human-or-entity has used.

`JOB-06 silver_order_book_reconstruction` turns Bronze orders into enriched Silver `order_events` (with member firm category, instrument metadata, ESM/ASM flags joined in), then derives `argus_${STUDENT_ID}_gold.order_book_snapshots` at 1-second resolution covering the prior hour. For instruments that fire alerts, drilldown to 100-millisecond and per-event resolution is available on demand.

## CDP services used

- **Cloudera Data Engineering (CDE) / Apache Spark** — batch transforms for both jobs
- **Apache Iceberg** — Silver (COW/Parquet) target with SCD2 effective windows; Gold (COW/Parquet) snapshots
- **Iceberg time-travel** — `FOR SYSTEM_TIME AS OF` queries against `order_book_snapshots` answer "what did the book look like at instant T?"

## Source files

| File | Purpose |
|---|---|
| [`src/transform/job_05_silver_identity_resolution.py`](../src/transform/job_05_silver_identity_resolution.py) | SCD2 master + fuzzy-match resolution |
| [`src/transform/job_06_silver_order_book_reconstruction.py`](../src/transform/job_06_silver_order_book_reconstruction.py) | Per-second book snapshots |
| [`src/ingest/seed_member_cdc.py`](../src/ingest/seed_member_cdc.py) | One-time bulk-load to bootstrap `member_cdc` from the synthetic CSVs |

## Labs

| Lab | What it does | Checkpoint |
|---|---|---|
| [Lab 2.1 — Order Book Reconstruction](../labs/lab-2-1-order-book-reconstruction.md) | Run JOB-06, verify time-travel query reproduces book state at planted timestamp | CP-05 |
| [Lab 2.2 — Fuzzy-Match Identity Resolution](../labs/lab-2-2-fuzzy-match.md) | Run JOB-05, verify cases 10–14 merged into single canonical entities | CP-06 |

## Measurable outcome

By end of module:

- `argus_${STUDENT_ID}_silver.order_events` populated with member + instrument enrichment for the prior trading session
- `argus_${STUDENT_ID}_gold.order_book_snapshots` has at least one snapshot per active instrument per second
- `FOR SYSTEM_TIME AS OF` queries against `order_book_snapshots` return correct book state for arbitrary historical timestamps
- `argus_${STUDENT_ID}_silver.member_master` populated with SCD2 windows; all 5 fuzzy-match cases (10–14) merged into single canonical `entity_id` values
- `member_master` has `is_current = TRUE` rows summing to the source investor count plus 5 deduplicated entities

## What this fixes

Before ARGUS, an analyst investigating a layering allegation against member firm BNXM-0042 had to file a ticket with the engineering team and wait days for a partial book reconstruction. After ARGUS, the same analyst opens an Impala session and writes a 3-line query: `SELECT bids, asks FROM argus_${STUDENT_ID}_gold.order_book_snapshots WHERE instrument_code = 'X' AND snapshot_ts BETWEEN A AND B`. The reconstruction is exact because Iceberg snapshots preserve every event. And because identity resolution runs upstream, when the analyst joins to `argus_${STUDENT_ID}_silver.member_master`, an investor manipulating through three broker accounts shows up as one canonical entity — not three.

> 💡 **Tip:** The fuzzy-match algorithm runs an O(n × k) self-join blocked on PAN prefix. At full scale (24M investors) the blocking step is critical. If the PAN distribution is too clustered (synthetic generator quirk), some buckets balloon and the join times out. The fix is to add a second blocking key — name first 3 chars — but for the lab synthetic data the default is fine.

> ⚠️ **Compliance gate:** The `investor_pan` column in `member_master` is classified `PII_HIGH_${STUDENT_ID}` in Module 7. JOB-05 also writes `investor_pan_hash` (SHA-256 of PAN) for k-anonymized retention. Module 7's DPDP §12 erasure workflow uses the hash as the join key, so the hash MUST be computed identically in Bronze, Silver, and the consent_audit table — any drift breaks erasure proofs.
