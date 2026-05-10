# Module 2 Primer — Read This Before Lab 2.1

> 📊 **Visual reference**: [Module 2 identity + book reconstruction](../assets/diagrams/03_module2_cde_identity.md) ([SVG](../assets/diagrams/03_module2_cde_identity.svg))

> 👋 **New to SCD2, fuzzy matching, or Iceberg time-travel?** This primer is for you. Module 2 is conceptually dense in 2 short labs — please read this first. About 20 minutes.

This is a **primer**, not a procedure. The actual hands-on work is in `labs/lab-2-1-order-book-reconstruction.md` and `labs/lab-2-2-fuzzy-match.md`. Read this first; do those next.

## The big picture in one paragraph

Module 2 is where ARGUS becomes able to reason about *who* and *when*. Module 1 ingested events; today we resolve identities (across brokers, across PAN typos, across name variants) and reconstruct the order book at any past timestamp using Iceberg time-travel. Two short labs, one big payoff: **the 11-week SEBI reconstruction project that the legacy platform demanded becomes a 3-line SQL query** (Lab 2.1), and **the multi-broker manipulation pattern that defeated rule-based surveillance gets resolved into a single canonical entity** (Lab 2.2). By the end of Module 2, every Module 3+ feature can be computed against the right person and the right historical state.

## The technologies and concepts you'll meet

### SCD-Type-2 (Slowly Changing Dimension)

A pattern for tracking *changes over time* in a dimension table. Instead of overwriting (`UPDATE` the row when the investor's name changes), SCD2 **inserts** a new row with `effective_from = today` and updates the previous row's `effective_to = today - 1ms`. The old row stays. The new row stays. A boolean `is_current` flag (or a NULL `effective_to`) marks the latest.

Why this matters for ARGUS: a manipulation case from January 2025 needs to be evaluated against the trader's profile *as it existed in January 2025*, not their profile today. With SCD2 you can write `WHERE effective_from <= '2025-01-15' AND (effective_to IS NULL OR effective_to > '2025-01-15')` and get the right historical row. Without SCD2 (just `UPDATE` semantics), the historical context is lost and audit becomes guesswork.

The cost: the table grows over time. `member_master` won't shrink even when an investor closes their account — their last row stays with `effective_to` set. For PII like investor names and PANs that's a feature: the audit trail for any past trade is still queryable. Combined with DPDP §12 erasure (Module 7), the SCD2 chain gets cleared with cryptographic proof when erasure happens.

In Lab 2.2 you'll write to `silver.member_master` with SCD2 semantics. Always filter `WHERE is_current` for "current state" queries; remove that filter for historical lookups.

### Fuzzy matching — beyond exact comparison

The 5 planted multi-broker manipulation cases (indices 10–14) require fuzzy matching to resolve. Each case has the same human (call him Tarun) appearing as three different "investors" across two or three brokers, with three signal variations:

1. **PAN typo** — `ABCDE1234F` at one broker, `ABCDE1294F` at another (one digit differs). Exact match fails.
2. **Name spelling variant** — "Tarun Patel" at one broker, "Tarun H Patel" at another. Exact match fails.
3. **Email variant** — `tarun@gmail.com` at one broker, `tarun.patel@gmail.com` at another. Exact match fails.

ARGUS's identity-resolution job (`JOB-05`) uses three fuzzy-match primitives:

- **Levenshtein edit distance** — counts character insertions/deletions/substitutions. PAN with 1-char typo has edit distance 1. The rule is `levenshtein(pan_a, pan_b) <= 1`.
- **Normalized name comparison** — strip whitespace, lowercase, remove honorifics, then exact-match. "Tarun Patel" and "tarun  patel" become identical.
- **Email local-part match** — split on `@`, compare the local part with same edit-distance rule.

JOB-05 declares two records the same entity if **at least 2 of the 3 signals match**. The `2-of-3` rule is the trade-off: 1-of-3 produces too many false merges (every Tarun in your data merges into one entity); 3-of-3 misses the multi-broker pattern (because no manipulator nails all three identifiers).

> 💡 **The 2-of-3 rule is calibrated for the lab data.** Real production identity resolution uses dozens of signals (date of birth, mobile, address, demat ID, bank account) with weighted ML-based matchers. The lab uses 3 signals so the algorithm fits in 100 lines of Python. Lab 2.2's Common Failure Mode #1 covers what to tune if your production data has different signal quality.

### Order-book reconstruction with Iceberg time-travel

The order book is the per-instrument state at any moment: the stack of bid orders (who's willing to buy at what price) and ask orders (who's willing to sell at what price). It changes thousands of times per second on a busy instrument.

Traditional surveillance had to *replay* events from a log to reconstruct the book at a target timestamp — slow, error-prone, manual. With Iceberg time-travel, you store snapshots of the book state at regular intervals (e.g., every second) and use `FOR SYSTEM_VERSION AS OF <snapshot_id>` or `FOR SYSTEM_TIME AS OF <ts>` to query the state at any past moment in milliseconds.

Module 2's `JOB-06` produces `gold.order_book_snapshots` — one row per (instrument, snapshot_ts) with the full bid/ask stacks as JSON. To reconstruct the book at the moment of a planted layering case:

```sql
SELECT bids, asks, bid_depth_total, ask_depth_total, spread_bps
FROM argus_${STUDENT_ID}_gold.order_book_snapshots
WHERE instrument_code = 'BNXM-0042'
  AND snapshot_ts = '2026-03-15T11:23:42'
LIMIT 1;
```

That's the 3-line query that took 11 weeks of engineering on the legacy platform.

> 💡 **What snapshot resolutions exist?** JOB-06 produces 1-second snapshots by default. For deep investigations, drilldown queries can request 100-millisecond or per-event resolution — but those are 600× and 60,000× more rows respectively. The lab generates 1-second only; production tunes resolution per investigation.

### The `entity_id` — what links cross-broker rows

The output of fuzzy matching is an `entity_id` — a deterministic hash of the canonical key that all variants resolve to. Three rows in `silver.member_master` with different `investor_acct`, different `member_firm_id`, slightly-different `investor_pan` values can share the same `entity_id`. That's the link that makes cross-broker manipulation visible.

Module 3 will compute features against `entity_id`, not `investor_acct`. That's how Tarun's coordinated activity across 3 brokers becomes one trader's behavior, with combined volume that easily clears the manipulation threshold.

### Order book vs trade tape — different things

The **order book** is intent: orders that have been placed but not yet executed. It's a live state.
The **trade tape** is execution: a log of orders that *did* execute, by whom, at what price.

Module 2's reconstruction is for the **order book**. The trade tape is straightforward — Bronze already has it in `trades_raw`. The interesting reconstruction is the order book because layering and spoofing are pre-execution patterns that the trade tape alone can't see.

## What Module 2 closes — ARG-2 part 1

ARG-2 has two parts:
1. **Cannot reconstruct order book state on demand** — closes in Lab 2.1 (CP-05). 11 weeks → 3 lines.
2. **Cannot resolve same person across multiple identifiers** — closes in Lab 2.2 (CP-06). Multi-broker manipulator pattern resolved.

ARG-2 part 2 (temporal/cross-product features) closes in Module 3.

## What Module 2 sets up — the lab-by-lab map

Two labs:

| Lab | What you do | Checkpoint | Time |
|---|---|---|---|
| 2.1 — Order book reconstruction | Run JOB-06; verify book state at planted Case 0 layering window via time-travel | CP-05 | ~60 min |
| 2.2 — Fuzzy-match identity resolution | Run JOB-05; verify all 5 fuzzy-match cases (indices 10–14) merged into single entities | CP-06 | ~75 min |

> 💡 **Why does Lab 2.1 reference Lab 2.2's outputs?** JOB-06 (book reconstruction) joins against `silver.member_master` (JOB-05's output) and `silver.instrument_master`. So technically Lab 2.2 must run before Lab 2.1 to populate the masters. The lab numbering is a presentation choice — the *concept* of book reconstruction (Lab 2.1) is more intuitive than fuzzy matching (Lab 2.2), so we present it first. **Operationally, run JOB-05 before JOB-06.** The labs' prereq checklists make this explicit.

## Things you'll find confusing the first time

### "What's the difference between investor_acct, entity_id, and member_firm_id?"

- **`investor_acct`** — a per-broker account ID. Tarun has 3 of these (one per broker).
- **`entity_id`** — the canonical person ID after fuzzy resolution. Tarun has 1.
- **`member_firm_id`** — the broker (firm) the account is held at. Tarun's 3 accounts span 3 brokers, so 3 firm IDs.

When Module 3 computes "Tarun's daily volume", it sums across all `investor_acct` values that share the same `entity_id`.

### "Why does SCD2 history grow forever?"

Because audit. A trade from 2024 needs to be evaluated against the trader's 2024 profile, not their 2026 profile. Iceberg's storage is cheap; the audit value is high. Module 7's DPDP §12 erasure clears the chain when legally required, with proof.

### "Time-travel returns 'snapshot not found' — what's wrong?"

Iceberg snapshot expiration. Default is 5 days. For tables that need long retention (`consent_audit`, `order_book_snapshots` for old investigations), set `history.expire.enabled = false` or extend the retention window. Lab 2.1 and Module 7 both have this in their failure-mode sections.

### "JOB-05 says 'merged 12,495 entities' but I have 12,505 input rows. Where did 10 go?"

That's exactly right — the 10 are the variants from the 5 fuzzy-match cases (5 cases × 2 variants each). They got merged INTO their base entities, not added as separate rows. The output count = (base unique entities) regardless of how many variant rows fed in.

## Success at end of Module 2

By the time you finish Lab 2.2, you should be able to:

- Reconstruct the order book at any past timestamp using one Impala SQL query
- Explain why exact-match identity resolution misses sophisticated manipulators
- Read JOB-05's fuzzy-match logic and predict which input rows will merge
- Distinguish between `investor_acct`, `entity_id`, and `member_firm_id` and pick the right one for any analytical question
- Trace SCD2 history for a specific entity, including effective windows

## What's NOT in Module 2

- New ingest paths (Module 1)
- Temporal feature engineering (Module 3)
- ML model training (Module 5)
- GenAI / RAG (Module 6)
- Compliance enforcement (Module 7)

If you find yourself wanting to compute a feature ("Tarun's last-5-day cancel ratio") — that's Module 3, on top of Module 2's outputs.

---

When you're ready, head to [Lab 2.1 — Order Book Reconstruction](../labs/lab-2-1-order-book-reconstruction.md) (operationally Lab 2.2 must run first; the labs' prereqs make this explicit). Allow about 75 minutes for Lab 2.2, then ~60 minutes for Lab 2.1.
