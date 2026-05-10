# Module 3 Primer — Read This Before Lab 3.1

> 📊 **Visual reference**: [Module 3 feature engineering pipeline](../assets/diagrams/04_module3_features.md) ([SVG](../assets/diagrams/04_module3_features.svg))

> 👋 **New to feature engineering, windowed aggregations, or cross-product correlation?** This primer is for you. About 15 minutes.

This is a **primer**, not a procedure. The actual hands-on work is in Module 3's three labs. Read this first.

## The big picture in one paragraph

Module 3 turns the events and identities from Modules 1 and 2 into **features** — pre-computed signals that say things like "this member firm cancelled 87% of orders in under 50ms over the last 5 trading days" or "this member's combined cash + futures + options delta exposure on this underlying was unbalanced by 7.2× the normal ratio." Three labs, three outputs: temporal features (per member-firm × instrument × day), cross-product features (per member × underlying × day), and the alert candidates produced by deterministic rules over those features. By the end of Module 3, every Module 5 ML model has a 60-feature payload to score, and every Module 4 governed view has clean Gold tables to expose.

## Concepts you'll meet

### Temporal features — windowed aggregations

A "temporal feature" is a numeric summary computed over a sliding time window. Examples from `gold.member_temporal_features`:
- `cancel_rate` — fraction of NEW orders that were CANCELLED, over the last 5 trading days
- `median_time_to_cancel_ms` — median ms between NEW and CANCEL events on the same `parent_order_id`
- `pct_cancelled_under_50ms` — fraction of cancellations that happened in under 50ms (the spoofing signature)
- `layered_stack_count` — number of distinct price levels the member placed orders at within the same instrument-second

The mechanic is: SQL self-joins on `silver.order_events` keyed on `parent_order_id` for the NEW↔CANCEL pair; window functions for ratios and percentiles. Spark does the heavy lifting; the SQL itself is a few hundred lines.

### Why distributions matter more than thresholds

A 90% cancel rate is **normal** for a tier-1 market maker on a quiet day. The same 90% from a retail-broker proprietary desk on F&O expiry Thursday is **suspicious**. Module 3 produces features without judgment — it computes the numbers. Module 5's ML model learns the distribution context (what's normal for whom, when) and ranks alerts accordingly. The features are the substrate; the ranking is downstream.

### Cross-product features — the Jane Street pattern

The 2024–25 SEBI Jane Street order alleged ₹4,843 crore in unlawful gains across 18 trading days. The mechanic: take a directional position in cash equities + a directional position in futures on the same underlying + a much larger position in options on that underlying. The cash + futures push the index toward a strike. The options profit from the move.

To detect this, you need to correlate positions across three different *products* that are normally owned by three different trading-tech teams with separate databases. Most exchanges can't. ARGUS can because:
1. Identity resolution (Module 2) gives one `entity_id` for the trader across products.
2. Iceberg + Spark gives one query layer that joins cash, futures, and options data.
3. JOB-07 computes per-(member × underlying × day) `cross_product_delta_imbalance` — the metric that lights up the Jane Street pattern.

CP-09 (Lab 3.3) is the demonstration: planted Case 2 should show `cross_product_delta_imbalance ≥ 7.0` and trigger rule R-104.

### Rule-based alert candidates (deterministic, defensible)

Module 3's last job (`JOB-08`) fires deterministic rule-based alerts. The rules are simple thresholds:
- **R-101 SPOOFING**: `pct_cancelled_under_50ms ≥ 0.50 AND cancel_rate ≥ 0.85`
- **R-102 LAYERING**: `layered_stack_count ≥ 5`
- **R-103 WASH**: cancellation pattern with self-cross signature
- **R-104 CROSS_PRODUCT_IMBALANCE**: `ABS(cross_product_delta_imbalance) ≥ 7.0`

Why deterministic rules (not just ML)? Because **regulators don't accept "the ML decided not to fire" as a defense for missed manipulation.** The rule layer produces auditable, regulator-defensible candidates that any junior analyst can replay against historical data. The ML model in Module 5 only *ranks* them; it doesn't generate or filter. That separation is what keeps the platform legally defensible.

The cost: rules fire on legitimate market makers too. The 2 negative cases (Case 6 — legitimate tier-1 MM, Case 7 — legitimate news-driven move) WILL produce alerts in Module 3, and Module 5's ML model is what separates signal from noise. Don't try to fix this in Module 3 by tightening rules — you'll miss real manipulation. The rules are intentionally permissive.

## What Module 3 closes — ARG-2 part 2

ARG-2 part 1 closed in Module 2 (book reconstruction + identity resolution). ARG-2 part 2 closes here:
1. **No temporal feature engineering** — closes in Lab 3.1 (CP-07). 60-feature payload computed.
2. **No alert candidate generation** — closes in Lab 3.2 (CP-08). All 10 manipulation cases surface.
3. **No cross-product correlation** — closes in Lab 3.3 (CP-09). Case 2 (Jane Street) flagged.

## Module 3's labs

| Lab | What you do | Checkpoint | Time |
|---|---|---|---|
| 3.1 — Temporal features | Run JOB-07; verify cancel-rate distributions per category match expectations | CP-07 | ~60 min |
| 3.2 — Alert candidates | Run JOB-08; verify all 10 planted cases (0–9) surface as alerts | CP-08 | ~75 min |
| 3.3 — Cross-product detection | Verify Case 2's cross_product_delta_imbalance and R-104 alert | CP-09 | ~45 min |

## Things confusing the first time

### "Why does Case 6 (legitimate market maker) fire alerts?"

Because the rules use cancel_rate + time_to_cancel thresholds, and a tier-1 MM legitimately has high values for both. **This is the point.** Module 5's ML model will see Case 6 and Case 0 with similar feature values, but it's been trained on labeled history that says BNXM-0001 (the Case 6 firm) has a clean 5-year record while BNXM-0042 (Case 0) has prior SEBI matters. The model learns to deprioritize Case 6's alert. That ranking is what reduces the 92% false-positive rate to 30% (ARG-3).

### "Should I tune R-101's threshold up so Case 6 doesn't fire?"

**No.** Tightening rules to suppress legitimate-MM alerts will also suppress some real manipulation. The right answer is: keep rules permissive, deprioritize via ML, surface the top-K to analysts. CP-08 verifies all 10 planted cases fire — including Case 6 and Case 7. That's by design.

### "Cancel rate is 1.0 for every member — what's wrong?"

Almost certainly the join between NEW and CANCEL events is broken. JOB-07 joins `parent_order_id` on cancels to `event_id` on news; if synthetic data has NULL `parent_order_id` on cancels, the join fails and every NEW looks unfollowed-up. Lab 3.1's Common Failure Mode #1 covers diagnosis.

### "Cross-product imbalance is NULL for Case 2 — but the alert R-104 still fires?"

The alert relies on `ABS(cross_product_delta_imbalance) >= 7.0`. NULL fails that comparison, so the alert WOULDN'T fire — if you see this, the feature isn't being computed. Lab 3.3's Common Failure Mode #1 covers it.

## Success at end of Module 3

- Read JOB-07's window-function SQL and predict which member firms will surface as outliers
- Distinguish between deterministic rule alerts (defensible) and ML rankings (signal-prioritization)
- Recognize the Jane Street cross-product pattern from `cross_product_features` data alone
- Diagnose feature computation issues (NULL values, degenerate distributions) and trace them back to upstream data quality

## What's NOT in Module 3

- ML training (Module 5) — Module 3 just builds the input features
- GenAI / RAG (Module 6)
- BI dashboards (Module 4)
- Real-time pattern detection (Module 1 — already done)

If you're tempted to "rank these alerts" — that's Module 5. Today, just produce them.

---

When ready, head to [Lab 3.1 — Temporal Features](../labs/lab-3-1-temporal-features.md). Allow ~60 minutes.
