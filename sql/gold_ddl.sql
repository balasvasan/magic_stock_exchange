-- =====================================================================
-- TEMPLATE — contains ${STUDENT_ID} placeholders. Run via envsubst:
--   export STUDENT_ID=<your-id>
--   envsubst < sql/gold_ddl.sql | hive -f -        (or impala-shell -f -)
-- =====================================================================
-- =====================================================================
-- ARGUS — Gold Layer DDL
-- =====================================================================
-- 8 tables — analytics-ready, served to surveillance UI, ML, GenAI, BI.
-- Format:    Iceberg with Parquet, copy-on-write.
-- Partition: trade_date (or computed_date for derived rollups).
-- Schema:    argus_${STUDENT_ID}_gold
--
-- Special: argus_${STUDENT_ID}_gold.consent_audit has 'history.expire.enabled' = 'false'
--          per framework requirement — consent and audit history NEVER
--          expire. This is non-negotiable for DPDP §12 evidentiary needs.
-- =====================================================================
-- Tables defined here:
--   1. argus_${STUDENT_ID}_gold.order_book_snapshots         (reconstructed book state)
--   2. argus_${STUDENT_ID}_gold.member_temporal_features     (per-member sequential features)
--   3. argus_${STUDENT_ID}_gold.cross_product_features       (cash + futures + options correlated)
--   4. argus_${STUDENT_ID}_gold.alert_candidates             (candidate alerts with feature payload)
--   5. argus_${STUDENT_ID}_gold.confirmed_manipulation_cases (truth set for ML + GenAI)
--   6. argus_${STUDENT_ID}_gold.member_risk_scores           (daily risk rollup per member)
--   7. argus_${STUDENT_ID}_gold.consent_audit                (DPDP audit log — NEVER EXPIRES)
--   8. argus_${STUDENT_ID}_gold.surveillance_kpis            (operational metrics for ops dashboard)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS argus_${STUDENT_ID}_gold
COMMENT 'ARGUS analytics-ready layer; consumed by surveillance UI, ML, GenAI, BI';

-- ---------------------------------------------------------------------
-- 1. order_book_snapshots — reconstructed book state (answers ARG-2)
-- ---------------------------------------------------------------------
-- Per-instrument book state at multiple resolutions. The 1s and 100ms
-- snapshots are the workhorse for analyst investigation; per-event is
-- only generated for instruments that fired alerts (otherwise the
-- volume is impractical).
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.order_book_snapshots;
CREATE TABLE argus_${STUDENT_ID}_gold.order_book_snapshots (
    snapshot_id        STRING       COMMENT 'UUID for the snapshot',
    instrument_code    STRING       COMMENT 'PRATEEK instrument code',
    snapshot_ts        TIMESTAMP    COMMENT 'Snapshot timestamp',
    snapshot_resolution STRING      COMMENT '1S | 100MS | PER_EVENT',
    bids               STRING       COMMENT 'JSON array of {price, qty, num_orders} top-10 bid levels',
    asks               STRING       COMMENT 'JSON array of {price, qty, num_orders} top-10 ask levels',
    bid_depth_total    BIGINT       COMMENT 'Sum of qty across top-10 bids',
    ask_depth_total    BIGINT       COMMENT 'Sum of qty across top-10 asks',
    spread_bps         DECIMAL(8,2) COMMENT 'Best-bid to best-ask spread in basis points',
    mid_price          DECIMAL(18,4) COMMENT '(best_bid + best_ask) / 2',
    last_trade_price   DECIMAL(18,4) COMMENT 'Last trade price at or before this timestamp',
    triggering_event_id STRING      COMMENT 'For PER_EVENT snapshots: the event that triggered',
    trade_date         DATE         COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (trade_date, snapshot_resolution)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'Reconstructed order book — Iceberg time-travel answers "what did the book look like at T?"'
);

-- ---------------------------------------------------------------------
-- 2. member_temporal_features — per-member sequential features
-- ---------------------------------------------------------------------
-- Computed per (member_firm × instrument × date). Rolling-window
-- features driving spoofing/layering detection.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.member_temporal_features;
CREATE TABLE argus_${STUDENT_ID}_gold.member_temporal_features (
    member_firm_id          STRING        COMMENT 'KAVACH member firm',
    instrument_code         STRING        COMMENT 'PRATEEK instrument',
    trade_date              DATE          COMMENT 'Business date',
    -- Order/trade volume
    orders_placed           BIGINT        COMMENT 'Total orders placed in the day',
    orders_cancelled        BIGINT        COMMENT 'Total orders cancelled',
    orders_filled           BIGINT        COMMENT 'Total orders filled (partial or full)',
    trades_executed         BIGINT        COMMENT 'Distinct trade count',
    notional_traded         DECIMAL(20,2) COMMENT 'INR notional executed',
    -- Cancellation behavior
    cancel_rate             DECIMAL(6,4)  COMMENT 'orders_cancelled / orders_placed',
    median_time_to_cancel_ms BIGINT       COMMENT 'Median ms between order placement and cancel',
    p95_time_to_cancel_ms   BIGINT        COMMENT 'P95 time-to-cancel — short-tail = spoofing signal',
    pct_cancelled_under_50ms DECIMAL(6,4) COMMENT 'Fraction of orders cancelled within 50ms (spoofing signature)',
    -- Layering features
    max_simultaneous_levels BIGINT        COMMENT 'Max distinct price levels with active orders at once',
    layered_stack_depth     BIGINT        COMMENT 'Max depth of stacked non-bona-fide orders ahead of bona-fide',
    layered_stack_count     BIGINT        COMMENT 'Number of distinct layering patterns detected in the day',
    -- Order/trade ratio
    order_to_trade_ratio_1m  DECIMAL(8,2) COMMENT 'orders_placed / trades_executed over rolling 1-min windows (max)',
    order_to_trade_ratio_5m  DECIMAL(8,2) COMMENT 'Same over 5-min windows',
    order_to_trade_ratio_30m DECIMAL(8,2) COMMENT 'Same over 30-min windows',
    -- Aggressiveness
    pct_orders_above_ltp    DECIMAL(6,4)  COMMENT 'Fraction of buys placed above last-traded-price (price-impacting)',
    pct_orders_below_ltp    DECIMAL(6,4)  COMMENT 'Fraction of sells placed below LTP (price-impacting)',
    -- Concentration
    pct_book_depth_owned    DECIMAL(6,4)  COMMENT 'Max share of total book depth this member contributed',
    computed_at             TIMESTAMP     COMMENT 'When the feature row was computed'
)
USING ICEBERG
PARTITIONED BY (trade_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'ARG-2 — per-member temporal features for spoofing/layering detection'
);

-- ---------------------------------------------------------------------
-- 3. cross_product_features — cash + futures + options correlated
-- ---------------------------------------------------------------------
-- The Jane Street pattern table. Per (member × underlying × date).
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.cross_product_features;
CREATE TABLE argus_${STUDENT_ID}_gold.cross_product_features (
    member_firm_id              STRING        COMMENT 'KAVACH member firm',
    underlying_code             STRING        COMMENT 'Underlying instrument (e.g. RELIANCE, BANKNIFTY)',
    trade_date                  DATE          COMMENT 'Business date',
    -- Position sizes by product
    cash_net_position           DECIMAL(20,2) COMMENT 'Net long/short in cash equity, INR notional',
    futures_net_position        DECIMAL(20,2) COMMENT 'Net long/short in single-stock or index futures',
    options_net_delta_exposure  DECIMAL(20,2) COMMENT 'Net delta-equivalent INR exposure across all options',
    -- Imbalance metrics
    cross_product_delta_imbalance DECIMAL(8,4) COMMENT 'options_delta / (cash + futures) — flag when >7 (Jane Street ratio)',
    directional_consistency_flag BOOLEAN      COMMENT 'TRUE if cash + futures + options all point same direction',
    -- Timing patterns
    pre_close_concentration_pct DECIMAL(6,4)  COMMENT 'Share of day''s volume in last 30 minutes (marking-the-close signal)',
    morning_pump_ratio          DECIMAL(6,4)  COMMENT '09:15-11:00 buy volume / total day buy volume',
    afternoon_dump_ratio        DECIMAL(6,4)  COMMENT '14:30-15:30 sell volume / total day sell volume',
    -- Expiry-day specific
    is_expiry_day               BOOLEAN       COMMENT 'TRUE if any option on this underlying expires today',
    days_to_nearest_expiry      INT           COMMENT 'Calendar days to next options expiry',
    -- P&L correlation
    cash_futures_pnl_inr        DECIMAL(20,2) COMMENT 'Realized P&L from cash + futures legs',
    options_pnl_inr             DECIMAL(20,2) COMMENT 'Realized + mark-to-market P&L from options',
    pnl_correlation_inverse     BOOLEAN       COMMENT 'TRUE when cash loss is offset by options gain (or vice versa)',
    computed_at                 TIMESTAMP     COMMENT 'When the row was computed'
)
USING ICEBERG
PARTITIONED BY (trade_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'Cross-product features — answers Jane Street-style index manipulation patterns'
);

-- ---------------------------------------------------------------------
-- 4. alert_candidates — candidate alerts with feature payload
-- ---------------------------------------------------------------------
-- Where deterministic rule-fired alerts land before ML scoring. Module 5
-- updates model_score and shap_explanations every 5 minutes.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.alert_candidates;
CREATE TABLE argus_${STUDENT_ID}_gold.alert_candidates (
    alert_id                STRING        COMMENT 'UUID generated at firing time',
    fired_ts                TIMESTAMP     COMMENT 'When the rule engine fired the alert',
    rule_id                 STRING        COMMENT 'Which deterministic rule fired',
    rule_version            STRING        COMMENT 'Rule version',
    pattern_type            STRING        COMMENT 'SPOOFING | LAYERING | MOMENTUM_IGNITION | WASH | CROSS_PRODUCT',
    severity                STRING        COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
    -- Subject of the alert
    member_firm_id          STRING        COMMENT 'Member firm under suspicion',
    trader_id               STRING        COMMENT 'Trader under suspicion',
    instrument_code         STRING        COMMENT 'Instrument',
    underlying_code         STRING        COMMENT 'For derivatives — underlying',
    -- Alert window
    window_start_ts         TIMESTAMP     COMMENT 'Start of the suspect window',
    window_end_ts           TIMESTAMP     COMMENT 'End of the suspect window',
    -- Feature payload (60+ features in a struct)
    features                STRING        COMMENT 'JSON of all 60 features used by the ML model',
    -- ML scoring (NULL until JOB-09 runs)
    model_score             DECIMAL(8,6)  COMMENT 'ML probability of confirmed manipulation; NULL until scored',
    model_version           STRING        COMMENT 'MLflow model version that produced the score',
    scored_at               TIMESTAMP     COMMENT 'When the score was written',
    shap_explanations       STRING        COMMENT 'JSON of top-10 feature SHAP contributions',
    -- Disposition (filled by analyst review)
    disposition             STRING        COMMENT 'PENDING | NO_ACTION | ESCALATED | CONFIRMED_MANIPULATION',
    disposition_ts          TIMESTAMP     COMMENT 'When dispositioned',
    disposition_analyst_id  STRING        COMMENT 'Analyst who dispositioned',
    -- STR linkage
    str_id                  STRING        COMMENT 'STR ID if filed; NULL otherwise',
    str_drafted_ts          TIMESTAMP     COMMENT 'When GenAI produced the draft',
    trade_date              DATE          COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (trade_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'ARG-3 + ARG-4 — alerts with ML scores and STR drafts'
);

-- ---------------------------------------------------------------------
-- 5. confirmed_manipulation_cases — truth set for ML + GenAI
-- ---------------------------------------------------------------------
-- One row per analyst-confirmed manipulation case. Joins history from
-- legacy_alerts (Bronze #6) with current alert_candidates. Used as:
--   - ML training labels (Module 5)
--   - GenAI exemplar STR retrieval set (Module 6)
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.confirmed_manipulation_cases;
CREATE TABLE argus_${STUDENT_ID}_gold.confirmed_manipulation_cases (
    case_id                 STRING        COMMENT 'Unique case ID',
    confirmed_ts            TIMESTAMP     COMMENT 'When the analyst confirmed',
    pattern_type            STRING        COMMENT 'SPOOFING | LAYERING | etc.',
    member_firm_id          STRING        COMMENT 'Confirmed manipulator',
    trader_id               STRING        COMMENT 'Confirmed manipulator (specific trader)',
    instrument_code         STRING        COMMENT 'Instrument',
    underlying_code         STRING        COMMENT 'Underlying',
    -- Linked alerts
    triggering_alert_ids    STRING        COMMENT 'JSON array of alert_id values that contributed to the case',
    -- Quantified impact
    price_move_pct          DECIMAL(6,4)  COMMENT 'Observed price move attributed to the manipulation',
    volume_during_window    BIGINT        COMMENT 'Total exchange volume during the suspect window',
    estimated_unlawful_gain DECIMAL(20,2) COMMENT 'INR estimate of manipulator''s gain',
    retail_account_impact   DECIMAL(20,2) COMMENT 'Estimated INR loss to retail investors',
    -- Regulatory
    pfutp_regulation_cited  STRING        COMMENT 'Specific PFUTP regulation alleged (e.g. Reg 4(2)(e))',
    str_id                  STRING        COMMENT 'STR filed with SEBI',
    str_filed_ts            TIMESTAMP     COMMENT 'When STR was filed',
    str_narrative           STRING        COMMENT 'The final approved STR narrative — used by GenAI as exemplar',
    sebi_outcome            STRING        COMMENT 'NONE | INQUIRY_OPENED | ACTION_TAKEN | DISMISSED',
    sebi_outcome_ts         TIMESTAMP     COMMENT 'When SEBI outcome was recorded',
    case_date               DATE          COMMENT 'Partition key — date of confirmation'
)
USING ICEBERG
PARTITIONED BY (case_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'Truth set — ML training labels and GenAI exemplar STRs'
);

-- ---------------------------------------------------------------------
-- 6. member_risk_scores — daily risk rollup per member
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.member_risk_scores;
CREATE TABLE argus_${STUDENT_ID}_gold.member_risk_scores (
    member_firm_id            STRING        COMMENT 'KAVACH member firm',
    score_date                DATE          COMMENT 'Date the score applies to',
    overall_risk_score        DECIMAL(6,4)  COMMENT '0.0–1.0 composite score',
    -- Component scores
    historical_disposition_score DECIMAL(6,4) COMMENT 'Based on past confirmation rate',
    sebi_action_score         DECIMAL(6,4)  COMMENT 'Based on open + recent SEBI matters',
    current_alert_volume_score DECIMAL(6,4) COMMENT 'Based on today''s alert volume vs baseline',
    cross_product_imbalance_score DECIMAL(6,4) COMMENT 'Based on cross-product imbalance features',
    -- Tier
    risk_tier                 STRING        COMMENT 'GREEN | AMBER | RED',
    risk_factors              STRING        COMMENT 'JSON array of contributing factors',
    computed_at               TIMESTAMP     COMMENT 'When the score was computed'
)
USING ICEBERG
PARTITIONED BY (score_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'Daily member risk score driving alert prioritization'
);

-- ---------------------------------------------------------------------
-- 7. consent_audit — DPDP audit log — HISTORY NEVER EXPIRES
-- ---------------------------------------------------------------------
-- Every consent grant, modification, withdrawal (DPDP §6(4)), and
-- erasure request (DPDP §12) is recorded here. This table is the
-- evidentiary record for any DPB or SEBI inquiry into MSE's data
-- handling. The 'history.expire.enabled = false' property is mandatory
-- per framework; do NOT remove it.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.consent_audit;
CREATE TABLE argus_${STUDENT_ID}_gold.consent_audit (
    audit_id              STRING       COMMENT 'UUID per audit event',
    event_ts              TIMESTAMP    COMMENT 'When the event occurred',
    event_type            STRING       COMMENT 'CONSENT_GRANTED | CONSENT_MODIFIED | CONSENT_WITHDRAWN | ERASURE_REQUESTED | ERASURE_COMPLETED | ACCESS_AUDIT',
    investor_pan_hash     STRING       COMMENT 'SHA-256 of PAN — never the raw PAN',
    investor_acct         STRING       COMMENT 'Investor account ID (PII_LOW)',
    consent_purpose       STRING       COMMENT 'Comma-separated DPDP purposes affected',
    legal_basis           STRING       COMMENT 'CONSENT | LEGITIMATE_USE_§7 | STATUTORY_§17',
    requestor_channel     STRING       COMMENT 'CONSENT_MANAGER | DIRECT_DPO | AUTOMATIC',
    request_id            STRING       COMMENT 'Originating request ID for traceability',
    actioned_by           STRING       COMMENT 'User/system that actioned the event',
    affected_tables       STRING       COMMENT 'JSON array of tables touched by the action',
    affected_row_count    BIGINT       COMMENT 'Number of rows touched',
    pre_action_snapshot   STRING       COMMENT 'Iceberg snapshot ID before the action (for time-travel proof)',
    post_action_snapshot  STRING       COMMENT 'Iceberg snapshot ID after the action',
    notes                 STRING       COMMENT 'Free-form notes',
    audit_date            DATE         COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (audit_date, event_type)
TBLPROPERTIES (
    'write.format.default'      = 'PARQUET',
    'write.merge.mode'          = 'copy-on-write',
    'history.expire.enabled'    = 'false',     -- MANDATORY: consent history never expires
    'comment'                   = 'DPDP audit log — history.expire.enabled=false (do not remove)'
);

-- ---------------------------------------------------------------------
-- 8. surveillance_kpis — operational metrics for the ops dashboard
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.surveillance_kpis;
CREATE TABLE argus_${STUDENT_ID}_gold.surveillance_kpis (
    kpi_date                  DATE         COMMENT 'Date the KPI applies to',
    -- Volume
    alerts_fired              BIGINT       COMMENT 'Total alerts fired in the day',
    alerts_dispositioned      BIGINT       COMMENT 'Alerts dispositioned in the day',
    alerts_pending_eod        BIGINT       COMMENT 'Backlog at end of day',
    -- Quality
    false_positive_rate       DECIMAL(6,4) COMMENT '(alerts dispositioned NO_ACTION) / total dispositioned',
    confirmed_manipulation_rate DECIMAL(6,4) COMMENT '(alerts CONFIRMED_MANIPULATION) / total dispositioned',
    -- Speed
    median_time_to_disposition_min BIGINT  COMMENT 'Median minutes from fire to disposition',
    p95_time_to_disposition_min    BIGINT  COMMENT 'P95',
    -- Backlog
    str_backlog_count         BIGINT       COMMENT 'STRs awaiting drafting or analyst review',
    str_oldest_pending_days   INT          COMMENT 'Age in days of the oldest pending STR',
    -- ML model performance (when applicable)
    model_top_decile_precision DECIMAL(6,4) COMMENT 'Production model precision in top 10% of scores',
    model_auc_rolling_7d       DECIMAL(6,4) COMMENT 'Rolling 7-day AUC',
    -- Compliance
    consent_withdrawals_today BIGINT       COMMENT 'DPDP §6(4) withdrawals processed',
    erasure_requests_today    BIGINT       COMMENT 'DPDP §12 requests processed',
    erasure_sla_breaches      INT          COMMENT 'Erasures that missed the 30-day SLA',
    computed_at               TIMESTAMP    COMMENT 'When the KPI row was computed'
)
USING ICEBERG
PARTITIONED BY (kpi_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'Daily operational KPIs for the surveillance ops dashboard'
);

-- =====================================================================
-- 9. argus_${STUDENT_ID}_gold.realtime_alert_stream (v1.2 amendment)
-- =====================================================================
-- Real-time alerts produced by JOB-10 (PyFlink CEP) and JOB-11 (SSB SQL).
-- Persisted by JOB-12 from argus.${STUDENT_ID}.realtime_alerts.v1.
-- Cross-checked against argus_${STUDENT_ID}_gold.alert_candidates from
-- JOB-08 batch — same event_id should appear in both within latency budgets.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_gold.realtime_alert_stream;
CREATE TABLE argus_${STUDENT_ID}_gold.realtime_alert_stream (
    alert_id             STRING       COMMENT 'Unique alert identifier from JOB-10/11',
    fired_ts             BIGINT       COMMENT 'Microsecond ts when pattern matched',
    source_engine        STRING       COMMENT 'FLINK | SSB — discriminates which engine fired',
    rule_id              STRING       COMMENT 'R-101 SPOOFING | R-102 LAYERING | R-104 X-PRODUCT',
    severity             STRING       COMMENT 'HIGH | CRITICAL',
    pattern_type         STRING       COMMENT 'SPOOFING | LAYERING | CROSS_PRODUCT_IMBALANCE',
    member_firm_id       STRING       COMMENT 'Firm whose pattern triggered',
    trader_id            STRING       COMMENT 'Trader (nullable for aggregated patterns)',
    instrument_code      STRING       COMMENT 'Specific contract',
    underlying_code      STRING       COMMENT 'Underlying for cross-product alerts',
    window_start_ts      BIGINT       COMMENT 'Start of detection window',
    window_end_ts        BIGINT       COMMENT 'End of detection window',
    evidence_json        STRING       COMMENT 'Pattern-specific evidence as JSON',
    detection_latency_ms BIGINT       COMMENT 'Latency from triggering event to alert emit',
    fired_date           DATE         COMMENT 'Partition column derived from fired_ts',
    ingested_at          TIMESTAMP    COMMENT 'When JOB-12 landed this row in Iceberg'
)
USING ICEBERG
PARTITIONED BY (fired_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'Real-time alerts from JOB-10 (Flink CEP) and JOB-11 (SSB SQL)'
);

-- =====================================================================
-- Verification
-- =====================================================================
-- SHOW TABLES IN argus_${STUDENT_ID}_gold;
-- DESCRIBE FORMATTED argus_${STUDENT_ID}_gold.consent_audit;
--   -- Verify history.expire.enabled = false in the TBLPROPERTIES output
