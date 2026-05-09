-- =====================================================================
-- TEMPLATE — contains ${STUDENT_ID} placeholders. Run via envsubst:
--   export STUDENT_ID=<your-id>
--   envsubst < sql/governed_views.sql | hive -f -        (or impala-shell -f -)
-- =====================================================================
-- =====================================================================
-- ARGUS — Module 4: Governed Views in CDW (Apache Impala)
-- =====================================================================
-- Closes ARG-5 part 1: coarse-grained access on Bronze/Silver/Gold tables.
--
-- Three view families:
--   1. SURVEILLANCE views — analyst-facing alert details, full notional but
--      masked PAN. Joined to member_master + instrument_master at view time.
--   2. COMPLIANCE views — DPO-facing, full PAN visible, includes consent
--      audit trail.
--   3. ANALYTICS views — research-team-facing, fully aggregated, no PII
--      and no firm-level identifiers.
--
-- The PII masking is applied via Ranger column-masking policies in
-- sql/ranger_policies.sql, NOT in the view DDL itself. The DDL projects
-- the raw columns; Ranger redacts at query time based on the user's role.
-- This is the correct CDP pattern — single view, multiple effective shapes
-- depending on who's querying.
--
-- Schema: argus_${STUDENT_ID}_views
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS argus_${STUDENT_ID}_views
COMMENT 'Module 4 governed views — analyst-facing, role-aware';


-- ---------------------------------------------------------------------
-- SURVEILLANCE views — analyst-facing
-- ---------------------------------------------------------------------

-- vw_alert_queue: the working queue for surveillance analysts.
-- Sorted by ML score (when present) then severity then fired_ts.
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_alert_queue;
CREATE VIEW argus_${STUDENT_ID}_views.vw_alert_queue AS
SELECT
    a.alert_id,
    a.fired_ts,
    a.rule_id,
    a.pattern_type,
    a.severity,
    a.member_firm_id,
    m.member_firm_name,
    m.member_firm_category,
    a.trader_id,
    -- trader_name is PII_HIGH; Ranger will mask for non-DPO roles
    m.trader_name,
    a.instrument_code,
    i.instrument_type,
    i.sector,
    i.market_cap_bucket,
    i.esm_flag,
    i.asm_flag,
    a.window_start_ts,
    a.window_end_ts,
    a.features,
    a.model_score,
    a.model_version,
    a.shap_explanations,
    a.disposition,
    a.disposition_ts,
    a.str_id,
    a.trade_date
FROM argus_${STUDENT_ID}_gold.alert_candidates a
LEFT JOIN argus_${STUDENT_ID}_silver.member_master m
       ON a.member_firm_id = m.member_firm_id
      AND COALESCE(a.trader_id, '') = COALESCE(m.trader_id, '')
      AND m.is_current
LEFT JOIN argus_${STUDENT_ID}_silver.instrument_master i
       ON a.instrument_code = i.instrument_code
      AND i.is_current
WHERE a.disposition = 'PENDING';

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_alert_queue IS
    'Pending alert queue for surveillance analysts; PII masked via Ranger column policies.';


-- vw_member_analytics: behavior-summary view for the surveillance team.
-- This view is used in the surveillance dashboards. Does NOT contain PAN/email/mobile.
-- The DPDP §6(4) consent filter (Ranger row-filter) is applied to this view —
-- investors who withdrew analytics consent are removed before aggregation.
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_member_analytics;
CREATE VIEW argus_${STUDENT_ID}_views.vw_member_analytics AS
SELECT
    f.member_firm_id,
    m.member_firm_name,
    m.member_firm_category,
    f.instrument_code,
    f.trade_date,
    f.orders_placed,
    f.orders_cancelled,
    f.cancel_rate,
    f.median_time_to_cancel_ms,
    f.p95_time_to_cancel_ms,
    f.pct_cancelled_under_50ms,
    f.max_simultaneous_levels,
    f.layered_stack_count,
    f.order_to_trade_ratio_1m,
    f.order_to_trade_ratio_5m,
    f.order_to_trade_ratio_30m,
    f.notional_traded,
    -- Risk score from member_risk_scores (joined by score_date)
    rs.overall_risk_score,
    rs.risk_tier,
    f.computed_at
FROM argus_${STUDENT_ID}_gold.member_temporal_features f
LEFT JOIN argus_${STUDENT_ID}_silver.member_master m
       ON f.member_firm_id = m.member_firm_id
      AND m.is_current
      AND m.trader_id IS NULL
      AND m.investor_acct IS NULL
LEFT JOIN argus_${STUDENT_ID}_gold.member_risk_scores rs
       ON f.member_firm_id = rs.member_firm_id
      AND f.trade_date = rs.score_date;

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_member_analytics IS
    'Per-member surveillance behavior summary; consent-withdrawn investors filtered via Ranger row policy.';


-- vw_cross_product_alerts: dedicated view for the Jane Street pattern.
-- Surveillance lead reviews this daily.
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_cross_product_alerts;
CREATE VIEW argus_${STUDENT_ID}_views.vw_cross_product_alerts AS
SELECT
    cp.member_firm_id,
    m.member_firm_name,
    cp.underlying_code,
    cp.trade_date,
    cp.cash_net_position,
    cp.futures_net_position,
    cp.options_net_delta_exposure,
    cp.cross_product_delta_imbalance,
    cp.directional_consistency_flag,
    cp.pre_close_concentration_pct,
    cp.is_expiry_day,
    cp.days_to_nearest_expiry,
    cp.cash_futures_pnl_inr,
    cp.options_pnl_inr,
    cp.pnl_correlation_inverse,
    -- Linked alerts for this (member, underlying, date)
    (SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.alert_candidates a
      WHERE a.rule_id = 'R-104'
        AND a.member_firm_id = cp.member_firm_id
        AND a.underlying_code = cp.underlying_code
        AND a.trade_date = cp.trade_date) AS r104_alerts_today
FROM argus_${STUDENT_ID}_gold.cross_product_features cp
LEFT JOIN argus_${STUDENT_ID}_silver.member_master m
       ON cp.member_firm_id = m.member_firm_id
      AND m.is_current
      AND m.trader_id IS NULL
      AND m.investor_acct IS NULL
WHERE ABS(cp.cross_product_delta_imbalance) > 5.0;  -- pre-filter to interesting rows

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_cross_product_alerts IS
    'Cross-product imbalance view for Jane Street-style detection; filtered to imbalance > 5.0.';


-- ---------------------------------------------------------------------
-- COMPLIANCE views — DPO-facing, fuller PII visibility
-- ---------------------------------------------------------------------

-- vw_surveillance_audit: full audit-trail join, used by Compliance and DPO.
-- This view is the "single pane of glass" for DPDP / SEBI inquiries.
-- Statutory tag SEBI_AUDIT_TRAIL means the Ranger consent-row-filter
-- does NOT apply here — DPDP §7 legitimate-use exception.
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_surveillance_audit;
CREATE VIEW argus_${STUDENT_ID}_views.vw_surveillance_audit AS
SELECT
    a.alert_id,
    a.fired_ts,
    a.rule_id,
    a.pattern_type,
    a.severity,
    a.member_firm_id,
    m.member_firm_name,
    m.member_firm_category,
    a.trader_id,
    m.trader_name,
    -- Investor PII visible to DPO role (Ranger column-mask policy applies for others)
    m.investor_acct,
    m.investor_pan,
    m.investor_pan_hash,
    m.investor_email,
    m.consent_status,
    m.consent_purpose,
    a.instrument_code,
    a.window_start_ts,
    a.window_end_ts,
    a.disposition,
    a.disposition_ts,
    a.disposition_analyst_id,
    a.str_id,
    a.str_drafted_ts,
    a.trade_date,
    -- Statutory tag — bypasses DPDP §6(4) consent filter under §7
    CAST('SEBI_AUDIT_TRAIL' AS STRING) AS atlas_classification
FROM argus_${STUDENT_ID}_gold.alert_candidates a
LEFT JOIN argus_${STUDENT_ID}_silver.member_master m
       ON a.member_firm_id = m.member_firm_id
      AND COALESCE(a.trader_id, '') = COALESCE(m.trader_id, '')
      AND m.is_current;

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_surveillance_audit IS
    'DPO + Compliance view; bypasses DPDP §6(4) row filter under §7 legitimate-use exception.';


-- vw_consent_audit: DPO-facing view of the consent audit trail.
-- Includes Iceberg snapshot IDs for time-travel proof of erasure.
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_consent_audit;
CREATE VIEW argus_${STUDENT_ID}_views.vw_consent_audit AS
SELECT
    audit_id,
    event_ts,
    event_type,
    investor_pan_hash,
    investor_acct,
    consent_purpose,
    legal_basis,
    requestor_channel,
    request_id,
    actioned_by,
    affected_tables,
    affected_row_count,
    pre_action_snapshot,
    post_action_snapshot,
    notes,
    audit_date
FROM argus_${STUDENT_ID}_gold.consent_audit;

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_consent_audit IS
    'Full DPDP audit trail; includes Iceberg snapshot IDs for time-travel proof.';


-- ---------------------------------------------------------------------
-- ANALYTICS views — research / data-science, no PII, aggregated only
-- ---------------------------------------------------------------------

-- vw_kpi_daily: surveillance KPIs for ops dashboards
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_kpi_daily;
CREATE VIEW argus_${STUDENT_ID}_views.vw_kpi_daily AS
SELECT
    kpi_date,
    alerts_fired,
    alerts_dispositioned,
    alerts_pending_eod,
    false_positive_rate,
    confirmed_manipulation_rate,
    median_time_to_disposition_min,
    p95_time_to_disposition_min,
    str_backlog_count,
    str_oldest_pending_days,
    model_top_decile_precision,
    model_auc_rolling_7d,
    consent_withdrawals_today,
    erasure_requests_today,
    erasure_sla_breaches
FROM argus_${STUDENT_ID}_gold.surveillance_kpis;

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_kpi_daily IS
    'Daily KPIs for surveillance ops dashboard; no PII, fully aggregated.';


-- vw_model_performance: ML model monitoring view for the data-science team.
-- Joins alert candidates to confirmed cases to compute precision over time.
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_model_performance;
CREATE VIEW argus_${STUDENT_ID}_views.vw_model_performance AS
SELECT
    a.trade_date,
    a.model_version,
    COUNT(*)                              AS scored_alerts,
    COUNT(c.case_id)                      AS confirmed_cases,
    SUM(CASE WHEN a.model_score >= 0.9 THEN 1 ELSE 0 END)   AS top_decile_alerts,
    SUM(CASE WHEN a.model_score >= 0.9 AND c.case_id IS NOT NULL THEN 1 ELSE 0 END)
                                          AS top_decile_confirmed,
    AVG(a.model_score)                    AS avg_score,
    APPROX_PERCENTILE(a.model_score, 0.5) AS median_score
FROM argus_${STUDENT_ID}_gold.alert_candidates a
LEFT JOIN argus_${STUDENT_ID}_gold.confirmed_manipulation_cases c
       ON a.alert_id IN (SELECT explode(from_json(c.triggering_alert_ids, 'array<string>')))
WHERE a.model_score IS NOT NULL
GROUP BY a.trade_date, a.model_version;

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_model_performance IS
    'Daily model performance: scored alerts, confirmed cases, top-decile precision.';


-- =====================================================================
-- Verification queries
-- =====================================================================
-- SHOW VIEWS IN argus_${STUDENT_ID}_views;        -- expect 7 views
-- DESCRIBE argus_${STUDENT_ID}_views.vw_alert_queue;
-- SELECT COUNT(*) FROM argus_${STUDENT_ID}_views.vw_alert_queue;
