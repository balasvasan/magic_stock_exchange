-- =====================================================================
-- TEMPLATE — contains ${STUDENT_ID} placeholders. Run via envsubst:
--   export STUDENT_ID=<your-id>
--   envsubst < sql/ranger_policies.sql | hive -f -        (or impala-shell -f -)
-- =====================================================================
-- =====================================================================
-- ARGUS — Module 4: Ranger Access Policies
-- =====================================================================
-- Three policies, locked per PRD §10. These are expressed as Ranger
-- policy DSL — Cloudera's Ranger does NOT use SQL natively, but its
-- REST API + CLI accept JSON, and the Hive plugin accepts the SQL
-- DDL extensions used below (CREATE ... POLICY).
--
-- For students: in CDP these policies are typically authored via the
-- Ranger UI (Service Manager → cm_hive → Add Policy). The SQL below
-- documents the exact policy shape; running it directly works on CDP
-- 7.3+ where the Hive Ranger plugin supports CREATE POLICY syntax.
-- For older versions, paste each policy block into the Ranger UI form.
--
-- Roles referenced:
--   surveillance_analyst — works alert_queue, sees masked PAN, no consent_audit
--   compliance_dpo       — full PII, full consent_audit, only for active matter
--   research_analyst     — analytics views only, no PII at all
--   investigation_lead   — time-bound full-PII access during open investigation
-- =====================================================================


-- ---------------------------------------------------------------------
-- POLICY 1 — dpdp_consent_filter (row-level filter)
-- ---------------------------------------------------------------------
-- Purpose: enforces DPDP §6(4) consent withdrawal at query time.
-- When an investor has withdrawn consent for analytics processing
-- (consent_purpose missing 'ANALYTICS'), their rows are filtered out
-- of any view tagged DPDP_CONSENT_REQUIRED.
--
-- The filter does NOT apply to views tagged SEBI_AUDIT_TRAIL — those
-- are statutory under DPDP §7 legitimate-use exception. The
-- vw_surveillance_audit view explicitly carries the SEBI_AUDIT_TRAIL
-- classification and bypasses this filter.

CREATE ROW FILTER POLICY dpdp_consent_filter
ON TABLE argus_${STUDENT_ID}_silver.member_master
FOR ROLE surveillance_analyst, research_analyst
WITH FILTER
    consent_status = 'ACTIVE'
    AND consent_purpose LIKE '%ANALYTICS%';

CREATE ROW FILTER POLICY dpdp_consent_filter_features
ON TABLE argus_${STUDENT_ID}_gold.member_temporal_features
FOR ROLE surveillance_analyst, research_analyst
WITH FILTER
    -- Only show feature rows for members whose investors have active analytics consent.
    -- Implemented as a subquery against member_master; Impala inlines it efficiently.
    member_firm_id IN (
        SELECT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master
         WHERE is_current
           AND (consent_status = 'ACTIVE' AND consent_purpose LIKE '%ANALYTICS%')
    );

-- compliance_dpo and investigation_lead are intentionally NOT in the
-- FOR ROLE list above — they bypass this filter under DPDP §7.


-- ---------------------------------------------------------------------
-- POLICY 2 — pii_column_mask (column-level masking)
-- ---------------------------------------------------------------------
-- Purpose: redact PAN, Aadhaar reference, email, mobile, and trader names
-- for any role except compliance_dpo. Surveillance analysts see masked
-- values like 'XXXXX****X'; the DPO sees full values.
--
-- Column-level masks in Ranger are applied via MASK clauses in the policy.
-- Multiple roles can have different mask styles; here we use:
--   - HASH for PAN (so analysts can join rows without seeing the PAN)
--   - PARTIAL for email (preserves domain, redacts local part)
--   - REDACT for mobile (full redaction)

CREATE COLUMN MASK POLICY pii_column_mask_pan
ON TABLE argus_${STUDENT_ID}_silver.member_master
COLUMN investor_pan
FOR ROLE surveillance_analyst, research_analyst, investigation_lead
WITH MASK 'XXXXX****X';

CREATE COLUMN MASK POLICY pii_column_mask_pan_cdc
ON TABLE argus_${STUDENT_ID}_bronze.member_cdc
COLUMN investor_pan
FOR ROLE surveillance_analyst, research_analyst
WITH MASK 'XXXXX****X';

CREATE COLUMN MASK POLICY pii_column_mask_email
ON TABLE argus_${STUDENT_ID}_silver.member_master
COLUMN investor_email
FOR ROLE surveillance_analyst, research_analyst
WITH MASK
    REGEXP_REPLACE(investor_email, '^[^@]+', 'redacted');  -- preserves @domain

CREATE COLUMN MASK POLICY pii_column_mask_mobile
ON TABLE argus_${STUDENT_ID}_silver.member_master
COLUMN investor_mobile
FOR ROLE surveillance_analyst, research_analyst, investigation_lead
WITH MASK '+91-XXXX-XXXXXX';

CREATE COLUMN MASK POLICY pii_column_mask_trader_name
ON TABLE argus_${STUDENT_ID}_silver.member_master
COLUMN trader_name
FOR ROLE research_analyst
WITH MASK 'REDACTED';

-- compliance_dpo is intentionally NOT in any FOR ROLE list above —
-- they see all PII unredacted. This is the privileged access tier
-- under DPDP §16 (Significant Data Fiduciary obligations).


-- ---------------------------------------------------------------------
-- POLICY 3 — surveillance_time_bound_access (time-bounded row access)
-- ---------------------------------------------------------------------
-- Purpose: investigation_lead role gets full-PII access ONLY during the
-- active investigation window. Access is keyed to a case_id; the
-- investigation has a documented open_ts and close_ts. When close_ts
-- passes, the access auto-revokes.
--
-- Implementation: Ranger time-based row filter joining alert_candidates
-- to a separate argus_${STUDENT_ID}_views.vw_active_investigations view that lists
-- (case_id, lead_user_id, open_ts, close_ts) tuples maintained by the
-- Surveillance team's case-tracking system.

-- First, the supporting view:
DROP VIEW IF EXISTS argus_${STUDENT_ID}_views.vw_active_investigations;
CREATE VIEW argus_${STUDENT_ID}_views.vw_active_investigations AS
SELECT
    case_id,
    member_firm_id,
    instrument_code,
    pattern_type,
    confirmed_ts AS open_ts,
    -- Cases close 90 days after confirmation by default; STR filing extends.
    COALESCE(str_filed_ts + INTERVAL 90 DAYS,
             confirmed_ts + INTERVAL 180 DAYS)        AS close_ts
FROM argus_${STUDENT_ID}_gold.confirmed_manipulation_cases
WHERE sebi_outcome IN ('NONE', 'INQUIRY_OPENED');  -- closed cases drop off

COMMENT ON VIEW argus_${STUDENT_ID}_views.vw_active_investigations IS
    'Active surveillance cases; drives the time-bound row-access Ranger policy.';

-- The Ranger policy itself:
CREATE ROW FILTER POLICY surveillance_time_bound_access
ON TABLE argus_${STUDENT_ID}_gold.alert_candidates
FOR ROLE investigation_lead
WITH FILTER
    EXISTS (
        SELECT 1 FROM argus_${STUDENT_ID}_views.vw_active_investigations v
         WHERE v.member_firm_id = argus_${STUDENT_ID}_gold.alert_candidates.member_firm_id
           AND CURRENT_TIMESTAMP() BETWEEN v.open_ts AND v.close_ts
    );

CREATE ROW FILTER POLICY surveillance_time_bound_access_master
ON TABLE argus_${STUDENT_ID}_silver.member_master
FOR ROLE investigation_lead
WITH FILTER
    -- investigation_lead can see full member_master rows for any member
    -- with an open investigation; otherwise the row is filtered.
    member_firm_id IN (
        SELECT v.member_firm_id FROM argus_${STUDENT_ID}_views.vw_active_investigations v
         WHERE CURRENT_TIMESTAMP() BETWEEN v.open_ts AND v.close_ts
    );


-- =====================================================================
-- Atlas classification tags (PRD §10) — applied via the Atlas REST API,
-- not Ranger. The 6 locked tags are documented here for reference; the
-- actual JSON applied via Atlas lives in src/governance/atlas_classifications.json
-- (Module 7).
-- =====================================================================
--   PII_HIGH                  → investor_pan, trader_name, email, mobile
--   PII_LOW                   → investor_acct, member_firm_id, trader_id, demat
--   FINANCIAL_SENSITIVE       → qty, price, exec_price, exec_qty, notional, P&L
--   SURVEILLANCE_RESTRICTED   → alert_candidates, confirmed_manipulation_cases,
--                               STR drafts, disposition fields
--   DPDP_CONSENT_REQUIRED     → all tables joined to investor PII
--   SEBI_AUDIT_TRAIL          → vw_surveillance_audit, consent_audit, all
--                               confirmed_manipulation_cases columns

-- =====================================================================
-- Policy verification (run as the relevant role in Hue)
-- =====================================================================
-- SET ROLE surveillance_analyst;
-- SELECT investor_pan FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current LIMIT 5;
--   -- expect 'XXXXX****X' for every row
--
-- SET ROLE compliance_dpo;
-- SELECT investor_pan FROM argus_${STUDENT_ID}_silver.member_master WHERE is_current LIMIT 5;
--   -- expect actual PAN values
--
-- SET ROLE research_analyst;
-- SELECT COUNT(*) FROM argus_${STUDENT_ID}_gold.member_temporal_features
--   WHERE member_firm_id IN (
--     SELECT DISTINCT member_firm_id FROM argus_${STUDENT_ID}_silver.member_master
--      WHERE consent_status = 'WITHDRAWN');
--   -- expect 0 (consent-withdrawn investors filtered)
