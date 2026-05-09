-- =====================================================================
-- TEMPLATE — contains ${STUDENT_ID} placeholders. Run via envsubst:
--   export STUDENT_ID=<your-id>
--   envsubst < sql/silver_ddl.sql | hive -f -        (or impala-shell -f -)
-- =====================================================================
-- =====================================================================
-- ARGUS — Silver Layer DDL
-- =====================================================================
-- 4 tables — cleaned, deduplicated, enriched.
-- Format:    Iceberg with Parquet files, copy-on-write mode for stable
--            analytics-friendly snapshots. Slower writes than MOR but
--            faster reads, which is what Silver consumers want.
-- Partition: trade_date (business date) — natural grouping for analytics.
-- Schema:    argus_${STUDENT_ID}_silver
-- =====================================================================
-- Tables defined here:
--   1. argus_${STUDENT_ID}_silver.order_events       (cleaned TARANG events with member + instrument enrichment)
--   2. argus_${STUDENT_ID}_silver.executed_trades    (cleaned NIPATAN with both-leg enrichment)
--   3. argus_${STUDENT_ID}_silver.member_master      (SCD2 KAVACH; PII columns Atlas-tagged)
--   4. argus_${STUDENT_ID}_silver.instrument_master  (SCD2 PRATEEK; corp-action-adjusted)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS argus_${STUDENT_ID}_silver
COMMENT 'ARGUS cleaned + enriched layer; copy-on-write Parquet for analytics';

-- ---------------------------------------------------------------------
-- 1. order_events — cleaned, enriched TARANG order stream
-- ---------------------------------------------------------------------
-- Bronze.orders_raw + Bronze.member_cdc (current state) + Bronze.instrument_cdc.
-- Deduplication on event_id; member_firm_name and instrument_type joined in.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_silver.order_events;
CREATE TABLE argus_${STUDENT_ID}_silver.order_events (
    event_id             STRING       COMMENT 'UUID per matching-engine event',
    ts_us                BIGINT       COMMENT 'Microsecond epoch from match engine',
    ts_event             TIMESTAMP    COMMENT 'Derived from ts_us for human-readable queries',
    member_firm_id       STRING       COMMENT 'KAVACH member firm',
    member_firm_name     STRING       COMMENT 'Joined from member_master',
    member_firm_category STRING       COMMENT 'TIER1_MM | PROP_TRADER | RETAIL_BROKER | INSTITUTIONAL',
    trader_id            STRING       COMMENT 'Named human trader',
    instrument_code      STRING       COMMENT 'PRATEEK instrument code',
    instrument_type      STRING       COMMENT 'EQUITY | ETF | FUTURE | OPTION | BOND',
    underlying_code      STRING       COMMENT 'For derivatives — joined from instrument_master',
    side                 STRING       COMMENT 'BUY | SELL',
    order_type           STRING       COMMENT 'LIMIT | MARKET | STOP | IOC | FOK',
    qty                  BIGINT       COMMENT 'Order quantity',
    price                DECIMAL(18,4) COMMENT 'Limit price; NULL for market orders',
    action               STRING       COMMENT 'NEW | MODIFY | CANCEL | PARTIAL_FILL | FULL_FILL',
    parent_order_id      STRING       COMMENT 'Original order_id for modifies/cancels',
    book_state_after     STRING       COMMENT 'JSON of affected price level after',
    esm_flag             STRING       COMMENT 'Joined surveillance state at ts_event',
    asm_flag             STRING       COMMENT 'Joined surveillance state at ts_event',
    trade_date           DATE         COMMENT 'Business date — partition key'
)
USING ICEBERG
PARTITIONED BY (trade_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'write.target-file-size-bytes' = '268435456',  -- 256 MiB
    'comment'                = 'ARG-2 — order events enriched for feature engineering'
);

-- ---------------------------------------------------------------------
-- 2. executed_trades — cleaned, both-leg-enriched trades
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS argus_${STUDENT_ID}_silver.executed_trades;
CREATE TABLE argus_${STUDENT_ID}_silver.executed_trades (
    trade_id              STRING       COMMENT 'Globally unique trade ID',
    ts_us                 BIGINT       COMMENT 'Microsecond epoch of execution',
    ts_event              TIMESTAMP    COMMENT 'Human-readable timestamp',
    instrument_code       STRING       COMMENT 'PRATEEK instrument code',
    instrument_type       STRING       COMMENT 'EQUITY | FUTURE | OPTION | etc.',
    underlying_code       STRING       COMMENT 'For derivatives',
    buy_member_firm_id    STRING       COMMENT 'Buyer member firm',
    buy_member_firm_name  STRING       COMMENT 'Joined name',
    buy_member_category   STRING       COMMENT 'Joined category',
    sell_member_firm_id   STRING       COMMENT 'Seller member firm',
    sell_member_firm_name STRING       COMMENT 'Joined name',
    sell_member_category  STRING       COMMENT 'Joined category',
    buy_investor_acct     STRING       COMMENT 'Buy-side investor account (hashed)',
    sell_investor_acct    STRING       COMMENT 'Sell-side investor account (hashed)',
    buy_trader_id         STRING       COMMENT 'Buy-side trader',
    sell_trader_id        STRING       COMMENT 'Sell-side trader',
    exec_price            DECIMAL(18,4) COMMENT 'Executed price',
    exec_qty              BIGINT       COMMENT 'Executed quantity',
    notional_value        DECIMAL(20,2) COMMENT 'exec_price * exec_qty * lot_size (computed)',
    settlement_date       DATE         COMMENT 'Settlement date',
    is_self_trade         BOOLEAN      COMMENT 'TRUE if buy_member = sell_member (potential wash trade flag)',
    trade_date            DATE         COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (trade_date)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'write.target-file-size-bytes' = '268435456',
    'comment'                = 'NIPATAN trades, both legs enriched, with self-trade flag'
);

-- ---------------------------------------------------------------------
-- 3. member_master — SCD2 KAVACH current + history
-- ---------------------------------------------------------------------
-- SCD2 with effective_from / effective_to. PII columns are tagged with
-- Atlas classifications (PII_HIGH, PII_LOW) in Module 7. Module 4 governed
-- views use Ranger column-masking on these classifications.
-- Module 7 erasure workflow uses Iceberg time-travel on this table to
-- prove DPDP §12 compliance.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_silver.member_master;
CREATE TABLE argus_${STUDENT_ID}_silver.member_master (
    member_firm_id       STRING       COMMENT 'Primary key',
    member_firm_name     STRING       COMMENT 'Registered name',
    member_firm_category STRING       COMMENT 'TIER1_MM | PROP_TRADER | RETAIL_BROKER | INSTITUTIONAL',
    sebi_registration    STRING       COMMENT 'PII_LOW',
    capital_adequacy     DECIMAL(18,2) COMMENT 'Net worth in INR crore',
    suspension_history   STRING       COMMENT 'JSON array',
    -- Trader-level (one row per (member, trader) when applicable)
    trader_id            STRING       COMMENT 'NULL for member-only rows',
    trader_name          STRING       COMMENT 'PII_HIGH',
    trader_tenure_days   INT          COMMENT 'Days since trader onboarded',
    -- Investor-level (one row per (member, investor))
    investor_acct        STRING       COMMENT 'PII_LOW',
    investor_pan         STRING       COMMENT 'PII_HIGH — DPDP-classified',
    investor_pan_hash    STRING       COMMENT 'SHA-256 of PAN — used for k-anonymized retention',
    investor_email       STRING       COMMENT 'PII_HIGH',
    investor_mobile      STRING       COMMENT 'PII_HIGH',
    investor_demat       STRING       COMMENT 'PII_LOW',
    investor_kyc_tier    INT          COMMENT 'KYC tier 1/2/3',
    consent_status       STRING       COMMENT 'ACTIVE | WITHDRAWN | ERASED',
    consent_purpose      STRING       COMMENT 'Comma-separated DPDP processing purposes',
    -- SCD2 effective window
    effective_from       TIMESTAMP    COMMENT 'When this row became current',
    effective_to         TIMESTAMP    COMMENT 'NULL for the current row; populated when superseded',
    is_current           BOOLEAN      COMMENT 'Convenience flag — TRUE for the latest row per key'
)
USING ICEBERG
PARTITIONED BY (member_firm_category)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'SCD2 member master — Atlas-classified PII; time-travel proves DPDP §12 erasure'
);

-- ---------------------------------------------------------------------
-- 4. instrument_master — SCD2 PRATEEK with corp-action history
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS argus_${STUDENT_ID}_silver.instrument_master;
CREATE TABLE argus_${STUDENT_ID}_silver.instrument_master (
    instrument_code     STRING       COMMENT 'Primary key',
    instrument_type     STRING       COMMENT 'EQUITY | ETF | FUTURE | OPTION | BOND',
    underlying_code     STRING       COMMENT 'For derivatives',
    expiry_date         DATE         COMMENT 'F&O expiry',
    strike_price        DECIMAL(18,4) COMMENT 'Options strike',
    lot_size            BIGINT       COMMENT 'F&O lot size',
    tick_size           DECIMAL(8,4) COMMENT 'Minimum price increment',
    sector              STRING       COMMENT 'NIFTY-50 | BANKING | PHARMA | etc.',
    market_cap_bucket   STRING       COMMENT 'LARGE | MID | SMALL | MICRO',
    avg_daily_volume    BIGINT       COMMENT 'Trailing 30-day ADV',
    esm_flag            STRING       COMMENT 'Y/N — current Enhanced Surveillance Measure',
    asm_flag            STRING       COMMENT 'Y/N — current Additional Surveillance Measure',
    circuit_band_pct    DECIMAL(5,2) COMMENT 'Daily circuit-breaker band',
    corp_action_history STRING       COMMENT 'JSON array of (date, type, ratio) tuples',
    last_close          DECIMAL(18,4) COMMENT 'Last close adjusted for corporate actions',
    effective_from      TIMESTAMP    COMMENT 'SCD2 effective window start',
    effective_to        TIMESTAMP    COMMENT 'SCD2 effective window end',
    is_current          BOOLEAN      COMMENT 'TRUE for the current row per instrument'
)
USING ICEBERG
PARTITIONED BY (instrument_type)
TBLPROPERTIES (
    'write.format.default'   = 'PARQUET',
    'write.merge.mode'       = 'copy-on-write',
    'comment'                = 'SCD2 instrument master with corporate-action-adjusted prices'
);

-- =====================================================================
-- Verification queries
-- =====================================================================
-- SHOW TABLES IN argus_${STUDENT_ID}_silver;
-- SELECT instrument_type, COUNT(*) FROM argus_${STUDENT_ID}_silver.instrument_master
--   WHERE is_current GROUP BY instrument_type;
