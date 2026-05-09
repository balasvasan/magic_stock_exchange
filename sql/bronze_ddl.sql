-- =====================================================================
-- TEMPLATE — contains ${STUDENT_ID} placeholders. Run via envsubst:
--   export STUDENT_ID=<your-id>
--   envsubst < sql/bronze_ddl.sql | hive -f -        (or impala-shell -f -)
-- =====================================================================
-- =====================================================================
-- ARGUS — Bronze Layer DDL
-- =====================================================================
-- 6 tables — raw landing zone for all source systems.
-- Format:    Iceberg with ORC files, merge-on-read mode for
--            late-arriving corrections without rewriting whole files.
-- Partition: ingest_date for streaming sources, disposition_date for
--            the batch-loaded legacy alerts.
-- Schema:    argus_${STUDENT_ID}_bronze
-- =====================================================================
-- Tables defined here:
--   1. argus_${STUDENT_ID}_bronze.orders_raw         (TARANG matching engine telemetry)
--   2. argus_${STUDENT_ID}_bronze.trades_raw         (NIPATAN executed trades)
--   3. argus_${STUDENT_ID}_bronze.member_cdc         (KAVACH CDC stream)
--   4. argus_${STUDENT_ID}_bronze.instrument_cdc     (PRATEEK reference + corp actions + surveillance state)
--   5. argus_${STUDENT_ID}_bronze.external_feeds     (SEBI feed + BBO + news, source-tagged union)
--   6. argus_${STUDENT_ID}_bronze.legacy_alerts      (SMRITI batch-loaded alert history)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS argus_${STUDENT_ID}_bronze
COMMENT 'ARGUS raw-landing layer; merge-on-read ORC for late-arriving corrections';

-- ---------------------------------------------------------------------
-- 1. orders_raw — TARANG firehose
-- ---------------------------------------------------------------------
-- The single highest-volume table in the capstone. Every order, modify,
-- cancel, partial-fill, and full-fill from the matching engine lands here.
-- Partitioned by ingest_date so daily compaction + retention are simple.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_bronze.orders_raw;
CREATE TABLE argus_${STUDENT_ID}_bronze.orders_raw (
    event_id            STRING       COMMENT 'UUID per matching-engine event',
    ts_us               BIGINT       COMMENT 'Microsecond epoch from match engine',
    ts_ingest           TIMESTAMP    COMMENT 'When NiFi landed the record in Bronze',
    member_firm_id      STRING       COMMENT 'KAVACH member firm — joins to member_master',
    trader_id           STRING       COMMENT 'Named human trader within the member firm',
    instrument_code     STRING       COMMENT 'PRATEEK instrument code (e.g. RELIANCE-EQ, NIFTY24SEPFUT)',
    side                STRING       COMMENT 'BUY | SELL',
    order_type          STRING       COMMENT 'LIMIT | MARKET | STOP | IOC | FOK',
    qty                 BIGINT       COMMENT 'Order quantity (shares or lots)',
    price               DECIMAL(18,4) COMMENT 'Limit price; NULL for market orders',
    action              STRING       COMMENT 'NEW | MODIFY | CANCEL | PARTIAL_FILL | FULL_FILL',
    parent_order_id     STRING       COMMENT 'Original order_id for modifies/cancels',
    book_state_after    STRING       COMMENT 'JSON snapshot of affected price level after the event',
    raw_payload         STRING       COMMENT 'Original Kafka message body (for replay/debug)',
    ingest_date         DATE         COMMENT 'Partition key — date NiFi landed the record'
)
USING ICEBERG
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'write.format.default'   = 'ORC',
    'write.merge.mode'       = 'merge-on-read',
    'write.target-file-size-bytes' = '134217728',  -- 128 MiB
    'commit.retry.num-retries'     = '4',
    'comment'                = 'ARG-1 — TARANG firehose, raw landing'
);

-- ---------------------------------------------------------------------
-- 2. trades_raw — NIPATAN executed trades
-- ---------------------------------------------------------------------
-- Post-match executed trades. Lower volume than orders (~280M/day vs 3.5B)
-- because only fills produce a trade record.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_bronze.trades_raw;
CREATE TABLE argus_${STUDENT_ID}_bronze.trades_raw (
    trade_id              STRING       COMMENT 'Globally unique trade ID from clearing',
    ts_us                 BIGINT       COMMENT 'Microsecond epoch of trade execution',
    ts_ingest             TIMESTAMP    COMMENT 'When NiFi landed the record',
    instrument_code       STRING       COMMENT 'PRATEEK instrument code',
    buy_member_firm_id    STRING       COMMENT 'Buyer member firm',
    sell_member_firm_id   STRING       COMMENT 'Seller member firm',
    buy_investor_acct     STRING       COMMENT 'Buy-side end-investor account (hashed)',
    sell_investor_acct    STRING       COMMENT 'Sell-side end-investor account (hashed)',
    buy_trader_id         STRING       COMMENT 'Buy-side trader at the member firm',
    sell_trader_id        STRING       COMMENT 'Sell-side trader at the member firm',
    exec_price            DECIMAL(18,4) COMMENT 'Executed price',
    exec_qty              BIGINT       COMMENT 'Executed quantity',
    settlement_date       DATE         COMMENT 'T+1 / T+2 settlement date',
    clearing_member_flag  STRING       COMMENT 'Y if traded under a clearing-member account',
    raw_payload           STRING       COMMENT 'Original Kafka message body',
    ingest_date           DATE         COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'write.format.default'   = 'ORC',
    'write.merge.mode'       = 'merge-on-read',
    'write.target-file-size-bytes' = '134217728',
    'comment'                = 'NIPATAN executed-trade landing'
);

-- ---------------------------------------------------------------------
-- 3. member_cdc — KAVACH CDC stream
-- ---------------------------------------------------------------------
-- Hourly CDC events from the KYC + member master. PII-bearing — every
-- row that touches a real human appears here. Atlas tags are applied
-- in Module 7 to drive Ranger masking; do NOT remove the column comments,
-- they're parsed by the classification automation.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_bronze.member_cdc;
CREATE TABLE argus_${STUDENT_ID}_bronze.member_cdc (
    cdc_op             STRING      COMMENT 'INSERT | UPDATE | DELETE from Debezium',
    cdc_ts             TIMESTAMP   COMMENT 'Source-system commit timestamp',
    ts_ingest          TIMESTAMP   COMMENT 'When NiFi landed the record',
    member_firm_id     STRING      COMMENT 'Member firm primary key',
    member_firm_name   STRING      COMMENT 'Registered name of the broker-dealer',
    sebi_registration  STRING      COMMENT 'SEBI registration number — PII_LOW classification',
    capital_adequacy   DECIMAL(18,2) COMMENT 'Net worth in INR crore',
    suspension_history STRING      COMMENT 'JSON array of past suspensions',
    trader_id          STRING      COMMENT 'Trader within the member firm (NULL for member-only updates)',
    trader_name        STRING      COMMENT 'Trader full name — PII_HIGH classification',
    investor_acct      STRING      COMMENT 'End-investor account ID — PII_LOW classification',
    investor_pan       STRING      COMMENT 'Investor PAN — PII_HIGH classification',
    investor_email     STRING      COMMENT 'Investor email — PII_HIGH classification',
    investor_mobile    STRING      COMMENT 'Investor mobile — PII_HIGH classification',
    investor_demat     STRING      COMMENT 'Demat account number — PII_LOW classification',
    investor_kyc_tier  INT         COMMENT 'KYC tier 1/2/3',
    raw_payload        STRING      COMMENT 'Original CDC message',
    ingest_date        DATE        COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'write.format.default'   = 'ORC',
    'write.merge.mode'       = 'merge-on-read',
    'comment'                = 'KAVACH CDC — PII-bearing; Atlas-classified in Module 7'
);

-- ---------------------------------------------------------------------
-- 4. instrument_cdc — PRATEEK reference + corp actions + surveillance state
-- ---------------------------------------------------------------------
-- Single Bronze table fed by two Kafka topics (instrument.cdc and
-- surveillance.state). Differentiated by event_kind.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_bronze.instrument_cdc;
CREATE TABLE argus_${STUDENT_ID}_bronze.instrument_cdc (
    cdc_op             STRING       COMMENT 'INSERT | UPDATE | DELETE',
    cdc_ts             TIMESTAMP    COMMENT 'Source-system commit timestamp',
    ts_ingest          TIMESTAMP    COMMENT 'When NiFi landed the record',
    event_kind         STRING       COMMENT 'INSTRUMENT | CORP_ACTION | SURVEILLANCE_STATE',
    instrument_code    STRING       COMMENT 'Primary key',
    instrument_type    STRING       COMMENT 'EQUITY | ETF | FUTURE | OPTION | BOND',
    underlying_code    STRING       COMMENT 'For derivatives — underlying instrument',
    expiry_date        DATE         COMMENT 'F&O expiry; NULL for cash',
    strike_price       DECIMAL(18,4) COMMENT 'Options strike; NULL otherwise',
    lot_size           BIGINT       COMMENT 'F&O lot size',
    tick_size          DECIMAL(8,4) COMMENT 'Minimum price increment',
    corp_action_type   STRING       COMMENT 'SPLIT | BONUS | DIVIDEND | MERGER (when event_kind = CORP_ACTION)',
    corp_action_ratio  STRING       COMMENT 'e.g. "1:5" for split',
    corp_action_date   DATE         COMMENT 'Ex-date for the corporate action',
    esm_flag           STRING       COMMENT 'Y/N — Enhanced Surveillance Measure',
    asm_flag           STRING       COMMENT 'Y/N — Additional Surveillance Measure',
    circuit_band_pct   DECIMAL(5,2) COMMENT 'Daily circuit-breaker band',
    raw_payload        STRING       COMMENT 'Original message',
    ingest_date        DATE         COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'write.format.default'   = 'ORC',
    'write.merge.mode'       = 'merge-on-read',
    'comment'                = 'PRATEEK reference + corporate actions + ESM/ASM state'
);

-- ---------------------------------------------------------------------
-- 5. external_feeds — SEBI + BBO + news (source-tagged union)
-- ---------------------------------------------------------------------
-- Single Bronze table for all three external feeds. The Silver layer
-- splits these into purpose-specific tables. Schema is intentionally
-- generic — payload carries source-specific fields as JSON.
DROP TABLE IF EXISTS argus_${STUDENT_ID}_bronze.external_feeds;
CREATE TABLE argus_${STUDENT_ID}_bronze.external_feeds (
    source             STRING       COMMENT 'SEBI | BBO | NEWS',
    event_id           STRING       COMMENT 'Source-assigned ID',
    event_ts           TIMESTAMP    COMMENT 'Event timestamp from source',
    ts_ingest          TIMESTAMP    COMMENT 'When NiFi landed the record',
    instrument_code    STRING       COMMENT 'Tagged instrument (NULL for SEBI feed)',
    venue              STRING       COMMENT 'NSE | BSE (for BBO source)',
    payload            STRING       COMMENT 'Source-specific JSON body',
    raw_message        STRING       COMMENT 'Verbatim Kafka message',
    ingest_date        DATE         COMMENT 'Partition key'
)
USING ICEBERG
PARTITIONED BY (ingest_date, source)
TBLPROPERTIES (
    'write.format.default'   = 'ORC',
    'write.merge.mode'       = 'merge-on-read',
    'comment'                = 'External feeds union — split in Silver'
);

-- ---------------------------------------------------------------------
-- 6. legacy_alerts — SMRITI batch-loaded alert history
-- ---------------------------------------------------------------------
-- Append-only nightly batch from the legacy vendor surveillance platform.
-- 4.8M historical alerts with analyst dispositions — the supervised ML
-- training set in Module 5. Partitioned by disposition_date because that's
-- the natural grouping (alert lifecycle ends at disposition).
DROP TABLE IF EXISTS argus_${STUDENT_ID}_bronze.legacy_alerts;
CREATE TABLE argus_${STUDENT_ID}_bronze.legacy_alerts (
    alert_id              STRING      COMMENT 'Vendor-assigned unique alert ID',
    fired_ts              TIMESTAMP   COMMENT 'When the rule engine fired the alert',
    rule_id               STRING      COMMENT 'Vendor rule that fired',
    rule_version          STRING      COMMENT 'Rule version at firing time',
    severity              STRING      COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
    member_firm_id        STRING      COMMENT 'Member firm subject of the alert',
    trader_id             STRING      COMMENT 'Trader subject of the alert',
    instrument_code       STRING      COMMENT 'Instrument under suspicion',
    pattern_type          STRING      COMMENT 'SPOOFING | LAYERING | MOMENTUM_IGNITION | WASH | OTHER',
    analyst_id            STRING      COMMENT 'Analyst who dispositioned the alert',
    disposition           STRING      COMMENT 'NO_ACTION | ESCALATED | CONFIRMED_MANIPULATION',
    disposition_ts        TIMESTAMP   COMMENT 'When the analyst made the call',
    disposition_rationale STRING      COMMENT 'Free-text rationale (training signal for the LLM)',
    linked_str_id         STRING      COMMENT 'STR ID if filed; NULL otherwise',
    sebi_outcome          STRING      COMMENT 'NONE | INQUIRY_OPENED | ACTION_TAKEN | DISMISSED',
    sebi_outcome_ts       TIMESTAMP   COMMENT 'When the SEBI outcome was recorded',
    is_confirmed_manipulation BOOLEAN COMMENT 'Computed: TRUE if disposition = CONFIRMED_MANIPULATION OR sebi_outcome = ACTION_TAKEN',
    disposition_date      DATE        COMMENT 'Partition key — date the analyst dispositioned'
)
USING ICEBERG
PARTITIONED BY (disposition_date)
TBLPROPERTIES (
    'write.format.default'   = 'ORC',
    'write.merge.mode'       = 'merge-on-read',
    'write.target-file-size-bytes' = '67108864',  -- 64 MiB; smaller files OK for the slow-changing batch source
    'comment'                = 'SMRITI legacy alerts — ML training set (4.8M rows)'
);

-- =====================================================================
-- Verification queries (run interactively after CREATE)
-- =====================================================================
-- SHOW TABLES IN argus_${STUDENT_ID}_bronze;
-- DESCRIBE FORMATTED argus_${STUDENT_ID}_bronze.orders_raw;
-- SELECT COUNT(*) FROM argus_${STUDENT_ID}_bronze.orders_raw;     -- expect 0 immediately after DDL
