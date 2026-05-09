-- =====================================================================
-- JOB-11 — realtime_cross_product_imbalance (SQL Stream Builder)
-- =====================================================================
-- TEMPLATE — contains ${STUDENT_ID} placeholders. Apply via SSB UI:
--   1. Set the SSB variable STUDENT_ID at the cluster level (Settings →
--      Materialized View Variables) OR run this file through envsubst
--      before pasting:
--          export STUDENT_ID=s001
--          envsubst < sql/job_11_cross_product_imbalance.sql
--      ...then paste the result into the SSB SQL editor.
--   2. Click "Execute Job" — SSB compiles to a Flink streaming app.
--
-- PRD reference: §7 (JOB-11); demonstrates the analyst-driven streaming-
-- SQL path. Same R-104 cross-product imbalance pattern as the JOB-08
-- batch rule, but as declarative SQL with no Java/Python required.
--
-- Architectural role: writes to argus.${STUDENT_ID}.realtime_alerts.v1
-- with source_engine='SSB' so JOB-12 can persist it to
-- argus_${STUDENT_ID}_gold.realtime_alert_stream alongside the
-- Flink-engine alerts from JOB-10.
-- =====================================================================

-- ----- 1. Source: orders.v1 -----
CREATE TABLE orders_stream_${STUDENT_ID} (
    event_id        STRING,
    ts_us           BIGINT,
    member_firm_id  STRING,
    trader_id       STRING,
    instrument_code STRING,
    side            STRING,                  -- BUY / SELL
    qty             BIGINT,
    price           DECIMAL(18, 4),
    action          STRING,                  -- NEW / CANCEL / FILL
    -- event-time watermark — bounded out-of-orderness 50ms
    event_time AS TO_TIMESTAMP_LTZ(ts_us, 6),
    WATERMARK FOR event_time AS event_time - INTERVAL '0.05' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'argus.${STUDENT_ID}.orders.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.group.id' = 'argus.${STUDENT_ID}.ssb.cross_product_v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- ----- 2. Reference: instrument_master (current state) -----
-- Joined to derive the underlying for each instrument so we can detect
-- imbalance across cash + futures + options on the SAME underlying.
CREATE TABLE instrument_master_${STUDENT_ID} (
    instrument_code STRING,
    underlying_code STRING,                  -- e.g. 'BNXM' for BNXM-FUT, BNXM-CE-1500 etc.
    product_type    STRING,                  -- 'CASH' / 'FUT' / 'OPT_CALL' / 'OPT_PUT'
    PRIMARY KEY (instrument_code) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'argus.${STUDENT_ID}.instrument.cdc.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'key.format' = 'raw',
    'value.format' = 'json'
);

-- ----- 3. Sink: realtime_alerts.v1 -----
CREATE TABLE realtime_alerts_sink_${STUDENT_ID} (
    alert_id             STRING,
    fired_ts             BIGINT,
    source_engine        STRING,
    rule_id              STRING,
    severity             STRING,
    pattern_type         STRING,
    member_firm_id       STRING,
    instrument_code      STRING,
    underlying_code      STRING,
    window_start_ts      BIGINT,
    window_end_ts        BIGINT,
    evidence_json        STRING,
    detection_latency_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'argus.${STUDENT_ID}.realtime_alerts.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'format' = 'json'
);

-- ----- 4. The actual detection — R-104 cross-product imbalance -----
-- Sliding 60-second window per (member_firm_id × underlying_code).
-- Imbalance metric: sum(BUY qty in cash) − sum(SELL qty equivalent in
-- futures+options × delta). When abs > 7.0, alert fires (matching JOB-08
-- batch rule R-104; see PRD §7).
INSERT INTO realtime_alerts_sink_${STUDENT_ID}
SELECT
    CONCAT('RT-XPROD-', o.member_firm_id, '-', i.underlying_code, '-',
           CAST(window_end AS STRING))           AS alert_id,
    UNIX_TIMESTAMP() * 1000000                    AS fired_ts,
    'SSB'                                         AS source_engine,
    'R-104'                                       AS rule_id,
    'CRITICAL'                                    AS severity,
    'CROSS_PRODUCT_IMBALANCE'                     AS pattern_type,
    o.member_firm_id                              AS member_firm_id,
    MAX(o.instrument_code)                        AS instrument_code,
    i.underlying_code                             AS underlying_code,
    UNIX_TIMESTAMP(window_start) * 1000000        AS window_start_ts,
    UNIX_TIMESTAMP(window_end)   * 1000000        AS window_end_ts,
    -- Evidence as JSON string for analyst review
    CONCAT('{"cash_buy":', SUM(CASE WHEN i.product_type = 'CASH' AND o.side = 'BUY'  THEN o.qty ELSE 0 END),
           ',"cash_sell":', SUM(CASE WHEN i.product_type = 'CASH' AND o.side = 'SELL' THEN o.qty ELSE 0 END),
           ',"fut_buy":',   SUM(CASE WHEN i.product_type = 'FUT'  AND o.side = 'BUY'  THEN o.qty ELSE 0 END),
           ',"fut_sell":',  SUM(CASE WHEN i.product_type = 'FUT'  AND o.side = 'SELL' THEN o.qty ELSE 0 END),
           ',"imbalance":', CAST(/* simplified — production uses option deltas */
                            (SUM(CASE WHEN i.product_type = 'CASH' AND o.side = 'BUY' THEN o.qty ELSE 0 END)
                           - SUM(CASE WHEN i.product_type = 'FUT' AND o.side = 'SELL' THEN o.qty ELSE 0 END))
                                   AS STRING),
           '}')                                  AS evidence_json,
    UNIX_TIMESTAMP() * 1000 - UNIX_TIMESTAMP(window_end) * 1000
                                                  AS detection_latency_ms
FROM TABLE(
    HOP(TABLE orders_stream_${STUDENT_ID},
        DESCRIPTOR(event_time),
        INTERVAL '10' SECOND,                   -- slide every 10s
        INTERVAL '60' SECOND)                   -- 60s window
) AS o
JOIN instrument_master_${STUDENT_ID} FOR SYSTEM_TIME AS OF o.event_time AS i
    ON o.instrument_code = i.instrument_code
WHERE o.action = 'FILL'                         -- only count actual fills
GROUP BY
    window_start, window_end,
    o.member_firm_id, i.underlying_code
HAVING
    -- The imbalance condition — calibrate threshold per the PRD lock
    ABS(SUM(CASE WHEN i.product_type = 'CASH' AND o.side = 'BUY' THEN o.qty ELSE 0 END)
      - SUM(CASE WHEN i.product_type = 'FUT' AND o.side = 'SELL' THEN o.qty ELSE 0 END))
    >= 70000;                                   -- 7.0 × 10000 lot size
