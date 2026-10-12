-- ============================================================
--  Analytical platform database (raw / report / ops schemas)
-- ============================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS report;
CREATE SCHEMA IF NOT EXISTS ops;

-- ------------------------------------------------------------
-- RAW LAYER
--   * one immutable snapshot of the source at CDC start
--   * an append-only log of every change event (JSONB payload)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.merchants_snapshot (
    merchant_id       bigint PRIMARY KEY,
    name              text,
    mcc               text,
    province          text,
    risk_level        text,
    owner_national_id text,
    owner_mobile      text,
    updated_at        timestamptz,
    attributes        jsonb,
    snapshot_lsn      pg_lsn,
    snapshot_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.transactions_snapshot (
    transaction_id bigint PRIMARY KEY,
    merchant_id    bigint,
    channel        text,
    card_bin       text,
    amount         bigint,
    fee_amount     bigint,
    status         text,
    created_at     timestamptz,
    updated_at     timestamptz,
    attributes     jsonb,
    snapshot_lsn   pg_lsn,
    snapshot_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.merchants_events (
    event_id        bigserial PRIMARY KEY,
    kafka_topic     text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    lsn             pg_lsn,
    txid            bigint,
    op              char(1) NOT NULL,          -- I / U / D (c,r -> I)
    old_key         jsonb,
    payload         jsonb NOT NULL,
    col_types       jsonb,
    ts_ms           bigint,
    commit_ts       timestamptz,
    received_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS raw.transactions_events (
    event_id        bigserial PRIMARY KEY,
    kafka_topic     text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    lsn             pg_lsn,
    txid            bigint,
    op              char(1) NOT NULL,          -- I / U / D (c,r -> I)
    old_key         jsonb,
    payload         jsonb NOT NULL,
    col_types       jsonb,
    ts_ms           bigint,
    commit_ts       timestamptz,
    received_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

-- ------------------------------------------------------------
-- OPS: checkpoints / watermarks / pipeline state / affected keys
-- ------------------------------------------------------------


CREATE TABLE IF NOT EXISTS ops.state (
    name       text PRIMARY KEY,
    value      text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.watermarks (
    name          text PRIMARY KEY,
    last_event_id bigint NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.aff_daily   (day date PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ops.aff_channel (day date, channel text, PRIMARY KEY (day, channel));
CREATE TABLE IF NOT EXISTS ops.aff_top     (month date, merchant_id bigint, PRIMARY KEY (month, merchant_id));
CREATE TABLE IF NOT EXISTS ops.aff_fee     (day date, mcc text, province text, PRIMARY KEY (day, mcc, province));
CREATE TABLE IF NOT EXISTS ops.aff_risk    (month date, risk_level text, PRIMARY KEY (month, risk_level));

-- ------------------------------------------------------------
-- REPORT LAYER (masked / enriched / history)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS report.merchants (
    merchant_id            bigint PRIMARY KEY,
    name                   text,
    mcc                    text,
    province               text,
    risk_level             text,
    owner_national_id_hash text,
    owner_mobile_hash      text,
    updated_at             timestamptz,
    last_event_id          bigint
);

CREATE TABLE IF NOT EXISTS report.merchant_history (
    merchant_id   bigint NOT NULL,
    mcc           text,
    province      text,
    risk_level    text,
    valid_from    timestamptz NOT NULL,
    valid_to      timestamptz NOT NULL DEFAULT 'infinity',
    is_current    boolean NOT NULL DEFAULT false,
    last_event_id bigint,
    PRIMARY KEY (merchant_id, valid_from)
);

CREATE TABLE IF NOT EXISTS report.transactions (
    transaction_id      bigint PRIMARY KEY,
    merchant_id         bigint,
    channel             text,
    card_bin            text,
    amount              bigint,
    fee_amount          bigint,
    status              text,
    created_at          timestamptz,
    updated_at          timestamptz,
    merchant_name       text,
    mcc                 text,
    risk_level_at_txn   text,
    province_at_txn     text,
    extra               jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_event_id       bigint
);

-- ------------------------------------------------------------
-- REPORT MARTS: one small table per dashboard card
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS report.mart_daily_sales (
    day        date PRIMARY KEY,
    amount     bigint NOT NULL DEFAULT 0,
    txn_count  bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS report.mart_daily_channel (
    day           date NOT NULL,
    channel       text NOT NULL,
    success_count bigint NOT NULL DEFAULT 0,
    total_count   bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (day, channel)
);

CREATE TABLE IF NOT EXISTS report.mart_top_merchants (
    month         date NOT NULL,
    merchant_id   bigint NOT NULL,
    merchant_name text,
    amount        bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (month, merchant_id)
);

CREATE TABLE IF NOT EXISTS report.mart_fee_mcc_province (
    day        date NOT NULL,
    mcc        text NOT NULL,
    province   text NOT NULL,
    fee_amount bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (day, mcc, province)
);

CREATE TABLE IF NOT EXISTS report.mart_monthly_success_risk (
    month      date NOT NULL,
    risk_level text NOT NULL,
    txn_count  bigint NOT NULL DEFAULT 0,
    amount     bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (month, risk_level)
);
