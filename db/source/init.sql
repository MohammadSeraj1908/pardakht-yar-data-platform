-- ============================================================
--  Source database: operational system of "Pardakht Yar"
--  Runs with wal_level=logical (see docker-compose command args)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.merchants (
    merchant_id        bigint PRIMARY KEY,
    name               text        NOT NULL,
    mcc                varchar(4)  NOT NULL,
    province           text        NOT NULL,
    risk_level         text        NOT NULL CHECK (risk_level IN ('LOW','MEDIUM','HIGH')),
    owner_national_id  varchar(10) NOT NULL,
    owner_mobile       varchar(11) NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.transactions (
    transaction_id bigint PRIMARY KEY,
    merchant_id    bigint      NOT NULL REFERENCES core.merchants(merchant_id),
    channel        text        NOT NULL CHECK (channel IN ('POS','IPG','QR')),
    card_bin       varchar(6)  NOT NULL,
    amount         bigint      NOT NULL,
    fee_amount     bigint      NOT NULL,
    status         text        NOT NULL CHECK (status IN ('PENDING','SUCCESS','FAILED','REVERSED')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Complete old tuples in the WAL stream (needed for reliable UPDATE/DELETE).
ALTER TABLE core.merchants    REPLICA IDENTITY FULL;
ALTER TABLE core.transactions REPLICA IDENTITY FULL;

-- Startup handshake between the simulator and the CDC worker:
--   seed_done            -> initial data is ready, CDC may take the snapshot
--   cdc_snapshot_done    -> snapshot is captured, simulator may start changing data
--   schema_change_done   -> the mid-run "installments" column was added
CREATE TABLE IF NOT EXISTS public.pipeline_control (
    step    text PRIMARY KEY,
    done_at timestamptz NOT NULL DEFAULT now()
);
