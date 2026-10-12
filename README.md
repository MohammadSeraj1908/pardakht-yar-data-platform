# Pardakht Yar Data Platform — Interview Task (Senior Data Engineer)

A simple but complete data platform: PostgreSQL source, CDC with **Debezium + Kafka Connect**, a raw/report warehouse, five Metabase cards, and automated tests. Everything starts with one command.

## Run with one command

```bash
docker compose up -d --build
```

| Service | Address |
|---|---|
| Metabase | http://localhost:3000 — `admin@pardakhtyar.local` / `PardakhtYar123!` |
| Source Postgres | `localhost:5432` — app/app |
| Warehouse Postgres | `localhost:5433` — app/app |
| Kafka | `localhost:9092` |
| Kafka Connect (REST) | http://localhost:8083 |
| Kafka UI | http://localhost:8080 |
| Debezium UI | http://localhost:8081 |

First run takes about **5 minutes** (Kafka/Connect up ~1m, seed 5M rows ~1m, snapshot ~2m, initial model ~40s). Later runs are fast. Full reset: `docker compose down -v`.

```bash
# health check on Linux / WSL (containers, data, CDC, reconciliation)
bash check.sh

# full report (source, Debezium, Kafka, warehouse, last hour)
bash full_check.sh

# or just show container status
docker compose ps -a

# E2E tests (6 tests: keys, domains, PII masking, reconciliation, live CDC, schema evolution)
docker compose --profile test run --rm tests

# benchmark before/after -> reports/runtime_report.md and reports/bench.json
docker compose --profile bench run --rm bench
```

## Architecture

```
                       +--------------------------+
   seed + changes      |  PostgreSQL Source       |   logical replication (pgoutput)
  +------------------> |  core.merchants          | -------------------------------+
  |                    |  core.transactions       |                                v
+-+--------------+     +--------------------------+                    +-----------------------+
|  Simulator     |                                                     |  Kafka Connect        |
+----------------+                                                     |  + Debezium connector |
                                                                       +-----------+-----------+
                    bulk snapshot (direct, not via Kafka)                          | JSON changes
        +------------------+                                                       v
        | Snapshot Loader  | --------------+                            +--------------------+
        +------------------+               |                            |  Kafka (KRaft)     |
                                           v                            +---------+----------+
                       +---------------------------------+                         |
                       |  Warehouse PostgreSQL           | <-----------------------+
                       |  raw  /  report  /  ops         |   Kafka Consumer (python)
                       +---------------+-----------------+
                                       | incremental modeler (watermark)
                                       v
                       +---------------------------------+
                       |  marts (5 dashboard cards)      |
                       +---------------+-----------------+
                                       v
                              +-----------------+
                              |    Metabase     |
                              +-----------------+
```

**Why these tools:** CDC with **Debezium** on **Kafka Connect** (standard, ordered by LSN, handles UPDATE/DELETE and schema changes) with a single-node KRaft Kafka and plain JSON (no Schema Registry/Avro) to keep it light. The 5M initial rows are loaded **directly** into `raw.*_snapshot` (bulk), so they do not pass through Kafka. **Alternatives rejected:** Debezium Server (limited sinks), Avro + Schema Registry (extra complexity), multi-broker Kafka/ZooKeeper (not needed for a 16 GB demo), custom test_decoding CDC (simple but not standard).

## Layers and rules

| Layer | Content | Rule |
|---|---|---|
| `raw` | `*_snapshot` + `*_events` (JSONB + Kafka offset + LSN) | append-only, schema-flexible, no transformation |
| `report` | `merchants`, `merchant_history` (SCD2), `transactions` | enriched and masked; incremental only (watermark); no raw PII |
| `ops` | watermark, state, affected keys | pipeline coordination only |

## CDC (Debezium + Kafka)

- **Initial snapshot**: direct bulk load into `raw.*_snapshot`; the connector is created after the snapshot so no change is lost.
- **Kafka Connect**: `PostgresConnector` with `pgoutput`, a dedicated slot and publication, `tombstones.on.delete=true`, JSON without schema.
- **Consumer**: reads the two topics (`pardakht.core.merchants`, `pardakht.core.transactions`) and writes `raw.*_events`; Kafka offsets are committed only after the warehouse commit (at-least-once) and `UNIQUE(topic, partition, offset)` removes duplicates.
- **Schema change**: adding `installments` mid-run does not break the pipeline; the new column is added dynamically in `report.transactions`.
- **UPDATE/DELETE**: `REPLICA IDENTITY FULL` gives the full `before` image.

## Data model and grain

| Table | Grain | Notes |
|---|---|---|
| `report.merchant_history` | merchant x time interval | risk/province/mcc over time (SCD2) |
| `report.transactions` | transaction | current state + `risk_level_at_txn`, `province_at_txn` |
| `mart_daily_sales` | day | successful amount and count |
| `mart_daily_channel` | day x channel | success / total |
| `mart_top_merchants` | month x merchant | successful amount |
| `mart_fee_mcc_province` | mcc x province | successful fee (province at transaction time) |
| `mart_monthly_success_risk` | month x risk | successful volume (risk at transaction time) |

Card 5 fixes the original problem (it showed the merchant's current risk for old transactions) by using a temporal join on `merchant_history`.

**Privacy:** `owner_national_id` and `owner_mobile` are hashed with SHA-256 and a salt before they enter `report`; raw values stay only in `raw` (restricted layer).

## Benchmark (5M transactions, real run)

| Card | Core (before) | Warehouse (after) | Speedup |
|---|---:|---:|---:|
| 1. Daily successful sales (90 days) | 777 ms | 0.5 ms | ~1553x |
| 2. Daily success rate by channel (30 days) | 630 ms | 0.7 ms | ~900x |
| 3. Top 10 merchants this month | 662 ms | 1.2 ms | ~552x |
| 4. Fee by mcc/province (90 days) | 692 ms | 6.0 ms | ~115x |
| 5. Risk at transaction time | 4441 ms | 0.4 ms | ~11103x |

Reconciliation for the last 30 days: Core and Warehouse are exactly equal. Cards 1-3 return the same result; cards 4 and 5 are intentionally different because they use province/risk **at transaction time**.

## Assumptions and limits

- The snapshot has no history of merchant risk before it was taken; seeded transactions use the initial risk.
- Single-node Kafka + JSON is for a simple demo; production would use multi-broker Kafka + Avro/Schema Registry.
- Volume and rate are configurable with env vars (`SEED_TRANSACTIONS`, `TXNS_PER_SECOND`, ...). The whole stack uses about 5 GB of RAM.
- Scale path: more brokers/partitions, distributed Kafka Connect, Iceberg/ClickHouse for higher volume, dbt for modeling.

