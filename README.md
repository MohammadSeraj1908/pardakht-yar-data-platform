# Pardakht Yar Data Platform

A simple end-to-end data platform for a payment company: PostgreSQL source, CDC with **Debezium + Kafka**, raw/report warehouse layers, five marts and Metabase dashboards. Everything starts with one command.

## Architecture

```
Simulator -> PostgreSQL source --(Debezium / Kafka)--> raw.*_events
                                                            |
                                        warehouse_modeler   v
              report.merchants / merchant_history / transactions -> marts -> Metabase
```

The initial 5,000,000 rows are loaded directly into `raw.*_snapshot`. Kafka carries **changes only**.

## Run

```bash
docker compose up -d --build
```

| Service | URL |
|---|---|
| Metabase | http://localhost:3000 — `admin@pardakhtyar.local` / `PardakhtYar123!` |
| Kafka UI | http://localhost:8080 |
| Debezium UI | http://localhost:8081 |
| Kafka Connect REST | http://localhost:8083 |
| Source Postgres | `localhost:5432` — app/app |
| Warehouse Postgres | `localhost:5433` — app/app |

First run takes about 5 minutes (5M rows, snapshot, first model). Full reset: `docker compose down -v`.

## Data flow

1. `source_simulator.py` seeds 2,000 merchants and 5,000,000 transactions, then keeps changing data (new transactions, status updates, reversals, risk changes, deletes, one schema change).
2. `source_snapshot_loader.py` loads the initial rows directly into `raw.*_snapshot`.
3. `debezium_connector_init.py` registers the Debezium connector; changes go to Kafka topics `pardakht.core.merchants` and `pardakht.core.transactions`.
4. `kafka_raw_consumer.py` writes each change to `raw.*_events` (at-least-once, dedup by Kafka offset).
5. `warehouse_modeler.py` builds `report.*` incrementally: SCD2 risk history, risk/province at transaction time, and only the affected groups in the five marts.
6. `metabase_setup.py` creates the datasources, 10 cards and 2 dashboards automatically.

## Design choices

- **Debezium + Kafka** for standard CDC: ordered changes, UPDATE/DELETE support, schema changes.
- **Direct bulk snapshot** instead of sending 5M historical rows through Kafka.
- **One PostgreSQL for raw and report**: simple, transactional, enough for this size.
- **Incremental modeler** with a watermark; never rebuilds the full data.
- **PII masking**: national ID and mobile are hashed (SHA-256 + salt) before the report layer.
- **Schema changes**: new fields go to the `extra` JSONB column, so the pipeline never stops.

## Commands

```bash
bash check.sh                                      # health check
bash full_check.sh                                 # full report (source, Debezium, Kafka, warehouse)
docker compose --profile test  run --rm tests      # 6 end-to-end tests
docker compose --profile bench run --rm bench      # writes reports/runtime_report.md
```

## Notes

- 5 cards run on the marts (fast) and 5 baseline cards run on the source (slow) for comparison.
- Configuration: copy `.env.example` to `.env` (seed size, transaction rate, schema change time).
- Scale path: multi-broker Kafka + Schema Registry, Iceberg/ClickHouse, dbt.
