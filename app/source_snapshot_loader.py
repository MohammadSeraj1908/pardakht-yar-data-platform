"""One-shot job: copy the initial rows into raw.*_snapshot."""
import time

import psycopg2.extras

from shared_common import SOURCE, WAREHOUSE, connect

MERCHANTS = "merchant_id,name,mcc,province,risk_level,owner_national_id,owner_mobile,updated_at"
TRANSACTIONS = "transaction_id,merchant_id,channel,card_bin,amount,fee_amount,status,created_at,updated_at"


def wait_for_seed(src):
    while True:
        with src.cursor() as cur:
            cur.execute("select 1 from public.pipeline_control where step = 'seed_done'")
            if cur.fetchone():
                return
        print("waiting for the seed ...", flush=True)
        time.sleep(2)


def copy(src, wh, table, columns, target, lsn):
    with wh.cursor() as cur:
        cur.execute(f"truncate {target}")
    total = 0
    with src.cursor(name=table) as reader:
        reader.itersize = 20000
        reader.execute(f"select {columns} from core.{table}")
        while True:
            rows = reader.fetchmany(20000)
            if not rows:
                break
            with wh.cursor() as writer:
                psycopg2.extras.execute_values(
                    writer,
                    f"insert into {target}({columns}, snapshot_lsn) values %s",
                    [tuple(row) + (lsn,) for row in rows],
                    page_size=5000,
                )
            wh.commit()
            total += len(rows)
            print(f"{table}: {total:,} rows", flush=True)
    return total


def main():
    src = connect(SOURCE, autocommit=True)
    wait_for_seed(src)
    wh = connect(WAREHOUSE)

    with wh.cursor() as cur:
        cur.execute("select 1 from ops.state where name = 'snapshot_loaded'")
        already = cur.fetchone()

    if already:
        print("snapshot already loaded", flush=True)
    else:
        with src.cursor() as cur:
            cur.execute("select pg_current_wal_lsn()::text")
            lsn = cur.fetchone()[0]
        print("loading the initial snapshot (direct, not through Kafka)", flush=True)

        reader = connect(SOURCE)
        copy(reader, wh, "merchants", MERCHANTS, "raw.merchants_snapshot", lsn)
        copy(reader, wh, "transactions", TRANSACTIONS, "raw.transactions_snapshot", lsn)
        reader.close()

        with wh.cursor() as cur:
            cur.execute("""insert into ops.state(name, value) values ('snapshot_loaded', %s)
                           on conflict (name) do update set value = excluded.value""", (lsn,))
        wh.commit()

    with src.cursor() as cur:
        cur.execute("insert into public.pipeline_control(step) values ('snapshot_ready') on conflict do nothing")
    print("snapshot_ready", flush=True)


if __name__ == "__main__":
    main()
