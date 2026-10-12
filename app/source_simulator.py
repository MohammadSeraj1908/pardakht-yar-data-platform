"""Create the source data and keep changing it."""
import os
import time

from shared_common import SOURCE, connect

MERCHANTS = int(os.getenv("SEED_MERCHANTS", "2000"))
TRANSACTIONS = int(os.getenv("SEED_TRANSACTIONS", "5000000"))
RATE = int(os.getenv("TXNS_PER_SECOND", "20"))
SCHEMA_AFTER = int(os.getenv("SCHEMA_CHANGE_AFTER_MINUTES", "3")) * 60
MERCHANT_EVERY = int(os.getenv("MERCHANT_UPDATE_SECONDS", "60"))
TEST_EVERY = int(os.getenv("TEST_TXN_SECONDS", "30"))

MCC = ["5411", "5812", "5541", "4111", "5912", "4814", "5999", "7011", "5651", "5942"]
PROVINCES = ["Tehran", "Isfahan", "Fars", "Khorasan", "East Azerbaijan", "Alborz", "Gilan", "Yazd"]
RISKS = ["LOW", "MEDIUM", "HIGH"]


def step_done(conn, name):
    with conn.cursor() as cur:
        cur.execute("select 1 from public.pipeline_control where step = %s", (name,))
        return cur.fetchone() is not None


def mark(conn, name):
    with conn.cursor() as cur:
        cur.execute("insert into public.pipeline_control(step) values (%s) on conflict do nothing", (name,))


def seed(conn):
    with conn.cursor() as cur:
        cur.execute("select count(*) from core.merchants")
        if cur.fetchone()[0] == 0:
            print("seeding merchants ...", flush=True)
            cur.execute("""
                with cfg as (select %s::text[] mcc, %s::text[] provinces, %s::text[] risks)
                insert into core.merchants
                    (merchant_id, name, mcc, province, risk_level, owner_national_id, owner_mobile, updated_at)
                select g, 'Merchant ' || lpad(g::text, 5, '0'),
                       cfg.mcc[1 + floor(random() * array_length(cfg.mcc, 1))::int],
                       cfg.provinces[1 + floor(random() * array_length(cfg.provinces, 1))::int],
                       cfg.risks[1 + floor(random() * array_length(cfg.risks, 1))::int],
                       lpad((1000000000 + floor(random() * 8999999999))::bigint::text, 10, '0'),
                       '09' || lpad(floor(random() * 1000000000)::bigint::text, 9, '0'),
                       now() - interval '180 days'
                from generate_series(1, %s) g, cfg""", (MCC, PROVINCES, RISKS, MERCHANTS))

        cur.execute("select count(*) from core.transactions")
        existing = cur.fetchone()[0]
        if existing < TRANSACTIONS:
            print("seeding transactions (this takes a while) ...", flush=True)
            cur.execute("set local synchronous_commit = off")
            cur.execute("""
                insert into core.transactions
                    (transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at)
                select %s + g,
                       1 + floor(random() * %s)::bigint,
                       (array['POS','IPG','QR'])[1 + floor(random() * 3)::int],
                       lpad(floor(random() * 1000000)::bigint::text, 6, '0'),
                       a.amount,
                       round(a.amount * (0.005 + random() * 0.015))::bigint,
                       case when random() < 0.80 then 'SUCCESS'
                            when random() < 0.50 then 'FAILED'
                            else 'REVERSED' end,
                       now() - (random() * interval '180 days'),
                       now() - (random() * interval '180 days')
                from generate_series(1, %s) g,
                     lateral (select (1 + floor(random() * 50000000))::bigint amount) a""",
                (existing, MERCHANTS, TRANSACTIONS - existing))

        cur.execute("create sequence if not exists core.txn_id_seq")
        cur.execute("""select setval('core.txn_id_seq',
                       (select greatest(coalesce(max(transaction_id), 0) + 1, 1) from core.transactions), false)""")
    conn.commit()
    mark(conn, "seed_done")
    print("seed done", flush=True)


def add_transactions(conn, count, installments):
    extra_col = ", installments" if installments else ""
    extra_val = ", case when random() < 0.3 then 1 + floor(random() * 12)::int end" if installments else ""
    with conn.cursor() as cur:
        cur.execute(f"""
            insert into core.transactions
                (transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at{extra_col})
            select nextval('core.txn_id_seq'),
                   1 + floor(random() * %s)::bigint,
                   (array['POS','IPG','QR'])[1 + floor(random() * 3)::int],
                   lpad(floor(random() * 1000000)::bigint::text, 6, '0'),
                   a.amount,
                   round(a.amount * (0.005 + random() * 0.015))::bigint,
                   'PENDING', now(), now(){extra_val}
            from generate_series(1, %s) g,
                 lateral (select (1 + floor(random() * 50000000))::bigint amount) a""",
            (MERCHANTS, count))


def finish_pending(conn):
    with conn.cursor() as cur:
        cur.execute("""
            with pick as (select transaction_id from core.transactions
                          where status = 'PENDING' and created_at < now() - interval '4 seconds'
                          order by transaction_id limit 60 for update skip locked)
            update core.transactions t
               set status = case when random() < 0.8 then 'SUCCESS' else 'FAILED' end, updated_at = now()
              from pick where t.transaction_id = pick.transaction_id""")


def reverse_some(conn):
    with conn.cursor() as cur:
        cur.execute("""
            with pick as (select transaction_id from core.transactions
                          where status = 'SUCCESS' and created_at < now() - interval '60 seconds'
                          order by transaction_id desc limit 2 for update skip locked)
            update core.transactions t set status = 'REVERSED', updated_at = now()
              from pick where t.transaction_id = pick.transaction_id and random() < 0.3""")


def change_merchants(conn):
    with conn.cursor() as cur:
        cur.execute("""
            update core.merchants c
               set risk_level = r.risks[1 + floor(random() * array_length(r.risks, 1))::int],
                   province = r.provinces[1 + floor(random() * array_length(r.provinces, 1))::int],
                   updated_at = now()
              from (select %s::text[] risks, %s::text[] provinces) r
             where c.merchant_id in (select merchant_id from core.merchants order by random() limit 2)""",
            (RISKS, PROVINCES))


def main():
    conn = connect(SOURCE)
    seed(conn)
    while not step_done(conn, "debezium_ready"):
        print("waiting for Debezium ...", flush=True)
        time.sleep(2)

    print("simulator started", flush=True)
    start = time.time()
    last_rate = last_finish = last_reverse = last_merchant = last_test = start
    installments = step_done(conn, "schema_change_done")
    pending = 0.0
    test_txns = []

    while True:
        now = time.time()
        pending += (now - last_rate) * RATE
        last_rate = now
        batch = int(pending)
        if batch:
            pending -= batch
            add_transactions(conn, batch, installments)

        if now - last_finish >= 1:
            finish_pending(conn)
            last_finish = now
        if now - last_reverse >= 5:
            reverse_some(conn)
            last_reverse = now
        if now - last_merchant >= MERCHANT_EVERY:
            change_merchants(conn)
            last_merchant = now
        if now - last_test >= TEST_EVERY:
            with conn.cursor() as cur:
                cur.execute("""insert into core.transactions
                    (transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at)
                    select nextval('core.txn_id_seq'), 1 + floor(random() * %s)::bigint,
                           'QR', '999999', 1000, 10, 'PENDING', now(), now() returning transaction_id""", (MERCHANTS,))
                test_txns.append((now + 25, cur.fetchone()[0]))
            last_test = now
        for due, tid in list(test_txns):
            if now >= due:
                with conn.cursor() as cur:
                    cur.execute("delete from core.transactions where transaction_id = %s", (tid,))
                test_txns.remove((due, tid))

        if not installments and now - start >= SCHEMA_AFTER:
            with conn.cursor() as cur:
                cur.execute("alter table core.transactions add column if not exists installments integer")
            mark(conn, "schema_change_done")
            installments = True
            print("added column installments", flush=True)

        conn.commit()
        time.sleep(0.1)


if __name__ == "__main__":
    main()
