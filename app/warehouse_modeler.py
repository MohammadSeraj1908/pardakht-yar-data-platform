"""Build and update the report layer and the marts from raw events."""
import json
import time

from psycopg2.extras import Json

from shared_common import SALT, WAREHOUSE, connect

KNOWN = {"transaction_id", "merchant_id", "channel", "card_bin", "amount",
         "fee_amount", "status", "created_at", "updated_at"}
KEYS = """select created_at::date, channel, date_trunc('month',created_at)::date, merchant_id,
                 mcc, coalesce(province_at_txn,'-'), coalesce(risk_level_at_txn,'-')
          from report.transactions where transaction_id = %s"""


def query(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def one(conn, sql, args=None):
    result = query(conn, sql, args)
    return result[0] if result else None


def run(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)


def as_dict(value):
    return value if isinstance(value, dict) else (json.loads(value) if value else {})


def state(conn, name):
    item = one(conn, "select value from ops.state where name = %s", (name,))
    return item[0] if item else None


def set_state(conn, name, value):
    run(conn, """insert into ops.state(name,value) values (%s,%s)
                 on conflict (name) do update set value = excluded.value""", (name, value))


def watermark(conn, name):
    item = one(conn, "select last_event_id from ops.watermarks where name = %s", (name,))
    return item[0] if item else 0


def set_watermark(conn, name, value):
    run(conn, """insert into ops.watermarks(name,last_event_id) values (%s,%s)
                 on conflict (name) do update set last_event_id = greatest(ops.watermarks.last_event_id, excluded.last_event_id)""",
        (name, value))


def initial_load(conn):
    print("building the report layer from the snapshots ...", flush=True)
    run(conn, "create extension if not exists pgcrypto")
    run(conn, "truncate report.transactions, report.merchant_history, report.merchants")
    run(conn, """truncate report.mart_daily_sales, report.mart_daily_channel, report.mart_top_merchants,
                 report.mart_fee_mcc_province, report.mart_monthly_success_risk""")
    run(conn, """insert into report.merchants
        (merchant_id, name, mcc, province, risk_level, owner_national_id_hash, owner_mobile_hash, updated_at, last_event_id)
        select merchant_id, name, mcc, province, risk_level,
               encode(digest(%s || coalesce(owner_national_id,''),'sha256'),'hex'),
               encode(digest(%s || coalesce(owner_mobile,''),'sha256'),'hex'), updated_at, 0
        from raw.merchants_snapshot""", (SALT, SALT))
    run(conn, """insert into report.merchant_history
        (merchant_id, mcc, province, risk_level, valid_from, valid_to, is_current, last_event_id)
        select merchant_id, mcc, province, risk_level, '-infinity', 'infinity', true, 0
        from raw.merchants_snapshot""")
    run(conn, """insert into report.transactions
        (transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at,
         merchant_name, mcc, risk_level_at_txn, province_at_txn, extra, last_event_id)
        select t.transaction_id, t.merchant_id, t.channel, t.card_bin, t.amount, t.fee_amount, t.status,
               t.created_at, t.updated_at, m.name, m.mcc, m.risk_level, m.province, '{}'::jsonb, 0
        from raw.transactions_snapshot t
        left join raw.merchants_snapshot m on m.merchant_id = t.merchant_id""")
    conn.commit()
    set_state(conn, "report_initialized", "1")
    print("initial report load done", flush=True)


def ensure_indexes(conn):
    run(conn, "create index if not exists ix_history on report.merchant_history (merchant_id, valid_from desc)")
    run(conn, "create index if not exists ix_tx_created on report.transactions (created_at)")
    run(conn, "create index if not exists ix_tx_status_created on report.transactions (status, created_at)")
    run(conn, "create index if not exists ix_tx_merchant_created on report.transactions (merchant_id, created_at)")


def init_marts(conn):
    print("building the marts ...", flush=True)
    run(conn, """truncate report.mart_daily_sales, report.mart_daily_channel, report.mart_top_merchants,
                 report.mart_fee_mcc_province, report.mart_monthly_success_risk""")
    run(conn, "insert into report.mart_daily_sales select created_at::date, sum(amount), count(*) from report.transactions where status='SUCCESS' group by 1")
    run(conn, "insert into report.mart_daily_channel select created_at::date, channel, count(*) filter (where status='SUCCESS'), count(*) from report.transactions group by 1,2")
    run(conn, "insert into report.mart_top_merchants select date_trunc('month',created_at)::date, merchant_id, max(merchant_name), sum(amount) from report.transactions where status='SUCCESS' group by 1,2")
    run(conn, "insert into report.mart_fee_mcc_province select created_at::date, mcc, province_at_txn, sum(fee_amount) from report.transactions where status='SUCCESS' and province_at_txn is not null group by 1,2,3")
    run(conn, "insert into report.mart_monthly_success_risk select date_trunc('month',created_at)::date, risk_level_at_txn, count(*), sum(amount) from report.transactions where status='SUCCESS' and risk_level_at_txn is not null group by 1,2")
    conn.commit()
    set_state(conn, "marts_initialized", "1")
    print("marts done", flush=True)


def merchant_event(conn, event):
    event_id, op, old_key, payload, commit_ts = event
    old, new = as_dict(old_key), as_dict(payload)
    merchant_id = int(new.get("merchant_id") or old.get("merchant_id"))
    ts = new.get("updated_at") or commit_ts

    if op == "D":
        run(conn, "delete from report.merchants where merchant_id = %s", (merchant_id,))
        run(conn, "update report.merchant_history set valid_to=%s, is_current=false where merchant_id=%s and is_current", (ts, merchant_id))
        return

    current = one(conn, "select risk_level, province, mcc from report.merchant_history where merchant_id=%s and is_current", (merchant_id,))
    run(conn, """insert into report.merchants
        (merchant_id, name, mcc, province, risk_level, owner_national_id_hash, owner_mobile_hash, updated_at, last_event_id)
        values (%s,%s,%s,%s,%s,
                encode(digest(%s || coalesce(%s,''),'sha256'),'hex'),
                encode(digest(%s || coalesce(%s,''),'sha256'),'hex'), %s, %s)
        on conflict (merchant_id) do update set
            name=excluded.name, mcc=excluded.mcc, province=excluded.province, risk_level=excluded.risk_level,
            owner_national_id_hash=excluded.owner_national_id_hash, owner_mobile_hash=excluded.owner_mobile_hash,
            updated_at=excluded.updated_at, last_event_id=excluded.last_event_id""",
        (merchant_id, new.get("name"), new.get("mcc"), new.get("province"), new.get("risk_level"),
         SALT, new.get("owner_national_id"), SALT, new.get("owner_mobile"), ts, event_id))

    if current == (new.get("risk_level"), new.get("province"), new.get("mcc")):
        return
    run(conn, """update report.merchant_history
                 set valid_to = greatest(%s::timestamptz, valid_from + interval '1 microsecond'), is_current=false
                 where merchant_id=%s and is_current""", (ts, merchant_id))
    run(conn, """insert into report.merchant_history
        (merchant_id, mcc, province, risk_level, valid_from, valid_to, is_current, last_event_id)
        values (%s,%s,%s,%s,%s::timestamptz,'infinity',true,%s)
        on conflict (merchant_id, valid_from) do update set
            mcc=excluded.mcc, province=excluded.province, risk_level=excluded.risk_level,
            valid_to='infinity', is_current=true, last_event_id=excluded.last_event_id""",
        (merchant_id, new.get("mcc"), new.get("province"), new.get("risk_level"), ts, event_id))


def transaction_event(conn, event):
    event_id, op, old_key, payload = event
    old, new = as_dict(old_key), as_dict(payload)
    transaction_id = (new or old).get("transaction_id")
    affected = []

    if op == "D":
        keys = one(conn, KEYS, (transaction_id,))
        if keys:
            run(conn, "delete from report.transactions where transaction_id = %s", (transaction_id,))
            affected.append(keys)
        return affected

    if op == "U":
        keys = one(conn, KEYS, (transaction_id,))
        if keys:
            affected.append(keys)

    merchant_id = new.get("merchant_id")
    history = one(conn, """select risk_level, province from report.merchant_history
                           where merchant_id=%s and valid_from <= %s::timestamptz
                           order by valid_from desc limit 1""", (merchant_id, new.get("created_at")))
    merchant = one(conn, "select name, mcc from report.merchants where merchant_id=%s", (merchant_id,))
    risk, province = history or (None, None)
    name, mcc = merchant or (None, None)
    extra = {key: value for key, value in new.items() if key not in KNOWN}

    run(conn, """insert into report.transactions
        (transaction_id, merchant_id, channel, card_bin, amount, fee_amount, status, created_at, updated_at,
         merchant_name, mcc, risk_level_at_txn, province_at_txn, extra, last_event_id)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (transaction_id) do update set
            status=excluded.status, updated_at=excluded.updated_at, amount=excluded.amount,
            fee_amount=excluded.fee_amount, channel=excluded.channel, merchant_name=excluded.merchant_name,
            mcc=excluded.mcc, risk_level_at_txn=excluded.risk_level_at_txn,
            province_at_txn=excluded.province_at_txn, extra=excluded.extra, last_event_id=excluded.last_event_id""",
        (transaction_id, merchant_id, new.get("channel"), new.get("card_bin"), new.get("amount"),
         new.get("fee_amount"), new.get("status"), new.get("created_at"), new.get("updated_at"),
         name, mcc, risk, province, Json(extra), event_id))

    keys = one(conn, KEYS, (transaction_id,))
    if keys:
        affected.append(keys)
    return affected


def refresh_marts(conn, affected):
    days, channels, tops, fees, risks = set(), set(), set(), set(), set()
    for row in affected:
        days.add(row[0])
        channels.add((row[0], row[1]))
        tops.add((row[2], row[3]))
        fees.add((row[0], row[4], row[5]))
        risks.add((row[2], row[6]))

    if days:
        values = list(days)
        run(conn, "delete from report.mart_daily_sales where day = any(%s)", (values,))
        run(conn, """insert into report.mart_daily_sales
                     select created_at::date, sum(amount), count(*) from report.transactions
                     where status='SUCCESS' and created_at::date = any(%s) group by 1""", (values,))

    if channels:
        day_values = [x[0] for x in channels]
        channel_values = [x[1] for x in channels]
        run(conn, """delete from report.mart_daily_channel where (day,channel) in
                     (select * from unnest(%s::date[], %s::text[]))""", (day_values, channel_values))
        run(conn, """insert into report.mart_daily_channel
                     select created_at::date, channel, count(*) filter (where status='SUCCESS'), count(*)
                     from report.transactions where (created_at::date,channel) in
                     (select * from unnest(%s::date[], %s::text[])) group by 1,2""", (day_values, channel_values))

    if tops:
        month_values = [x[0] for x in tops]
        id_values = [x[1] for x in tops]
        run(conn, """delete from report.mart_top_merchants where (month,merchant_id) in
                     (select * from unnest(%s::date[], %s::bigint[]))""", (month_values, id_values))
        run(conn, """insert into report.mart_top_merchants
                     select date_trunc('month',t.created_at)::date, t.merchant_id, max(m.name), sum(t.amount)
                     from report.transactions t left join report.merchants m using (merchant_id)
                     where t.status='SUCCESS' and (date_trunc('month',t.created_at)::date, t.merchant_id) in
                     (select * from unnest(%s::date[], %s::bigint[])) group by 1,2""", (month_values, id_values))

    if fees:
        day_values = [x[0] for x in fees]
        mcc_values = [x[1] for x in fees]
        province_values = [x[2] for x in fees]
        run(conn, """delete from report.mart_fee_mcc_province where (day,mcc,province) in
                     (select * from unnest(%s::date[], %s::text[], %s::text[]))""",
            (day_values, mcc_values, province_values))
        run(conn, """insert into report.mart_fee_mcc_province
                     select created_at::date, mcc, coalesce(province_at_txn,'-'), sum(fee_amount)
                     from report.transactions where status='SUCCESS' and
                     (created_at::date, mcc, coalesce(province_at_txn,'-')) in
                     (select * from unnest(%s::date[], %s::text[], %s::text[])) group by 1,2,3""",
            (day_values, mcc_values, province_values))

    if risks:
        month_values = [x[0] for x in risks]
        risk_values = [x[1] for x in risks]
        run(conn, """delete from report.mart_monthly_success_risk where (month,risk_level) in
                     (select * from unnest(%s::date[], %s::text[]))""", (month_values, risk_values))
        run(conn, """insert into report.mart_monthly_success_risk
                     select date_trunc('month',created_at)::date, coalesce(risk_level_at_txn,'-'), count(*), sum(amount)
                     from report.transactions where status='SUCCESS' and
                     (date_trunc('month',created_at)::date, coalesce(risk_level_at_txn,'-')) in
                     (select * from unnest(%s::date[], %s::text[])) group by 1,2""", (month_values, risk_values))


def main():
    conn = connect(WAREHOUSE)
    while not state(conn, "snapshot_loaded"):
        print("waiting for the snapshot ...", flush=True)
        time.sleep(3)

    if not state(conn, "report_initialized"):
        initial_load(conn)
    ensure_indexes(conn)
    if not state(conn, "marts_initialized"):
        init_marts(conn)
    conn.commit()
    print("modeler started", flush=True)

    while True:
        try:
            merchants = query(conn, """select event_id, op, old_key, payload, commit_ts from raw.merchants_events
                                       where event_id > %s order by event_id limit 2000""",
                              (watermark(conn, "merchants"),))
            for event in merchants:
                merchant_event(conn, event)
            if merchants:
                set_watermark(conn, "merchants", merchants[-1][0])

            events = query(conn, """select event_id, op, old_key, payload from raw.transactions_events
                                    where event_id > %s order by event_id limit 5000""",
                           (watermark(conn, "transactions"),))
            affected = []
            for event in events:
                affected += transaction_event(conn, event)
            if events:
                set_watermark(conn, "transactions", events[-1][0])
            if affected:
                refresh_marts(conn, affected)

            conn.commit()
            if not merchants and not events:
                time.sleep(1)
        except Exception as exc:
            print("modeler error:", exc, flush=True)
            conn.rollback()
            time.sleep(2)


if __name__ == "__main__":
    main()
