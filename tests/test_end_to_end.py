"""End-to-end tests for the data platform (run while the stack is up)."""
import hashlib
import os
import time

import psycopg2
import pytest

SOURCE_DSN = os.getenv("SOURCE_DSN", "postgresql://app:app@localhost:5432/app")
WAREHOUSE_DSN = os.getenv("WAREHOUSE_DSN", "postgresql://app:app@localhost:5433/warehouse")
PII_SALT = os.getenv("PII_SALT", "pardakht-yar-demo-salt")

TEST_MERCHANT = 9_900_000_001
TEST_TXN_1 = 8_800_000_001
TEST_TXN_2 = 8_800_000_002


def connect(dsn):
    conn = psycopg2.connect(dsn, connect_timeout=5)
    conn.autocommit = True
    return conn


@pytest.fixture(scope="session")
def core():
    conn = connect(SOURCE_DSN)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def warehouse():
    conn = connect(WAREHOUSE_DSN)
    yield conn
    conn.close()


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def wait_for(check, description, timeout=150):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = check()
        if last:
            return last
        time.sleep(2)
    pytest.fail(f"timed out waiting for {description}; last={last}")


# ---------------------------------------------------------------- correctness
def test_keys_are_unique(warehouse):
    assert query(warehouse, """
        SELECT count(*) FROM (
            SELECT merchant_id FROM report.merchants GROUP BY 1 HAVING count(*) > 1
        ) d""")[0][0] == 0
    assert query(warehouse, """
        SELECT count(*) FROM (
            SELECT transaction_id FROM report.transactions GROUP BY 1 HAVING count(*) > 1
        ) d""")[0][0] == 0


def test_allowed_domains(warehouse):
    assert query(warehouse, """
        SELECT count(*) FROM report.transactions
        WHERE status NOT IN ('PENDING','SUCCESS','FAILED','REVERSED')
           OR channel NOT IN ('POS','IPG','QR')""")[0][0] == 0
    assert query(warehouse, """
        SELECT count(*) FROM report.merchants
        WHERE risk_level NOT IN ('LOW','MEDIUM','HIGH')""")[0][0] == 0


def test_pii_is_hashed(core, warehouse):
    columns = {r[0] for r in query(warehouse, """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'report' AND table_name = 'merchants'""")}
    assert "owner_national_id" not in columns
    assert "owner_mobile" not in columns
    assert {"owner_national_id_hash", "owner_mobile_hash"} <= columns

    row = query(core, """
        SELECT owner_national_id, owner_mobile FROM core.merchants
        ORDER BY merchant_id LIMIT 1""")[0]
    expected_nid = hashlib.sha256((PII_SALT + row[0]).encode()).hexdigest()
    expected_mobile = hashlib.sha256((PII_SALT + row[1]).encode()).hexdigest()
    hashes = query(warehouse, """
        SELECT owner_national_id_hash, owner_mobile_hash FROM report.merchants
        ORDER BY merchant_id LIMIT 1""")[0]
    assert hashes == (expected_nid, expected_mobile)


# ---------------------------------------------------------------- reconciliation
def test_reconciliation_with_core(core, warehouse):
    sql = """
        SELECT count(*), coalesce(sum(amount), 0)
        FROM {table}
        WHERE status = 'SUCCESS'
          AND created_at >= current_date - 30 AND created_at < current_date
    """
    def current():
        a = query(core, sql.format(table="core.transactions"))[0]
        b = query(warehouse, sql.format(table="report.transactions"))[0]
        return a if a == b else None
    wait_for(current, "core/report reconciliation", timeout=180)


# ---------------------------------------------------------------- live CDC flow
def test_cdc_insert_update_delete(core, warehouse):
    with core.cursor() as cur:
        cur.execute("DELETE FROM core.transactions WHERE transaction_id IN (%s, %s)", (TEST_TXN_1, TEST_TXN_2))
        cur.execute("""
            INSERT INTO core.merchants
                (merchant_id, name, mcc, province, risk_level,
                 owner_national_id, owner_mobile, updated_at)
            VALUES (%s, 'تست CDC', '5411', 'تهران', 'LOW', '0012345678', '09120000000', now())
            ON CONFLICT (merchant_id) DO UPDATE
                SET risk_level = 'LOW', province = 'تهران', updated_at = now()""", (TEST_MERCHANT,))
        cur.execute("""
            INSERT INTO core.transactions
                (transaction_id, merchant_id, channel, card_bin, amount, fee_amount,
                 status, created_at, updated_at)
            VALUES (%s, %s, 'POS', '123456', 123456, 123, 'SUCCESS', now(), now())""",
            (TEST_TXN_1, TEST_MERCHANT))

    # insert must arrive with the risk at transaction time
    def first_txn():
        rows = query(warehouse, """
            SELECT risk_level_at_txn, province_at_txn FROM report.transactions
            WHERE transaction_id = %s""", (TEST_TXN_1,))
        return rows[0] if rows else None
    assert wait_for(first_txn, "inserted transaction in report") == ("LOW", "تهران")

    # change risk and check that a *new* transaction keeps the new value
    with core.cursor() as cur:
        cur.execute("UPDATE core.merchants SET risk_level = 'HIGH', updated_at = now() WHERE merchant_id = %s",
                    (TEST_MERCHANT,))
    wait_for(lambda: query(warehouse, "SELECT risk_level FROM report.merchants WHERE merchant_id = %s",
                           (TEST_MERCHANT,)) == [("HIGH",)], "merchant update in report")
    time.sleep(2)
    with core.cursor() as cur:
        cur.execute("""
            INSERT INTO core.transactions
                (transaction_id, merchant_id, channel, card_bin, amount, fee_amount,
                 status, created_at, updated_at)
            VALUES (%s, %s, 'QR', '123456', 5000, 50, 'SUCCESS', now(), now())""",
            (TEST_TXN_2, TEST_MERCHANT))
    assert wait_for(lambda: query(warehouse, """
            SELECT risk_level_at_txn FROM report.transactions WHERE transaction_id = %s""",
            (TEST_TXN_2,)) == [("HIGH",)], "risk at transaction time for the new transaction")

    # hard delete must propagate
    with core.cursor() as cur:
        cur.execute("DELETE FROM core.transactions WHERE transaction_id = %s", (TEST_TXN_1,))
    wait_for(lambda: query(warehouse, "SELECT 1 FROM report.transactions WHERE transaction_id = %s",
                           (TEST_TXN_1,)) == [], "hard delete in report")

    with core.cursor() as cur:
        cur.execute("DELETE FROM core.transactions WHERE transaction_id IN (%s, %s)", (TEST_TXN_1, TEST_TXN_2))
        cur.execute("DELETE FROM core.merchants WHERE merchant_id = %s", (TEST_MERCHANT,))


# ---------------------------------------------------------------- schema evolution
def test_schema_evolution_installments(core, warehouse):
    source_has_column = query(core, """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'transactions'
          AND column_name = 'installments'""")
    if not source_has_column:
        pytest.skip("simulator has not added installments yet")

    wait_for(lambda: query(warehouse, """
        SELECT count(*) FROM raw.transactions_events
        WHERE payload ? 'installments'""")[0][0] > 0, "installments in raw events")

    wait_for(lambda: query(warehouse, """
        SELECT count(*) FROM report.transactions
        WHERE extra ? 'installments'""")[0][0] > 0, "installments in report extra")
