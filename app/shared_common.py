"""Shared helpers. Kept small on purpose."""
import json
import os
import time
import urllib.request

import psycopg2

SOURCE = os.getenv("SOURCE_DSN", "postgresql://app:app@localhost:5432/app")
WAREHOUSE = os.getenv("WAREHOUSE_DSN", "postgresql://app:app@localhost:5433/warehouse")
SALT = os.getenv("PII_SALT", "pardakht-yar-demo-salt")


def connect(dsn, autocommit=False):
    """Connect and wait until the database is ready."""
    for _ in range(60):
        try:
            conn = psycopg2.connect(dsn, connect_timeout=5)
            conn.autocommit = autocommit
            return conn
        except psycopg2.OperationalError as exc:
            print("waiting for database:", exc, flush=True)
            time.sleep(2)
    raise SystemExit("database is not reachable")


def http(method, url, data=None, headers=None):
    """Tiny JSON HTTP call."""
    request = urllib.request.Request(
        url,
        data=json.dumps(data).encode() if data is not None else None,
        method=method,
        headers={**(headers or {}), "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            return response.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as exc:
        return exc.code, None
