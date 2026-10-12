"""Read changes from Kafka and write them to raw.*_events."""
import json
import time
from datetime import datetime, timezone

import psycopg2.extras
from kafka import KafkaConsumer

from shared_common import WAREHOUSE, connect

TOPICS = {
    "pardakht.core.merchants": "raw.merchants_events",
    "pardakht.core.transactions": "raw.transactions_events",
}
OPS = {"c": "I", "r": "I", "u": "U", "d": "D"}


def pg_type(value):
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "numeric"
    if isinstance(value, (dict, list)):
        return "jsonb"
    return "text"


def make_row(topic, message):
    event = json.loads(message.value.decode())
    op = OPS.get(event["op"])
    if op is None:
        return None
    after = event.get("after") or {}
    before = event.get("before") or {}
    source = event.get("source") or {}
    values = after if op in ("I", "U") else before
    lsn = source.get("lsn")
    if isinstance(lsn, int):
        lsn = "%X/%X" % (lsn >> 32, lsn & 0xFFFFFFFF)
    ts_ms = event.get("ts_ms")
    return {
        "topic": topic,
        "partition": message.partition,
        "offset": message.offset,
        "lsn": lsn,
        "txid": source.get("txId"),
        "op": op,
        "old": before if op in ("U", "D") else {},
        "new": after if op in ("I", "U") else {},
        "types": {key: pg_type(value) for key, value in values.items()},
        "ts_ms": ts_ms,
        "commit_ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else None,
    }


def save(wh, consumer, rows):
    with wh.cursor() as cur:
        for row in rows:
            if row is None:
                continue
            cur.execute(
                f"""insert into {TOPICS[row['topic']]}
                    (kafka_topic, kafka_partition, kafka_offset, lsn, txid, op, old_key, payload, col_types, ts_ms, commit_ts)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict (kafka_topic, kafka_partition, kafka_offset) do nothing""",
                (row["topic"], row["partition"], row["offset"], row["lsn"], row["txid"], row["op"],
                 psycopg2.extras.Json(row["old"]), psycopg2.extras.Json(row["new"]),
                 psycopg2.extras.Json(row["types"]), row["ts_ms"], row["commit_ts"]),
            )
    wh.commit()
    consumer.commit()
    return sum(1 for row in rows if row)


def main():
    wh = connect(WAREHOUSE)
    consumer = KafkaConsumer(
        bootstrap_servers="kafka:9092",
        group_id="raw-loader",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    consumer.subscribe(list(TOPICS))
    print("listening to Kafka ...", flush=True)

    rows, total, last = [], 0, time.time()
    while True:
        for _topic, messages in consumer.poll(timeout_ms=1000, max_records=500).items():
            for message in messages:
                try:
                    rows.append(None if message.value is None else make_row(message.topic, message))
                except Exception as exc:
                    print("bad message:", exc, flush=True)
                    rows.append(None)
        if rows and (len(rows) >= 500 or time.time() - last >= 1):
            total += save(wh, consumer, rows)
            rows, last = [], time.time()
            print(f"stored {total:,} changes", flush=True)


if __name__ == "__main__":
    main()
