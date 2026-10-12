"""One-shot job: register the Debezium connector."""
import time

from shared_common import SOURCE, connect, http

CONNECT = "http://kafka-connect:8083"
NAME = "pardakht-postgres-connector"
CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "app",
    "database.password": "app",
    "database.dbname": "app",
    "topic.prefix": "pardakht",
    "table.include.list": "core.merchants,core.transactions",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication",
    "publication.autocreate.mode": "filtered",
    "snapshot.mode": "never",
    "tombstones.on.delete": "true",
    "heartbeat.interval.ms": "10000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
}


def main():
    while http("GET", CONNECT + "/connectors")[0] != 200:
        print("waiting for Kafka Connect ...", flush=True)
        time.sleep(5)

    src = connect(SOURCE, autocommit=True)
    while True:
        with src.cursor() as cur:
            cur.execute("select 1 from public.pipeline_control where step = 'snapshot_ready'")
            if cur.fetchone():
                break
        print("waiting for the snapshot ...", flush=True)
        time.sleep(3)

    status, _ = http("POST", CONNECT + "/connectors", {"name": NAME, "config": CONFIG})
    if status == 409:
        http("PUT", f"{CONNECT}/connectors/{NAME}/config", CONFIG)
    print("connector registered", flush=True)

    while True:
        _, status = http("GET", f"{CONNECT}/connectors/{NAME}/status")
        if status and status["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in status["tasks"]):
            break
        print("waiting for the connector ...", flush=True)
        time.sleep(3)

    time.sleep(3)
    with src.cursor() as cur:
        cur.execute("insert into public.pipeline_control(step) values ('debezium_ready') on conflict do nothing")
    print("debezium_ready", flush=True)


if __name__ == "__main__":
    main()
