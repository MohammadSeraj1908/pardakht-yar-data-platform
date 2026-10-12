"""Create the Metabase datasources, cards and dashboards."""
import time

from shared_common import http

MB = "http://metabase:3000"
EMAIL = "admin@pardakhtyar.local"
PASSWORD = "PardakhtYar123!"

CORE_DB = {"engine": "postgres", "name": "Core (source)",
           "details": {"host": "postgres-source", "port": 5432, "dbname": "app",
                       "user": "app", "password": "app", "ssl": False}}
WAREHOUSE_DB = {"engine": "postgres", "name": "Warehouse (report)",
                "details": {"host": "postgres-warehouse", "port": 5432, "dbname": "warehouse",
                            "user": "app", "password": "app", "ssl": False}}

CORE_CARDS = [
    ("Baseline 1 - daily sales (Core)", "line",
     "select created_at::date as day, sum(amount) as amount, count(*) as txn_count from core.transactions where status='SUCCESS' and created_at >= now() - interval '90 days' group by 1 order by 1"),
    ("Baseline 2 - channel success (Core)", "line",
     "select created_at::date as day, channel, count(*) filter (where status='SUCCESS') as success_count, count(*) as total_count from core.transactions where created_at >= now() - interval '30 days' group by 1,2 order by 1,2"),
    ("Baseline 3 - top merchants (Core)", "table",
     "select t.merchant_id, m.name as merchant_name, sum(t.amount) as amount from core.transactions t join core.merchants m using (merchant_id) where t.status='SUCCESS' and date_trunc('month',t.created_at)=date_trunc('month',now()) group by 1,2 order by 3 desc limit 10"),
    ("Baseline 4 - fee by mcc/province (Core)", "bar",
     "select m.mcc, m.province, sum(t.fee_amount) as fee_amount from core.transactions t join core.merchants m using (merchant_id) where t.status='SUCCESS' and t.created_at >= now() - interval '90 days' group by 1,2 order by 3 desc"),
    ("Baseline 5 - current risk (Core)", "bar",
     "select date_trunc('month',t.created_at)::date as month, m.risk_level, count(*) as txn_count, sum(t.amount) as amount from core.transactions t join core.merchants m using (merchant_id) where t.status='SUCCESS' group by 1,2 order by 1,2"),
]
REPORT_CARDS = [
    ("1 - daily sales (Warehouse)", "line",
     "select day, amount, txn_count from report.mart_daily_sales where day >= (now() - interval '90 days')::date order by day"),
    ("2 - channel success (Warehouse)", "line",
     "select day, channel, success_count, total_count from report.mart_daily_channel where day >= (now() - interval '30 days')::date order by day, channel"),
    ("3 - top merchants (Warehouse)", "table",
     "select merchant_id, merchant_name, amount from report.mart_top_merchants where month=date_trunc('month',now())::date order by amount desc limit 10"),
    ("4 - fee by mcc/province (Warehouse)", "bar",
     "select mcc, province, sum(fee_amount) as fee_amount from report.mart_fee_mcc_province where day >= (now() - interval '90 days')::date group by 1,2 order by 3 desc"),
    ("5 - risk at transaction time (Warehouse)", "bar",
     "select month, risk_level, txn_count, amount from report.mart_monthly_success_risk order by month, risk_level"),
]


def call(method, path, data=None, headers=None):
    return http(method, MB + path, data, headers)


def wait_for_metabase():
    while call("GET", "/api/health")[0] != 200:
        print("waiting for Metabase ...", flush=True)
        time.sleep(5)


def login():
    _, props = call("GET", "/api/session/properties")
    token = (props or {}).get("setup-token")
    if token:
        call("POST", "/api/setup", {
            "token": token,
            "user": {"first_name": "Data", "last_name": "Platform", "email": EMAIL, "password": PASSWORD},
            "prefs": {"site_name": "Pardakht Yar", "allow_tracking": False},
        })
    _, session = call("POST", "/api/session", {"username": EMAIL, "password": PASSWORD})
    return {"X-Metabase-Session": session["id"]}


def ensure_database(headers, spec):
    _, body = call("GET", "/api/database", headers=headers)
    databases = body.get("data", body) if isinstance(body, dict) else body
    for database in databases or []:
        if database["name"] == spec["name"]:
            return database["id"]
    _, created = call("POST", "/api/database", {**spec, "is_full_sync": True}, headers)
    return created["id"]


def cards_by_name(headers):
    _, body = call("GET", "/api/card?f=all", headers=headers)
    cards = body.get("data", body) if isinstance(body, dict) else body
    return {card["name"]: card["id"] for card in cards or []}


def ensure_card(headers, name, display, sql, database_id, cards):
    data = {"name": name, "display": display, "visualization_settings": {},
            "dataset_query": {"database": database_id, "type": "native",
                              "native": {"query": sql, "template-tags": {}}}}
    if name in cards:
        call("PUT", f"/api/card/{cards[name]}", data, headers)
        return cards[name]
    _, created = call("POST", "/api/card", data, headers)
    return created["id"]


def ensure_dashboard(headers, name):
    _, body = call("GET", "/api/dashboard", headers=headers)
    dashboards = body.get("data", body) if isinstance(body, dict) else body
    for dashboard in dashboards or []:
        if dashboard["name"] == name:
            return dashboard["id"]
    _, created = call("POST", "/api/dashboard", {"name": name}, headers)
    return created["id"]


def add_cards(headers, dashboard_id, card_ids):
    _, dashboard = call("GET", f"/api/dashboard/{dashboard_id}", headers=headers)
    cards = [{"id": c["id"], "card_id": c.get("card_id"), "row": c.get("row", 0),
              "col": c.get("col", 0), "size_x": 12, "size_y": 4} for c in dashboard.get("dashcards", [])]
    present = {c["card_id"] for c in cards}
    row = max((c["row"] + c["size_y"] for c in cards), default=0)
    for card_id in card_ids:
        if card_id not in present:
            cards.append({"id": -(len(cards) + 1), "card_id": card_id, "row": row, "col": 0, "size_x": 12, "size_y": 4})
            row += 4
    call("PUT", f"/api/dashboard/{dashboard_id}/cards", {"cards": cards}, headers)


def main():
    wait_for_metabase()
    headers = login()
    core_id = ensure_database(headers, CORE_DB)
    warehouse_id = ensure_database(headers, WAREHOUSE_DB)

    cards = cards_by_name(headers)
    core_ids = [ensure_card(headers, n, d, q, core_id, cards) for n, d, q in CORE_CARDS]
    report_ids = [ensure_card(headers, n, d, q, warehouse_id, cards) for n, d, q in REPORT_CARDS]

    add_cards(headers, ensure_dashboard(headers, "Daily Sales - Current (Core)"), core_ids)
    add_cards(headers, ensure_dashboard(headers, "Daily Sales - Optimized (Warehouse)"), report_ids)
    print("Metabase ready: http://localhost:3000", flush=True)


if __name__ == "__main__":
    main()
