"""Measure the five cards on Core and on Warehouse."""
import json
import os
import time
from statistics import median

from shared_common import SOURCE, WAREHOUSE, connect

ROUNDS = int(os.getenv("BENCH_ITERATIONS", "3"))

CORE_SQL = [
    "select created_at::date as day, sum(amount), count(*) from core.transactions where status='SUCCESS' and created_at >= now() - interval '90 days' group by 1 order by 1",
    "select created_at::date as day, channel, count(*) filter (where status='SUCCESS'), count(*) from core.transactions where created_at >= now() - interval '30 days' group by 1,2 order by 1,2",
    "select t.merchant_id, m.name, sum(t.amount) from core.transactions t join core.merchants m using (merchant_id) where t.status='SUCCESS' and date_trunc('month',t.created_at)=date_trunc('month',now()) group by 1,2 order by 3 desc limit 10",
    "select m.mcc, m.province, sum(t.fee_amount) from core.transactions t join core.merchants m using (merchant_id) where t.status='SUCCESS' and t.created_at >= now() - interval '90 days' group by 1,2 order by 3 desc",
    "select date_trunc('month',t.created_at)::date, m.risk_level, count(*), sum(t.amount) from core.transactions t join core.merchants m using (merchant_id) where t.status='SUCCESS' group by 1,2 order by 1,2",
]
WAREHOUSE_SQL = [
    "select day, amount, txn_count from report.mart_daily_sales where day >= (now() - interval '90 days')::date order by day",
    "select day, channel, success_count, total_count from report.mart_daily_channel where day >= (now() - interval '30 days')::date order by day, channel",
    "select merchant_id, merchant_name, amount from report.mart_top_merchants where month=date_trunc('month',now())::date order by amount desc limit 10",
    "select mcc, province, sum(fee_amount) from report.mart_fee_mcc_province where day >= (now() - interval '90 days')::date group by 1,2 order by 3 desc",
    "select month, risk_level, txn_count, amount from report.mart_monthly_success_risk order by month, risk_level",
]
NAMES = ["1 - daily sales", "2 - channel success", "3 - top merchants", "4 - fee by mcc/province", "5 - risk"]


def measure(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.fetchall()
        conn.rollback()
        times = []
        for _ in range(ROUNDS):
            start = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            times.append((time.perf_counter() - start) * 1000)
            conn.rollback()
    return median(times)


def main():
    core = connect(SOURCE)
    warehouse = connect(WAREHOUSE)
    results = []
    for name, core_sql, wh_sql in zip(NAMES, CORE_SQL, WAREHOUSE_SQL):
        core_ms = measure(core, core_sql)
        wh_ms = measure(warehouse, wh_sql)
        results.append({"card": name, "core_ms": round(core_ms, 1), "warehouse_ms": round(wh_ms, 1)})
        print(f"{name}: core={core_ms:.0f}ms warehouse={wh_ms:.1f}ms", flush=True)

    rows = ["# Dashboard card runtimes (before / after)", "",
            "| Card | Core (ms) | Warehouse (ms) | Speedup |", "|---|---:|---:|---:|"]
    for item in results:
        rows.append(f"| {item['card']} | {item['core_ms']} | {item['warehouse_ms']} | {item['core_ms'] / item['warehouse_ms']:.0f}x |")
    open("/reports/runtime_report.md", "w", encoding="utf-8").write("\n".join(rows) + "\n")
    json.dump(results, open("/reports/bench.json", "w", encoding="utf-8"), indent=2)
    print("wrote /reports/runtime_report.md", flush=True)


if __name__ == "__main__":
    main()
