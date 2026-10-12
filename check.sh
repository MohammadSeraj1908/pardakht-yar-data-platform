#!/usr/bin/env bash
# Simple health check for Linux / WSL.
# Usage: bash check.sh
set -u
pass=0; fail=0
ok(){ echo "  [OK] $1"; pass=$((pass+1)); }
bad(){ echo "  [!!] $1"; fail=$((fail+1)); }
src(){ docker compose exec -T postgres-source psql -U app -d app -tAc "$1" | tr -d '\r'; }
wh(){  docker compose exec -T postgres-warehouse psql -U app -d warehouse -tAc "$1" | tr -d '\r'; }

echo "=== Docker ==="
docker info >/dev/null 2>&1 && ok "Docker engine is available" || bad "Docker engine is not available"

running="$(docker compose ps --services --filter status=running 2>/dev/null)"
for s in postgres-source postgres-warehouse kafka kafka-connect kafka-consumer modeler simulator metabase kafka-ui debezium-ui; do
  echo "$running" | grep -qx "$s" && ok "service $s is running" || bad "service $s is not running"
done
for s in snapshot-loader debezium-init metabase-init; do
  docker compose ps -a --format '{{.Name}}={{.Status}}' 2>/dev/null | grep -q "$s.*Exited (0)" \
    && ok "init job $s finished" || bad "init job $s did not finish"
done

echo "=== Source ==="
m="$(src 'select count(*) from core.merchants')"; t="$(src 'select count(*) from core.transactions')"
[ "$m" = "2000" ] && ok "merchants = 2000" || bad "merchants = $m"
[ "${t:-0}" -ge 5000000 ] && ok "transactions >= 5,000,000 ($t)" || bad "transactions = $t"

echo "=== Warehouse ==="
snap="$(wh 'select count(*) from raw.transactions_snapshot')"; rep="$(wh 'select count(*) from report.transactions')"
[ "$snap" = "5000000" ] && ok "initial snapshot = 5,000,000" || bad "initial snapshot = $snap"
ok "report transactions = $rep"

echo "=== Reconciliation (last 30 days) ==="
core="$(src "select count(*)||'|'||coalesce(sum(amount),0) from core.transactions where status='SUCCESS' and created_at >= current_date-30 and created_at < current_date")"
warehouse="$(wh "select count(*)||'|'||coalesce(sum(amount),0) from report.transactions where status='SUCCESS' and created_at >= current_date-30 and created_at < current_date")"
[ "$core" = "$warehouse" ] && ok "Core and Warehouse are equal" || bad "Core=$core Warehouse=$warehouse"

echo "=== Debezium / Metabase ==="
curl -s --max-time 20 http://localhost:8083/connectors/pardakht-postgres-connector/status | grep -q '"state":"RUNNING"' \
  && ok "Debezium connector is RUNNING" || bad "Debezium connector is not RUNNING"
[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://localhost:3000/api/health)" = "200" ] \
  && ok "Metabase is healthy" || bad "Metabase is not healthy"

echo
echo "Result: $pass OK / $fail FAIL"
[ "$fail" = "0" ] && echo "Everything is healthy." || echo "Fix the red items."
