#!/usr/bin/env bash
# =============================================================================
#  full_check.sh - complete health report for the Pardakht Yar data platform
#  Usage: bash full_check.sh
# =============================================================================
set -u
PASS=0; FAIL=0; ISSUES=""
ok(){   echo "   [OK]  $1"; PASS=$((PASS+1)); }
bad(){  echo "   [!!]  $1"; FAIL=$((FAIL+1)); ISSUES="${ISSUES}\n   - $1"; }
check(){ if [ "${2:-0}" = "1" ]; then ok "$1"; else bad "$1"; fi; }
section(){ echo; echo "############################################################################"; echo "#  $1"; echo "############################################################################"; }
line(){ echo "   $1"; }

src(){ docker compose exec -T postgres-source    psql -U app -d app       -tAc "$1" 2>/dev/null | tr -d '\r'; }
wh(){  docker compose exec -T postgres-warehouse psql -U app -d warehouse -tAc "$1" 2>/dev/null | tr -d '\r'; }
kt(){  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh         "$@" 2>/dev/null | tr -d '\r'; }
kcg(){ docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh "$@" 2>/dev/null | tr -d '\r'; }
koff(){ docker compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell "$@" 2>/dev/null | tr -d '\r'; }
topic_count(){ koff --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
port_open(){ (echo > "/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1 && echo 1 || echo 0; }

echo "======================================================================"
echo " Pardakht Yar - FULL CHECK"
echo " time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================================"

# -----------------------------------------------------------------------------
section "1) DOCKER CONTAINERS"
docker compose ps -a --format '   {{.Name}}  |  {{.Status}}' 2>/dev/null | sort
running="$(docker compose ps --services --filter status=running 2>/dev/null)"
for s in postgres-source postgres-warehouse kafka kafka-connect kafka-consumer modeler simulator metabase kafka-ui debezium-ui; do
  if echo "$running" | grep -qx "$s"; then ok "service $s is running"; else bad "service $s is NOT running"; fi
done
for s in snapshot-loader debezium-init metabase-init; do
  if docker compose ps -a --format '{{.Name}}={{.Status}}' 2>/dev/null | grep -q "$s.*Exited (0)"; then
    ok "init job $s finished (Exited 0)"
  else
    bad "init job $s did not finish yet"
  fi
done

# -----------------------------------------------------------------------------
section "2) PORTS"
for p in 3000 5432 5433 8080 8081 8083 9092; do
  check "port $p is open" "$(port_open $p)"
done

# -----------------------------------------------------------------------------
section "3) SOURCE DATABASE (core)"
line "version       : $(src 'show server_version')"
line "database size : $(src "select pg_size_pretty(pg_database_size(current_database()))")"
echo
line "table row counts:"
for t in core.merchants core.transactions public.pipeline_control; do
  line "   $t = $(src "select count(*) from $t")"
done
echo
line "core.merchants by risk_level:"
src "select risk_level || ' = ' || count(*) from core.merchants group by 1 order by 1" | sed 's/^/   /'
echo
line "core.transactions by status:"
src "select status || ' = ' || count(*) from core.transactions group by 1 order by 2 desc" | sed 's/^/   /'
echo
line "data in the last hour  : $(src "select count(*) from core.transactions where created_at > now() - interval '1 hour'") transactions"
line "data in the last 24h   : $(src "select count(*) from core.transactions where created_at > now() - interval '24 hours'") transactions"
line "newest transaction     : id=$(src 'select max(transaction_id) from core.transactions')  at $(src 'select max(created_at) from core.transactions')"
line "table size transactions: $(src "select pg_size_pretty(pg_total_relation_size('core.transactions'))")"
line "installments column    : $(src "select count(*) from information_schema.columns where table_schema='core' and table_name='transactions' and column_name='installments'")"
echo
line "pipeline handshake steps:"
src "select step || ' @ ' || done_at from public.pipeline_control order by done_at" | sed 's/^/   /'
echo
line "replication slot:"
src "select slot_name || ' | active=' || active || ' | plugin=' || plugin || ' | confirmed=' || confirmed_flush_lsn || ' | retained=' || pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) || ' | bytes_behind=' || pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint from pg_replication_slots" | sed 's/^/   /'
line "publication     : $(src "select pubname from pg_publication") ($(src "select string_agg(schemaname||'.'||tablename, ', ') from pg_publication_tables") )"

# -----------------------------------------------------------------------------
section "4) DEBEZIUM / KAFKA CONNECT"
conn_json="$(curl -s --max-time 10 http://localhost:8083/connectors/pardakht-postgres-connector/status)"
conn_state="$(echo "$conn_json" | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4)"
task_states="$(echo "$conn_json" | grep -o '"state":"[A-Z]*"' | tail -n +2 | cut -d'"' -f4 | paste -sd, -)"
line "connector name : pardakht-postgres-connector"
line "connector state: ${conn_state:-unknown}"
line "task states    : ${task_states:-unknown}"
check "Debezium connector is RUNNING" "$([ "$conn_state" = "RUNNING" ] && echo 1 || echo 0)"
if echo "$task_states" | grep -qv 'RUNNING'; then bad "one or more connector tasks are not RUNNING"; else ok "all connector tasks are RUNNING"; fi
echo
line "connector config (main keys):"
curl -s --max-time 10 http://localhost:8083/connectors/pardakht-postgres-connector/config | tr ',' '\n' | grep -E 'topic.prefix|slot.name|publication.name|snapshot.mode|table.include|plugin.name' | sed 's/^/   /'

# -----------------------------------------------------------------------------
section "5) KAFKA - TOPICS AND MESSAGES"
line "topics:"
kt --bootstrap-server localhost:9092 --list 2>/dev/null | sed 's/^/   /'
echo
line "message count per topic:"
for t in pardakht.core.merchants pardakht.core.transactions connect_configs connect_offsets connect_statuses; do
  line "   $t = $(topic_count $t)"
done
echo
line "main topics detail:"
kt --bootstrap-server localhost:9092 --describe --topic pardakht.core.transactions 2>/dev/null | sed 's/^/   /'
kt --bootstrap-server localhost:9092 --describe --topic pardakht.core.merchants 2>/dev/null | sed 's/^/   /'

# -----------------------------------------------------------------------------
section "6) KAFKA - CONSUMER GROUP raw-loader (lag)"
kcg --bootstrap-server localhost:9092 --describe --group raw-loader 2>/dev/null | sed 's/^/   /'
total_lag="$(kcg --bootstrap-server localhost:9092 --describe --group raw-loader 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9-]+$/ {s+=$6} END{print s+0}')"
line "total lag = ${total_lag:-0}"
check "consumer lag is small (< 5000)" "$([ "${total_lag:-0}" -lt 5000 ] && echo 1 || echo 0)"

# -----------------------------------------------------------------------------
section "7) WAREHOUSE - RAW LAYER"
line "database size : $(wh "select pg_size_pretty(pg_database_size(current_database()))")"
line "raw.merchants_snapshot     = $(wh 'select count(*) from raw.merchants_snapshot')"
line "raw.transactions_snapshot  = $(wh 'select count(*) from raw.transactions_snapshot')"
line "raw.merchants_events       = $(wh 'select count(*) from raw.merchants_events')"
line "raw.transactions_events    = $(wh 'select count(*) from raw.transactions_events')"
echo
line "transaction events by op:"
wh "select op || ' = ' || count(*) from raw.transactions_events group by 1 order by 1" | sed 's/^/   /'
echo
line "events in the last hour    : $(wh "select count(*) from raw.transactions_events where received_at > now() - interval '1 hour'")"
line "events in the last 24h     : $(wh "select count(*) from raw.transactions_events where received_at > now() - interval '24 hours'")"
line "last event received        : $(wh 'select max(received_at) from raw.transactions_events')"
line "last source commit (ts_ms) : $(wh 'select max(commit_ts) from raw.transactions_events')"
line "lag since last event (sec) : $(wh "select coalesce(extract(epoch from now() - max(received_at))::int, -1) from raw.transactions_events")"
line "events with installments   : $(wh "select count(*) from raw.transactions_events where payload ? 'installments'")"
echo
line "watermarks:"
wh "select name || ' = ' || last_event_id from ops.watermarks order by name" | sed 's/^/   /'
line "pipeline state:"
wh "select name || ' = ' || value from ops.state order by name" | sed 's/^/   /'

# -----------------------------------------------------------------------------
section "8) WAREHOUSE - REPORT LAYER"
line "report.merchants        = $(wh 'select count(*) from report.merchants')"
line "report.merchant_history = $(wh 'select count(*) from report.merchant_history')"
line "report.transactions     = $(wh 'select count(*) from report.transactions')"
echo
line "report transactions by status:"
wh "select status || ' = ' || count(*) from report.transactions group by 1 order by 2 desc" | sed 's/^/   /'
echo
line "report transactions last hour: $(wh "select count(*) from report.transactions where created_at > now() - interval '1 hour'")"
line "merchants with risk history   : $(wh 'select count(*) from (select merchant_id from report.merchant_history group by 1 having count(*) > 1) x')"
line "duplicate transactions        : $(wh 'select count(*) from (select transaction_id from report.transactions group by 1 having count(*) > 1) x')"
line "installments in extra         : $(wh "select count(*) from report.transactions where extra ? 'installments'")"
line "PII columns hashed (64 chars) : $(wh "select count(*) from report.merchants where length(owner_national_id_hash) = 64 and length(owner_mobile_hash) = 64") / $(wh 'select count(*) from report.merchants')"
line "raw PII columns present       : $(wh "select count(*) from information_schema.columns where table_schema='report' and table_name='merchants' and column_name in ('owner_national_id','owner_mobile')")  (should be 0)"

# -----------------------------------------------------------------------------
section "9) WAREHOUSE - MARTS"
for m in mart_daily_sales mart_daily_channel mart_top_merchants mart_fee_mcc_province mart_monthly_success_risk; do
  line "report.$m = $(wh "select count(*) from report.$m")"
done
echo
line "latest daily sales:"
wh "select day || ' | amount=' || amount || ' | count=' || txn_count from report.mart_daily_sales order by day desc limit 3" | sed 's/^/   /'
line "monthly risk (latest):"
wh "select month || ' | risk=' || risk_level || ' | count=' || txn_count || ' | amount=' || amount from report.mart_monthly_success_risk order by month desc, risk_level limit 3" | sed 's/^/   /'

# -----------------------------------------------------------------------------
section "10) RECONCILIATION - CORE vs WAREHOUSE (last 30 days)"
core_rec="$(src "select count(*)||'|'||coalesce(sum(amount),0) from core.transactions where status='SUCCESS' and created_at >= current_date-30 and created_at < current_date")"
wh_rec="$(wh "select count(*)||'|'||coalesce(sum(amount),0) from report.transactions where status='SUCCESS' and created_at >= current_date-30 and created_at < current_date")"
line "Core      : $core_rec"
line "Warehouse : $wh_rec"
check "Core and Warehouse are exactly equal" "$([ "$core_rec" = "$wh_rec" ] && echo 1 || echo 0)"

source_count="${source_count:-0}"; report_count="${report_count:-0}"
source_count="$(src 'select count(*) from core.transactions')"; source_count="${source_count:-0}"
report_count="$(wh 'select count(*) from report.transactions')"; report_count="${report_count:-0}"
difference=$(( source_count - report_count )); [ "$difference" -lt 0 ] && difference=$(( -difference ))
line "source transactions = $source_count"
line "report transactions = $report_count"
line "difference          = $difference  (small differences are normal pipeline lag)"
check "report is close to source (< 5000 difference)" "$([ "$difference" -lt 5000 ] && echo 1 || echo 0)"

# -----------------------------------------------------------------------------
section "11) METABASE AND UIs"
check "Metabase is healthy (port 3000)" "$([ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://localhost:3000/api/health)" = "200" ] && echo 1 || echo 0)"
check "Kafka UI is up (port 8080)" "$(port_open 8080)"
check "Debezium UI is up (port 8081)" "$(port_open 8081)"
check "Kafka Connect REST is up (port 8083)" "$(port_open 8083)"

# -----------------------------------------------------------------------------
section "SUMMARY"
echo "   checks passed : $PASS"
echo "   checks failed : $FAIL"
if [ "$FAIL" = "0" ]; then
  echo "   Everything is healthy."
else
  echo "   Issues:"; echo -e "$ISSUES"
  echo "   Tip: wait a few minutes while the snapshot/connector/modeler finish, then run again."
fi
echo