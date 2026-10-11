#!/usr/bin/env bash
# Replay proof for the orders pipeline.
#
# Rewinds the streamline-orders consumer group to the earliest offset, lets
# consumer-go reprocess the whole topic, then checks that the deduplicated
# counts in ClickHouse did not change. The reconciliation query at the end
# compares the sum-based MV revenue with orders_raw FINAL revenue. That
# difference is printed and does not fail the run (see docs/FAILURE_HANDLING.md).
#
# Needs the compose stack running with the topics and schemas bootstrapped and
# consumer-go up. Needs a live stack, so it is not run by `make test`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
# Defaults match .env.example, so a clean clone without .env works with the compose defaults.
: "${CLICKHOUSE_USER:=streamline}"
: "${CLICKHOUSE_PASSWORD:=change-me-local-only}"
: "${CLICKHOUSE_DB:=streamline}"
: "${ORDERS_TOPIC:=shop.orders.v1}"

GROUP="streamline-orders"
TOPIC="$ORDERS_TOPIC"
TABLE="streamline.orders_raw"
LAG_TIMEOUT_SEC=120
INACTIVE_TIMEOUT_SEC=60

ch() {
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" "$@"
}

kafka_groups() {
  docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 "$@"
}

# Prints "count<TAB>uniqExact" for orders_raw.
query_basic() {
  ch --query "SELECT count(), uniqExact(event_id) FROM ${TABLE} FORMAT TSV"
}

# Prints the FINAL row count for orders_raw.
query_final() {
  ch --query "SELECT count() FROM ${TABLE} FINAL FORMAT TSV"
}

# Prints the summed lag over the topic's partitions, or NA when no partition is reported yet.
lag_total() {
  kafka_groups --describe --group "$GROUP" 2>/dev/null | awk -v t="$TOPIC" '
    $2 == t && $6 ~ /^[0-9]+$/ { sum += $6; rows++ }
    END { if (rows == 0) print "NA"; else print sum }'
}

# Waits until the group has no active members. kafka-consumer-groups refuses
# --reset-offsets while members are still attached.
wait_group_inactive() {
  local deadline state out
  deadline=$(( $(date +%s) + INACTIVE_TIMEOUT_SEC ))
  while (( $(date +%s) < deadline )); do
    # A group that never committed offsets is reported as "does not exist".
    # That is inactive too, and --reset-offsets accepts it.
    out="$(kafka_groups --describe --group "$GROUP" --state 2>&1 || true)"
    if grep -q 'does not exist' <<<"$out"; then
      return 0
    fi
    state="$(grep -oE 'Empty|Dead|Stable|PreparingRebalance|CompletingRebalance' <<<"$out" \
      | head -n 1 || true)"
    if [[ "$state" == "Empty" || "$state" == "Dead" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "FAIL: consumer group ${GROUP} still active after ${INACTIVE_TIMEOUT_SEC}s (state: ${state:-unknown})" >&2
  exit 1
}

# Waits until the summed lag on the topic is 0, or fails after LAG_TIMEOUT_SEC.
wait_lag_zero() {
  local start_ts lag
  start_ts=$(date +%s)
  while true; do
    lag="$(lag_total || true)"
    if [[ "$lag" == "0" ]]; then
      echo "lag reached 0"
      return 0
    fi
    if (( $(date +%s) - start_ts >= LAG_TIMEOUT_SEC )); then
      echo "FAIL: lag did not reach 0 within ${LAG_TIMEOUT_SEC}s (last lag: ${lag:-unknown})" >&2
      exit 1
    fi
    sleep 2
  done
}

echo "== (0) wait for ${GROUP} to catch up before the baseline (timeout ${LAG_TIMEOUT_SEC}s)"
echo "   A baseline taken while consumer-go is still behind would make the replay look wrong."
wait_lag_zero

echo "== (a) counts before replay"
BEFORE="$(query_basic)"
UNIQ_BEFORE="$(printf '%s\n' "$BEFORE" | cut -f2)"
echo "SELECT count(), uniqExact(event_id) FROM ${TABLE}"
echo "  count=$(printf '%s\n' "$BEFORE" | cut -f1)  uniqExact(event_id)=${UNIQ_BEFORE}"
echo "SELECT count() FROM ${TABLE} FINAL"
echo "  count=$(query_final)"

echo "== (b) stop consumer-go"
docker compose stop consumer-go

echo "== (c) reset ${GROUP} on ${TOPIC} to earliest"
wait_group_inactive
kafka_groups --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-earliest --execute

echo "== (d) start consumer-go and wait for lag 0 (timeout ${LAG_TIMEOUT_SEC}s)"
docker compose up -d consumer-go
wait_lag_zero

echo "== (e) counts after replay, then OPTIMIZE and FINAL count"
AFTER="$(query_basic)"
UNIQ_AFTER="$(printf '%s\n' "$AFTER" | cut -f2)"
echo "SELECT count(), uniqExact(event_id) FROM ${TABLE}"
echo "  count=$(printf '%s\n' "$AFTER" | cut -f1)  uniqExact(event_id)=${UNIQ_AFTER}"
echo "OPTIMIZE TABLE streamline.orders_raw FINAL"
ch --query "OPTIMIZE TABLE ${TABLE} FINAL"
FINAL_AFTER="$(query_final)"
echo "SELECT count() FROM ${TABLE} FINAL"
echo "  count=${FINAL_AFTER}"

echo "== reconciliation: MV revenue vs orders_raw FINAL revenue (informational, not a failure)"
ch --query "SELECT mv, raw, mv - raw AS difference_cents FROM (
  SELECT
    (SELECT sum(revenue_cents) FROM streamline.orders_per_minute) AS mv,
    (SELECT sumIf(total_cents, event_type = 'ORDER_PAID') FROM streamline.orders_raw FINAL) AS raw
) FORMAT TSVWithNames"

echo "== verdict"
echo "uniqExact(event_id): before=${UNIQ_BEFORE} after=${UNIQ_AFTER}"
echo "FINAL count after OPTIMIZE: ${FINAL_AFTER}"
if [[ "$UNIQ_BEFORE" == "$UNIQ_AFTER" && "$FINAL_AFTER" == "$UNIQ_AFTER" ]]; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi
