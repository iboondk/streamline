#!/usr/bin/env bash
# Creates the Kafka topics used by the pipeline. Run from anywhere; requires the compose stack to be up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

create_topic() {
  local topic="$1"
  local partitions="$2"
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
}

create_topic shop.orders.v1 6
create_topic shop.inventory.v1 6
create_topic shop.orders.dlq 1
create_topic shop.inventory.dlq 1

echo "topics ready"
