#!/usr/bin/env bash
# Registers the Avro value schemas and sets BACKWARD compatibility per subject (TopicNameStrategy).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
: "${SCHEMA_REGISTRY_URL:=http://localhost:8081}"

register_schema() {
  local topic="$1"
  local file="$2"
  local subject="${topic}-value"
  local body

  # Wrap the .avsc text as a JSON string under "schema", as the registry API requires.
  body="$(python3 -c 'import json, sys; print(json.dumps({"schema": open(sys.argv[1], encoding="utf-8").read()}))' "$file")"

  curl -fsS -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$body" \
    "$SCHEMA_REGISTRY_URL/subjects/$subject/versions"
  echo

  curl -fsS -X PUT \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility":"BACKWARD"}' \
    "$SCHEMA_REGISTRY_URL/config/$subject"
  echo
}

register_schema shop.orders.v1 "$ROOT/schemas/order_event.avsc"
register_schema shop.inventory.v1 "$ROOT/schemas/inventory_event.avsc"

echo "schemas registered"
