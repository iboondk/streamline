#!/usr/bin/env python3
"""Registers the Avro value schemas with Schema Registry and sets BACKWARD compatibility.

Standard library only, so it runs unchanged in the python:3.12-alpine image used by the
init-schemas compose service. Safe to run repeatedly: re-posting an identical schema returns
the existing id, and setting the same compatibility again is a no-op.

Usage: python3 scripts/register_schemas.py [schemas_dir]
Environment: SCHEMA_REGISTRY_URL (default http://localhost:8081)
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

# (topic, schema file name). Subject follows TopicNameStrategy: <topic>-value.
SCHEMAS = [
    ("shop.orders.v1", "order_event.avsc"),
    ("shop.inventory.v1", "inventory_event.avsc"),
]


def request(method, path, payload, retries=30):
    """Sends one request, retrying while the registry is still starting up."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        REGISTRY + path, data=data, method=method, headers={"Content-Type": CONTENT_TYPE}
    )
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # 4xx/5xx from the registry is a real error, not a startup race.
            raise SystemExit(f"{method} {path} failed: HTTP {exc.code} {exc.read().decode('utf-8', 'replace')}")
        except OSError as exc:
            if attempt == retries:
                raise SystemExit(f"{method} {path} failed after {retries} attempts: {exc}")
            time.sleep(2)


def main():
    schemas_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schemas"
    )
    for topic, filename in SCHEMAS:
        subject = f"{topic}-value"
        with open(os.path.join(schemas_dir, filename), encoding="utf-8") as fh:
            schema_text = fh.read()
        result = request("POST", f"/subjects/{subject}/versions", {"schema": schema_text})
        print(f"registered {subject} id={result.get('id')}")
        result = request("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        print(f"compatibility {subject} = {result.get('compatibility')}")
    print("schemas registered")


if __name__ == "__main__":
    main()
