#!/usr/bin/env python3
"""Static checks for the Streamline project. Needs no Docker and no third-party packages.

Run from the project root: python3 scripts/check-static.py
"""
import glob
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
errors = []


def err(msg):
    errors.append(msg)
    print(f"FAIL {msg}")


def path(*parts):
    return os.path.join(ROOT, *parts)


def rel(p):
    return os.path.relpath(p, ROOT)


# Required fields per docs/ARCHITECTURE.md section 3.
REQUIRED_FIELDS = {
    "schemas/order_event.avsc": {
        "event_id", "event_type", "order_id", "customer_id", "shop_id",
        "currency", "total_cents", "line_items", "occurred_at",
    },
    "schemas/inventory_event.avsc": {
        "event_id", "event_type", "sku", "warehouse_id", "delta", "occurred_at",
    },
}

REQUIRED_TABLES = ["orders_raw", "inventory_raw", "orders_per_minute", "inventory_by_sku"]


def check_avsc():
    files = sorted(glob.glob(path("schemas", "*.avsc")))
    if not files:
        err("no schemas/*.avsc files found")
    for f in files:
        name = rel(f)
        try:
            with open(f, encoding="utf-8") as fh:
                doc = json.load(fh)
        except (OSError, ValueError) as exc:
            err(f"{name}: not valid JSON ({exc})")
            continue
        if not isinstance(doc, dict):
            err(f"{name}: top level must be an object")
            continue
        if doc.get("type") != "record":
            err(f"{name}: type must be 'record'")
        for key in ("name", "namespace", "fields"):
            if key not in doc:
                err(f"{name}: missing '{key}'")
        fields = doc.get("fields") or []
        if not isinstance(fields, list):
            err(f"{name}: 'fields' must be a list")
            continue
        names = {fld.get("name") for fld in fields if isinstance(fld, dict)}
        for req in sorted(REQUIRED_FIELDS.get(name, set()) - names):
            err(f"{name}: required field '{req}' is missing")
        for fld in fields:
            if isinstance(fld, dict) and fld.get("name") == "occurred_at":
                t = fld.get("type")
                if not (isinstance(t, dict) and t.get("logicalType") == "timestamp-millis"):
                    err(f"{name}: occurred_at must be long with logicalType timestamp-millis")
        if not errors or all(name not in e for e in errors):
            print(f"ok   {name}")


def check_grafana_json():
    files = sorted(glob.glob(path("grafana", "**", "*.json"), recursive=True))
    if not files:
        print("info no grafana/**/*.json files yet")
    for f in files:
        name = rel(f)
        try:
            with open(f, encoding="utf-8") as fh:
                json.load(fh)
            print(f"ok   {name}")
        except (OSError, ValueError) as exc:
            err(f"{name}: not valid JSON ({exc})")


def check_yaml():
    try:
        import yaml  # noqa: F401
    except ImportError:
        # GitHub Actions sets CI=true. There a missing PyYAML must fail the build, not skip silently.
        if os.environ.get("CI"):
            err("PyYAML is not installed; CI requires it to check YAML (pip install pyyaml)")
            return
        print("SKIP yaml (install pyyaml to check YAML files)")
        return
    import yaml as pyyaml
    files = [path("docker-compose.yml")]
    files += glob.glob(path("grafana", "**", "*.yml"), recursive=True)
    files += glob.glob(path("grafana", "**", "*.yaml"), recursive=True)
    for f in files:
        if not os.path.exists(f):
            continue
        try:
            with open(f, encoding="utf-8") as fh:
                pyyaml.safe_load(fh)
            print(f"ok   {rel(f)}")
        except Exception as exc:  # yaml errors vary by version
            err(f"{rel(f)}: not valid YAML ({exc})")


# Consumer services: compose service name -> (source dir, file suffix, regex for require() calls or None).
CONSUMERS = {
    "consumer-go": ("consumer-go/cmd", ".go", re.compile(r'require\("([A-Z][A-Z0-9_]*)"\)')),
    "consumer-node": ("consumer-node/src", ".js", None),
}
# Env reads per language. Go reads through env("X") or os.Getenv("X") (both end in env("),
# Node through env.X where env = process.env.
ENV_READ_RE = {
    ".go": re.compile(r'env\("([A-Z][A-Z0-9_]*)"'),
    ".js": re.compile(r'\benv\.([A-Z][A-Z0-9_]*)\b'),
}
# Variables that carry credentials. Code that reads them must get them from compose.
CREDENTIAL_RE = re.compile(r"PASSWORD|USER|SECRET|TOKEN")


def compose_env_names(service):
    env = service.get("environment") or {}
    if isinstance(env, list):
        return {item.split("=", 1)[0] for item in env}
    return set(env)


def check_compose_env():
    """Each consumer must set, in compose, every env var its code requires or uses for credentials.

    A clean clone runs compose without .env, so a missing entry here breaks the quick start.
    """
    try:
        import yaml as pyyaml
    except ImportError:
        if os.environ.get("CI"):
            err("PyYAML is not installed; CI requires it to check compose env (pip install pyyaml)")
            return
        print("SKIP compose env (install pyyaml to check consumer env)")
        return
    with open(path("docker-compose.yml"), encoding="utf-8") as fh:
        services = (pyyaml.safe_load(fh) or {}).get("services") or {}
    for name, (src_dir, suffix, require_re) in CONSUMERS.items():
        svc = services.get(name)
        if not isinstance(svc, dict):
            err(f"docker-compose.yml: consumer service '{name}' is missing")
            continue
        files = glob.glob(os.path.join(path(src_dir), "**", "*" + suffix), recursive=True)
        if not files:
            err(f"{src_dir}: no {suffix} files found for '{name}'")
            continue
        required, reads = set(), set()
        for f in files:
            with open(f, encoding="utf-8") as fh:
                text = fh.read()
            if require_re is not None:
                required |= set(require_re.findall(text))
            reads |= set(ENV_READ_RE[suffix].findall(text))
        required |= {v for v in reads if CREDENTIAL_RE.search(v)}
        missing = sorted(required - compose_env_names(svc))
        for var in missing:
            err(f"docker-compose.yml: service '{name}' does not set {var}, which its code requires")
        if not missing:
            print(f"ok   docker-compose.yml: '{name}' sets all {len(required)} required env vars")


def check_sql():
    files = sorted(glob.glob(path("sql", "*.sql")))
    if not files:
        err("no sql/*.sql files found")
        return
    text = ""
    for f in files:
        with open(f, encoding="utf-8") as fh:
            text += fh.read() + "\n"
        print(f"ok   {rel(f)}")
    for table in REQUIRED_TABLES:
        if table not in text:
            err(f"sql/*.sql does not mention table '{table}'")


def check_shell():
    files = sorted(glob.glob(path("scripts", "*.sh")))
    if not files:
        err("no scripts/*.sh files found")
    for f in files:
        proc = subprocess.run(["bash", "-n", f], capture_output=True, text=True)
        if proc.returncode != 0:
            err(f"{rel(f)}: bash -n failed: {proc.stderr.strip()}")
        else:
            print(f"ok   {rel(f)} (bash -n)")


def main():
    check_avsc()
    check_grafana_json()
    check_yaml()
    check_compose_env()
    check_sql()
    check_shell()
    if errors:
        print(f"\n{len(errors)} check(s) failed")
        return 1
    print("\nall static checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
