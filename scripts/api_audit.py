"""Best Buy API audit — Phase 2 reconnaissance.

Runs ~15 probes against the Products API to:
  1. measure total catalog size under different `active=` filters
  2. map the full 2026 field schema (show=all)
  3. verify pagination (page 1 vs page 2 disjoint, totalPages)
  4. test the itemUpdateDate filter (for incremental metadata diffs)
  5. time a handful of paged requests and extrapolate full-sweep wall time

Writes findings to `audit_report.json` and prints a human summary.
Uses stdlib only.

Usage:
    python scripts/api_audit.py
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE = "https://api.bestbuy.com/v1"

# Pacing: Best Buy's stated per-second cap is 5 req/s, but in practice we
# observed 403s at ~2-3 req/s from a single client. Keep a conservative
# floor of 1.2s between requests; the audit is small so this is painless.
MIN_INTERVAL_SECONDS = 1.2
_last_fetch_at = 0.0


def pace() -> None:
    global _last_fetch_at
    now = time.monotonic()
    wait = MIN_INTERVAL_SECONDS - (now - _last_fetch_at)
    if wait > 0:
        time.sleep(wait)
    _last_fetch_at = time.monotonic()


def load_env() -> None:
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def fetch(path: str, params: dict[str, Any], key: str) -> dict:
    pace()
    params = {**params, "apiKey": key, "format": "json"}
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def safe_fetch(path: str, params: dict[str, Any], key: str,
               retries: int = 2) -> tuple[dict | None, str | None]:
    last_err: str | None = None
    for attempt in range(retries + 1):
        try:
            return fetch(path, params, key), None
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8")[:200]
            except Exception:
                pass
            last_err = f"HTTP {e.code}: {body}"
            # 403 here is rate-limiting, not auth; back off and retry.
            if e.code == 403 and attempt < retries:
                time.sleep(3.0 * (attempt + 1))
                continue
            return None, last_err
        except urllib.error.URLError as e:
            last_err = f"network: {e}"
            if attempt < retries:
                time.sleep(2.0)
                continue
            return None, last_err
    return None, last_err


def main() -> int:
    load_env()
    key = os.environ.get("BESTBUY_API_KEY")
    if not key:
        print("ERROR: BESTBUY_API_KEY not set.", file=sys.stderr)
        return 1

    report: dict[str, Any] = {"ran_at": datetime.now(timezone.utc).isoformat()}

    # -------- Probe 1: catalog size under different filters --------
    print("Probe 1: catalog size under different filters")
    counts: dict[str, Any] = {}
    for label, path in [
        ("no filter", "/products"),
        ("(active=true)", "/products(active=true)"),
        ("(active=false)", "/products(active=false)"),
    ]:
        r, err = safe_fetch(path, {"pageSize": 1, "show": "sku"}, key)
        if r is not None:
            print(f"  {label:20s} -> total = {r.get('total'):,}")
            counts[label] = r.get("total")
        else:
            print(f"  {label:20s} -> {err}")
            counts[label] = err
    report["catalog_counts"] = counts

    # -------- Probe 2: full schema via show=all --------
    print()
    print("Probe 2: full field schema (show=all, 3 products)")
    r, err = safe_fetch("/products", {"pageSize": 3, "show": "all"}, key)
    if r is not None:
        products = r.get("products", [])
        all_fields: set[str] = set()
        sample: list[dict] = []
        for p in products:
            all_fields.update(p.keys())
            sample.append({"sku": p.get("sku"), "name": (p.get("name") or "")[:60],
                           "field_count": len(p)})
        print(f"  unique fields across 3 sample products: {len(all_fields)}")
        print(f"  first 25 fields: {sorted(all_fields)[:25]}")
        report["schema_fields"] = sorted(all_fields)
        report["schema_sample"] = sample
    else:
        print(f"  failed: {err}")

    # -------- Probe 3: pagination --------
    print()
    print("Probe 3: pagination (pageSize=100, page=1 vs page=2)")
    r1, err1 = safe_fetch("/products", {"pageSize": 100, "page": 1, "show": "sku"}, key)
    r2, err2 = safe_fetch("/products", {"pageSize": 100, "page": 2, "show": "sku"}, key)
    if r1 and r2:
        s1 = {p["sku"] for p in r1.get("products", [])}
        s2 = {p["sku"] for p in r2.get("products", [])}
        print(f"  page 1: {len(s1)} SKUs, page 2: {len(s2)} SKUs, overlap: {len(s1 & s2)}")
        print(f"  totalPages reported: {r1.get('totalPages')}")
        report["pagination"] = {
            "page1_size": len(s1), "page2_size": len(s2),
            "overlap": len(s1 & s2), "totalPages": r1.get("totalPages"),
            "total": r1.get("total"),
        }
    else:
        print(f"  failed: {err1 or err2}")

    # -------- Probe 4: itemUpdateDate filter --------
    print()
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    print(f"Probe 4: itemUpdateDate filter (since {since})")
    r, err = safe_fetch(f"/products(itemUpdateDate>={since})",
                        {"pageSize": 1, "show": "sku,itemUpdateDate"}, key)
    if r is not None:
        print(f"  products updated in last 7 days: {r.get('total'):,}")
        report["itemUpdateDate_filter"] = {"since": since, "total": r.get("total")}
    else:
        print(f"  failed: {err}")
        report["itemUpdateDate_filter"] = err

    # -------- Probe 5: latency --------
    print()
    print("Probe 5: latency (5x pageSize=100 requests)")
    latencies: list[float] = []
    for i in range(5):
        t0 = time.monotonic()
        _, err = safe_fetch("/products",
                            {"pageSize": 100, "page": i + 1,
                             "show": "sku,name,regularPrice,salePrice,onSale"}, key)
        if err:
            print(f"  request {i+1}: {err}")
            continue
        latencies.append(time.monotonic() - t0)
    if latencies:
        med = statistics.median(latencies)
        total = report.get("pagination", {}).get("total", 56347)
        pages = (total + 99) // 100
        # Serial, ignoring rate limit, how long would a sweep take?
        serial_wall = pages * med
        # At 5 req/s, how long?
        rl_wall = max(serial_wall, pages / 5)
        print(f"  latency: min={min(latencies):.2f}s med={med:.2f}s max={max(latencies):.2f}s")
        print(f"  full sweep ({pages:,} pages): ~{serial_wall:.0f}s serial, "
              f"~{rl_wall:.0f}s at 5 req/s cap")
        report["latency_seconds"] = {
            "samples": latencies, "min": min(latencies),
            "median": med, "max": max(latencies),
            "full_sweep_serial_estimate_s": serial_wall,
            "full_sweep_rate_limited_estimate_s": rl_wall,
        }

    # -------- Write report --------
    report_path = PROJECT_ROOT / "audit_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print()
    print(f"Report written to: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
