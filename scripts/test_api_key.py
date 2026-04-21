"""Quick sanity check that BESTBUY_API_KEY works.

Usage:
    python scripts/test_api_key.py

Loads the key from `.env` (or the environment) and hits the Products API
with pageSize=1 to confirm auth + reachability. Prints the total catalog
count and a sample product. Uses stdlib only — no pip installs required.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def load_env() -> None:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def main() -> int:
    load_env()
    key = os.environ.get("BESTBUY_API_KEY")
    if not key:
        print("ERROR: BESTBUY_API_KEY not set (check .env or environment).", file=sys.stderr)
        return 1

    params = {
        "apiKey": key,
        "format": "json",
        "pageSize": 1,
        "show": "sku,name,regularPrice,salePrice,onSale",
    }
    url = f"https://api.bestbuy.com/v1/products?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"ERROR: API returned HTTP {e.code}", file=sys.stderr)
        try:
            print(e.read().decode("utf-8"), file=sys.stderr)
        except Exception:
            pass
        return 1
    except urllib.error.URLError as e:
        print(f"ERROR: Network problem: {e}", file=sys.stderr)
        return 1

    total = data.get("total", 0)
    products = data.get("products", [])
    print(f"Catalog reports {total:,} products reachable with this query.")
    if products:
        p = products[0]
        print("Sample product:")
        print(f"  sku          : {p.get('sku')}")
        print(f"  name         : {p.get('name')}")
        print(f"  regularPrice : {p.get('regularPrice')}")
        print(f"  salePrice    : {p.get('salePrice')}")
        print(f"  onSale       : {p.get('onSale')}")
    print()
    print("Best Buy API key works.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
