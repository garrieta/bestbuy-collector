"""Smoke-test / demo of the bbpipeline.analysis loaders.

Runs every public loader against your local Dropbox sync path and
prints a sample so you can verify everything works.

Usage:
    python scripts/demo_analysis.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `bbpipeline` importable when running from `scripts/`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# UTF-8 console: polars' DataFrame formatting uses box-drawing chars
# that trip up the default Windows cp1252 encoding.
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

import polars as pl  # noqa: E402

from bbpipeline.analysis import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    duckdb_connection,
    load_metadata_at,
    load_metadata_current,
    load_panel,
    load_prices,
    load_prices_latest,
    runs_summary,
)


def banner(title: str) -> None:
    bar = "─" * (len(title) + 4)
    print(f"\n{bar}\n  {title}\n{bar}")


def main() -> int:
    pl.Config.set_tbl_cols(-1)         # show all columns
    pl.Config.set_tbl_rows(5)

    print(f"Reading from: {DEFAULT_DATA_ROOT}")
    if not DEFAULT_DATA_ROOT.exists():
        print("  (folder does not exist — is Dropbox synced?)")
        return 1

    banner("runs_summary()")
    rs = runs_summary()
    print(f"runs: {rs.height}")
    print(rs.select(["started_at", "status", "products_fetched",
                     "metadata_changed_rows", "api_calls"]))

    banner("load_prices_latest()")
    latest = load_prices_latest()
    print(f"shape: {latest.shape}")
    print(latest.head(3).select(
        ["sku", "regularPrice", "salePrice", "onSale",
         "customerReviewCount", "fetched_at"]
    ))

    banner("load_prices(skus=[first 3 SKUs])")
    sample_skus = latest["sku"].head(3).to_list()
    hist = load_prices(skus=sample_skus)
    print(f"shape: {hist.shape}")
    print(hist.select(["sku", "fetched_at", "regularPrice", "salePrice", "onSale"]))

    banner("load_metadata_current()")
    mc = load_metadata_current()
    print(f"shape: {mc.shape}")
    print(mc.head(3).select(["sku", "name", "manufacturer", "department", "class"]))

    banner("load_metadata_at('2026-04-22T00:00:00Z')")
    snap = load_metadata_at("2026-04-22T00:00:00Z")
    print(f"shape: {snap.shape}")
    print(snap.head(3).select(["sku", "name", "department", "observed_at"]))

    banner("load_panel(start='2026-04-22')")
    panel = load_panel(start="2026-04-22")
    print(f"shape: {panel.shape}")
    print(panel.head(5).select(
        ["sku", "fetched_at", "salePrice", "onSale", "name", "department"]
    ))

    banner("SQL via duckdb_connection(): top 10 departments")
    con = duckdb_connection()
    tops = con.execute("""
        SELECT department, COUNT(*) AS n_skus
        FROM metadata_current
        WHERE department IS NOT NULL
        GROUP BY 1 ORDER BY n_skus DESC LIMIT 10
    """).pl()
    print(tops)

    banner("SQL: how many SKUs are currently on sale?")
    on_sale = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE onSale)                    AS on_sale,
            COUNT(*)                                          AS total,
            ROUND(100.0 * COUNT(*) FILTER (WHERE onSale)
                  / COUNT(*), 2)                              AS pct_on_sale
        FROM prices
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM prices)
    """).pl()
    print(on_sale)

    print("\nAll loaders ran. You're good.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
