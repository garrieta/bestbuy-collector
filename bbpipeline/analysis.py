"""Analysis-ready loaders for the Best Buy panel data.

Reads parquet files from your local Dropbox sync path. Default is
`~/Dropbox/Apps/AIGarpDataPipeline/`. Override by:
  - passing `data_root=...` to any loader, or
  - setting the BB_DATA_ROOT environment variable.

All loaders return polars DataFrames. If you prefer SQL, use
`duckdb_connection()` which returns a DuckDB connection with three
views pre-registered: `prices`, `metadata_current`, `metadata_history`.

Typical usage:
    from bbpipeline.analysis import (
        load_prices_latest, load_prices, load_metadata_current,
        load_metadata_at, load_panel, duckdb_connection,
    )

    # Most recent snapshot — one row per SKU
    latest = load_prices_latest()

    # Price history for specific SKUs
    hist = load_prices(start="2026-04-20", skus=["1234567", "8901234"])

    # Current metadata (wide: name, manufacturer, categoryPath, ...)
    meta = load_metadata_current()

    # Metadata as it stood at a past moment (SCD2 as-of)
    snap = load_metadata_at("2026-04-22T00:00:00Z")

    # Prices time-series joined with current metadata
    panel = load_panel(start="2026-04-21", end="2026-04-22")

    # Or drop into SQL for anything custom:
    con = duckdb_connection()
    con.execute("SELECT department, COUNT(*) FROM metadata_current GROUP BY 1").pl()
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Sequence

import duckdb
import polars as pl

DEFAULT_DATA_ROOT = Path.home() / "Dropbox" / "Apps" / "AIGarpDataPipeline"


def _resolve_data_root(data_root: str | Path | None = None) -> Path:
    if data_root is not None:
        return Path(data_root)
    env = os.environ.get("BB_DATA_ROOT")
    if env:
        return Path(env)
    return DEFAULT_DATA_ROOT


def _posix(path: Path) -> str:
    """DuckDB's glob parser prefers forward slashes, even on Windows."""
    return str(path.resolve()).replace("\\", "/")


def duckdb_connection(data_root: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with views registered over
    the parquet files. Views: `prices`, `metadata_current`, `metadata_history`.
    """
    root = _resolve_data_root(data_root)
    con = duckdb.connect()

    prices_glob = _posix(root / "prices" / "**" / "*.parquet")
    con.execute(f"""
        CREATE OR REPLACE VIEW prices AS
        SELECT * FROM read_parquet('{prices_glob}', hive_partitioning = true)
    """)

    meta_current = root / "metadata" / "current" / "metadata_current.parquet"
    if meta_current.exists():
        con.execute(f"""
            CREATE OR REPLACE VIEW metadata_current AS
            SELECT * FROM read_parquet('{_posix(meta_current)}')
        """)

    hist_glob = _posix(root / "metadata" / "history" / "**" / "*.parquet")
    con.execute(f"""
        CREATE OR REPLACE VIEW metadata_history AS
        SELECT * FROM read_parquet('{hist_glob}', hive_partitioning = true)
    """)

    return con


def load_prices_latest(
    skus: Sequence[str] | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """Latest prices snapshot: one row per SKU at the most recent `fetched_at`."""
    con = duckdb_connection(data_root)
    params: list = []
    where = "WHERE fetched_at = (SELECT MAX(fetched_at) FROM prices)"
    if skus:
        where += " AND sku IN (SELECT UNNEST(?))"
        params.append(list(skus))
    return con.execute(f"SELECT * FROM prices {where}", params).pl()


def load_prices(
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    skus: Sequence[str] | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """All prices rows with `fetched_at` in [start, end], optionally
    restricted to `skus`. Dates may be 'YYYY-MM-DD' strings or datetimes."""
    con = duckdb_connection(data_root)
    filters: list[str] = []
    params: list = []
    if start is not None:
        filters.append("fetched_at >= ?")
        params.append(start)
    if end is not None:
        filters.append("fetched_at <= ?")
        params.append(end)
    if skus:
        filters.append("sku IN (SELECT UNNEST(?))")
        params.append(list(skus))
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    q = f"SELECT * FROM prices {where} ORDER BY sku, fetched_at"
    return con.execute(q, params).pl()


def load_metadata_current(
    skus: Sequence[str] | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """Latest full-state metadata (wide)."""
    con = duckdb_connection(data_root)
    params: list = []
    where = ""
    if skus:
        where = "WHERE sku IN (SELECT UNNEST(?))"
        params.append(list(skus))
    return con.execute(f"SELECT * FROM metadata_current {where}", params).pl()


def load_metadata_at(
    as_of: str | datetime,
    skus: Sequence[str] | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """SCD2 as-of query: for each SKU, the metadata row with the latest
    `observed_at <= as_of`. Returns one row per SKU.

    SKUs that have never been observed on or before `as_of` are omitted."""
    con = duckdb_connection(data_root)
    params: list = [as_of]
    where_sku = ""
    if skus:
        where_sku = " AND sku IN (SELECT UNNEST(?))"
        params.append(list(skus))
    q = f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY sku
                                         ORDER BY observed_at DESC) AS _rn
            FROM metadata_history
            WHERE observed_at <= ? {where_sku}
        )
        SELECT * EXCLUDE (_rn) FROM ranked WHERE _rn = 1
    """
    return con.execute(q, params).pl()


def load_panel(
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    skus: Sequence[str] | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame:
    """Join prices (time-varying) with *current* metadata — one row per
    (sku, fetched_at).

    NOTE: uses current metadata for every row. If you need point-in-time
    metadata per observation (e.g., a SKU's department at the time of
    each price reading), use `load_metadata_at(fetched_at, skus=...)`
    separately and join yourself.
    """
    con = duckdb_connection(data_root)
    filters: list[str] = []
    params: list = []
    if start is not None:
        filters.append("p.fetched_at >= ?")
        params.append(start)
    if end is not None:
        filters.append("p.fetched_at <= ?")
        params.append(end)
    if skus:
        filters.append("p.sku IN (SELECT UNNEST(?))")
        params.append(list(skus))
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    q = f"""
        SELECT p.*, m.name, m.manufacturer, m.categoryPath, m.class,
               m.subclass, m.department, m.type, m.new, m.preowned,
               m.digital, m.releaseDate
        FROM prices p
        LEFT JOIN metadata_current m USING (sku)
        {where}
        ORDER BY p.sku, p.fetched_at
    """
    return con.execute(q, params).pl()


def runs_summary(data_root: str | Path | None = None) -> pl.DataFrame:
    """Tidy table of per-run summaries from `/logs/runs/*.json`.

    Useful for spotting failures, slowdowns, or quota spikes.
    """
    import json

    root = _resolve_data_root(data_root)
    runs_dir = root / "logs" / "runs"
    if not runs_dir.exists():
        return pl.DataFrame()

    rows: list[dict] = []
    for p in sorted(runs_dir.glob("*.json")):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows.append({
            "started_at": d.get("started_at"),
            "finished_at": d.get("finished_at"),
            "status": d.get("status"),
            "api_calls": d.get("api_calls"),
            "products_fetched": d.get("products_fetched"),
            "prices_rows": d.get("prices_rows"),
            "metadata_changed_rows": d.get("metadata_changed_rows"),
            "prices_bytes": d.get("prices_bytes"),
            "metadata_history_bytes": d.get("metadata_history_bytes"),
            "errors": d.get("errors"),
        })
    return pl.DataFrame(rows)
