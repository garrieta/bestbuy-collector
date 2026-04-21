"""Build and write the per-run prices parquet file."""

from __future__ import annotations

import io
from datetime import datetime
from typing import Iterable, Protocol

import polars as pl

from . import config


class _Sink(Protocol):
    def upload_bytes(self, path: str, data: bytes) -> None: ...


def _normalize(v):
    if isinstance(v, (list, dict)):
        import json
        return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return v


def build_prices_frame(products: Iterable[dict], fetched_at: datetime) -> pl.DataFrame:
    rows = []
    for p in products:
        row = {f: _normalize(p.get(f)) for f in config.PRICE_FIELDS}
        row["fetched_at"] = fetched_at
        rows.append(row)
    df = pl.DataFrame(rows)
    if "sku" in df.columns:
        df = df.with_columns(pl.col("sku").cast(pl.Utf8))
    return df


def dropbox_path(fetched_at: datetime) -> str:
    stamp = fetched_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    return (
        f"{config.DROPBOX_PRICES_PREFIX}"
        f"/year={fetched_at.year:04d}"
        f"/month={fetched_at.month:02d}"
        f"/day={fetched_at.day:02d}"
        f"/{stamp}.parquet"
    )


def write_prices(
    df: pl.DataFrame, sink: _Sink, fetched_at: datetime
) -> tuple[str, int]:
    path = dropbox_path(fetched_at)
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    sink.upload_bytes(path, data)
    return path, len(data)
