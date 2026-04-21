"""SCD2-style metadata writes via a content-hash index.

Each run:
  1. Build `current` metadata frame from fetched products.
  2. Load `hash_index` parquet from storage (sku -> content_hash).
  3. diff(current, hash_index) returns rows whose hash changed (or new).
  4. Write changed rows to the day's `/metadata/history/.../day=DD.parquet`
     (appending to the day file if it already exists).
  5. Overwrite `hash_index.parquet` with the updated (sku, hash, seen) table.
  6. Overwrite `/metadata/current/metadata_current.parquet` with the new full state.
"""

from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime
from typing import Iterable, Protocol

import polars as pl

from . import config


class _Sink(Protocol):
    def upload_bytes(self, path: str, data: bytes) -> None: ...
    def download_bytes(self, path: str) -> bytes | None: ...


_HASH_INDEX_SCHEMA = {
    "sku": pl.Utf8,
    "content_hash": pl.Utf8,
    "first_seen": pl.Datetime(time_unit="us", time_zone="UTC"),
    "last_seen": pl.Datetime(time_unit="us", time_zone="UTC"),
}


def canonical_json(obj) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"),
        default=str, ensure_ascii=False,
    )


def compute_row_hash(row: dict) -> str:
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()


def _normalize(v):
    if isinstance(v, (list, dict)):
        return canonical_json(v)
    return v


def build_metadata_frame(
    products: Iterable[dict], fetched_at: datetime
) -> pl.DataFrame:
    rows: list[dict] = []
    for p in products:
        row = {f: _normalize(p.get(f)) for f in config.METADATA_FIELDS}
        content_hash = compute_row_hash(row)
        rows.append({**row, "content_hash": content_hash, "observed_at": fetched_at})
    df = pl.DataFrame(rows)
    if "sku" in df.columns:
        df = df.with_columns(pl.col("sku").cast(pl.Utf8))
    return df


def load_hash_index(sink: _Sink) -> pl.DataFrame:
    data = sink.download_bytes(config.DROPBOX_HASH_INDEX)
    if data is None:
        return pl.DataFrame(schema=_HASH_INDEX_SCHEMA)
    return pl.read_parquet(io.BytesIO(data))


def diff(
    current: pl.DataFrame, index: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (changed_rows, updated_index).

    changed_rows: full metadata for SKUs that are new or whose
      content_hash differs from the stored index.
    updated_index: index reflecting the new canonical state. SKUs
      present in `index` but absent from `current` are carried forward
      (observed earlier but not in this run's active catalog).
    """
    # Find changed SKUs by joining hashes.
    cur_hashes = current.select(["sku", "content_hash"])
    idx_hashes = index.select(["sku", pl.col("content_hash").alias("old_hash")])
    merged = cur_hashes.join(idx_hashes, on="sku", how="left")
    changed_mask = (
        merged["old_hash"].is_null() | (merged["content_hash"] != merged["old_hash"])
    )
    changed_skus = merged.filter(changed_mask)["sku"]
    changed_rows = current.filter(pl.col("sku").is_in(changed_skus))

    # Build the updated index.
    cur_skus = set(current["sku"].to_list())

    if index.height > 0:
        carry_forward = index.filter(~pl.col("sku").is_in(list(cur_skus)))
        existing_first_seen = index.select(["sku", "first_seen"])
    else:
        carry_forward = pl.DataFrame(schema=_HASH_INDEX_SCHEMA)
        existing_first_seen = pl.DataFrame(schema={
            "sku": pl.Utf8,
            "first_seen": _HASH_INDEX_SCHEMA["first_seen"],
        })

    in_current = (
        current
        .select(["sku", "content_hash", pl.col("observed_at").alias("last_seen")])
        .join(existing_first_seen, on="sku", how="left")
        .with_columns(
            pl.coalesce([pl.col("first_seen"), pl.col("last_seen")]).alias("first_seen")
        )
        .select(["sku", "content_hash", "first_seen", "last_seen"])
    )

    if carry_forward.height > 0:
        updated_index = pl.concat([carry_forward, in_current], how="vertical_relaxed")
    else:
        updated_index = in_current

    return changed_rows, updated_index


def history_path(fetched_at: datetime) -> str:
    return (
        f"{config.DROPBOX_METADATA_HISTORY_PREFIX}"
        f"/year={fetched_at.year:04d}"
        f"/month={fetched_at.month:02d}"
        f"/day={fetched_at.day:02d}.parquet"
    )


def write_history(
    changed: pl.DataFrame, sink: _Sink, fetched_at: datetime
) -> tuple[str, int]:
    """Append changed rows to the day's history parquet.

    If the file already exists, we vstack (same SKU may change multiple
    times in a day; we want every change preserved).
    """
    path = history_path(fetched_at)
    existing = sink.download_bytes(path)
    if existing is not None:
        old = pl.read_parquet(io.BytesIO(existing))
        combined = pl.concat([old, changed], how="vertical_relaxed")
    else:
        combined = changed
    buf = io.BytesIO()
    combined.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    sink.upload_bytes(path, data)
    return path, len(data)


def write_hash_index(index: pl.DataFrame, sink: _Sink) -> int:
    buf = io.BytesIO()
    index.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    sink.upload_bytes(config.DROPBOX_HASH_INDEX, data)
    return len(data)


def write_current(current: pl.DataFrame, sink: _Sink) -> int:
    buf = io.BytesIO()
    current.write_parquet(buf, compression="zstd")
    data = buf.getvalue()
    sink.upload_bytes(config.DROPBOX_METADATA_CURRENT, data)
    return len(data)
