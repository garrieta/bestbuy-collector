"""Peek at the parquet files produced by a --local run.

Usage:
    python scripts/inspect_parquet.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parent.parent / ".local_dropbox"


def peek(label: str, path: Path) -> None:
    print(f"--- {label} ({path.relative_to(ROOT)}) ---")
    if not path.exists():
        print("  (missing)")
        return
    df = pl.read_parquet(path)
    print(f"  rows: {df.height}, cols: {df.width}")
    print(f"  schema: {df.schema}")
    if df.height > 0:
        print("  sample row (first):")
        for k, v in df.row(0, named=True).items():
            val = str(v)
            if len(val) > 80:
                val = val[:77] + "..."
            print(f"    {k:25s} = {val}")
    print()


def main() -> int:
    if not ROOT.exists():
        print(f"No {ROOT} found — run `python -m bbpipeline.run --local ...` first.")
        return 1

    prices = sorted(ROOT.glob("prices/**/*.parquet"))
    if prices:
        peek("latest prices", prices[-1])

    peek("metadata_current", ROOT / "metadata/current/metadata_current.parquet")
    peek("hash_index", ROOT / "metadata/index/hash_index.parquet")

    hist = sorted(ROOT.glob("metadata/history/**/*.parquet"))
    if hist:
        peek("metadata history (latest day)", hist[-1])

    return 0


if __name__ == "__main__":
    sys.exit(main())
