"""Export pipeline data to CSV for non-Python consumers.

Usage:
    python scripts/export_csv.py metadata-current
    python scripts/export_csv.py prices --start 2026-04-21 --end 2026-04-22
    python scripts/export_csv.py panel  --start 2026-04-21
    python scripts/export_csv.py runs-summary

By default CSVs land in `./csv/` inside the project folder. Pass
`--output <dir>` to change the destination (e.g. a shared drive).

All timestamps in the CSVs are in UTC ISO 8601.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make bbpipeline importable when running from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl  # noqa: E402

from bbpipeline.analysis import (  # noqa: E402
    load_metadata_current,
    load_panel,
    load_prices,
    runs_summary,
)


def _flatten_nested(df: pl.DataFrame) -> pl.DataFrame:
    """Serialize List/Struct columns to JSON strings so CSV writers accept them."""
    updates = []
    for name, dtype in df.schema.items():
        type_str = str(dtype)
        if type_str.startswith(("List", "Struct", "Array")):
            updates.append(
                pl.col(name).map_elements(
                    lambda v: json.dumps(v, default=str) if v is not None else None,
                    return_dtype=pl.Utf8,
                ).alias(name)
            )
    return df.with_columns(updates) if updates else df


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "kind",
        choices=["prices", "metadata-current", "panel", "runs-summary"],
        help="What to export.",
    )
    parser.add_argument("--start", default=None, help="ISO date, e.g. 2026-04-21")
    parser.add_argument("--end", default=None, help="ISO date, e.g. 2026-04-22")
    parser.add_argument("--skus", default=None,
                        help="Comma-separated list of SKUs to restrict to.")
    parser.add_argument("--output", default="./csv", help="Output directory.")
    parser.add_argument("--data-root", default=None,
                        help="Override local parquet root. Default = Dropbox sync.")
    args = parser.parse_args()

    skus = [s.strip() for s in args.skus.split(",")] if args.skus else None

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    def _range_suffix() -> str:
        lo = args.start or "all"
        hi = args.end or "all"
        return f"{lo}_to_{hi}"

    if args.kind == "metadata-current":
        df = load_metadata_current(skus=skus, data_root=args.data_root)
        target = out_dir / "metadata_current.csv"
    elif args.kind == "prices":
        df = load_prices(start=args.start, end=args.end, skus=skus,
                         data_root=args.data_root)
        target = out_dir / f"prices_{_range_suffix()}.csv"
    elif args.kind == "panel":
        df = load_panel(start=args.start, end=args.end, skus=skus,
                        data_root=args.data_root)
        target = out_dir / f"panel_{_range_suffix()}.csv"
    elif args.kind == "runs-summary":
        df = runs_summary(data_root=args.data_root)
        target = out_dir / "runs_summary.csv"
    else:
        print(f"Unknown kind: {args.kind}", file=sys.stderr)
        return 2

    df = _flatten_nested(df)
    print(f"Writing {df.height:,} rows x {df.width} cols to {target}")
    df.write_csv(target)
    print(f"Done. {target.stat().st_size:,} bytes on disk.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
