"""Pipeline entrypoint.

Usage:
    python -m bbpipeline.run [--local] [--max-pages N]

--local      : write to ./.local_dropbox/ instead of real Dropbox. For
               dry-runs and debugging; mirrors the app-folder tree.
--max-pages  : stop after N pages (for testing; 1 page = 100 products).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from . import config, metadata, pricing
from .bbapi import BestBuyAPI
from .logging_utils import RunLog
from .storage import DropboxClient


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_env() -> None:
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)


def _require(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"ERROR: required env var {name!r} not set.", file=sys.stderr)
        sys.exit(2)
    return v


class LocalSink:
    """Stand-in for DropboxClient used with --local. Mirrors the Dropbox
    app-folder layout under ./.local_dropbox/."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _full(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def upload_bytes(self, path: str, data: bytes) -> None:
        p = self._full(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def download_bytes(self, path: str) -> bytes | None:
        p = self._full(path)
        return p.read_bytes() if p.exists() else None

    def exists(self, path: str) -> bool:
        return self._full(path).exists()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true",
                        help="Write to ./.local_dropbox instead of Dropbox")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Stop after N pages (100 products/page)")
    args = parser.parse_args(argv)

    _load_env()
    bb_key = _require("BESTBUY_API_KEY")

    if args.local:
        sink = LocalSink(PROJECT_ROOT / ".local_dropbox")
        print("Sink: LOCAL (./.local_dropbox)")
    else:
        sink = DropboxClient(
            app_key=_require("DROPBOX_APP_KEY"),
            app_secret=_require("DROPBOX_APP_SECRET"),
            refresh_token=_require("DROPBOX_REFRESH_TOKEN"),
        )
        print("Sink: Dropbox app folder")

    log = RunLog()
    fetched_at = log.started_at
    api = BestBuyAPI(bb_key)

    try:
        print(f"Fetching products (max_pages={args.max_pages or 'ALL'})...")
        products = list(api.iter_all_products(max_pages=args.max_pages))
        log.products_fetched = len(products)
        log.api_calls = api.call_count
        print(f"  fetched {len(products):,} products in {api.call_count} API calls")

        # Prices
        prices_df = pricing.build_prices_frame(products, fetched_at)
        log.prices_rows = prices_df.height
        prices_path, prices_bytes = pricing.write_prices(prices_df, sink, fetched_at)
        log.prices_path, log.prices_bytes = prices_path, prices_bytes
        print(f"  prices         : {prices_bytes:>10,} B -> {prices_path}")

        # Metadata diff
        cur_meta = metadata.build_metadata_frame(products, fetched_at)
        hash_index = metadata.load_hash_index(sink)
        changed, new_index = metadata.diff(cur_meta, hash_index)
        log.metadata_changed_rows = changed.height
        print(f"  metadata diff  : {changed.height:,} changed of {cur_meta.height:,} total")

        if changed.height > 0:
            hist_path, hist_bytes = metadata.write_history(changed, sink, fetched_at)
            log.metadata_history_path = hist_path
            log.metadata_history_bytes = hist_bytes
            print(f"  metadata hist  : {hist_bytes:>10,} B -> {hist_path}")

        log.hash_index_bytes = metadata.write_hash_index(new_index, sink)
        print(f"  hash index     : {log.hash_index_bytes:>10,} B -> {config.DROPBOX_HASH_INDEX}")

        log.metadata_current_bytes = metadata.write_current(cur_meta, sink)
        print(f"  metadata curr. : {log.metadata_current_bytes:>10,} B -> {config.DROPBOX_METADATA_CURRENT}")

        log.status = "ok"
    except Exception as e:
        log.status = "error"
        log.errors.append(repr(e))
        raise
    finally:
        log.finished_at = datetime.now(timezone.utc)
        api.close()
        runlog_path = (
            f"{config.DROPBOX_LOGS_RUNS_PREFIX}"
            f"/{log.started_at.strftime('%Y-%m-%dT%H-%M-%SZ')}.json"
        )
        try:
            sink.upload_bytes(runlog_path, log.to_json().encode("utf-8"))
            print(f"  run log        : -> {runlog_path}")
        except Exception as e:
            print(f"  (warning) run log upload failed: {e}", file=sys.stderr)

    print()
    print(f"Status   : {log.status}")
    print(f"Duration : {log.duration_seconds:.1f}s")
    return 0 if log.status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
