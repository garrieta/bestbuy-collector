"""Quick status check of the pipeline's Dropbox app folder.

Lists counts + newest files under each prefix so we can see whether
scheduled runs are landing their artifacts.

Usage:
    python scripts/dropbox_status.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Make bbpipeline importable when running this script from `scripts/`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from bbpipeline.storage import DropboxClient  # noqa: E402


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    client = DropboxClient(
        app_key=os.environ["DROPBOX_APP_KEY"],
        app_secret=os.environ["DROPBOX_APP_SECRET"],
        refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
    )

    sections = [
        ("prices",   "/prices"),
        ("metadata", "/metadata"),
        ("logs",     "/logs"),
    ]

    for name, prefix in sections:
        try:
            files = sorted(client.list_files(prefix, recursive=True))
        except Exception as e:
            if "not_found" in str(e).lower():
                print(f"[{name}] {prefix}: (folder does not exist yet)")
                continue
            print(f"[{name}] error: {e}")
            continue
        print(f"[{name}] {prefix}: {len(files)} files")
        for f in files[-10:]:
            print(f"    {f}")
        print()

    # Peek at the latest run log for status
    try:
        logs = sorted(client.list_files("/logs/runs", recursive=False))
        if logs:
            latest = logs[-1]
            data = client.download_bytes(latest)
            if data:
                payload = json.loads(data)
                print("Latest run log:")
                print(f"  path       : {latest}")
                print(f"  status     : {payload.get('status')}")
                print(f"  started_at : {payload.get('started_at')}")
                print(f"  duration   : "
                      f"{payload.get('finished_at')} "
                      f"(fetched {payload.get('products_fetched'):,} products, "
                      f"{payload.get('api_calls')} API calls)")
                print(f"  changed    : {payload.get('metadata_changed_rows'):,} metadata rows")
                if payload.get("errors"):
                    print(f"  errors     : {payload['errors']}")
    except Exception as e:
        print(f"(couldn't fetch latest run log: {e})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
