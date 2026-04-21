"""End-to-end Dropbox sanity check.

Uses the four Dropbox env vars to:
  1. authenticate via refresh-token flow
  2. report the connected account
  3. upload a tiny file to /connection_test.txt
  4. download and verify contents
  5. delete the test file

Usage:
    pip install -r requirements.txt
    python scripts/test_dropbox_connection.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    import dropbox
    from dropbox.exceptions import ApiError, AuthError
except ImportError:
    print("ERROR: `dropbox` package not installed.", file=sys.stderr)
    print("Run: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)


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

    app_key = os.environ.get("DROPBOX_APP_KEY")
    app_secret = os.environ.get("DROPBOX_APP_SECRET")
    refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")

    missing = [
        name for name, val in (
            ("DROPBOX_APP_KEY", app_key),
            ("DROPBOX_APP_SECRET", app_secret),
            ("DROPBOX_REFRESH_TOKEN", refresh_token),
        ) if not val
    ]
    if missing:
        print(f"ERROR: missing env vars: {missing}", file=sys.stderr)
        return 1

    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
        account = dbx.users_get_current_account()
    except AuthError as e:
        print(f"ERROR: Dropbox auth failed: {e}", file=sys.stderr)
        return 1

    print(f"Authenticated as: {account.name.display_name} <{account.email}>")

    test_path = "/connection_test.txt"
    test_content = b"AI GARP pipeline connection test - safe to delete."

    try:
        dbx.files_upload(test_content, test_path, mode=dropbox.files.WriteMode.overwrite)
        print(f"  uploaded   : {test_path} ({len(test_content)} bytes)")

        _, resp = dbx.files_download(test_path)
        retrieved = resp.content
        if retrieved != test_content:
            print("ERROR: downloaded content did not match uploaded content.", file=sys.stderr)
            return 1
        print(f"  downloaded : {len(retrieved)} bytes, content matches")

        dbx.files_delete_v2(test_path)
        print(f"  deleted    : {test_path}")
    except ApiError as e:
        print(f"ERROR: Dropbox API error: {e}", file=sys.stderr)
        return 1

    print()
    print("Dropbox connection OK. App-folder roundtrip successful.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
