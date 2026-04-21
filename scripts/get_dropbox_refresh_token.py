"""
One-shot helper to generate a Dropbox refresh token for the
AIGarpDataPipeline app (App-folder scope).

Run this ONCE locally. The refresh token it prints goes into
GitHub Secrets as DROPBOX_REFRESH_TOKEN.

Usage:
    python scripts/get_dropbox_refresh_token.py

Requirements: Python 3.7+, stdlib only. No pip installs needed.

Notes:
- Uses the out-of-band OAuth2 flow (no redirect URI required).
- Requests token_access_type=offline so Dropbox returns a refresh token
  (the short-lived access token is ignored; the pipeline will mint new
  ones from the refresh token on every run).
- The refresh token does not expire unless explicitly revoked.
"""

import base64
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from getpass import getpass

AUTHORIZE_URL = "https://www.dropbox.com/oauth2/authorize"
TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"


def main() -> int:
    print("Dropbox refresh-token helper (AIGarpDataPipeline)")
    print("-" * 52)
    print("Find your App key and App secret at:")
    print("  https://www.dropbox.com/developers/apps")
    print("(Settings tab of your AIGarpDataPipeline app)")
    print()

    app_key = input("App key: ").strip()
    app_secret = getpass("App secret (hidden while typing): ").strip()

    if not app_key or not app_secret:
        print("ERROR: App key and secret are both required.", file=sys.stderr)
        return 1

    params = {
        "client_id": app_key,
        "response_type": "code",
        "token_access_type": "offline",
    }
    auth_url = f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

    print()
    print("Opening the authorization URL in your browser.")
    print("If it doesn't open, paste this URL manually:")
    print()
    print(auth_url)
    print()
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    print("On the Dropbox page: click 'Continue' / 'Allow'.")
    print("Dropbox will then display an authorization code.")
    print()
    code = input("Paste the code here: ").strip()
    if not code:
        print("ERROR: No code provided.", file=sys.stderr)
        return 1

    auth_header = base64.b64encode(f"{app_key}:{app_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "code": code,
        "grant_type": "authorization_code",
    }).encode()

    req = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"ERROR: Token exchange failed (HTTP {e.code})", file=sys.stderr)
        try:
            print(e.read().decode(), file=sys.stderr)
        except Exception:
            pass
        print()
        print("Common causes:", file=sys.stderr)
        print("  - Code was already used (they're one-shot; re-run to get a new one)", file=sys.stderr)
        print("  - App key or secret typo", file=sys.stderr)
        print("  - Code was truncated when pasting", file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print(f"ERROR: Network problem: {e}", file=sys.stderr)
        return 1

    refresh_token = payload.get("refresh_token")
    scope = payload.get("scope", "")

    if not refresh_token:
        print("ERROR: Response did not include a refresh token.", file=sys.stderr)
        print(f"Full response: {payload}", file=sys.stderr)
        print()
        print("Most likely: token_access_type=offline was not honored.", file=sys.stderr)
        return 1

    expected_scopes = {
        "files.content.read",
        "files.content.write",
        "files.metadata.read",
        "files.metadata.write",
    }
    granted = set(scope.split())
    missing = expected_scopes - granted

    print()
    print("=" * 64)
    print("SUCCESS")
    print("=" * 64)
    print()
    print("Refresh token (add to GitHub Secrets as DROPBOX_REFRESH_TOKEN):")
    print()
    print(f"    {refresh_token}")
    print()
    if missing:
        print("WARNING: the token is missing expected scopes:")
        for s in sorted(missing):
            print(f"  - {s}")
        print("Check the Permissions tab in the Dropbox app settings, click")
        print("Submit, then re-run this script to get a fresh token.")
    else:
        print("All four required scopes are present.")
    print()
    print("Never commit this token. It belongs only in GitHub Secrets and,")
    print("optionally, a gitignored local .env for development.")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
