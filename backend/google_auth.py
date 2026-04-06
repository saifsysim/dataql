"""
Google OAuth2 authentication for Gmail and Google Sheets.

Usage:
  1. Place your OAuth2 credentials.json in this directory
  2. Run: python google_auth.py
  3. A browser window opens → sign in and consent
  4. token.json is saved for reuse

The token auto-refreshes. If it expires beyond repair, delete token.json and re-run.
"""

import os
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Scopes — read-only access to Gmail and Sheets
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

BASE_DIR = Path(__file__).parent
DEFAULT_CREDENTIALS = BASE_DIR / "credentials.json"
DEFAULT_TOKEN = BASE_DIR / "token.json"


def get_google_credentials(
    credentials_file: str = None,
    token_file: str = None,
    scopes: list[str] = None,
) -> Credentials:
    """
    Get valid Google OAuth2 credentials.

    - If token.json exists and is valid → use it
    - If token is expired but refreshable → refresh it
    - Otherwise → run interactive OAuth flow (opens browser)
    """
    creds_path = Path(credentials_file) if credentials_file else DEFAULT_CREDENTIALS
    tok_path = Path(token_file) if token_file else DEFAULT_TOKEN
    scopes = scopes or SCOPES

    creds = None

    # Try loading existing token
    if tok_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(tok_path), scopes)
        except Exception:
            creds = None

    # Refresh or re-authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            if not creds_path.exists():
                raise FileNotFoundError(
                    f"Google OAuth credentials not found at {creds_path}.\n"
                    "Download from Google Cloud Console → APIs & Services → Credentials → "
                    "OAuth 2.0 Client IDs → Download JSON → save as credentials.json"
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), scopes)
            creds = flow.run_local_server(port=8090, open_browser=True)

        # Save token for next time
        with open(tok_path, "w") as f:
            f.write(creds.to_json())
        print(f"✅ Token saved to {tok_path}")

    return creds


def check_google_auth(credentials_file: str = None, token_file: str = None) -> tuple[bool, str]:
    """Check if we have valid Google credentials without triggering interactive flow."""
    tok_path = Path(token_file) if token_file else DEFAULT_TOKEN

    if not tok_path.exists():
        return False, "No token.json found. Run: python google_auth.py"

    try:
        creds = Credentials.from_authorized_user_file(str(tok_path), SCOPES)
        if creds.valid:
            return True, "Authenticated"
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # Save refreshed token
            with open(tok_path, "w") as f:
                f.write(creds.to_json())
            return True, "Token refreshed"
        return False, "Token expired. Re-run: python google_auth.py"
    except Exception as e:
        return False, f"Auth error: {e}"


if __name__ == "__main__":
    print("🔐 Google OAuth2 Setup for DataQL")
    print("=" * 40)
    print(f"Credentials: {DEFAULT_CREDENTIALS}")
    print(f"Token: {DEFAULT_TOKEN}")
    print(f"Scopes: {', '.join(SCOPES)}")
    print()

    try:
        creds = get_google_credentials()
        print(f"✅ Authenticated successfully!")
        print(f"   Token valid: {creds.valid}")
        print(f"   Scopes: {creds.scopes}")
    except FileNotFoundError as e:
        print(f"❌ {e}")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
