"""
Gmail Connector for DataQL.

Fetches emails from the user's Gmail via the Gmail API and loads them into
an in-memory SQLite database for SQL querying.

Tables created:
  - gmail_messages: id, thread_id, from_email, from_name, to_email, subject,
                    snippet, date, labels, is_unread, has_attachments
  - gmail_labels: id, name, type, messages_total, messages_unread
"""

from __future__ import annotations
import re
import sqlite3
import email.utils
from datetime import datetime
from typing import Any, Optional

from data_connectors import APIConnector


class GmailConnector(APIConnector):
    """Gmail API connector — fetches messages into queryable SQL tables."""

    connector_type = "api"
    connector_name = "Gmail"
    connector_icon = "📧"
    source_id = "gmail"
    _cache_ttl_seconds = 120  # 2 minute cache

    def __init__(self, credentials_file: str = None, token_file: str = None, max_messages: int = 200):
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.max_messages = max_messages

    def _get_service(self):
        from googleapiclient.discovery import build
        from google_auth import get_google_credentials
        creds = get_google_credentials(self.credentials_file, self.token_file)
        return build("gmail", "v1", credentials=creds)

    def test_connection(self) -> tuple[bool, str]:
        try:
            from google_auth import check_google_auth
            ok, msg = check_google_auth(self.credentials_file, self.token_file)
            if not ok:
                return False, msg
            service = self._get_service()
            profile = service.users().getProfile(userId="me").execute()
            email_addr = profile.get("emailAddress", "unknown")
            total = profile.get("messagesTotal", 0)
            return True, f"Connected to {email_addr} ({total:,} messages)"
        except ImportError:
            return False, "Install: pip install google-api-python-client google-auth-oauthlib"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        """Fetch Gmail messages and labels into SQLite."""
        try:
            service = self._get_service()
            self._sync_labels(service, db)
            self._sync_messages(service, db)
            db.commit()
        except Exception as e:
            print(f"[Gmail] Sync error: {e}")
            self._create_empty_tables(db)

    def _sync_labels(self, service, db: sqlite3.Connection):
        """Fetch all Gmail labels."""
        db.execute("""
            CREATE TABLE IF NOT EXISTS gmail_labels (
                id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                messages_total INTEGER DEFAULT 0,
                messages_unread INTEGER DEFAULT 0
            )
        """)

        results = service.users().labels().list(userId="me").execute()
        labels = results.get("labels", [])

        for label in labels:
            # Get detailed label info for counts
            try:
                detail = service.users().labels().get(userId="me", id=label["id"]).execute()
                db.execute(
                    "INSERT OR REPLACE INTO gmail_labels VALUES (?,?,?,?,?)",
                    (
                        detail["id"],
                        detail["name"],
                        detail.get("type", "user"),
                        detail.get("messagesTotal", 0),
                        detail.get("messagesUnread", 0),
                    ),
                )
            except Exception:
                db.execute(
                    "INSERT OR REPLACE INTO gmail_labels VALUES (?,?,?,?,?)",
                    (label["id"], label["name"], label.get("type", "user"), 0, 0),
                )

    def _sync_messages(self, service, db: sqlite3.Connection):
        """Fetch recent messages with metadata."""
        db.execute("""
            CREATE TABLE IF NOT EXISTS gmail_messages (
                id TEXT PRIMARY KEY,
                thread_id TEXT,
                from_email TEXT,
                from_name TEXT,
                to_email TEXT,
                subject TEXT,
                snippet TEXT,
                date TEXT,
                date_unix INTEGER,
                labels TEXT,
                is_unread INTEGER DEFAULT 0,
                has_attachments INTEGER DEFAULT 0
            )
        """)

        # Fetch message IDs (paginated)
        msg_ids = []
        page_token = None
        while len(msg_ids) < self.max_messages:
            batch_size = min(100, self.max_messages - len(msg_ids))
            results = service.users().messages().list(
                userId="me",
                maxResults=batch_size,
                pageToken=page_token,
            ).execute()

            messages = results.get("messages", [])
            msg_ids.extend(messages)

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        # Fetch full message details
        for msg_ref in msg_ids:
            try:
                msg = service.users().messages().get(
                    userId="me",
                    id=msg_ref["id"],
                    format="metadata",
                    metadataHeaders=["From", "To", "Subject", "Date"],
                ).execute()

                headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}

                from_raw = headers.get("from", "")
                from_name, from_email = self._parse_email_address(from_raw)
                to_raw = headers.get("to", "")
                _, to_email = self._parse_email_address(to_raw)

                subject = headers.get("subject", "(no subject)")
                date_str = headers.get("date", "")
                date_parsed = self._parse_date(date_str)

                label_ids = msg.get("labelIds", [])
                is_unread = 1 if "UNREAD" in label_ids else 0
                has_attachments = 1 if self._has_attachments(msg) else 0

                db.execute(
                    "INSERT OR REPLACE INTO gmail_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        msg["id"],
                        msg.get("threadId", ""),
                        from_email,
                        from_name,
                        to_email,
                        subject,
                        msg.get("snippet", ""),
                        date_parsed,
                        int(msg.get("internalDate", 0)) // 1000,
                        ",".join(label_ids),
                        is_unread,
                        has_attachments,
                    ),
                )
            except Exception as e:
                continue  # Skip individual message errors

    @staticmethod
    def _parse_email_address(raw: str) -> tuple[str, str]:
        """Parse 'John Doe <john@example.com>' into (name, email)."""
        if not raw:
            return ("", "")
        match = re.match(r'^"?([^"<]*)"?\s*<?([^>]*)>?$', raw.strip())
        if match:
            name = match.group(1).strip().strip('"')
            addr = match.group(2).strip() or raw.strip()
            return (name, addr)
        return ("", raw.strip())

    @staticmethod
    def _parse_date(date_str: str) -> str:
        """Parse email date header into ISO format."""
        if not date_str:
            return ""
        try:
            parsed = email.utils.parsedate_to_datetime(date_str)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return date_str

    @staticmethod
    def _has_attachments(msg: dict) -> bool:
        """Check if a message has attachments."""
        payload = msg.get("payload", {})
        parts = payload.get("parts", [])
        return any(
            p.get("filename") and p.get("body", {}).get("attachmentId")
            for p in parts
        )

    def _create_empty_tables(self, db: sqlite3.Connection):
        db.execute("""
            CREATE TABLE IF NOT EXISTS gmail_messages (
                id TEXT, thread_id TEXT, from_email TEXT, from_name TEXT,
                to_email TEXT, subject TEXT, snippet TEXT, date TEXT,
                date_unix INTEGER, labels TEXT, is_unread INTEGER, has_attachments INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS gmail_labels (
                id TEXT, name TEXT, type TEXT, messages_total INTEGER, messages_unread INTEGER
            )
        """)
        db.commit()
