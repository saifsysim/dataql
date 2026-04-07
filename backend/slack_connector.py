"""
Slack Connector for DataQL.

Fetches users, channels, and recent messages from a Slack workspace
into an in-memory SQLite database for SQL querying.

Tables created:
  - slack_users: id, name, real_name, display_name, email, is_admin, is_bot, status_text, timezone
  - slack_channels: id, name, topic, purpose, num_members, is_private, created_date
  - slack_messages: channel_id, channel_name, user_id, text, timestamp, reply_count, reaction_count
"""

from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta

from data_connectors import APIConnector


class SlackConnector(APIConnector):
    """Slack connector — indexes channels, messages, and users."""

    connector_name = "Slack"
    connector_icon = "💬"
    source_id = "slack"

    def __init__(self, bot_token: str):
        self.bot_token = bot_token

    def test_connection(self) -> tuple[bool, str]:
        try:
            import httpx
            resp = httpx.get(
                "https://slack.com/api/auth.test",
                headers={"Authorization": f"Bearer {self.bot_token}"},
                timeout=10,
            )
            data = resp.json()
            if data.get("ok"):
                return True, f"Connected to Slack workspace: {data.get('team')}"
            return False, data.get("error", "Auth failed")
        except ImportError:
            return False, "Install: pip install httpx"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            import httpx
            headers = {"Authorization": f"Bearer {self.bot_token}"}

            # Users
            db.execute("""
                CREATE TABLE slack_users (
                    id TEXT PRIMARY KEY, name TEXT, real_name TEXT,
                    display_name TEXT, email TEXT, is_admin INTEGER,
                    is_bot INTEGER, status_text TEXT, timezone TEXT
                )
            """)
            resp = httpx.get("https://slack.com/api/users.list", headers=headers, timeout=30)
            if resp.status_code == 200 and resp.json().get("ok"):
                for m in resp.json()["members"]:
                    if m.get("deleted"):
                        continue
                    profile = m.get("profile", {})
                    db.execute(
                        "INSERT OR IGNORE INTO slack_users VALUES (?,?,?,?,?,?,?,?,?)",
                        (m["id"], m.get("name"), m.get("real_name"),
                         profile.get("display_name"), profile.get("email"),
                         int(m.get("is_admin", False)), int(m.get("is_bot", False)),
                         profile.get("status_text"), m.get("tz")),
                    )

            # Channels
            db.execute("""
                CREATE TABLE slack_channels (
                    id TEXT PRIMARY KEY, name TEXT, topic TEXT,
                    purpose TEXT, num_members INTEGER,
                    is_private INTEGER, created_date TEXT
                )
            """)
            resp2 = httpx.get(
                "https://slack.com/api/conversations.list",
                params={"types": "public_channel,private_channel", "limit": 500},
                headers=headers, timeout=30,
            )
            if resp2.status_code == 200 and resp2.json().get("ok"):
                for ch in resp2.json()["channels"]:
                    ts = ch.get("created", 0)
                    created = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else ""
                    db.execute(
                        "INSERT OR IGNORE INTO slack_channels VALUES (?,?,?,?,?,?,?)",
                        (ch["id"], ch.get("name"), ch.get("topic", {}).get("value", ""),
                         ch.get("purpose", {}).get("value", ""), ch.get("num_members", 0),
                         int(ch.get("is_private", False)), created),
                    )

            # Recent messages from top channels (last 7 days)
            db.execute("""
                CREATE TABLE slack_messages (
                    channel_id TEXT, channel_name TEXT, user_id TEXT,
                    text TEXT, timestamp TEXT, reply_count INTEGER,
                    reaction_count INTEGER
                )
            """)
            one_week_ago = str((datetime.utcnow() - timedelta(days=7)).timestamp())
            channels = db.execute("SELECT id, name FROM slack_channels LIMIT 20").fetchall()
            for ch in channels:
                try:
                    resp3 = httpx.get(
                        "https://slack.com/api/conversations.history",
                        params={"channel": ch["id"], "oldest": one_week_ago, "limit": 100},
                        headers=headers, timeout=10,
                    )
                    if resp3.status_code == 200 and resp3.json().get("ok"):
                        for msg in resp3.json().get("messages", []):
                            if msg.get("subtype"):
                                continue
                            ts = float(msg.get("ts", 0))
                            dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else ""
                            reactions = sum(r.get("count", 0) for r in msg.get("reactions", []))
                            db.execute(
                                "INSERT INTO slack_messages VALUES (?,?,?,?,?,?,?)",
                                (ch["id"], ch["name"], msg.get("user"),
                                 (msg.get("text", ""))[:500], dt,
                                 msg.get("reply_count", 0), reactions),
                            )
                except Exception:
                    continue

            db.commit()
        except ImportError:
            self._create_placeholder(db)
        except Exception:
            self._create_placeholder(db)

    def _create_placeholder(self, db):
        db.execute("CREATE TABLE IF NOT EXISTS slack_users (id TEXT, name TEXT, real_name TEXT, display_name TEXT, email TEXT, is_admin INTEGER, is_bot INTEGER, status_text TEXT, timezone TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS slack_channels (id TEXT, name TEXT, topic TEXT, purpose TEXT, num_members INTEGER, is_private INTEGER, created_date TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS slack_messages (channel_id TEXT, channel_name TEXT, user_id TEXT, text TEXT, timestamp TEXT, reply_count INTEGER, reaction_count INTEGER)")
        db.commit()
