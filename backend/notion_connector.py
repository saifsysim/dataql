"""
Notion Connector for DataQL.
Queries Notion databases via the REST API (text-only fields, max 500 rows per DB).
"""

from __future__ import annotations
import json
import sqlite3
from typing import Any, Optional

from data_connectors import APIConnector
from connector_helpers import sanitize_name, sanitize_col


class NotionConnector(APIConnector):
    """Query Notion databases via the Notion API."""

    connector_name = "Notion"
    connector_icon = "📝"
    source_id = "notion"
    _cache_ttl_seconds = 180

    API = "https://api.notion.com/v1"

    def __init__(self, api_key: str, database_ids: list[str]):
        self.api_key = api_key
        self.database_ids = [d.strip() for d in database_ids if d.strip()]

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            import httpx
            resp = httpx.get(
                f"{self.API}/users/me", headers=self._headers(), timeout=10,
            )
            if resp.status_code == 200:
                name = resp.json().get("name", "")
                return True, f"Connected to Notion as {name}"
            return False, f"HTTP {resp.status_code}"
        except ImportError:
            return False, "Install: pip install httpx"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            import httpx
            for db_id in self.database_ids:
                try:
                    self._load_database(httpx, db, db_id)
                except Exception as e:
                    print(f"[Notion] Error loading {db_id}: {e}")
            db.commit()
        except ImportError:
            pass

    def _load_database(self, httpx_mod, db: sqlite3.Connection, database_id: str):
        meta = httpx_mod.get(
            f"{self.API}/databases/{database_id}",
            headers=self._headers(), timeout=15,
        ).json()

        title_parts = meta.get("title", [])
        db_title = title_parts[0].get("plain_text", database_id) if title_parts else database_id
        table_name = sanitize_name(f"notion_{db_title}")

        props = meta.get("properties", {})
        columns = ["id"] + [sanitize_col(name, i + 1) for i, name in enumerate(props.keys())]
        prop_names = list(props.keys())

        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        db.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

        has_more = True
        start_cursor = None
        total = 0
        while has_more and total < 500:
            body: dict[str, Any] = {"page_size": 100}
            if start_cursor:
                body["start_cursor"] = start_cursor
            resp = httpx_mod.post(
                f"{self.API}/databases/{database_id}/query",
                headers=self._headers(), json=body, timeout=20,
            )
            data = resp.json()
            for page in data.get("results", []):
                row_vals = [page["id"]]
                for pname in prop_names:
                    row_vals.append(self._extract_property(page.get("properties", {}).get(pname, {})))
                placeholders = ", ".join(["?"] * len(columns))
                db.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', row_vals)
                total += 1
                if total >= 500:
                    break
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")

    @staticmethod
    def _extract_property(prop: dict) -> str:
        """Extract a plain-text value from a Notion property object."""
        ptype = prop.get("type", "")
        if ptype == "title":
            return " ".join(t.get("plain_text", "") for t in prop.get("title", []))
        if ptype == "rich_text":
            return " ".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
        if ptype == "number":
            return str(prop.get("number", "")) if prop.get("number") is not None else ""
        if ptype == "select":
            sel = prop.get("select")
            return sel.get("name", "") if sel else ""
        if ptype == "multi_select":
            return ", ".join(s.get("name", "") for s in prop.get("multi_select", []))
        if ptype == "date":
            d = prop.get("date")
            return d.get("start", "") if d else ""
        if ptype == "checkbox":
            return str(prop.get("checkbox", False))
        if ptype == "url":
            return prop.get("url", "") or ""
        if ptype == "email":
            return prop.get("email", "") or ""
        if ptype == "phone_number":
            return prop.get("phone_number", "") or ""
        if ptype == "status":
            s = prop.get("status")
            return s.get("name", "") if s else ""
        return ""
