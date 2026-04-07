"""
Airtable Connector for DataQL.
Queries Airtable tables via the REST API (max 500 records per table).
"""

from __future__ import annotations
import json
import sqlite3
from typing import Any, Optional

from data_connectors import APIConnector
from connector_helpers import sanitize_name, sanitize_col


class AirtableConnector(APIConnector):
    """Query Airtable tables via the REST API."""

    connector_name = "Airtable"
    connector_icon = "🗂️"
    source_id = "airtable"
    _cache_ttl_seconds = 180

    API = "https://api.airtable.com/v0"

    def __init__(self, pat: str, base_id: str, table_ids: list[str]):
        self.pat = pat
        self.base_id = base_id
        self.table_ids = [t.strip() for t in table_ids if t.strip()]

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.pat}"}

    def test_connection(self) -> tuple[bool, str]:
        try:
            import httpx
            resp = httpx.get(
                f"https://api.airtable.com/v0/meta/bases/{self.base_id}/tables",
                headers=self._headers(), timeout=10,
            )
            if resp.status_code == 200:
                tables = resp.json().get("tables", [])
                return True, f"Connected to Airtable base ({len(tables)} tables)"
            return False, f"HTTP {resp.status_code}"
        except ImportError:
            return False, "Install: pip install httpx"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            import httpx
            for table_id in self.table_ids:
                try:
                    self._load_table(httpx, db, table_id)
                except Exception as e:
                    print(f"[Airtable] Error loading {table_id}: {e}")
            db.commit()
        except ImportError:
            pass

    def _load_table(self, httpx_mod, db: sqlite3.Connection, table_id: str):
        all_records: list[dict] = []
        offset: Optional[str] = None
        while len(all_records) < 500:
            params: dict[str, Any] = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            resp = httpx_mod.get(
                f"{self.API}/{self.base_id}/{table_id}",
                headers=self._headers(), params=params, timeout=20,
            )
            data = resp.json()
            records = data.get("records", [])
            all_records.extend(records)
            offset = data.get("offset")
            if not offset or not records:
                break

        if not all_records:
            return

        all_keys: list[str] = []
        seen: set[str] = set()
        for rec in all_records:
            for k in rec.get("fields", {}):
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        table_name = sanitize_name(f"at_{table_id}")
        columns = ["id"] + [sanitize_col(k, i + 1) for i, k in enumerate(all_keys)]

        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        db.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

        placeholders = ", ".join(["?"] * len(columns))
        for rec in all_records[:500]:
            fields = rec.get("fields", {})
            vals = [rec.get("id", "")]
            for k in all_keys:
                v = fields.get(k)
                if isinstance(v, (list, dict)):
                    vals.append(json.dumps(v))
                elif v is not None:
                    vals.append(str(v))
                else:
                    vals.append(None)
            db.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', vals)
