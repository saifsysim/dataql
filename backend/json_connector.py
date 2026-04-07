"""
JSON Connector for DataQL.
Loads .json files from a directory into queryable SQL tables (one table per file).
Nested objects are flattened one level deep.
"""

from __future__ import annotations
import json
import sqlite3
from pathlib import Path

from data_connectors import APIConnector
from connector_helpers import sanitize_name, sanitize_col, detect_types, cast, flatten


class JSONConnector(APIConnector):
    """Load .json files from a directory into queryable SQL tables."""

    connector_name = "JSON Files"
    connector_icon = "📦"
    source_id = "json"
    _cache_ttl_seconds = 60

    def __init__(self, directory: str, max_rows: int = 5000):
        self.directory = Path(directory)
        self.max_rows = max_rows

    def test_connection(self) -> tuple[bool, str]:
        if not self.directory.exists():
            return False, f"Directory not found: {self.directory}"
        json_files = list(self.directory.glob("*.json"))
        if not json_files:
            return False, f"No .json files in {self.directory}"
        return True, f"Found {len(json_files)} JSON file(s) in {self.directory}"

    def _sync_data(self, db: sqlite3.Connection):
        for json_path in self.directory.glob("*.json"):
            try:
                self._load_json(db, json_path)
            except Exception as e:
                print(f"[JSON] Error loading {json_path.name}: {e}")
        db.commit()

    def _load_json(self, db: sqlite3.Connection, path: Path):
        table_name = sanitize_name(path.stem)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Normalise to list of flat dicts
        if isinstance(data, dict):
            list_keys = [k for k, v in data.items() if isinstance(v, list)]
            if list_keys:
                data = data[list_keys[0]]
            else:
                data = [data]

        if not isinstance(data, list) or not data:
            return

        records = [flatten(r) for r in data if isinstance(r, dict)]
        if not records:
            return
        records = records[: self.max_rows]

        # Derive columns from all records
        all_keys: list[str] = []
        seen: set[str] = set()
        for rec in records:
            for k in rec:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        columns = [sanitize_col(k, i) for i, k in enumerate(all_keys)]
        sample_rows = [[rec.get(k) for k in all_keys] for rec in records[:50]]
        col_types = detect_types(columns, sample_rows)

        col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(columns, col_types))
        db.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

        placeholders = ", ".join(["?"] * len(columns))
        for rec in records:
            vals = [cast(rec.get(k), t) for k, t in zip(all_keys, col_types)]
            db.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', vals)
