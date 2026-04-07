"""
CSV Connector for DataQL.
Loads .csv files from a directory into queryable SQL tables (one table per file).
"""

from __future__ import annotations
import csv
import sqlite3
from pathlib import Path

from data_connectors import APIConnector
from connector_helpers import sanitize_name, sanitize_col, detect_types, cast


class CSVConnector(APIConnector):
    """Load .csv files from a directory into queryable SQL tables."""

    connector_name = "CSV Files"
    connector_icon = "📋"
    source_id = "csv"
    _cache_ttl_seconds = 60

    def __init__(self, directory: str, max_rows: int = 5000):
        self.directory = Path(directory)
        self.max_rows = max_rows

    def test_connection(self) -> tuple[bool, str]:
        if not self.directory.exists():
            return False, f"Directory not found: {self.directory}"
        csv_files = list(self.directory.glob("*.csv"))
        if not csv_files:
            return False, f"No .csv files in {self.directory}"
        return True, f"Found {len(csv_files)} CSV file(s) in {self.directory}"

    def _sync_data(self, db: sqlite3.Connection):
        for csv_path in self.directory.glob("*.csv"):
            try:
                self._load_csv(db, csv_path)
            except Exception as e:
                print(f"[CSV] Error loading {csv_path.name}: {e}")
        db.commit()

    def _load_csv(self, db: sqlite3.Connection, path: Path):
        table_name = sanitize_name(path.stem)
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            raw_headers = next(reader, None)
            if not raw_headers:
                return
            columns = [sanitize_col(h, i) for i, h in enumerate(raw_headers)]
            rows = []
            for i, row in enumerate(reader):
                if i >= self.max_rows:
                    break
                rows.append(row)

        col_types = detect_types(columns, rows)
        col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(columns, col_types))
        db.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

        placeholders = ", ".join(["?"] * len(columns))
        for row in rows:
            padded = list(row) + [None] * (len(columns) - len(row))
            padded = padded[: len(columns)]
            casted = [cast(v, t) for v, t in zip(padded, col_types)]
            db.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', casted)
