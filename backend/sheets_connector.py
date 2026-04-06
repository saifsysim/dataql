"""
Google Sheets Connector for DataQL.

Reads specified Google Sheets spreadsheets and creates one SQLite table per
sheet tab, with auto-detected column types.

Tables created:
  - One per sheet tab, named: {spreadsheet_name}_{tab_name}
    (sanitized to valid SQL identifiers)
"""

from __future__ import annotations
import re
import sqlite3
from typing import Any, Optional

from data_connectors import APIConnector


class GoogleSheetsConnector(APIConnector):
    """Google Sheets API connector — loads sheets into queryable SQL tables."""

    connector_type = "api"
    connector_name = "Google Sheets"
    connector_icon = "📊"
    source_id = "google_sheets"
    _cache_ttl_seconds = 120  # 2 minute cache

    def __init__(self, spreadsheet_ids: list[str], credentials_file: str = None, token_file: str = None):
        """
        Args:
            spreadsheet_ids: List of Google Sheets spreadsheet IDs to load
            credentials_file: Path to OAuth credentials.json
            token_file: Path to token.json
        """
        self.spreadsheet_ids = [sid.strip() for sid in spreadsheet_ids if sid.strip()]
        self.credentials_file = credentials_file
        self.token_file = token_file
        self._sheet_names: list[str] = []  # Track loaded sheet names

    def _get_service(self):
        from googleapiclient.discovery import build
        from google_auth import get_google_credentials
        creds = get_google_credentials(self.credentials_file, self.token_file)
        return build("sheets", "v4", credentials=creds)

    def test_connection(self) -> tuple[bool, str]:
        try:
            from google_auth import check_google_auth
            ok, msg = check_google_auth(self.credentials_file, self.token_file)
            if not ok:
                return False, msg
            if not self.spreadsheet_ids:
                return False, "No spreadsheet IDs configured. Set GOOGLE_SHEETS_IDS in .env"
            service = self._get_service()
            # Test reading first spreadsheet
            meta = service.spreadsheets().get(spreadsheetId=self.spreadsheet_ids[0]).execute()
            title = meta.get("properties", {}).get("title", "Unknown")
            n_sheets = len(meta.get("sheets", []))
            total = len(self.spreadsheet_ids)
            return True, f"Connected — {total} spreadsheet(s), first: '{title}' ({n_sheets} tabs)"
        except ImportError:
            return False, "Install: pip install google-api-python-client google-auth-oauthlib"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        """Fetch all configured spreadsheets into SQLite tables."""
        try:
            service = self._get_service()
            self._sheet_names = []

            for sheet_id in self.spreadsheet_ids:
                try:
                    self._load_spreadsheet(service, db, sheet_id)
                except Exception as e:
                    print(f"[Sheets] Error loading {sheet_id}: {e}")
                    continue

            db.commit()
        except Exception as e:
            print(f"[Sheets] Sync error: {e}")

    def _load_spreadsheet(self, service, db: sqlite3.Connection, spreadsheet_id: str):
        """Load all tabs from a single spreadsheet."""
        # Get spreadsheet metadata
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        ss_title = meta.get("properties", {}).get("title", "sheet")
        sheets = meta.get("sheets", [])

        for sheet in sheets:
            tab_title = sheet.get("properties", {}).get("title", "Sheet1")
            table_name = self._make_table_name(ss_title, tab_title)

            try:
                # Read all data from the tab
                result = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{tab_title}'",
                    valueRenderOption="UNFORMATTED_VALUE",
                    dateTimeRenderOption="FORMATTED_STRING",
                ).execute()

                values = result.get("values", [])
                if len(values) < 2:
                    continue  # Need at least header + 1 row

                headers = values[0]
                rows = values[1:]

                # Sanitize column names
                columns = [self._sanitize_col(h, i) for i, h in enumerate(headers)]

                # Detect column types from data
                col_types = self._detect_types(columns, rows)

                # Create table
                col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(columns, col_types))
                db.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

                # Insert rows
                placeholders = ", ".join(["?"] * len(columns))
                for row in rows:
                    # Pad or trim row to match columns
                    padded = list(row) + [None] * (len(columns) - len(row))
                    padded = padded[:len(columns)]

                    # Cast values
                    casted = []
                    for val, typ in zip(padded, col_types):
                        casted.append(self._cast_value(val, typ))

                    db.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', casted)

                self._sheet_names.append(table_name)

            except Exception as e:
                print(f"[Sheets] Error loading tab '{tab_title}': {e}")

    @staticmethod
    def _make_table_name(spreadsheet_title: str, tab_title: str) -> str:
        """Create a clean SQL table name from spreadsheet + tab names."""
        combined = f"{spreadsheet_title}_{tab_title}"
        # Remove non-alphanumeric chars, collapse spaces/underscores
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', combined)
        clean = re.sub(r'_+', '_', clean).strip('_').lower()
        return clean[:63]  # Max identifier length

    @staticmethod
    def _sanitize_col(header: Any, index: int) -> str:
        """Sanitize a column header into a valid SQL column name."""
        if not header or not str(header).strip():
            return f"column_{index}"
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', str(header))
        clean = re.sub(r'_+', '_', clean).strip('_').lower()
        if not clean or clean[0].isdigit():
            clean = f"col_{clean}"
        return clean[:63]

    @staticmethod
    def _detect_types(columns: list[str], rows: list[list]) -> list[str]:
        """Auto-detect column types based on data samples."""
        types = ["TEXT"] * len(columns)

        for col_idx in range(len(columns)):
            int_count = 0
            float_count = 0
            total = 0

            for row in rows[:50]:  # Sample first 50 rows
                if col_idx >= len(row) or row[col_idx] is None or row[col_idx] == "":
                    continue
                val = row[col_idx]
                total += 1

                if isinstance(val, int):
                    int_count += 1
                elif isinstance(val, float):
                    float_count += 1
                elif isinstance(val, str):
                    try:
                        int(val)
                        int_count += 1
                    except ValueError:
                        try:
                            float(val)
                            float_count += 1
                        except ValueError:
                            pass

            if total > 0:
                if int_count / total > 0.8:
                    types[col_idx] = "INTEGER"
                elif (int_count + float_count) / total > 0.8:
                    types[col_idx] = "REAL"

        return types

    @staticmethod
    def _cast_value(val: Any, target_type: str) -> Any:
        """Cast a cell value to the target SQL type."""
        if val is None or val == "":
            return None
        if target_type == "INTEGER":
            try:
                return int(val) if not isinstance(val, int) else val
            except (ValueError, TypeError):
                return None
        if target_type == "REAL":
            try:
                return float(val) if not isinstance(val, float) else val
            except (ValueError, TypeError):
                return None
        return str(val)
