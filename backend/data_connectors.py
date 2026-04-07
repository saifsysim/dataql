"""
DataQL Connector Registry — base classes, connector registry, and all data source connectors.

Connectors fall into two categories:
  1. SQL Connectors (SQLite, PostgreSQL, ClickHouse) — execute SQL directly
  2. API Connectors (Google Analytics, Salesforce, Marketo, Google Docs, Slack)
     — fetch data from APIs and expose it as virtual tables queryable via SQL

API connectors work by fetching data into an in-memory SQLite database,
making them queryable via the same SQL interface.
"""

from __future__ import annotations
import re
import os
import time
import json
import sqlite3
import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timedelta

# ── Guardrails ────────────────────────────────────────────────

BLOCKED_PATTERNS = [
    r"\bDROP\b", r"\bDELETE\b", r"\bTRUNCATE\b", r"\bALTER\b",
    r"\bINSERT\b", r"\bUPDATE\b", r"\bCREATE\b", r"\bGRANT\b",
    r"\bREVOKE\b", r"\bEXEC\b", r"\bEXECUTE\b",
]


def validate_sql(sql: str) -> tuple[bool, Optional[str]]:
    """Check SQL for dangerous operations. Returns (is_safe, error_message)."""
    sql_upper = sql.upper().strip()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return False, "Only SELECT queries are allowed."
    for pattern in BLOCKED_PATTERNS:
        if re.search(pattern, sql_upper):
            return False, f"Blocked: forbidden keyword matching '{pattern}'."
    cleaned = re.sub(r"'[^']*'", "", sql)
    if ";" in cleaned.strip().rstrip(";"):
        return False, "Multiple statements are not allowed."
    return True, None


# ══════════════════════════════════════════════════════════════
#  BASE CONNECTOR
# ══════════════════════════════════════════════════════════════

class BaseConnector(ABC):
    """Abstract base for all DataQL connectors."""

    connector_type: str = "unknown"       # "sql" or "api"
    connector_name: str = "Unknown"       # Display name
    connector_icon: str = "🔌"            # Emoji icon
    source_id: str = "unknown"            # Unique ID

    @abstractmethod
    def test_connection(self) -> tuple[bool, str]:
        """Test if the connector can reach its data source. Returns (ok, message)."""
        ...

    @abstractmethod
    def execute_query(self, sql: str) -> dict[str, Any]:
        """Execute a SQL query. Returns {columns, rows, row_count, execution_time_ms}."""
        ...

    @abstractmethod
    def get_tables(self) -> list[str]:
        """Return list of available table names."""
        ...

    @abstractmethod
    def get_table_info(self, table_name: str) -> list[dict]:
        """Return column info for a table: [{name, data_type, nullable, is_primary_key}]."""
        ...

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        """Return FK info. Override in SQL connectors; API connectors return []."""
        return []

    def get_row_count(self, table_name: str) -> int:
        """Return row count for a table."""
        result = self.execute_query(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
        if result["rows"]:
            return result["rows"][0].get("cnt", 0)
        return 0

    def get_status(self) -> dict:
        """Return connector status info for the UI."""
        ok, msg = self.test_connection()
        return {
            "source_id": self.source_id,
            "name": self.connector_name,
            "icon": self.connector_icon,
            "type": self.connector_type,
            "connected": ok,
            "message": msg,
        }


# ══════════════════════════════════════════════════════════════
#  SQL CONNECTORS
# ══════════════════════════════════════════════════════════════

class SQLiteConnector(BaseConnector):
    """SQLite connector for local databases."""

    connector_type = "sql"
    connector_name = "SQLite"
    connector_icon = "🗃️"
    source_id = "sqlite"

    def __init__(self, db_path: str = "demo.db"):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._conn()
            conn.execute("SELECT 1")
            conn.close()
            return True, f"Connected to {self.db_path}"
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        conn = self._conn()
        try:
            cursor = conn.execute(sql)
            rows_raw = cursor.fetchall()
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = [dict(r) for r in rows_raw]
            return {
                "columns": columns, "rows": rows,
                "row_count": len(rows),
                "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
            }
        finally:
            conn.close()

    def get_tables(self) -> list[str]:
        conn = self._conn()
        try:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [r["name"] for r in cur.fetchall()]
        finally:
            conn.close()

    def get_table_info(self, table_name: str) -> list[dict]:
        conn = self._conn()
        try:
            cur = conn.execute(f"PRAGMA table_info('{table_name}')")
            return [{
                "name": r["name"], "data_type": r["type"] or "TEXT",
                "nullable": not r["notnull"], "is_primary_key": bool(r["pk"]),
            } for r in cur.fetchall()]
        finally:
            conn.close()

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        conn = self._conn()
        try:
            cur = conn.execute(f"PRAGMA foreign_key_list('{table_name}')")
            return [{
                "from_column": r["from"], "to_table": r["table"], "to_column": r["to"],
            } for r in cur.fetchall()]
        finally:
            conn.close()


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL connector using psycopg2."""

    connector_type = "sql"
    connector_name = "PostgreSQL"
    connector_icon = "🐘"
    source_id = "postgresql"

    def __init__(self, connection_url: str):
        self.connection_url = connection_url

    def _conn(self):
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise ImportError("Install psycopg2-binary: pip install psycopg2-binary")
        return psycopg2.connect(self.connection_url)

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True, "Connected to PostgreSQL"
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        conn = self._conn()
        try:
            import psycopg2.extras
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
            columns = [d.name for d in cur.description] if cur.description else []
            return {
                "columns": columns, "rows": rows,
                "row_count": len(rows),
                "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
            }
        finally:
            conn.close()

    def get_tables(self) -> list[str]:
        result = self.execute_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        return [r["table_name"] for r in result["rows"]]

    def get_table_info(self, table_name: str) -> list[dict]:
        result = self.execute_query(
            f"SELECT column_name, data_type, is_nullable, "
            f"CASE WHEN column_name IN ("
            f"  SELECT kcu.column_name FROM information_schema.key_column_usage kcu "
            f"  JOIN information_schema.table_constraints tc ON tc.constraint_name = kcu.constraint_name "
            f"  WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'"
            f") THEN 'YES' ELSE 'NO' END as is_pk "
            f"FROM information_schema.columns WHERE table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
        return [{
            "name": r["column_name"], "data_type": r["data_type"],
            "nullable": r["is_nullable"] == "YES", "is_primary_key": r["is_pk"] == "YES",
        } for r in result["rows"]]

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        result = self.execute_query(
            f"SELECT kcu.column_name as from_column, ccu.table_name as to_table, "
            f"ccu.column_name as to_column "
            f"FROM information_schema.key_column_usage kcu "
            f"JOIN information_schema.referential_constraints rc ON rc.constraint_name = kcu.constraint_name "
            f"JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = rc.unique_constraint_name "
            f"WHERE kcu.table_name = '{table_name}'"
        )
        return result["rows"]


class ClickHouseConnector(BaseConnector):
    """ClickHouse connector using clickhouse-connect."""

    connector_type = "sql"
    connector_name = "ClickHouse"
    connector_icon = "⚡"
    source_id = "clickhouse"

    def __init__(self, url: str):
        # Parse: clickhouse://user:pass@host:port/database
        self.url = url
        self._parse_url(url)

    def _parse_url(self, url: str):
        """Parse clickhouse:// URL into components."""
        from urllib.parse import urlparse
        parsed = urlparse(url.replace("clickhouse://", "http://"))
        self.host = parsed.hostname or "localhost"
        self.port = parsed.port or 8123
        self.user = parsed.username or "default"
        self.password = parsed.password or ""
        self.database = parsed.path.lstrip("/") or "default"

    def _client(self):
        try:
            import clickhouse_connect
        except ImportError:
            raise ImportError("Install clickhouse-connect: pip install clickhouse-connect")
        return clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.user, password=self.password,
            database=self.database,
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self._client()
            client.query("SELECT 1")
            return True, f"Connected to ClickHouse at {self.host}:{self.port}"
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        client = self._client()
        result = client.query(sql)
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows]
        return {
            "columns": list(columns), "rows": rows,
            "row_count": len(rows),
            "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
        }

    def get_tables(self) -> list[str]:
        result = self.execute_query("SELECT name FROM system.tables WHERE database = currentDatabase()")
        return [r["name"] for r in result["rows"]]

    def get_table_info(self, table_name: str) -> list[dict]:
        result = self.execute_query(
            f"SELECT name, type, default_kind "
            f"FROM system.columns WHERE table = '{table_name}' AND database = currentDatabase()"
        )
        return [{
            "name": r["name"], "data_type": r["type"],
            "nullable": "Nullable" in r.get("type", ""),
            "is_primary_key": False,
        } for r in result["rows"]]


class SnowflakeConnector(BaseConnector):
    """Snowflake connector using snowflake-connector-python."""

    connector_type = "sql"
    connector_name = "Snowflake"
    connector_icon = "❄️"
    source_id = "snowflake"

    def __init__(self, account: str, user: str, password: str, warehouse: str,
                 database: str, schema: str = "PUBLIC"):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def _conn(self):
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError("Install snowflake-connector-python: pip install snowflake-connector-python")
        return snowflake.connector.connect(
            account=self.account, user=self.user, password=self.password,
            warehouse=self.warehouse, database=self.database, schema=self.schema,
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._conn()
            conn.cursor().execute("SELECT 1")
            conn.close()
            return True, f"Connected to Snowflake ({self.account})"
        except ImportError as e:
            return False, str(e)
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return {
                "columns": columns, "rows": rows,
                "row_count": len(rows),
                "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
            }
        finally:
            conn.close()

    def get_tables(self) -> list[str]:
        result = self.execute_query(
            f"SELECT TABLE_NAME FROM {self.database}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{self.schema}' ORDER BY TABLE_NAME"
        )
        return [r["TABLE_NAME"] for r in result["rows"]]

    def get_table_info(self, table_name: str) -> list[dict]:
        result = self.execute_query(
            f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
            f"FROM {self.database}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{self.schema}' AND TABLE_NAME = '{table_name}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        return [{
            "name": r["COLUMN_NAME"], "data_type": r["DATA_TYPE"],
            "nullable": r["IS_NULLABLE"] == "YES", "is_primary_key": False,
        } for r in result["rows"]]


class DatabricksConnector(BaseConnector):
    """Databricks SQL connector using databricks-sql-connector."""

    connector_type = "sql"
    connector_name = "Databricks"
    connector_icon = "🧱"
    source_id = "databricks"

    def __init__(self, host: str, http_path: str, access_token: str, catalog: str = "main", schema: str = "default"):
        self.host = host
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema

    def _conn(self):
        try:
            from databricks import sql
        except ImportError:
            raise ImportError("Install databricks-sql-connector: pip install databricks-sql-connector")
        return sql.connect(
            server_hostname=self.host,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True, f"Connected to Databricks ({self.host})"
        except ImportError as e:
            return False, str(e)
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql_query: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql_query)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql_query)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return {
                "columns": columns, "rows": rows,
                "row_count": len(rows),
                "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
            }
        finally:
            conn.close()

    def get_tables(self) -> list[str]:
        result = self.execute_query(
            f"SELECT table_name FROM {self.catalog}.information_schema.tables "
            f"WHERE table_schema = '{self.schema}' ORDER BY table_name"
        )
        return [r["table_name"] for r in result["rows"]]

    def get_table_info(self, table_name: str) -> list[dict]:
        result = self.execute_query(
            f"SELECT column_name, data_type, is_nullable "
            f"FROM {self.catalog}.information_schema.columns "
            f"WHERE table_schema = '{self.schema}' AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
        return [{
            "name": r["column_name"], "data_type": r["data_type"],
            "nullable": r["is_nullable"] == "YES", "is_primary_key": False,
        } for r in result["rows"]]


class RedshiftConnector(BaseConnector):
    """Amazon Redshift connector using redshift_connector."""

    connector_type = "sql"
    connector_name = "Redshift"
    connector_icon = "🔴"
    source_id = "redshift"

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def _conn(self):
        try:
            import redshift_connector
        except ImportError:
            raise ImportError("Install redshift_connector: pip install redshift_connector")
        return redshift_connector.connect(
            host=self.host, port=self.port, database=self.database,
            user=self.user, password=self.password,
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True, f"Connected to Redshift ({self.host})"
        except ImportError as e:
            return False, str(e)
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return {
                "columns": columns, "rows": rows,
                "row_count": len(rows),
                "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
            }
        finally:
            conn.close()

    def get_tables(self) -> list[str]:
        result = self.execute_query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        return [r["tablename"] for r in result["rows"]]

    def get_table_info(self, table_name: str) -> list[dict]:
        result = self.execute_query(
            f"SELECT column_name, data_type, is_nullable "
            f"FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
        return [{
            "name": r["column_name"], "data_type": r["data_type"],
            "nullable": r["is_nullable"] == "YES", "is_primary_key": False,
        } for r in result["rows"]]


# ══════════════════════════════════════════════════════════════
#  API CONNECTOR BASE (fetches data → in-memory SQLite)
# ══════════════════════════════════════════════════════════════

class APIConnector(BaseConnector):
    """
    Base for API-based connectors. These fetch data from external APIs
    and load it into an in-memory SQLite database so it can be queried
    via SQL just like any other connector.
    """

    connector_type = "api"
    _cache_ttl_seconds = 300  # 5 minutes
    _last_sync: Optional[datetime] = None
    _mem_db: Optional[sqlite3.Connection] = None

    def _get_mem_db(self) -> sqlite3.Connection:
        """Get or create the in-memory SQLite database."""
        now = datetime.utcnow()
        if (
            self._mem_db is None
            or self._last_sync is None
            or (now - self._last_sync).total_seconds() > self._cache_ttl_seconds
        ):
            self._mem_db = sqlite3.connect(":memory:")
            self._mem_db.row_factory = sqlite3.Row
            self._sync_data(self._mem_db)
            self._last_sync = now
        return self._mem_db

    @abstractmethod
    def _sync_data(self, db: sqlite3.Connection):
        """Fetch data from the API and load into the in-memory SQLite db."""
        ...

    def execute_query(self, sql: str) -> dict[str, Any]:
        is_safe, error = validate_sql(sql)
        if not is_safe:
            raise ValueError(f"Query blocked: {error}")
        start = time.perf_counter()
        db = self._get_mem_db()
        cursor = db.execute(sql)
        rows_raw = cursor.fetchall()
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = [dict(r) for r in rows_raw]
        return {
            "columns": columns, "rows": rows,
            "row_count": len(rows),
            "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
        }

    def get_tables(self) -> list[str]:
        db = self._get_mem_db()
        cur = db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [r["name"] for r in cur.fetchall()]

    def get_table_info(self, table_name: str) -> list[dict]:
        db = self._get_mem_db()
        cur = db.execute(f"PRAGMA table_info('{table_name}')")
        return [{
            "name": r["name"], "data_type": r["type"] or "TEXT",
            "nullable": not r["notnull"], "is_primary_key": bool(r["pk"]),
        } for r in cur.fetchall()]

    def force_refresh(self):
        """Force a data re-sync on next query."""
        self._last_sync = None
        self._mem_db = None


# ══════════════════════════════════════════════════════════════
#  API CONNECTORS
# ══════════════════════════════════════════════════════════════

class GoogleAnalyticsConnector(APIConnector):
    """Google Analytics 4 connector using the GA4 Data API."""

    connector_name = "Google Analytics"
    connector_icon = "📊"
    source_id = "google_analytics"

    def __init__(self, property_id: str, credentials_json: str):
        self.property_id = property_id
        self.credentials_json = credentials_json

    def test_connection(self) -> tuple[bool, str]:
        try:
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_json,
                scopes=["https://www.googleapis.com/auth/analytics.readonly"],
            )
            client = BetaAnalyticsDataClient(credentials=creds)
            return True, f"Connected to GA4 property {self.property_id}"
        except ImportError:
            return False, "Install: pip install google-analytics-data"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(
                self.credentials_json,
                scopes=["https://www.googleapis.com/auth/analytics.readonly"],
            )
            client = BetaAnalyticsDataClient(credentials=creds)

            # Fetch page views, sessions, users by date
            request = RunReportRequest(
                property=self.property_id,
                dimensions=[Dimension(name="date"), Dimension(name="pagePath"), Dimension(name="country")],
                metrics=[
                    Metric(name="sessions"), Metric(name="activeUsers"),
                    Metric(name="screenPageViews"), Metric(name="bounceRate"),
                ],
                date_ranges=[DateRange(start_date="90daysAgo", end_date="today")],
                limit=10000,
            )
            response = client.run_report(request=request)

            db.execute("""
                CREATE TABLE ga_page_views (
                    date TEXT, page_path TEXT, country TEXT,
                    sessions INTEGER, active_users INTEGER,
                    page_views INTEGER, bounce_rate REAL
                )
            """)
            for row in response.rows:
                db.execute(
                    "INSERT INTO ga_page_views VALUES (?,?,?,?,?,?,?)",
                    (
                        row.dimension_values[0].value,
                        row.dimension_values[1].value,
                        row.dimension_values[2].value,
                        int(row.metric_values[0].value),
                        int(row.metric_values[1].value),
                        int(row.metric_values[2].value),
                        float(row.metric_values[3].value),
                    ),
                )

            # Fetch traffic sources
            request2 = RunReportRequest(
                property=self.property_id,
                dimensions=[Dimension(name="date"), Dimension(name="sessionSource"), Dimension(name="sessionMedium")],
                metrics=[Metric(name="sessions"), Metric(name="activeUsers")],
                date_ranges=[DateRange(start_date="90daysAgo", end_date="today")],
                limit=10000,
            )
            response2 = client.run_report(request=request2)

            db.execute("""
                CREATE TABLE ga_traffic_sources (
                    date TEXT, source TEXT, medium TEXT,
                    sessions INTEGER, active_users INTEGER
                )
            """)
            for row in response2.rows:
                db.execute(
                    "INSERT INTO ga_traffic_sources VALUES (?,?,?,?,?)",
                    (
                        row.dimension_values[0].value,
                        row.dimension_values[1].value,
                        row.dimension_values[2].value,
                        int(row.metric_values[0].value),
                        int(row.metric_values[1].value),
                    ),
                )
            db.commit()
        except ImportError:
            self._create_placeholder_tables(db, "google_analytics")
        except Exception as e:
            self._create_placeholder_tables(db, "google_analytics", str(e))

    def _create_placeholder_tables(self, db, source, error=None):
        db.execute("CREATE TABLE IF NOT EXISTS ga_page_views (date TEXT, page_path TEXT, country TEXT, sessions INTEGER, active_users INTEGER, page_views INTEGER, bounce_rate REAL)")
        db.execute("CREATE TABLE IF NOT EXISTS ga_traffic_sources (date TEXT, source TEXT, medium TEXT, sessions INTEGER, active_users INTEGER)")
        db.commit()


class SalesforceConnector(APIConnector):
    """Salesforce connector using the REST API."""

    connector_name = "Salesforce"
    connector_icon = "☁️"
    source_id = "salesforce"

    OBJECTS = ["Account", "Contact", "Opportunity", "Lead", "Case"]

    def __init__(self, instance_url: str, access_token: str):
        self.instance_url = instance_url.rstrip("/")
        self.access_token = access_token

    def test_connection(self) -> tuple[bool, str]:
        try:
            import httpx
            resp = httpx.get(
                f"{self.instance_url}/services/data/v59.0/sobjects/",
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=10,
            )
            if resp.status_code == 200:
                return True, f"Connected to Salesforce at {self.instance_url}"
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        except ImportError:
            return False, "Install: pip install httpx"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            import httpx
            headers = {"Authorization": f"Bearer {self.access_token}"}

            for obj in self.OBJECTS:
                try:
                    # Describe the object to get fields
                    desc_resp = httpx.get(
                        f"{self.instance_url}/services/data/v59.0/sobjects/{obj}/describe/",
                        headers=headers, timeout=30,
                    )
                    if desc_resp.status_code != 200:
                        continue

                    desc = desc_resp.json()
                    fields = [f for f in desc["fields"] if f["type"] in (
                        "string", "textarea", "email", "phone", "url",
                        "double", "currency", "int", "percent",
                        "boolean", "date", "datetime", "id", "reference",
                    )][:20]  # Limit to 20 most relevant fields

                    field_names = [f["name"] for f in fields]
                    soql = f"SELECT {','.join(field_names)} FROM {obj} LIMIT 5000"

                    query_resp = httpx.get(
                        f"{self.instance_url}/services/data/v59.0/query/",
                        params={"q": soql},
                        headers=headers, timeout=30,
                    )
                    if query_resp.status_code != 200:
                        continue

                    records = query_resp.json().get("records", [])

                    # Create table
                    col_defs = ", ".join(f'"{fn}" TEXT' for fn in field_names)
                    table_name = f"sf_{obj.lower()}"
                    db.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

                    for rec in records:
                        vals = [str(rec.get(fn, "")) if rec.get(fn) is not None else None for fn in field_names]
                        placeholders = ",".join(["?"] * len(field_names))
                        db.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', vals)

                except Exception:
                    continue

            db.commit()
        except ImportError:
            self._create_placeholder(db)
        except Exception:
            self._create_placeholder(db)

    def _create_placeholder(self, db):
        for obj in self.OBJECTS:
            table = f"sf_{obj.lower()}"
            db.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (id TEXT, name TEXT, created_date TEXT)')
        db.commit()


class MarketoConnector(APIConnector):
    """Marketo connector using the REST API."""

    connector_name = "Marketo"
    connector_icon = "📧"
    source_id = "marketo"

    def __init__(self, endpoint: str, client_id: str, client_secret: str):
        self.endpoint = endpoint.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None

    def _get_token(self) -> str:
        import httpx
        resp = httpx.get(
            f"{self.endpoint}/identity/oauth/token",
            params={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=10,
        )
        data = resp.json()
        self._token = data["access_token"]
        return self._token

    def test_connection(self) -> tuple[bool, str]:
        try:
            import httpx
            token = self._get_token()
            return True, f"Connected to Marketo at {self.endpoint}"
        except ImportError:
            return False, "Install: pip install httpx"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            import httpx
            token = self._get_token()
            headers = {"Authorization": f"Bearer {token}"}

            # Leads
            db.execute("""
                CREATE TABLE marketo_leads (
                    id INTEGER, email TEXT, first_name TEXT, last_name TEXT,
                    company TEXT, title TEXT, created_at TEXT, updated_at TEXT
                )
            """)
            resp = httpx.get(
                f"{self.endpoint}/rest/v1/leads.json",
                params={"filterType": "id", "filterValues": "1", "fields": "id,email,firstName,lastName,company,title,createdAt,updatedAt"},
                headers=headers, timeout=30,
            )
            if resp.status_code == 200:
                for lead in resp.json().get("result", []):
                    db.execute(
                        "INSERT INTO marketo_leads VALUES (?,?,?,?,?,?,?,?)",
                        (lead.get("id"), lead.get("email"), lead.get("firstName"),
                         lead.get("lastName"), lead.get("company"), lead.get("title"),
                         lead.get("createdAt"), lead.get("updatedAt")),
                    )

            # Activities
            db.execute("""
                CREATE TABLE marketo_activities (
                    id INTEGER, lead_id INTEGER, activity_type TEXT,
                    activity_date TEXT, primary_attribute TEXT
                )
            """)

            # Programs/Campaigns
            db.execute("""
                CREATE TABLE marketo_programs (
                    id INTEGER, name TEXT, type TEXT, channel TEXT,
                    status TEXT, created_at TEXT
                )
            """)
            resp2 = httpx.get(
                f"{self.endpoint}/rest/asset/v1/programs.json",
                params={"maxReturn": 200},
                headers=headers, timeout=30,
            )
            if resp2.status_code == 200:
                for prog in resp2.json().get("result", []):
                    db.execute(
                        "INSERT INTO marketo_programs VALUES (?,?,?,?,?,?)",
                        (prog.get("id"), prog.get("name"), prog.get("type"),
                         prog.get("channel"), prog.get("status"), prog.get("createdAt")),
                    )
            db.commit()
        except ImportError:
            self._create_placeholder(db)
        except Exception:
            self._create_placeholder(db)

    def _create_placeholder(self, db):
        db.execute("CREATE TABLE IF NOT EXISTS marketo_leads (id INTEGER, email TEXT, first_name TEXT, last_name TEXT, company TEXT, title TEXT, created_at TEXT, updated_at TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS marketo_activities (id INTEGER, lead_id INTEGER, activity_type TEXT, activity_date TEXT, primary_attribute TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS marketo_programs (id INTEGER, name TEXT, type TEXT, channel TEXT, status TEXT, created_at TEXT)")
        db.commit()


class GoogleDocsConnector(APIConnector):
    """Google Docs/Drive connector — indexes document metadata and content."""

    connector_name = "Google Docs"
    connector_icon = "📄"
    source_id = "google_docs"

    def __init__(self, credentials_json: str):
        self.credentials_json = credentials_json

    def test_connection(self) -> tuple[bool, str]:
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_json,
                scopes=["https://www.googleapis.com/auth/drive.readonly"],
            )
            service = build("drive", "v3", credentials=creds)
            service.files().list(pageSize=1).execute()
            return True, "Connected to Google Drive"
        except ImportError:
            return False, "Install: pip install google-api-python-client google-auth"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            creds = service_account.Credentials.from_service_account_file(
                self.credentials_json,
                scopes=[
                    "https://www.googleapis.com/auth/drive.readonly",
                    "https://www.googleapis.com/auth/documents.readonly",
                ],
            )

            drive = build("drive", "v3", credentials=creds)
            docs = build("docs", "v1", credentials=creds)

            db.execute("""
                CREATE TABLE gdocs_files (
                    id TEXT PRIMARY KEY, name TEXT, mime_type TEXT,
                    created_time TEXT, modified_time TEXT,
                    size_bytes INTEGER, owner_email TEXT, shared INTEGER
                )
            """)

            db.execute("""
                CREATE TABLE gdocs_content (
                    doc_id TEXT, title TEXT, content_preview TEXT,
                    word_count INTEGER
                )
            """)

            # List files
            results = drive.files().list(
                pageSize=500,
                fields="files(id,name,mimeType,createdTime,modifiedTime,size,owners,shared)",
                q="trashed=false",
            ).execute()

            for f in results.get("files", []):
                owner = f.get("owners", [{}])[0].get("emailAddress", "")
                db.execute(
                    "INSERT INTO gdocs_files VALUES (?,?,?,?,?,?,?,?)",
                    (f["id"], f["name"], f["mimeType"],
                     f.get("createdTime"), f.get("modifiedTime"),
                     int(f.get("size", 0)), owner, int(f.get("shared", False))),
                )

                # Get content for Google Docs
                if f["mimeType"] == "application/vnd.google-apps.document":
                    try:
                        doc = docs.documents().get(documentId=f["id"]).execute()
                        body = doc.get("body", {}).get("content", [])
                        text_parts = []
                        for el in body:
                            if "paragraph" in el:
                                for pe in el["paragraph"].get("elements", []):
                                    if "textRun" in pe:
                                        text_parts.append(pe["textRun"]["content"])
                        full_text = " ".join(text_parts)
                        preview = full_text[:500]
                        word_count = len(full_text.split())
                        db.execute(
                            "INSERT INTO gdocs_content VALUES (?,?,?,?)",
                            (f["id"], f["name"], preview, word_count),
                        )
                    except Exception:
                        pass
            db.commit()
        except ImportError:
            self._create_placeholder(db)
        except Exception:
            self._create_placeholder(db)

    def _create_placeholder(self, db):
        db.execute("CREATE TABLE IF NOT EXISTS gdocs_files (id TEXT, name TEXT, mime_type TEXT, created_time TEXT, modified_time TEXT, size_bytes INTEGER, owner_email TEXT, shared INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS gdocs_content (doc_id TEXT, title TEXT, content_preview TEXT, word_count INTEGER)")
        db.commit()




# ══════════════════════════════════════════════════════════════
#  CONNECTOR REGISTRY
# ══════════════════════════════════════════════════════════════

class ConnectorRegistry:
    """
    Central registry for all data source connectors.
    The query engine uses this to route queries to the right connector.
    """

    def __init__(self):
        self.connectors: dict[str, BaseConnector] = {}

    def register(self, connector: BaseConnector):
        """Register a connector by its source_id."""
        self.connectors[connector.source_id] = connector

    def get(self, source_id: str) -> Optional[BaseConnector]:
        return self.connectors.get(source_id)

    def get_primary(self) -> BaseConnector:
        """Return the first registered connector (primary data source)."""
        if not self.connectors:
            raise ValueError("No connectors registered")
        return next(iter(self.connectors.values()))

    def list_all(self) -> list[dict]:
        """Return status info for all registered connectors."""
        return [c.get_status() for c in self.connectors.values()]

    def get_all(self) -> list[BaseConnector]:
        return list(self.connectors.values())


def build_registry_from_env() -> ConnectorRegistry:
    """
    Build the connector registry from environment variables.
    Always registers SQLite with the demo DB. Optionally registers
    other connectors based on env vars.
    """
    registry = ConnectorRegistry()

    # Always register SQLite
    db_url = os.getenv("DATABASE_URL", "sqlite:///demo.db")
    db_path = db_url.replace("sqlite:///", "") or "demo.db"
    registry.register(SQLiteConnector(db_path))

    # PostgreSQL
    pg_url = os.getenv("POSTGRES_URL")
    if pg_url:
        registry.register(PostgreSQLConnector(pg_url))

    # ClickHouse
    ch_url = os.getenv("CLICKHOUSE_URL")
    if ch_url:
        registry.register(ClickHouseConnector(ch_url))

    # Snowflake
    sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_user = os.getenv("SNOWFLAKE_USER")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    sf_database = os.getenv("SNOWFLAKE_DATABASE")
    sf_schema_name = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    if sf_account and sf_user and sf_password and sf_warehouse and sf_database:
        registry.register(SnowflakeConnector(
            sf_account, sf_user, sf_password, sf_warehouse, sf_database, sf_schema_name,
        ))

    # Databricks
    db_host = os.getenv("DATABRICKS_HOST")
    db_path = os.getenv("DATABRICKS_HTTP_PATH")
    db_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    db_catalog = os.getenv("DATABRICKS_CATALOG", "main")
    db_schema = os.getenv("DATABRICKS_SCHEMA", "default")
    if db_host and db_path and db_token:
        registry.register(DatabricksConnector(db_host, db_path, db_token, db_catalog, db_schema))

    # Redshift
    rs_host = os.getenv("REDSHIFT_HOST")
    rs_port = int(os.getenv("REDSHIFT_PORT", "5439"))
    rs_db = os.getenv("REDSHIFT_DATABASE")
    rs_user = os.getenv("REDSHIFT_USER")
    rs_pass = os.getenv("REDSHIFT_PASSWORD")
    if rs_host and rs_db and rs_user and rs_pass:
        registry.register(RedshiftConnector(rs_host, rs_port, rs_db, rs_user, rs_pass))

    # Google Analytics
    ga_property = os.getenv("GA_PROPERTY_ID")
    ga_creds = os.getenv("GA_CREDENTIALS_JSON")
    if ga_property and ga_creds:
        registry.register(GoogleAnalyticsConnector(ga_property, ga_creds))

    # Salesforce
    sf_url = os.getenv("SALESFORCE_INSTANCE_URL")
    sf_token = os.getenv("SALESFORCE_ACCESS_TOKEN")
    if sf_url and sf_token:
        registry.register(SalesforceConnector(sf_url, sf_token))

    # Marketo
    mk_endpoint = os.getenv("MARKETO_ENDPOINT")
    mk_id = os.getenv("MARKETO_CLIENT_ID")
    mk_secret = os.getenv("MARKETO_CLIENT_SECRET")
    if mk_endpoint and mk_id and mk_secret:
        registry.register(MarketoConnector(mk_endpoint, mk_id, mk_secret))

    # Google Docs
    gd_creds = os.getenv("GOOGLE_DOCS_CREDENTIALS_JSON")
    if gd_creds:
        registry.register(GoogleDocsConnector(gd_creds))

    # Slack
    slack_token = os.getenv("SLACK_BOT_TOKEN")
    if slack_token:
        try:
            from slack_connector import SlackConnector
            registry.register(SlackConnector(slack_token))
        except Exception as e:
            print(f"[Registry] Slack connector skipped: {e}")

    # Gmail (OAuth2)
    google_creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE")
    google_token_file = os.getenv("GOOGLE_TOKEN_FILE")
    if google_creds_file or (Path(__file__).parent / "token.json").exists():
        try:
            from gmail_connector import GmailConnector
            max_msgs = int(os.getenv("GMAIL_MAX_MESSAGES", "200"))
            registry.register(GmailConnector(google_creds_file, google_token_file, max_msgs))
        except Exception as e:
            print(f"[Registry] Gmail connector skipped: {e}")

    # Google Sheets (OAuth2)
    sheets_ids_str = os.getenv("GOOGLE_SHEETS_IDS", "")
    if sheets_ids_str.strip():
        try:
            from sheets_connector import GoogleSheetsConnector
            sheet_ids = [s.strip() for s in sheets_ids_str.split(",") if s.strip()]
            registry.register(GoogleSheetsConnector(sheet_ids, google_creds_file, google_token_file))
        except Exception as e:
            print(f"[Registry] Sheets connector skipped: {e}")

    # ── Lightweight Connectors ────────────────────────────────

    # CSV files
    csv_dir = os.getenv("CSV_DIRECTORY")
    if csv_dir:
        try:
            from csv_connector import CSVConnector
            registry.register(CSVConnector(csv_dir))
        except Exception as e:
            print(f"[Registry] CSV connector skipped: {e}")

    # JSON files
    json_dir = os.getenv("JSON_DIRECTORY")
    if json_dir:
        try:
            from json_connector import JSONConnector
            registry.register(JSONConnector(json_dir))
        except Exception as e:
            print(f"[Registry] JSON connector skipped: {e}")

    # Config/ENV files
    config_files = os.getenv("CONFIG_FILES")
    if config_files:
        try:
            from config_connector import EnvConfigConnector
            paths = [p.strip() for p in config_files.split(",") if p.strip()]
            registry.register(EnvConfigConnector(paths))
        except Exception as e:
            print(f"[Registry] Config connector skipped: {e}")

    # Notion
    notion_key = os.getenv("NOTION_API_KEY")
    notion_dbs = os.getenv("NOTION_DATABASE_IDS")
    if notion_key and notion_dbs:
        try:
            from notion_connector import NotionConnector
            db_ids = [d.strip() for d in notion_dbs.split(",") if d.strip()]
            registry.register(NotionConnector(notion_key, db_ids))
        except Exception as e:
            print(f"[Registry] Notion connector skipped: {e}")

    # Airtable
    at_pat = os.getenv("AIRTABLE_PAT")
    at_base = os.getenv("AIRTABLE_BASE_ID")
    at_tables = os.getenv("AIRTABLE_TABLE_IDS")
    if at_pat and at_base and at_tables:
        try:
            from airtable_connector import AirtableConnector
            tbl_ids = [t.strip() for t in at_tables.split(",") if t.strip()]
            registry.register(AirtableConnector(at_pat, at_base, tbl_ids))
        except Exception as e:
            print(f"[Registry] Airtable connector skipped: {e}")

    # GitHub Issues
    gh_pat = os.getenv("GITHUB_PAT")
    gh_repos = os.getenv("GITHUB_REPOS")
    if gh_pat and gh_repos:
        try:
            from github_connector import GitHubIssuesConnector
            repos = [r.strip() for r in gh_repos.split(",") if r.strip()]
            registry.register(GitHubIssuesConnector(gh_pat, repos))
        except Exception as e:
            print(f"[Registry] GitHub connector skipped: {e}")

    return registry


# Backward compat
def get_connector(database_url: str = "sqlite:///demo.db"):
    db_path = database_url.replace("sqlite:///", "") or "demo.db"
    return SQLiteConnector(db_path)
