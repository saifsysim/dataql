"""
Config Connector for DataQL.
Parses .env, .ini, and .yaml config files into a queryable key-value table.
"""

from __future__ import annotations
import sqlite3
from pathlib import Path

from data_connectors import APIConnector
from connector_helpers import flatten


class EnvConfigConnector(APIConnector):
    """Parse .env, .ini, and .yaml config files into queryable key-value tables."""

    connector_name = "Config Files"
    connector_icon = "⚙️"
    source_id = "config"
    _cache_ttl_seconds = 120

    def __init__(self, file_paths: list[str]):
        self.file_paths = [Path(p.strip()) for p in file_paths if p.strip()]

    def test_connection(self) -> tuple[bool, str]:
        existing = [p for p in self.file_paths if p.exists()]
        if not existing:
            return False, "No config files found at the specified paths"
        return True, f"{len(existing)} config file(s) accessible"

    def _sync_data(self, db: sqlite3.Connection):
        db.execute("""
            CREATE TABLE config_entries (
                file TEXT, section TEXT, key TEXT,
                value TEXT, line_number INTEGER
            )
        """)
        for path in self.file_paths:
            if not path.exists():
                continue
            try:
                suffix = path.suffix.lower()
                if suffix in (".env", ""):
                    self._parse_env(db, path)
                elif suffix in (".ini", ".cfg"):
                    self._parse_ini(db, path)
                elif suffix in (".yaml", ".yml"):
                    self._parse_yaml(db, path)
            except Exception as e:
                print(f"[Config] Error parsing {path}: {e}")
        db.commit()

    def _parse_env(self, db: sqlite3.Connection, path: Path):
        fname = path.name
        with open(path, "r") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    value = value.strip().strip("'\"")
                    db.execute(
                        "INSERT INTO config_entries VALUES (?,?,?,?,?)",
                        (fname, "default", key.strip(), value, lineno),
                    )

    def _parse_ini(self, db: sqlite3.Connection, path: Path):
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(path)
        fname = path.name
        for section in cfg.sections():
            for key, value in cfg.items(section):
                db.execute(
                    "INSERT INTO config_entries VALUES (?,?,?,?,?)",
                    (fname, section, key, value, 0),
                )

    def _parse_yaml(self, db: sqlite3.Connection, path: Path):
        try:
            import yaml
        except ImportError:
            return
        fname = path.name
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        if isinstance(data, dict):
            for key, value in flatten(data).items():
                db.execute(
                    "INSERT INTO config_entries VALUES (?,?,?,?,?)",
                    (fname, "root", key, str(value) if value is not None else None, 0),
                )
