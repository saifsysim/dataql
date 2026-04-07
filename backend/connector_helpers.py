"""
Shared helpers for lightweight DataQL connectors.
Type detection, sanitization, and flattening utilities.
"""

from __future__ import annotations
import json
import re
from typing import Any


def sanitize_name(name: str) -> str:
    """Make a valid SQL table name."""
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    clean = re.sub(r"_+", "_", clean).strip("_").lower()
    return clean[:63] or "table"


def sanitize_col(header: Any, index: int) -> str:
    """Make a valid SQL column name."""
    if not header or not str(header).strip():
        return f"column_{index}"
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", str(header))
    clean = re.sub(r"_+", "_", clean).strip("_").lower()
    if not clean or clean[0].isdigit():
        clean = f"col_{clean}"
    return clean[:63]


def detect_types(columns: list[str], rows: list[list]) -> list[str]:
    """Auto-detect SQL column types from sample rows."""
    types = ["TEXT"] * len(columns)
    for col_idx in range(len(columns)):
        int_count = float_count = total = 0
        for row in rows[:50]:
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


def cast(val: Any, target_type: str) -> Any:
    """Cast a value to the target SQL type."""
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


def flatten(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    """Flatten a nested dict one level deep."""
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)
