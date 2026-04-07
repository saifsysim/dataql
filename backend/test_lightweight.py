"""
Automated test for the lightweight connectors (CSV, JSON, Config).
These are fully local and need no API keys.
"""
import os
import sys
import json
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(__file__))

from csv_connector import CSVConnector
from json_connector import JSONConnector
from config_connector import EnvConfigConnector

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
errors = []


def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label} — {detail}")
        errors.append(label)


def test_csv():
    print("\n━━━ CSVConnector ━━━")
    tmp = tempfile.mkdtemp(prefix="dataql_csv_")
    try:
        with open(os.path.join(tmp, "sales.csv"), "w") as f:
            f.write("product,quantity,price\n")
            f.write("Widget,10,9.99\n")
            f.write("Gadget,5,24.50\n")
            f.write("Doohickey,100,1.25\n")

        c = CSVConnector(tmp)

        ok, msg = c.test_connection()
        check("test_connection", ok, msg)

        tables = c.get_tables()
        check("get_tables returns 'sales'", "sales" in tables, str(tables))

        info = c.get_table_info("sales")
        col_names = [col["name"] for col in info]
        check("columns: product, quantity, price", col_names == ["product", "quantity", "price"], str(col_names))

        result = c.execute_query("SELECT product, quantity FROM sales ORDER BY quantity DESC LIMIT 1")
        check("query returns top row", result["rows"][0]["product"] == "Doohickey", str(result["rows"]))
        check("row_count is 1", result["row_count"] == 1)

        types = {col["name"]: col["data_type"] for col in info}
        check("quantity is INTEGER", types["quantity"] == "INTEGER", types.get("quantity"))
        check("price is REAL", types["price"] == "REAL", types.get("price"))
    finally:
        shutil.rmtree(tmp)


def test_json():
    print("\n━━━ JSONConnector ━━━")
    tmp = tempfile.mkdtemp(prefix="dataql_json_")
    try:
        data = [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
            {"name": "Charlie", "age": 35, "city": "Chicago"},
        ]
        with open(os.path.join(tmp, "users.json"), "w") as f:
            json.dump(data, f)

        nested = {
            "results": [
                {"id": 1, "meta": {"score": 95, "grade": "A"}},
                {"id": 2, "meta": {"score": 82, "grade": "B"}},
            ]
        }
        with open(os.path.join(tmp, "scores.json"), "w") as f:
            json.dump(nested, f)

        c = JSONConnector(tmp)

        ok, msg = c.test_connection()
        check("test_connection", ok, msg)

        tables = c.get_tables()
        check("has 'users' table", "users" in tables, str(tables))
        check("has 'scores' table", "scores" in tables, str(tables))

        result = c.execute_query("SELECT name FROM users WHERE age > 28 ORDER BY name")
        names = [r["name"] for r in result["rows"]]
        check("filtered query correct", names == ["Alice", "Charlie"], str(names))

        result2 = c.execute_query("SELECT id, meta_score FROM scores ORDER BY id")
        check("nested flattening works", len(result2["rows"]) == 2, str(result2))
        check("meta_score value correct", str(result2["rows"][0]["meta_score"]) == "95",
              str(result2["rows"][0]))
    finally:
        shutil.rmtree(tmp)


def test_config():
    print("\n━━━ EnvConfigConnector ━━━")
    tmp = tempfile.mkdtemp(prefix="dataql_cfg_")
    try:
        env_path = os.path.join(tmp, "app.env")
        with open(env_path, "w") as f:
            f.write("# Database\nDB_HOST=localhost\nDB_PORT=5432\nAPP_NAME=DataQL\n")

        c = EnvConfigConnector([env_path])

        ok, msg = c.test_connection()
        check("test_connection", ok, msg)

        tables = c.get_tables()
        check("has config_entries table", "config_entries" in tables, str(tables))

        result = c.execute_query("SELECT key, value FROM config_entries WHERE key = 'DB_HOST'")
        check("reads DB_HOST", result["rows"][0]["value"] == "localhost", str(result["rows"]))

        result2 = c.execute_query("SELECT COUNT(*) as cnt FROM config_entries")
        check("3 entries total", result2["rows"][0]["cnt"] == 3, str(result2["rows"]))
    finally:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    print("=" * 50)
    print("  Lightweight Connectors — Automated Tests")
    print("=" * 50)

    test_csv()
    test_json()
    test_config()

    print()
    if errors:
        print(f"\033[91m✗ {len(errors)} test(s) failed:\033[0m")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("\033[92m✓ All tests passed!\033[0m")
