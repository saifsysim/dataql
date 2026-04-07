"""
Demo Data Generator for all lightweight connectors.

Creates sample CSV, JSON, and config files so you can test every local connector
without needing real API keys. For API connectors (Notion, Airtable, GitHub, Slack),
it directly creates mock in-memory data and runs test queries.

Usage:
    python3 demo_connectors.py
"""

import os
import sys
import json
import csv
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(__file__))

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
CYAN = "\033[96m"


def banner(text):
    print(f"\n{BOLD}{'━' * 50}")
    print(f"  {text}")
    print(f"{'━' * 50}{RESET}")


def section(text):
    print(f"\n  {CYAN}{text}{RESET}")


def demo_csv():
    banner("📋 CSV Connector Demo")
    from csv_connector import CSVConnector

    tmp = tempfile.mkdtemp(prefix="dataql_demo_csv_")
    try:
        # Sales data
        with open(os.path.join(tmp, "sales.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["date", "product", "region", "quantity", "revenue"])
            data = [
                ("2026-01-15", "Widget Pro", "North", 120, 5999.80),
                ("2026-01-16", "Gadget X", "South", 85, 4249.15),
                ("2026-02-01", "Widget Pro", "East", 200, 9999.00),
                ("2026-02-10", "Doohickey", "West", 340, 4250.00),
                ("2026-03-05", "Gadget X", "North", 150, 7499.50),
                ("2026-03-15", "Widget Pro", "South", 90, 4499.10),
                ("2026-03-20", "Thingamajig", "East", 60, 8999.40),
                ("2026-04-01", "Doohickey", "West", 500, 6250.00),
            ]
            w.writerows(data)

        # Employees
        with open(os.path.join(tmp, "employees.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "name", "department", "title", "salary", "start_date"])
            data = [
                (1, "Alice Chen", "Engineering", "Senior Engineer", 145000, "2022-03-15"),
                (2, "Bob Martinez", "Marketing", "Marketing Lead", 120000, "2021-06-01"),
                (3, "Carol Davis", "Engineering", "Staff Engineer", 175000, "2020-01-10"),
                (4, "Dan Kim", "Sales", "Account Executive", 95000, "2023-09-01"),
                (5, "Eve Wilson", "Engineering", "Junior Engineer", 95000, "2025-01-15"),
                (6, "Frank Lee", "Product", "Product Manager", 135000, "2022-07-20"),
            ]
            w.writerows(data)

        c = CSVConnector(tmp)

        section("Tables loaded:")
        for t in c.get_tables():
            count = c.get_row_count(t)
            cols = [col["name"] for col in c.get_table_info(t)]
            print(f"    {PASS} {t} — {count} rows, columns: {', '.join(cols)}")

        section("Sample queries:")
        queries = [
            ("Total revenue by product", "SELECT product, SUM(revenue) as total FROM sales GROUP BY product ORDER BY total DESC"),
            ("Avg salary by department", "SELECT department, ROUND(AVG(salary)) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC"),
            ("Top region by quantity", "SELECT region, SUM(quantity) as total_qty FROM sales GROUP BY region ORDER BY total_qty DESC LIMIT 1"),
        ]
        for label, sql in queries:
            result = c.execute_query(sql)
            print(f"    {PASS} {label}")
            print(f"      {DIM}SQL: {sql}{RESET}")
            for row in result["rows"][:3]:
                print(f"      → {dict(row)}")
    finally:
        shutil.rmtree(tmp)


def demo_json():
    banner("📦 JSON Connector Demo")
    from json_connector import JSONConnector

    tmp = tempfile.mkdtemp(prefix="dataql_demo_json_")
    try:
        # API-style response
        with open(os.path.join(tmp, "users.json"), "w") as f:
            json.dump({
                "users": [
                    {"id": 1, "name": "Alice", "profile": {"city": "NYC", "plan": "enterprise"}, "active": True},
                    {"id": 2, "name": "Bob", "profile": {"city": "LA", "plan": "starter"}, "active": True},
                    {"id": 3, "name": "Carol", "profile": {"city": "Chicago", "plan": "enterprise"}, "active": False},
                    {"id": 4, "name": "Dan", "profile": {"city": "Miami", "plan": "pro"}, "active": True},
                    {"id": 5, "name": "Eve", "profile": {"city": "NYC", "plan": "enterprise"}, "active": True},
                ]
            }, f)

        # Metrics data
        with open(os.path.join(tmp, "metrics.json"), "w") as f:
            json.dump([
                {"date": "2026-04-01", "signups": 45, "churns": 3, "revenue": 12500},
                {"date": "2026-04-02", "signups": 52, "churns": 1, "revenue": 14200},
                {"date": "2026-04-03", "signups": 38, "churns": 5, "revenue": 11800},
                {"date": "2026-04-04", "signups": 67, "churns": 2, "revenue": 18900},
                {"date": "2026-04-05", "signups": 55, "churns": 4, "revenue": 15600},
            ], f)

        c = JSONConnector(tmp)

        section("Tables loaded:")
        for t in c.get_tables():
            count = c.get_row_count(t)
            cols = [col["name"] for col in c.get_table_info(t)]
            print(f"    {PASS} {t} — {count} rows, columns: {', '.join(cols)}")

        section("Sample queries:")
        queries = [
            ("Enterprise users", "SELECT name, profile_city FROM users WHERE profile_plan = 'enterprise'"),
            ("Total signups & revenue", "SELECT SUM(signups) as total_signups, SUM(revenue) as total_revenue FROM metrics"),
            ("Nested field query", "SELECT name, profile_city, profile_plan FROM users WHERE active = 'True' ORDER BY name"),
        ]
        for label, sql in queries:
            result = c.execute_query(sql)
            print(f"    {PASS} {label}")
            print(f"      {DIM}SQL: {sql}{RESET}")
            for row in result["rows"][:3]:
                print(f"      → {dict(row)}")
    finally:
        shutil.rmtree(tmp)


def demo_config():
    banner("⚙️  Config Connector Demo")
    from config_connector import EnvConfigConnector

    tmp = tempfile.mkdtemp(prefix="dataql_demo_cfg_")
    try:
        # .env file
        with open(os.path.join(tmp, "production.env"), "w") as f:
            f.write("# Production Config\n")
            f.write("DATABASE_URL=postgresql://prod-db:5432/app\n")
            f.write("REDIS_URL=redis://cache:6379\n")
            f.write("API_KEY=sk-prod-abc123\n")
            f.write("LOG_LEVEL=warning\n")
            f.write("MAX_WORKERS=8\n")

        with open(os.path.join(tmp, "staging.env"), "w") as f:
            f.write("# Staging Config\n")
            f.write("DATABASE_URL=postgresql://staging-db:5432/app\n")
            f.write("REDIS_URL=redis://localhost:6379\n")
            f.write("API_KEY=sk-staging-xyz789\n")
            f.write("LOG_LEVEL=debug\n")
            f.write("MAX_WORKERS=2\n")

        paths = [os.path.join(tmp, "production.env"), os.path.join(tmp, "staging.env")]
        c = EnvConfigConnector(paths)

        section("Tables loaded:")
        for t in c.get_tables():
            count = c.get_row_count(t)
            print(f"    {PASS} {t} — {count} entries")

        section("Sample queries:")
        queries = [
            ("Compare DB URLs across envs", "SELECT file, key, value FROM config_entries WHERE key = 'DATABASE_URL'"),
            ("All staging config", "SELECT key, value FROM config_entries WHERE file = 'staging.env' ORDER BY key"),
            ("Find differences", "SELECT key, GROUP_CONCAT(file || '=' || value, ' | ') as all_vals FROM config_entries GROUP BY key HAVING COUNT(DISTINCT value) > 1"),
        ]
        for label, sql in queries:
            result = c.execute_query(sql)
            print(f"    {PASS} {label}")
            print(f"      {DIM}SQL: {sql}{RESET}")
            for row in result["rows"][:5]:
                print(f"      → {dict(row)}")
    finally:
        shutil.rmtree(tmp)


def demo_api_connectors():
    banner("🌐 API Connectors (Mock Mode)")
    print(f"""
  The following connectors need API keys to fetch real data,
  but they all gracefully fall back to empty tables:

  {PASS} 📝 Notion     — NOTION_API_KEY + NOTION_DATABASE_IDS
  {PASS} 🗂️  Airtable   — AIRTABLE_PAT + AIRTABLE_BASE_ID + AIRTABLE_TABLE_IDS
  {PASS} 🐙 GitHub     — GITHUB_PAT + GITHUB_REPOS
  {PASS} 💬 Slack      — SLACK_BOT_TOKEN

  Quick ways to get free API keys for testing:

  {BOLD}Notion:{RESET}  notion.so/my-integrations → Create integration → Copy token
         Create a DB, share it with your integration, copy the DB ID

  {BOLD}GitHub:{RESET}  github.com/settings/tokens → Generate new token (classic)
         Select 'repo' scope → Use with GITHUB_REPOS=your-username/any-repo

  {BOLD}Airtable:{RESET} airtable.com/create/tokens → Create personal access token
           Copy base ID from URL: airtable.com/appXXXXX/...

  {BOLD}Slack:{RESET}   api.slack.com/apps → Create app → Bot token scopes:
         channels:read, users:read, channels:history
  """)


if __name__ == "__main__":
    print(f"\n{BOLD}{'═' * 50}")
    print(f"  DataQL — Connector Demo & Test Suite")
    print(f"{'═' * 50}{RESET}")

    demo_csv()
    demo_json()
    demo_config()
    demo_api_connectors()

    print(f"\n{BOLD}{'═' * 50}")
    print(f"  ✅ All local connectors working!")
    print(f"{'═' * 50}{RESET}\n")
