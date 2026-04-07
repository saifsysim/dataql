"""
GitHub Issues Connector for DataQL.
Queries GitHub issues & pull requests for specified repos (last 90 days, max 500).
"""

from __future__ import annotations
import sqlite3
from typing import Any, Optional

from data_connectors import APIConnector


class GitHubIssuesConnector(APIConnector):
    """Query GitHub issues & pull requests for specified repos."""

    connector_name = "GitHub Issues"
    connector_icon = "🐙"
    source_id = "github_issues"
    _cache_ttl_seconds = 300

    API = "https://api.github.com"

    def __init__(self, pat: str, repos: list[str]):
        self.pat = pat
        self.repos = [r.strip() for r in repos if r.strip()]

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.pat}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            import httpx
            resp = httpx.get(
                f"{self.API}/user", headers=self._headers(), timeout=10,
            )
            if resp.status_code == 200:
                login = resp.json().get("login", "unknown")
                return True, f"Connected to GitHub as {login} ({len(self.repos)} repo(s))"
            return False, f"HTTP {resp.status_code}"
        except ImportError:
            return False, "Install: pip install httpx"
        except Exception as e:
            return False, str(e)

    def _sync_data(self, db: sqlite3.Connection):
        try:
            import httpx
            from datetime import datetime, timedelta

            db.execute("""
                CREATE TABLE gh_issues (
                    id INTEGER, repo TEXT, number INTEGER, title TEXT,
                    state TEXT, author TEXT, labels TEXT,
                    created_at TEXT, updated_at TEXT, closed_at TEXT,
                    comments INTEGER, body_preview TEXT
                )
            """)
            db.execute("""
                CREATE TABLE gh_pull_requests (
                    id INTEGER, repo TEXT, number INTEGER, title TEXT,
                    state TEXT, author TEXT, labels TEXT,
                    created_at TEXT, updated_at TEXT, merged_at TEXT,
                    comments INTEGER, draft INTEGER, body_preview TEXT
                )
            """)

            since = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")

            for repo in self.repos:
                try:
                    self._load_repo_issues(httpx, db, repo, since)
                except Exception as e:
                    print(f"[GitHub] Error loading {repo}: {e}")

            db.commit()
        except ImportError:
            pass

    def _load_repo_issues(self, httpx_mod, db: sqlite3.Connection, repo: str, since: str):
        page = 1
        total = 0
        while total < 500:
            resp = httpx_mod.get(
                f"{self.API}/repos/{repo}/issues",
                headers=self._headers(),
                params={
                    "state": "all",
                    "since": since,
                    "per_page": 100,
                    "page": page,
                    "sort": "updated",
                    "direction": "desc",
                },
                timeout=20,
            )
            if resp.status_code != 200:
                break
            items = resp.json()
            if not items:
                break

            for item in items:
                labels = ", ".join(l.get("name", "") for l in item.get("labels", []))
                author = (item.get("user") or {}).get("login", "")
                body_preview = (item.get("body") or "")[:300]

                if item.get("pull_request"):
                    db.execute(
                        "INSERT INTO gh_pull_requests VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            item["id"], repo, item["number"], item["title"],
                            item["state"], author, labels,
                            item.get("created_at"), item.get("updated_at"),
                            item.get("pull_request", {}).get("merged_at"),
                            item.get("comments", 0),
                            1 if item.get("draft") else 0,
                            body_preview,
                        ),
                    )
                else:
                    db.execute(
                        "INSERT INTO gh_issues VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            item["id"], repo, item["number"], item["title"],
                            item["state"], author, labels,
                            item.get("created_at"), item.get("updated_at"),
                            item.get("closed_at"),
                            item.get("comments", 0),
                            body_preview,
                        ),
                    )
                total += 1
                if total >= 500:
                    break

            page += 1
