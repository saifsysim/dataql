"""
Comprehensive demo data seeder for ALL DataQL connectors.

Seeds realistic data so every connector is testable without real API keys or DB connections:
  - SQLite demo.db: e-commerce + Jira + GitHub + Slack + Notion + Airtable + Postgres/Snowflake/ClickHouse-style tables
  - CSV files: CRM contacts, revenue, support tickets, web analytics
  - JSON files: product events, team directory, feature flags, deployments
  - Config files: production/staging/development environments

Usage:
    python3 seed_demo_data.py
"""

import csv
import json
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

BASE = Path(__file__).parent
DATA_DIR = BASE / "demo_data"
DB_PATH = BASE / "demo.db"

# ─── Shared Data Pools ───────────────────────────────────────

FIRST_NAMES = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia",
               "Mason", "Isabella", "Lucas", "Mia", "Logan", "Charlotte", "Aiden",
               "Amelia", "Jackson", "Harper", "Sebastian", "Evelyn", "Caleb", "Luna",
               "Owen", "Ella", "Daniel", "Chloe", "Henry", "Penelope", "Alexander", "Layla"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson", "Thomas", "Taylor",
              "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris",
              "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen"]

COMPANIES = ["Acme Corp", "TechFlow Inc", "DataVerse", "CloudNine Systems", "BluePeak Analytics",
             "Nexus Digital", "PrimePath", "QuantumLeap AI", "RedShift Labs", "Stellar.io",
             "Catalyst Group", "FusionWorks", "Ironclad Security", "MintBridge", "Apex Solutions"]

CITIES = ["New York", "San Francisco", "Austin", "Chicago", "Seattle", "Denver",
          "Los Angeles", "Boston", "Miami", "Portland", "Nashville", "Atlanta"]

DEPARTMENTS = ["Engineering", "Sales", "Marketing", "Product", "Design", "Support", "Finance", "HR"]

CHANNELS = ["#general", "#engineering", "#sales", "#product", "#random",
            "#support", "#marketing", "#design", "#announcements", "#watercooler"]


def rdate(start_days_ago=365, end_days_ago=0):
    delta = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=delta)).strftime("%Y-%m-%d")

def rdatetime(start_days_ago=90, end_days_ago=0):
    delta_days = random.randint(end_days_ago, start_days_ago)
    dt = datetime.now() - timedelta(days=delta_days, hours=random.randint(0, 23), minutes=random.randint(0, 59))
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def rname():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def remail(name):
    return f"{name.lower().replace(' ', '.')}@company.com"


BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
GREEN = "\033[92m"
CYAN = "\033[96m"
CHECK = f"{GREEN}✓{RESET}"


# ══════════════════════════════════════════════════════════════
#  SQLITE DEMO.DB — All connector tables
# ══════════════════════════════════════════════════════════════

def seed_sqlite():
    print(f"\n  {CYAN}── SQLite demo.db ──{RESET}\n")
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # ── Jira Tables ───────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS jira_issues")
    cur.execute("DROP TABLE IF EXISTS jira_sprints")
    cur.execute("DROP TABLE IF EXISTS jira_users")

    cur.execute("""
        CREATE TABLE jira_issues (
            id INTEGER PRIMARY KEY, issue_key TEXT, project TEXT,
            summary TEXT, description TEXT, issue_type TEXT,
            status TEXT, priority TEXT, assignee TEXT, reporter TEXT,
            sprint TEXT, story_points INTEGER, labels TEXT,
            created_at TEXT, updated_at TEXT, resolved_at TEXT,
            components TEXT, fix_version TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE jira_sprints (
            id INTEGER PRIMARY KEY, name TEXT, state TEXT,
            start_date TEXT, end_date TEXT, goal TEXT,
            total_points INTEGER, completed_points INTEGER
        )
    """)

    projects = ["DATAQL", "INFRA", "MOBILE", "API"]
    types = ["Bug", "Story", "Task", "Epic", "Sub-task"]
    statuses = ["To Do", "In Progress", "In Review", "QA", "Done", "Blocked"]
    priorities = ["Highest", "High", "Medium", "Low", "Lowest"]
    components = ["Backend", "Frontend", "API", "Auth", "Billing", "Search", "Analytics", "Infrastructure"]
    sprint_names = [f"Sprint {i}" for i in range(20, 27)]
    fix_versions = ["v2.14", "v2.15", "v2.16", "v3.0-beta", "Backlog"]

    jira_summaries = [
        "Implement OAuth2 PKCE flow for mobile", "Fix N+1 query in dashboard endpoint",
        "Add rate limiting to public API", "Migrate user sessions to Redis",
        "Implement real-time notifications via WebSocket", "Add Stripe webhook signature verification",
        "Refactor search indexer for Elasticsearch 8.x", "Build admin audit log viewer",
        "Fix timezone bug in scheduled reports", "Add CSV export for analytics",
        "Implement RBAC permission system", "Optimize Docker image size (reduce by 60%)",
        "Add health check endpoints for k8s", "Implement graceful shutdown handling",
        "Build feature flag management UI", "Add OpenTelemetry distributed tracing",
        "Fix memory leak in worker process pool", "Implement database connection pooling",
        "Add end-to-end encryption for file uploads", "Build CI/CD pipeline for staging",
        "Implement API versioning strategy", "Add Prometheus metrics endpoint",
        "Fix CORS configuration for subdomain access", "Build automated backup system",
        "Implement multi-tenant data isolation", "Add GraphQL subscriptions support",
        "Fix race condition in concurrent writes", "Build data migration toolkit",
        "Implement SSO with SAML 2.0", "Add support for custom webhooks",
        "Optimize PostgreSQL query plans", "Build automated changelog generator",
        "Implement circuit breaker for external APIs", "Add request/response logging middleware",
        "Fix file upload size limit handling", "Build A/B testing framework",
        "Implement soft delete with data retention", "Add bulk import API endpoint",
        "Fix email template rendering engine", "Build team capacity planning dashboard",
    ]

    assignees = [rname() for _ in range(10)]
    for i, summary in enumerate(jira_summaries):
        proj = random.choice(projects)
        sprint = random.choice(sprint_names + ["Backlog"])
        status = random.choice(statuses)
        resolved = rdate(14, 0) if status == "Done" else None
        cur.execute(
            "INSERT INTO jira_issues VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (i + 1, f"{proj}-{100+i}", proj, summary,
             f"Detailed description for: {summary}",
             random.choice(types), status, random.choice(priorities),
             random.choice(assignees), random.choice(assignees),
             sprint, random.choice([1, 2, 3, 5, 8, 13]),
             ", ".join(random.sample(["backend", "frontend", "security", "performance", "ux", "devops"], random.randint(1, 3))),
             rdate(120, 5), rdate(5, 0), resolved,
             ", ".join(random.sample(components, random.randint(1, 2))),
             random.choice(fix_versions)),
        )

    for i, name in enumerate(sprint_names):
        total = random.randint(20, 50)
        completed = random.randint(int(total * 0.5), total) if i < len(sprint_names) - 1 else random.randint(0, int(total * 0.4))
        state = "closed" if i < len(sprint_names) - 2 else ("active" if i == len(sprint_names) - 2 else "future")
        start = datetime.now() - timedelta(days=(len(sprint_names) - i) * 14)
        end = start + timedelta(days=14)
        cur.execute(
            "INSERT INTO jira_sprints VALUES (?,?,?,?,?,?,?,?)",
            (i + 1, name, state, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"),
             f"Deliver {random.choice(['performance', 'security', 'feature', 'reliability'])} improvements",
             total, completed),
        )
    print(f"  {CHECK} jira_issues — 40 issues across 4 projects")
    print(f"  {CHECK} jira_sprints — 7 sprints")

    # ── GitHub Tables ─────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS gh_issues")
    cur.execute("DROP TABLE IF EXISTS gh_pull_requests")
    cur.execute("DROP TABLE IF EXISTS gh_commits")

    cur.execute("""
        CREATE TABLE gh_issues (
            id INTEGER, repo TEXT, number INTEGER, title TEXT,
            state TEXT, author TEXT, labels TEXT,
            created_at TEXT, updated_at TEXT, closed_at TEXT,
            comments INTEGER, body_preview TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE gh_pull_requests (
            id INTEGER, repo TEXT, number INTEGER, title TEXT,
            state TEXT, author TEXT, labels TEXT,
            created_at TEXT, updated_at TEXT, merged_at TEXT,
            comments INTEGER, draft INTEGER, body_preview TEXT,
            additions INTEGER, deletions INTEGER, changed_files INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE gh_commits (
            sha TEXT, repo TEXT, author TEXT, message TEXT,
            date TEXT, additions INTEGER, deletions INTEGER, files_changed INTEGER
        )
    """)

    gh_authors = ["alice-dev", "bob-eng", "carol-ops", "dan-frontend", "eve-backend",
                   "frank-pm", "grace-qa", "henry-data", "iris-devops", "jack-sec"]
    gh_labels = ["bug", "enhancement", "documentation", "performance", "security",
                 "good first issue", "help wanted", "priority:high", "breaking-change"]
    repos = ["dataql/backend", "dataql/frontend", "dataql/sdk-python", "dataql/infra"]

    gh_issue_titles = [
        "Race condition in concurrent query execution", "Add retry logic for transient API failures",
        "Memory usage spikes during large CSV imports", "Implement query result caching layer",
        "Fix SQL injection vulnerability in raw query mode", "Add support for window functions",
        "Dashboard loading time exceeds 3s on cold start", "WebSocket connection drops after 30min",
        "Incorrect aggregation with NULL values", "Add CORS headers for cross-origin API calls",
        "TypeScript types out of sync with API spec", "Fix: secrets exposed in error logs",
        "Implement query cost estimation", "Add pagination cursor support",
        "Background job queue memory leak", "Support for nested JSON column queries",
    ]
    for i, title in enumerate(gh_issue_titles):
        repo = random.choice(repos)
        state = random.choice(["open", "closed", "closed", "closed"])
        cur.execute(
            "INSERT INTO gh_issues VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (2000 + i, repo, i + 1, title, state, random.choice(gh_authors),
             ", ".join(random.sample(gh_labels, random.randint(1, 3))),
             rdatetime(90, 5), rdatetime(5, 0),
             rdatetime(3, 0) if state == "closed" else None,
             random.randint(0, 20), f"Details: {title}"),
        )

    pr_titles = [
        "feat: add query result caching with Redis", "fix: resolve memory leak in worker pool",
        "refactor: extract connector base class", "feat: implement real-time query streaming",
        "fix: correct timezone handling in date functions", "chore: upgrade httpx to v0.27",
        "feat: add Snowflake connector support", "fix: prevent SQL injection in dynamic queries",
        "feat: implement query plan visualization", "test: add integration tests for connectors",
        "docs: update API reference for v2.15", "feat: add export to Parquet format",
        "fix: handle NULL in aggregate functions", "feat: implement query suggestions",
        "perf: optimize JOIN query planning", "ci: add automated security scanning",
        "feat: add webhook notifications for query completion", "fix: resolve deadlock in connection pool",
        "feat: implement multi-tenant query isolation", "refactor: migrate to async/await pattern",
    ]
    for i, title in enumerate(pr_titles):
        repo = random.choice(repos)
        state = random.choice(["open", "closed", "merged", "merged", "merged"])
        merged = rdatetime(5, 0) if state == "merged" else None
        cur.execute(
            "INSERT INTO gh_pull_requests VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (3000 + i, repo, 100 + i, title, state, random.choice(gh_authors),
             ", ".join(random.sample(gh_labels[:5], random.randint(1, 2))),
             rdatetime(60, 3), rdatetime(3, 0), merged,
             random.randint(1, 15), 1 if random.random() < 0.15 else 0,
             f"PR description: {title}",
             random.randint(5, 500), random.randint(2, 200), random.randint(1, 25)),
        )

    commit_msgs = [
        "fix: resolve null pointer in query parser", "feat: add support for HAVING clause",
        "chore: update dependencies", "test: add connector integration tests",
        "fix: correct date parsing for ISO8601", "refactor: simplify error handling",
        "feat: implement query history", "perf: add index for lookup queries",
        "docs: update connector setup guide", "fix: handle empty result sets gracefully",
        "feat: add query cost estimation", "ci: fix flaky test in pipeline",
        "feat: implement natural language search", "fix: memory leak in long-running queries",
        "chore: bump version to 2.15.1", "feat: add JSON export format",
    ]
    for i in range(50):
        cur.execute(
            "INSERT INTO gh_commits VALUES (?,?,?,?,?,?,?,?)",
            (f"{random.randint(1000000, 9999999):07x}", random.choice(repos),
             random.choice(gh_authors), random.choice(commit_msgs),
             rdatetime(60, 0), random.randint(1, 300),
             random.randint(0, 150), random.randint(1, 15)),
        )
    print(f"  {CHECK} gh_issues — 16 issues across 4 repos")
    print(f"  {CHECK} gh_pull_requests — 20 PRs with additions/deletions")
    print(f"  {CHECK} gh_commits — 50 commits")

    # ── Slack Tables ──────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS slack_users")
    cur.execute("DROP TABLE IF EXISTS slack_channels")
    cur.execute("DROP TABLE IF EXISTS slack_messages")

    cur.execute("""
        CREATE TABLE slack_users (
            id TEXT PRIMARY KEY, name TEXT, real_name TEXT,
            display_name TEXT, email TEXT, is_admin INTEGER,
            is_bot INTEGER, status_text TEXT, timezone TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE slack_channels (
            id TEXT PRIMARY KEY, name TEXT, topic TEXT,
            purpose TEXT, num_members INTEGER,
            is_private INTEGER, created_date TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE slack_messages (
            channel_id TEXT, channel_name TEXT, user_id TEXT,
            text TEXT, timestamp TEXT, reply_count INTEGER,
            reaction_count INTEGER
        )
    """)

    slack_users = []
    for i in range(20):
        fn = FIRST_NAMES[i]
        ln = LAST_NAMES[i]
        uid = f"U{i+1:04d}"
        name = f"{fn.lower()}.{ln.lower()}"
        slack_users.append(uid)
        cur.execute(
            "INSERT INTO slack_users VALUES (?,?,?,?,?,?,?,?,?)",
            (uid, name, f"{fn} {ln}", fn, f"{name}@company.com",
             1 if i < 3 else 0, 0,
             random.choice(["", "In meetings", "Heads down", "On PTO", "Working remotely"]),
             random.choice(["US/Eastern", "US/Pacific", "US/Central", "Europe/London"])),
        )

    channel_topics = {
        "#general": "Company-wide updates and announcements",
        "#engineering": "Engineering discussions, architecture decisions, and code reviews",
        "#sales": "Pipeline updates, deal discussions, and competitive intel",
        "#product": "Feature specs, roadmap planning, and user feedback",
        "#random": "Water cooler chat, memes, and fun stuff",
        "#support": "Customer escalations and support queue management",
        "#marketing": "Campaign launches, content calendar, and brand updates",
        "#design": "Design reviews, UI/UX feedback, and assets",
        "#announcements": "Official company announcements only",
        "#watercooler": "Casual conversations, hobbies, and interests",
    }
    for i, (ch, topic) in enumerate(channel_topics.items()):
        cid = f"C{i+1:04d}"
        cur.execute(
            "INSERT INTO slack_channels VALUES (?,?,?,?,?,?,?)",
            (cid, ch.replace("#", ""), topic, f"Purpose: {topic}",
             random.randint(5, 20), 1 if ch == "#support" else 0, rdate(800, 100)),
        )

    slack_messages_text = [
        "Just deployed v2.15 to production 🚀 All metrics looking good",
        "Anyone seeing elevated error rates on the /api/query endpoint?",
        "PR #342 ready for review — implements the new caching layer",
        "Standup in 5 minutes, join the huddle",
        "Great work on the Q1 metrics everyone! Revenue up 23% QoQ",
        "New enterprise customer signed: Acme Corp — $120k ARR deal 🎉",
        "CI is green across all branches, merging the feature flag PR now",
        "Security audit findings reviewed — 2 high, 5 medium, 12 low items",
        "Updated the API docs for the v3 endpoints, please review",
        "Found a regression in the billing calculation — hotfix incoming",
        "Weekly analytics: 15k DAU, up from 12k last week. Nice growth!",
        "Onboarding two new engineers next Monday — please update the wiki",
        "Database migration completed successfully, zero downtime 💪",
        "Customer feedback: 'The new dashboard is incredibly fast!'",
        "Reminder: company all-hands Thursday at 3pm EST",
        "Investigating slow queries on the reports page — looks like missing index",
        "Feature flag for AI copilot now at 50% rollout, monitoring metrics",
        "Happy Friday team! Weekend plans anyone? 🎉",
        "Server costs came in 15% under budget this month — great optimization work",
        "Hotfix for the login SSO issue is live. Root cause was cert expiry",
        "Just finished the Snowflake connector — 3x faster than the old approach",
        "Incident resolved: API latency spike was due to upstream DNS issue",
        "Demo to Apex Solutions went great — they want to start a POC next week",
        "New blog post draft: 'Building Data Connectors at Scale' — feedback welcome",
        "Sprint review: completed 42 of 48 story points this sprint",
    ]
    for i in range(80):
        ch_idx = random.randint(0, len(CHANNELS) - 1)
        cur.execute(
            "INSERT INTO slack_messages VALUES (?,?,?,?,?,?,?)",
            (f"C{ch_idx+1:04d}", CHANNELS[ch_idx].replace("#", ""),
             random.choice(slack_users), random.choice(slack_messages_text),
             rdatetime(7, 0), random.randint(0, 12), random.randint(0, 15)),
        )
    print(f"  {CHECK} slack_users — 20 team members")
    print(f"  {CHECK} slack_channels — 10 channels")
    print(f"  {CHECK} slack_messages — 80 messages (last 7 days)")

    # ── Notion Tables ─────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS notion_projects")
    cur.execute("DROP TABLE IF EXISTS notion_tasks")
    cur.execute("DROP TABLE IF EXISTS notion_wiki")

    cur.execute("""
        CREATE TABLE notion_projects (
            id TEXT, project_name TEXT, owner TEXT,
            status TEXT, priority TEXT, sprint TEXT,
            start_date TEXT, due_date TEXT, completion_pct INTEGER,
            tags TEXT, department TEXT, budget INTEGER
        )
    """)
    project_names = [
        "DataQL v2.0 Launch", "Mobile App Redesign", "API Gateway Migration",
        "SOC 2 Type II Compliance", "AI Copilot Beta Release", "Customer Portal Rebuild",
        "Infrastructure Cost Optimization", "Design System v3",
        "Onboarding Flow Revamp", "Search Performance Overhaul",
        "Multi-Region Deployment", "Analytics Dashboard v2",
        "Self-Serve Billing Portal", "Developer SDK (Python/JS)",
        "Zero-Downtime Migration Framework",
    ]
    for i, name in enumerate(project_names):
        cur.execute(
            "INSERT INTO notion_projects VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"proj-{i+1:03d}", name, rname(),
             random.choice(["Not Started", "In Progress", "In Review", "Done", "Blocked"]),
             random.choice(["P0", "P1", "P2", "P3"]),
             random.choice(["Sprint 22", "Sprint 23", "Sprint 24", "Sprint 25", "Backlog"]),
             rdate(90, 30), rdate(30, 0), random.randint(0, 100),
             ", ".join(random.sample(["backend", "frontend", "infra", "design", "security", "data"], random.randint(1, 3))),
             random.choice(DEPARTMENTS), random.randint(5000, 200000)),
        )

    cur.execute("""
        CREATE TABLE notion_wiki (
            id TEXT, title TEXT, author TEXT, category TEXT,
            last_edited TEXT, word_count INTEGER, views INTEGER, status TEXT
        )
    """)
    wiki_pages = [
        "Architecture Decision Records", "API Design Guidelines",
        "Incident Response Playbook", "Code Review Standards",
        "Data Model Documentation", "Security Best Practices",
        "Deployment Runbook", "Onboarding Checklist",
        "Feature Flag Policy", "Performance Budget Guidelines",
    ]
    for i, title in enumerate(wiki_pages):
        cur.execute(
            "INSERT INTO notion_wiki VALUES (?,?,?,?,?,?,?,?)",
            (f"wiki-{i+1:03d}", title, rname(),
             random.choice(["Engineering", "Product", "Security", "Operations"]),
             rdate(30, 0), random.randint(200, 5000),
             random.randint(10, 500), random.choice(["Published", "Draft", "Under Review"])),
        )
    print(f"  {CHECK} notion_projects — 15 projects")
    print(f"  {CHECK} notion_wiki — 10 wiki pages")

    # ── Airtable Tables ───────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS at_content_calendar")
    cur.execute("DROP TABLE IF EXISTS at_product_roadmap")

    cur.execute("""
        CREATE TABLE at_content_calendar (
            id TEXT, title TEXT, content_type TEXT, status TEXT,
            author TEXT, publish_date TEXT, channel TEXT,
            views INTEGER, leads_generated INTEGER, campaign TEXT
        )
    """)
    content_types = ["Blog Post", "Case Study", "Webinar", "Newsletter", "Social Post",
                     "Video", "Podcast Episode", "Whitepaper", "Tutorial"]
    channels = ["blog", "youtube", "linkedin", "twitter", "email", "medium"]
    campaigns = ["Q1 Product Launch", "Developer Conference", "Case Study Series",
                 "Thought Leadership", "SEO Growth", "Customer Stories"]
    content_titles = [
        "How to Build Data Pipelines at Scale", "Why AI Safety Matters in Production",
        "The Future of Natural Language Querying", "Building Connectors: A Deep Dive",
        "Understanding Data Mesh Architecture", "From SQL to Natural Language",
        "Enterprise Data Security Best Practices", "Real-time Analytics with DataQL",
        "10x Your Data Team Productivity", "The Cost of Bad Data Quality",
        "Migrating from Legacy BI Tools", "DataQL vs Traditional ETL",
        "Building a Data Culture", "API-First Data Architecture",
        "Zero-Copy Data Sharing Patterns", "Lakehouse Architecture Explained",
    ]
    for i, title in enumerate(content_titles):
        cur.execute(
            "INSERT INTO at_content_calendar VALUES (?,?,?,?,?,?,?,?,?,?)",
            (f"rec{random.randint(100000, 999999)}", title,
             random.choice(content_types),
             random.choice(["Draft", "In Review", "Approved", "Published", "Archived"]),
             rname(), rdate(90, 0), random.choice(channels),
             random.randint(0, 25000), random.randint(0, 75),
             random.choice(campaigns)),
        )

    cur.execute("""
        CREATE TABLE at_product_roadmap (
            id TEXT, feature_name TEXT, description TEXT, quarter TEXT,
            status TEXT, priority TEXT, team TEXT, estimated_effort TEXT,
            customer_requests INTEGER, revenue_impact TEXT
        )
    """)
    roadmap_items = [
        ("AI Query Copilot", "Natural language query suggestions and auto-completion"),
        ("Multi-Cloud Connectors", "Support for AWS, GCP, and Azure native data services"),
        ("Real-Time Streaming", "Live data streaming with sub-second latency"),
        ("Advanced Visualizations", "Built-in charts, graphs, and dashboards"),
        ("Team Workspaces", "Collaborative query editing with shared contexts"),
        ("Audit & Compliance", "SOC2/GDPR compliant audit logging"),
        ("Custom Notebooks", "Jupyter-style notebooks with SQL + Python"),
        ("Data Catalog", "Automated schema discovery and documentation"),
        ("Query Marketplace", "Share and discover community queries"),
        ("Mobile App", "iOS/Android app for querying on the go"),
    ]
    for i, (name, desc) in enumerate(roadmap_items):
        cur.execute(
            "INSERT INTO at_product_roadmap VALUES (?,?,?,?,?,?,?,?,?,?)",
            (f"feat-{i+1:03d}", name, desc,
             random.choice(["Q1 2026", "Q2 2026", "Q3 2026", "Q4 2026"]),
             random.choice(["Planned", "In Development", "Beta", "GA", "Deferred"]),
             random.choice(["P0", "P1", "P2"]),
             random.choice(["Core Platform", "Connectors", "Frontend", "ML/AI", "Infrastructure"]),
             random.choice(["S", "M", "L", "XL"]),
             random.randint(0, 150),
             random.choice(["High", "Medium", "Low"])),
        )
    print(f"  {CHECK} at_content_calendar — 16 content items")
    print(f"  {CHECK} at_product_roadmap — 10 roadmap features")

    # ── PostgreSQL-style Tables ───────────────────────────────
    cur.execute("DROP TABLE IF EXISTS pg_user_events")
    cur.execute("DROP TABLE IF EXISTS pg_subscriptions")
    cur.execute("DROP TABLE IF EXISTS pg_invoices")

    cur.execute("""
        CREATE TABLE pg_user_events (
            id INTEGER PRIMARY KEY, user_id INTEGER, event_type TEXT,
            event_data TEXT, ip_address TEXT, user_agent TEXT,
            session_id TEXT, created_at TEXT
        )
    """)
    event_types = ["login", "logout", "page_view", "query_run", "export",
                   "settings_change", "password_reset", "api_call", "signup", "upgrade"]
    for i in range(200):
        cur.execute(
            "INSERT INTO pg_user_events VALUES (?,?,?,?,?,?,?,?)",
            (i + 1, random.randint(1, 50), random.choice(event_types),
             json.dumps({"page": random.choice(["/dashboard", "/queries", "/settings", "/connectors", "/billing"])}),
             f"192.168.{random.randint(1, 10)}.{random.randint(1, 254)}",
             random.choice(["Chrome/120", "Firefox/121", "Safari/17", "Edge/120"]),
             f"sess_{random.randint(10000, 99999)}", rdatetime(30, 0)),
        )

    cur.execute("""
        CREATE TABLE pg_subscriptions (
            id INTEGER PRIMARY KEY, customer_id INTEGER, plan TEXT,
            status TEXT, mrr REAL, start_date TEXT, trial_end TEXT,
            billing_cycle TEXT, seats INTEGER, usage_pct REAL
        )
    """)
    plans = [("Starter", 29), ("Professional", 79), ("Business", 149), ("Enterprise", 349), ("Platform", 799)]
    for i in range(60):
        plan_name, base_price = random.choice(plans)
        seats = random.randint(1, 50)
        cur.execute(
            "INSERT INTO pg_subscriptions VALUES (?,?,?,?,?,?,?,?,?,?)",
            (i + 1, random.randint(1, 80), plan_name,
             random.choice(["active", "active", "active", "trialing", "past_due", "canceled"]),
             base_price * seats, rdate(365, 0),
             rdate(14, 0) if random.random() < 0.2 else None,
             random.choice(["monthly", "annual"]),
             seats, round(random.uniform(5, 95), 1)),
        )

    cur.execute("""
        CREATE TABLE pg_invoices (
            id INTEGER PRIMARY KEY, customer_id INTEGER, invoice_number TEXT,
            amount REAL, currency TEXT, status TEXT,
            issued_date TEXT, due_date TEXT, paid_date TEXT, line_items TEXT
        )
    """)
    for i in range(100):
        status = random.choice(["paid", "paid", "paid", "pending", "overdue", "void"])
        issued = rdate(180, 5)
        cur.execute(
            "INSERT INTO pg_invoices VALUES (?,?,?,?,?,?,?,?,?,?)",
            (i + 1, random.randint(1, 80), f"INV-{2024000 + i}",
             round(random.uniform(29, 15000), 2), "USD", status,
             issued, rdate(30, 0),
             rdate(5, 0) if status == "paid" else None,
             json.dumps([{"item": random.choice(["Platform License", "API Calls", "Storage", "Support"]),
                         "amount": round(random.uniform(29, 5000), 2)}])),
        )
    print(f"  {CHECK} pg_user_events — 200 user activity events")
    print(f"  {CHECK} pg_subscriptions — 60 subscriptions")
    print(f"  {CHECK} pg_invoices — 100 invoices")

    # ── Snowflake-style Tables ────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS sf_query_history")
    cur.execute("DROP TABLE IF EXISTS sf_warehouse_usage")

    cur.execute("""
        CREATE TABLE sf_query_history (
            query_id TEXT, warehouse TEXT, database_name TEXT, schema_name TEXT,
            query_text TEXT, query_type TEXT, status TEXT,
            execution_time_ms INTEGER, rows_produced INTEGER,
            bytes_scanned INTEGER, user_name TEXT, role_name TEXT,
            start_time TEXT, end_time TEXT
        )
    """)
    warehouses = ["ANALYTICS_WH", "ETL_WH", "REPORTING_WH", "DEV_WH"]
    databases = ["PRODUCTION", "ANALYTICS", "RAW_DATA", "STAGING"]
    query_types = ["SELECT", "INSERT", "CREATE TABLE", "MERGE", "COPY"]
    sf_users = ["etl_service", "analyst_team", "data_scientist", "admin", "dbt_runner", "reporting_svc"]

    for i in range(75):
        status = random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAIL", "TIMEOUT"])
        cur.execute(
            "INSERT INTO sf_query_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"01b{random.randint(10000, 99999)}-{random.randint(1000, 9999)}",
             random.choice(warehouses), random.choice(databases), "PUBLIC",
             random.choice([
                 "SELECT * FROM events WHERE date > DATEADD(day, -7, CURRENT_DATE())",
                 "SELECT user_id, COUNT(*) FROM page_views GROUP BY 1",
                 "INSERT INTO analytics.daily_metrics SELECT ...",
                 "MERGE INTO dim_customers USING staging.customers ON ...",
                 "CREATE TABLE analytics.user_segments AS SELECT ...",
                 "SELECT department, SUM(revenue) FROM sales GROUP BY 1",
             ]),
             random.choice(query_types), status,
             random.randint(100, 300000), random.randint(0, 1000000),
             random.randint(1000, 50000000000),
             random.choice(sf_users), random.choice(["SYSADMIN", "ANALYST", "ETL_ROLE", "PUBLIC"]),
             rdatetime(30, 0), rdatetime(30, 0)),
        )

    cur.execute("""
        CREATE TABLE sf_warehouse_usage (
            warehouse_name TEXT, date TEXT, credits_used REAL,
            queries_executed INTEGER, avg_execution_time_ms INTEGER,
            bytes_scanned INTEGER, active_hours REAL
        )
    """)
    for wh in warehouses:
        for d in range(30):
            dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
            cur.execute(
                "INSERT INTO sf_warehouse_usage VALUES (?,?,?,?,?,?,?)",
                (wh, dt, round(random.uniform(0.5, 25.0), 2),
                 random.randint(10, 500), random.randint(200, 30000),
                 random.randint(100000, 10000000000),
                 round(random.uniform(0.5, 23.5), 1)),
            )
    print(f"  {CHECK} sf_query_history — 75 query records")
    print(f"  {CHECK} sf_warehouse_usage — 120 daily usage records (4 warehouses × 30 days)")

    # ── ClickHouse-style Tables ───────────────────────────────
    cur.execute("DROP TABLE IF EXISTS ch_page_views")
    cur.execute("DROP TABLE IF EXISTS ch_api_requests")

    cur.execute("""
        CREATE TABLE ch_page_views (
            event_id TEXT, timestamp TEXT, user_id INTEGER,
            page_path TEXT, referrer TEXT, utm_source TEXT,
            utm_medium TEXT, country TEXT, device_type TEXT,
            browser TEXT, session_duration_sec INTEGER, is_bounce INTEGER
        )
    """)
    pages = ["/", "/pricing", "/docs", "/blog", "/signup", "/login",
             "/dashboard", "/settings", "/api-docs", "/changelog"]
    referrers = ["google.com", "twitter.com", "github.com", "linkedin.com", "producthunt.com",
                 "news.ycombinator.com", "(direct)", "reddit.com"]
    countries = ["US", "GB", "DE", "FR", "CA", "AU", "IN", "JP", "BR", "NL"]
    utm_sources = ["google", "twitter", "linkedin", "newsletter", "blog", "partner", "(none)"]
    devices = ["desktop", "mobile", "tablet"]

    for i in range(300):
        cur.execute(
            "INSERT INTO ch_page_views VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"pv_{random.randint(100000, 999999)}", rdatetime(30, 0),
             random.randint(1, 200), random.choice(pages),
             random.choice(referrers), random.choice(utm_sources),
             random.choice(["organic", "cpc", "social", "email", "(none)"]),
             random.choice(countries), random.choice(devices),
             random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
             random.randint(5, 1800), 1 if random.random() < 0.35 else 0),
        )

    cur.execute("""
        CREATE TABLE ch_api_requests (
            request_id TEXT, timestamp TEXT, method TEXT, path TEXT,
            status_code INTEGER, response_time_ms INTEGER,
            request_size_bytes INTEGER, response_size_bytes INTEGER,
            user_id INTEGER, api_key_prefix TEXT, error_message TEXT
        )
    """)
    api_paths = ["/api/v1/query", "/api/v1/connectors", "/api/v1/schemas",
                 "/api/v1/users", "/api/v1/export", "/api/v1/health",
                 "/api/v1/webhooks", "/api/v1/auth/token"]
    methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]
    for i in range(250):
        status = random.choices([200, 201, 204, 400, 401, 403, 404, 429, 500, 502, 503],
                                weights=[50, 10, 5, 8, 5, 3, 5, 4, 3, 2, 2])[0]
        cur.execute(
            "INSERT INTO ch_api_requests VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (f"req_{random.randint(100000, 999999)}", rdatetime(14, 0),
             random.choice(methods), random.choice(api_paths),
             status, random.randint(5, 5000),
             random.randint(50, 50000), random.randint(100, 500000),
             random.randint(1, 80), f"sk_{random.randint(1000, 9999)}",
             "Internal Server Error" if status >= 500 else ("Rate limit exceeded" if status == 429 else None)),
        )
    print(f"  {CHECK} ch_page_views — 300 pageview events")
    print(f"  {CHECK} ch_api_requests — 250 API request logs")

    db.commit()
    db.close()


# ══════════════════════════════════════════════════════════════
#  CSV FILES
# ══════════════════════════════════════════════════════════════

def seed_csv():
    print(f"\n  {CYAN}── CSV Files ──{RESET}\n")
    csv_dir = DATA_DIR / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # CRM Contacts
    with open(csv_dir / "crm_contacts.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "first_name", "last_name", "email", "company", "title",
                     "city", "deal_value", "status", "source", "created_date"])
        statuses = ["lead", "qualified", "proposal", "negotiation", "won", "lost"]
        sources = ["website", "referral", "linkedin", "conference", "cold_outreach"]
        titles = ["CTO", "VP Engineering", "Director of Data", "Head of Product",
                  "Engineering Manager", "Data Scientist", "Chief Data Officer"]
        for i in range(80):
            fn, ln = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
            company = random.choice(COMPANIES)
            w.writerow([i + 1, fn, ln,
                        f"{fn.lower()}.{ln.lower()}@{company.lower().replace(' ', '')}.com",
                        company, random.choice(titles), random.choice(CITIES),
                        random.randint(5000, 250000), random.choice(statuses),
                        random.choice(sources), rdate(365, 0)])
    print(f"  {CHECK} crm_contacts.csv — 80 contacts")

    # Monthly revenue
    with open(csv_dir / "monthly_revenue.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["month", "mrr", "arr", "new_mrr", "churned_mrr", "expansion_mrr",
                     "customers", "new_customers", "churned_customers"])
        base_mrr, customers = 45000, 120
        for m in range(24):
            dt = datetime(2024, 4, 1) + timedelta(days=30 * m)
            new_mrr = random.randint(3000, 12000)
            churned = random.randint(500, 3000)
            expansion = random.randint(1000, 5000)
            base_mrr += new_mrr - churned + expansion
            new_c, churn_c = random.randint(5, 25), random.randint(1, 8)
            customers += new_c - churn_c
            w.writerow([dt.strftime("%Y-%m"), base_mrr, base_mrr * 12,
                        new_mrr, churned, expansion, customers, new_c, churn_c])
    print(f"  {CHECK} monthly_revenue.csv — 24 months of MRR/ARR")

    # Support tickets
    with open(csv_dir / "support_tickets.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticket_id", "customer_email", "subject", "priority", "category",
                     "status", "assigned_to", "created_at", "resolved_at", "satisfaction_score"])
        subjects = ["Can't access dashboard", "API rate limit exceeded", "Billing discrepancy",
                    "Data export failing", "SSO integration issue", "Performance degradation",
                    "Webhook not firing", "Permission error on reports", "Mobile app crash",
                    "Two-factor auth locked out"]
        for i in range(150):
            created = rdatetime(90, 0)
            resolved = rdatetime(10, 0) if random.random() > 0.2 else ""
            status = "resolved" if resolved else random.choice(["open", "in_progress", "waiting"])
            w.writerow([f"TKT-{1000+i}", remail(rname()), random.choice(subjects),
                        random.choice(["critical", "high", "medium", "low"]),
                        random.choice(["bug", "billing", "feature_request", "security"]),
                        status, rname(), created, resolved,
                        random.choice([1, 2, 3, 4, 5, ""]) if status == "resolved" else ""])
    print(f"  {CHECK} support_tickets.csv — 150 tickets")

    return str(csv_dir)


# ══════════════════════════════════════════════════════════════
#  JSON FILES
# ══════════════════════════════════════════════════════════════

def seed_json():
    print(f"\n  {CYAN}── JSON Files ──{RESET}\n")
    json_dir = DATA_DIR / "json"
    json_dir.mkdir(parents=True, exist_ok=True)

    # Product usage events
    events = []
    features = ["dashboard", "reports", "api", "integrations", "settings",
                "billing", "team_management", "data_export", "alerts", "search"]
    for _ in range(200):
        events.append({
            "event_id": f"evt_{random.randint(100000, 999999)}",
            "user_id": f"usr_{random.randint(1000, 1200)}",
            "feature": random.choice(features),
            "action": random.choice(["view", "click", "create", "update", "delete", "export"]),
            "timestamp": rdatetime(30, 0),
            "duration_ms": random.randint(100, 30000),
            "metadata": {
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "os": random.choice(["macOS", "Windows", "Linux", "iOS", "Android"]),
                "plan": random.choice(["starter", "pro", "business", "enterprise"]),
            }
        })
    with open(json_dir / "product_events.json", "w") as f:
        json.dump(events, f, indent=2)
    print(f"  {CHECK} product_events.json — 200 usage events")

    # Team directory
    team = []
    for i in range(35):
        fn, ln = FIRST_NAMES[i % len(FIRST_NAMES)], LAST_NAMES[i % len(LAST_NAMES)]
        team.append({
            "id": i + 1, "name": f"{fn} {ln}", "email": f"{fn.lower()}.{ln.lower()}@company.com",
            "department": random.choice(DEPARTMENTS),
            "title": random.choice(["Software Engineer", "Senior Engineer", "Tech Lead",
                                    "Product Manager", "Designer", "Data Analyst"]),
            "location": {"city": random.choice(CITIES), "remote": random.choice([True, False])},
            "start_date": rdate(1500, 30), "salary": random.randint(80000, 220000),
        })
    with open(json_dir / "team_directory.json", "w") as f:
        json.dump(team, f, indent=2)
    print(f"  {CHECK} team_directory.json — 35 employees")

    # Feature flags
    flags = [{"name": name, "enabled": random.choice([True, False]),
              "rollout_percentage": random.choice([0, 10, 25, 50, 75, 100]),
              "targeting": {"plans": random.sample(["starter", "pro", "business", "enterprise"], 2)},
              "created_at": rdate(180, 0)}
             for name in ["dark_mode", "ai_copilot", "bulk_export", "sso_v2", "advanced_analytics",
                          "custom_branding", "api_v3", "real_time_sync", "multi_tenant", "webhook_retry"]]
    with open(json_dir / "feature_flags.json", "w") as f:
        json.dump(flags, f, indent=2)
    print(f"  {CHECK} feature_flags.json — 10 feature flags")

    # Deployments log
    deploys = []
    services = ["api-gateway", "web-app", "worker", "ml-pipeline", "auth-service", "billing-service"]
    for _ in range(60):
        success = random.random() > 0.08
        deploys.append({
            "deploy_id": f"deploy-{random.randint(10000, 99999)}",
            "service": random.choice(services),
            "version": f"{random.randint(1, 5)}.{random.randint(0, 30)}.{random.randint(0, 99)}",
            "environment": random.choice(["production", "staging", "preview"]),
            "status": "success" if success else "failed",
            "deployed_by": remail(rname()),
            "timestamp": rdatetime(60, 0),
            "duration_seconds": random.randint(30, 900),
        })
    with open(json_dir / "deployments.json", "w") as f:
        json.dump(deploys, f, indent=2)
    print(f"  {CHECK} deployments.json — 60 deployment records")

    return str(json_dir)


# ══════════════════════════════════════════════════════════════
#  CONFIG FILES
# ══════════════════════════════════════════════════════════════

def seed_config():
    print(f"\n  {CYAN}── Config Files ──{RESET}\n")
    cfg_dir = DATA_DIR / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)

    envs = {
        "production.env": {
            "DATABASE_URL": "postgresql://prod-rds.us-east-1.amazonaws.com:5432/app_production",
            "REDIS_URL": "redis://prod-elasticache.us-east-1.amazonaws.com:6379/0",
            "API_BASE_URL": "https://api.dataql.io/v1",
            "SECRET_KEY": "sk_prod_a7b2c9d4e5f6g7h8",
            "STRIPE_KEY": "DEMO_stripe_key_replace_with_real_key",
            "LOG_LEVEL": "warning", "MAX_WORKERS": "16", "RATE_LIMIT_RPM": "1000",
        },
        "staging.env": {
            "DATABASE_URL": "postgresql://staging-db.internal:5432/app_staging",
            "REDIS_URL": "redis://localhost:6379/1",
            "API_BASE_URL": "https://staging-api.dataql.io/v1",
            "SECRET_KEY": "sk_staging_x1y2z3a4b5c6",
            "STRIPE_KEY": "DEMO_stripe_test_key_replace_me",
            "LOG_LEVEL": "debug", "MAX_WORKERS": "4", "RATE_LIMIT_RPM": "5000",
        },
        "development.env": {
            "DATABASE_URL": "postgresql://localhost:5432/app_dev",
            "REDIS_URL": "redis://localhost:6379/2",
            "API_BASE_URL": "http://localhost:8000/v1",
            "SECRET_KEY": "sk_dev_localdev123",
            "STRIPE_KEY": "sk_test_localdev",
            "LOG_LEVEL": "debug", "MAX_WORKERS": "2", "RATE_LIMIT_RPM": "99999",
        },
    }
    for filename, kvs in envs.items():
        with open(cfg_dir / filename, "w") as f:
            f.write(f"# {filename.replace('.env', '').title()} Environment\n")
            for k, v in kvs.items():
                f.write(f"{k}={v}\n")
        print(f"  {CHECK} {filename}")

    return ",".join([str(cfg_dir / fn) for fn in envs])


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def update_env(csv_dir, json_dir, config_paths):
    env_path = BASE / ".env"
    content = env_path.read_text()
    lines = [l for l in content.split("\n")
             if not l.startswith("# ── Demo Data") and not l.startswith("CSV_DIRECTORY=")
             and not l.startswith("JSON_DIRECTORY=") and not l.startswith("CONFIG_FILES=")]
    lines.append("")
    lines.append("# ── Demo Data (auto-generated by seed_demo_data.py) ──")
    lines.append(f"CSV_DIRECTORY={csv_dir}")
    lines.append(f"JSON_DIRECTORY={json_dir}")
    lines.append(f"CONFIG_FILES={config_paths}")
    lines.append("")
    env_path.write_text("\n".join(lines))


if __name__ == "__main__":
    print(f"\n{BOLD}{'═' * 58}")
    print(f"  DataQL — Full Demo Data Seeder (All Connectors)")
    print(f"{'═' * 58}{RESET}")

    seed_sqlite()
    csv_dir = seed_csv()
    json_dir = seed_json()
    config_paths = seed_config()
    update_env(csv_dir, json_dir, config_paths)

    print(f"\n  {CYAN}── .env updated ──{RESET}\n")
    print(f"  {CHECK} CSV_DIRECTORY → demo_data/csv/")
    print(f"  {CHECK} JSON_DIRECTORY → demo_data/json/")
    print(f"  {CHECK} CONFIG_FILES → demo_data/config/")

    # Final summary
    db = sqlite3.connect(str(DB_PATH))
    tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    total_rows = sum(db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables)
    db.close()

    csv_count = sum(1 for _ in (DATA_DIR / "csv").glob("*.csv")) if (DATA_DIR / "csv").exists() else 0
    json_count = sum(1 for _ in (DATA_DIR / "json").glob("*.json")) if (DATA_DIR / "json").exists() else 0
    cfg_count = sum(1 for _ in (DATA_DIR / "config").glob("*")) if (DATA_DIR / "config").exists() else 0

    print(f"\n{BOLD}{'═' * 58}")
    print(f"  ✅ Demo data generated!")
    print(f"{'═' * 58}{RESET}")
    print(f"""
  📊 SQLite (demo.db):  {len(tables)} tables, {total_rows:,} total rows
     ├─ E-commerce:  customers, orders, products, order_items
     ├─ Jira:        jira_issues, jira_sprints
     ├─ GitHub:      gh_issues, gh_pull_requests, gh_commits
     ├─ Slack:       slack_users, slack_channels, slack_messages
     ├─ Notion:      notion_projects, notion_wiki
     ├─ Airtable:    at_content_calendar, at_product_roadmap
     ├─ Postgres:    pg_user_events, pg_subscriptions, pg_invoices
     ├─ Snowflake:   sf_query_history, sf_warehouse_usage
     └─ ClickHouse:  ch_page_views, ch_api_requests

  📋 CSV files:       {csv_count} files (crm, revenue, tickets)
  📦 JSON files:      {json_count} files (events, team, flags, deploys)
  ⚙️  Config files:    {cfg_count} files (prod, staging, dev)

  Restart the server to load everything:
    python3 -m uvicorn main:app --reload --port 8000
""")
