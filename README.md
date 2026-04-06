<div align="center">

# ⚡ DataQL

### Ask questions. Get answers. From any data source.

**DataQL is an AI-powered natural language query engine that connects to your databases, Gmail, Google Sheets, and 20+ other sources — then lets you ask questions in plain English.**

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-19-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev)
[![Claude](https://img.shields.io/badge/Claude_AI-Anthropic-6366f1?style=for-the-badge)](https://anthropic.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

---

</div>

## 🎯 What It Does

```
You:     "Show me my unread emails from this week"

DataQL:  → Generates SQL: SELECT * FROM gmail_messages WHERE is_unread = 1
         → Executes against your real Gmail data
         → Returns: "You have 8 unread emails from 5 senders..."
```

Every data source becomes a SQL table. Ask anything.

---

## ✨ Key Features

| | Feature | Description |
|---|---------|------------|
| 🧠 | **AI Query Planner** | Claude converts natural language → multi-step SQL with chain-of-thought reasoning |
| 🔄 | **Self-Correction** | Failed queries are automatically fixed and retried (up to 3x) |
| 📧 | **Gmail Integration** | Real OAuth2 — your inbox becomes a queryable `gmail_messages` table |
| 📊 | **Google Sheets** | Each spreadsheet tab auto-converts to a typed SQL table |
| 🎯 | **Reliability Scoring** | Every answer gets a confidence score (0-100) based on execution quality |
| 🔒 | **SQL Guardrails** | Only `SELECT` allowed — mutations (`DROP`, `DELETE`, `INSERT`) are blocked |
| 💬 | **Threaded Chat** | Conversational interface with full query plan transparency |
| 🌙 | **Premium Dark UI** | Glassmorphism modals, animated glow cards, category-based connector grid |

---

## 🔌 Connectors

<table>
<tr>
<td width="50%">

### SQL Databases (Direct)
- 🗃️ **SQLite** — Local files
- 🐘 **PostgreSQL** — Remote servers
- ⚡ **ClickHouse** — Analytics
- ❄️ **Snowflake** — Cloud warehouse
- 🧱 **Databricks** — Lakehouse
- 🔴 **Redshift** — AWS warehouse

</td>
<td width="50%">

### APIs (Fetched → SQL)
- 📧 **Gmail** — Inbox & labels
- 📊 **Google Sheets** — Spreadsheets
- ☁️ **Salesforce** — CRM data
- 📈 **Google Analytics** — Traffic
- 💬 **Slack** — Messages
- 🎯 **Marketo** — Marketing

</td>
</tr>
</table>

> API connectors fetch data from external services, load it into in-memory SQLite, and expose it through the same SQL interface. The AI doesn't know the difference.

---

## 🚀 Quick Start

### 1. Backend

```bash
cd backend
pip install -r requirements.txt
```

Create `.env` with your API key:
```env
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=sqlite:///demo.db
```

```bash
python seed_db.py          # Create demo database
uvicorn main:app --reload  # Start API server
```

### 2. Frontend

```bash
cd frontend
npm install
npm run dev                # http://localhost:5173
```

### 3. Connect Gmail *(optional)*

```bash
# 1. Get OAuth2 credentials from Google Cloud Console
# 2. Enable Gmail API + Sheets API
# 3. Download credentials.json → backend/

python google_auth.py      # Opens browser → authorize → done
```

Now ask: *"show me my unread emails"* 🎉

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        React Frontend                           │
│              Chat UI · Connector Grid · Schema Explorer          │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP/JSON
┌────────────────────────────┴────────────────────────────────────┐
│                      FastAPI Backend                             │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
│  │ Query Planner│→ │  Execution   │→ │  Self-Correction      │  │
│  │ (Claude AI)  │  │  Engine      │  │  (retry on failure)   │  │
│  └──────────────┘  └──────┬───────┘  └───────────────────────┘  │
│                           │                                      │
│  ┌────────────────────────┴─────────────────────────────────┐   │
│  │              Connector Registry                           │   │
│  │  ┌─────────┐ ┌──────────┐ ┌───────┐ ┌────────┐          │   │
│  │  │ SQLite  │ │PostgreSQL│ │ Gmail │ │ Sheets │  ...24+   │   │
│  │  └─────────┘ └──────────┘ └───────┘ └────────┘          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### How a Query Flows

1. **User asks** a natural language question
2. **Query Planner** sends question + full schema context to Claude
3. **Claude returns** a structured JSON plan with SQL steps
4. **Execution Engine** runs each step against the right connector
5. **Self-Correction** catches SQL errors → asks Claude to fix → retries
6. **Reliability Score** is computed from retries, timing, and data quality
7. **Answer** is formatted and returned to the chat

---

## ⚙️ Environment Variables

```env
# Required
ANTHROPIC_API_KEY=sk-ant-...          # Claude API key
DATABASE_URL=sqlite:///demo.db         # Default database

# Gmail & Sheets (OAuth2)
GOOGLE_CREDENTIALS_FILE=credentials.json
GOOGLE_SHEETS_IDS=sheet-id-1,sheet-id-2

# SQL Databases
POSTGRES_URL=postgresql://user:pass@host:5432/db
CLICKHOUSE_URL=clickhouse://user:pass@host:8123/db

# API Connectors
SALESFORCE_INSTANCE_URL=https://your-instance.salesforce.com
SALESFORCE_ACCESS_TOKEN=your-token
SLACK_BOT_TOKEN=xoxb-your-token
```

---

## 📁 Project Structure

```
dataql/
├── backend/
│   ├── main.py                 # FastAPI routes & orchestration
│   ├── query_planner.py        # NL → SQL via Claude
│   ├── execution_engine.py     # Step-by-step plan executor
│   ├── self_correction.py      # Auto-fix failed queries
│   ├── data_connectors.py      # Base classes + all connectors
│   ├── gmail_connector.py      # Gmail API → SQL tables
│   ├── sheets_connector.py     # Sheets API → SQL tables
│   ├── google_auth.py          # OAuth2 token management
│   ├── ai_primitives.py        # Reliability scoring
│   ├── semantic_metadata.py    # LLM-generated schema descriptions
│   └── schema_introspector.py  # Table/column discovery
├── frontend/
│   ├── src/
│   │   ├── App.jsx             # Main app shell
│   │   ├── components/
│   │   │   ├── ChatPanel.jsx       # Conversational interface
│   │   │   ├── ConnectorsPage.jsx  # Dark-themed connector grid
│   │   │   ├── SchemaExplorer.jsx  # Database schema viewer
│   │   │   └── ThreadSidebar.jsx   # Chat thread navigation
│   │   └── index.css           # Premium dark theme
│   └── index.html
└── docs/
    └── use-cases.md
```

---

## 🛡️ Security

- **SQL Guardrails** — `validate_sql()` blocks all mutations (`DROP`, `DELETE`, `INSERT`, etc.)
- **OAuth2** — Gmail/Sheets use Google's standard OAuth2 flow with token refresh
- **Read-Only** — All connectors operate in read-only mode by design
- **No Data Storage** — API data lives in-memory only, never written to disk

---

## 📄 License

MIT — use it however you want.

---

<div align="center">

**Built with [Claude AI](https://anthropic.com) · [FastAPI](https://fastapi.tiangolo.com) · [React](https://react.dev)**

</div>
