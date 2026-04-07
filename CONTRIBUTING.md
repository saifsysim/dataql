# Contributing to DataQL

Thanks for your interest in contributing! DataQL is an AI-powered natural language data engine, and we welcome contributions of all kinds.

## Getting Started

### Prerequisites
- Python 3.9+
- Node.js 18+
- An Anthropic API key ([get one here](https://console.anthropic.com))

### Local Setup

```bash
# Clone the repo
git clone https://github.com/saifsysim/dataql.git
cd dataql

# Backend
cd backend
pip install -r requirements.txt
cp ../.env.example .env
# Add your ANTHROPIC_API_KEY to .env
python3 seed_demo_data.py   # Seed demo database
python3 -m uvicorn main:app --reload

# Frontend (new terminal)
cd frontend
npm install
npm run dev
```

Open http://localhost:5173 and ask a question!

## How to Contribute

### 🐛 Bug Reports
- Use [GitHub Issues](https://github.com/saifsysim/dataql/issues)
- Include steps to reproduce, expected vs actual behavior
- Include browser console errors if frontend-related

### ✨ Feature Requests
- Open an issue with the `enhancement` label
- Describe the use case, not just the solution
- Check existing issues first to avoid duplicates

### 🔧 Pull Requests

1. **Fork** the repo and create a feature branch from `main`
2. **Write clean code** — follow existing patterns in the codebase
3. **Test your changes** — make sure `npm run build` passes for frontend
4. **Keep PRs focused** — one feature/fix per PR
5. **Write a clear description** — explain what and why

### Good First Issues

Look for issues labeled [`good first issue`](https://github.com/saifsysim/dataql/issues?q=is%3Aopen+label%3A%22good+first+issue%22) — these are specifically chosen for new contributors.

## Areas We Need Help

| Area | What we need |
|------|-------------|
| **Connectors** | Real OAuth flows for Gmail, Sheets, Slack, GitHub |
| **Testing** | Unit tests for query planner, execution engine, self-correction |
| **UI/UX** | Responsive mobile layout, accessibility improvements |
| **Docs** | Usage guides, video walkthroughs, API documentation |
| **Connectors** | New connectors: MongoDB, BigQuery, Supabase, HubSpot |
| **DevOps** | Docker compose, CI/CD pipeline, one-click deploy buttons |

## Code Style

- **Python**: Follow PEP 8. Use type hints where possible.
- **JavaScript/React**: Functional components with hooks. No class components.
- **CSS**: Vanilla CSS with CSS custom properties. No Tailwind.

## Community

- Be respectful and constructive
- Help others in issues and discussions
- Credit your sources if borrowing patterns

---

**Thank you for helping make DataQL better!** ⚡
