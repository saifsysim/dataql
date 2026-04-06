# DataQL — Use Cases & Business Scenarios

## Who It Helps

### 1. Non-Technical Teams Querying Data Directly
> *"What was our revenue last quarter?"*

Without DataQL, a marketing manager would file a Jira ticket to the data team and wait 2 days. With DataQL, they type the question in plain English and get the answer in seconds — and the semantic metadata ensures "revenue" correctly excludes cancelled orders, every time.

### 2. Sales Teams Monitoring Pipelines
> *"Show me all Salesforce deals over $50K that haven't been updated in 30 days"*

Connect Salesforce via env var → DataQL syncs opportunities into a queryable format → the sales VP gets pipeline insights without learning SOQL or waiting for a dashboard to be built.

### 3. E-Commerce Operations
> *"Which products are running low on stock but had the highest sales this month?"*

Combines inventory data with order data. The semantic metadata knows "low stock = stock < 10" and "sales = non-cancelled orders," so the AI writes the correct query automatically.

### 4. Cross-Source Analytics (The Killer Feature)
> *"Compare our Google Analytics traffic to Salesforce pipeline — which marketing channels drive the most revenue?"*

DataQL queries GA4 for traffic data AND Salesforce for deals, then joins them. No ETL pipeline needed — API connectors sync data into SQLite and the LLM plans the cross-source join.

### 5. Customer Support Teams
> *"Pull all Slack messages from #support that mention 'billing' in the last week"*

Connect Slack → DataQL indexes messages → support leads can search and analyze patterns without building a custom tool.

### 6. Executive Dashboards on Demand
> *"Give me a breakdown of revenue by product category, customer tier, and month for Q1"*

Instead of requesting a new Looker dashboard (weeks), the CFO asks in natural language and gets a result table in seconds.

### 7. Data Team Productivity
> *"What's the average order value for platinum customers who signed up this year?"*

Even for engineers who know SQL, DataQL is faster. The semantic metadata handles edge cases (exclude cancelled orders, correct date filters) that analysts often forget.

---

## The Semantic Metadata Difference

| Without Metadata | With Metadata |
|---|---|
| "What's our revenue?" → `SELECT SUM(total_amount) FROM orders` ❌ (includes cancelled) | "What's our revenue?" → `...WHERE status != 'cancelled'` ✅ |
| "Show active customers" → AI guesses what "active" means | "Show active customers" → Knows it means "ordered in last 90 days" |
| "Low stock products" → AI picks an arbitrary threshold | "Low stock products" → Uses `stock < 10` per business rule |

The metadata turns a **generic SQL generator** into a **domain expert** that knows your company's specific definitions.

---

## Company Types That Benefit Most

| Company Type | Why DataQL Helps |
|---|---|
| **Startups** | No dedicated BI team, everyone needs data access |
| **E-commerce** | Complex order/inventory/customer queries daily |
| **SaaS companies** | Data scattered across Salesforce, GA, Slack, Marketo |
| **Agencies** | Need to query client data across multiple tools quickly |
| **Enterprise data teams** | Reduce ticket load by empowering business users |

---

## Bottom Line

**DataQL eliminates the data team bottleneck** by letting anyone in the company ask questions in English and get accurate, business-context-aware answers from any connected source.
