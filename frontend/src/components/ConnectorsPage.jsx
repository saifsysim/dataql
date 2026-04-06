import { useState, useEffect, useMemo, useRef } from 'react';

const API = 'http://localhost:8000';

const INTEGRATIONS = [
  { id: 'postgres', name: 'PostgreSQL', authType: 'Connection String', scope: 'Shared', color: '#4169E1', cat: 'database',
    logo: 'https://cdn.simpleicons.org/postgresql/4169E1',
    docsUrl: 'https://www.postgresql.org/docs/current/libpq-connect.html',
    connectUrl: 'https://neon.tech/',
    desc: 'Connect to PostgreSQL databases for full SQL query access. Supports schemas, views, and complex joins across tables.',
    connString: 'postgresql://user:password@host:5432/database', isDataSource: true },
  { id: 'snowflake', name: 'Snowflake', authType: 'Connection String', scope: 'Shared', color: '#29B5E8', cat: 'database',
    logo: 'https://cdn.simpleicons.org/snowflake/29B5E8',
    docsUrl: 'https://docs.snowflake.com/en/developer-guide/sql-api/index',
    connectUrl: 'https://app.snowflake.com/',
    desc: 'Connect to Snowflake cloud data warehouse. Query tables across databases and schemas with full SQL support.',
    connString: 'snowflake://account.snowflakecomputing.com/?db=DB&schema=PUBLIC&warehouse=WH', isDataSource: true },
  { id: 'clickhouse', name: 'ClickHouse', authType: 'Connection String', scope: 'Shared', color: '#FADB14', cat: 'database',
    logo: 'https://cdn.simpleicons.org/clickhouse/FADB14',
    docsUrl: 'https://clickhouse.com/docs',
    connectUrl: 'https://clickhouse.cloud/signUp',
    desc: 'Connect to ClickHouse for high-performance analytical queries on billions of rows.',
    connString: 'jdbc:clickhouse://host:8443/database?user=xxx&password=xxx', isDataSource: true },
  { id: 'databricks', name: 'Databricks', authType: 'Token', scope: 'Shared', color: '#FF3621', cat: 'database',
    logo: 'https://cdn.simpleicons.org/databricks/FF3621',
    docsUrl: 'https://docs.databricks.com/en/dev-tools/api/index.html',
    connectUrl: 'https://accounts.cloud.databricks.com/',
    desc: 'Connect to Databricks SQL warehouses and Unity Catalog. Query Delta tables on your lakehouse.',
    connString: 'jdbc:databricks://host:443/default;transportMode=http;ssl=1;httpPath=xxx', isDataSource: true },
  { id: 'redshift', name: 'Redshift', authType: 'Connection String', scope: 'Shared', color: '#8C4FFF', cat: 'database',
    logo: 'https://cdn.worldvectorlogo.com/logos/aws-redshift-logo.svg',
    docsUrl: 'https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html',
    connectUrl: 'https://console.aws.amazon.com/redshiftv2/',
    desc: 'Query Amazon Redshift data warehouse clusters with columnar storage optimization.',
    connString: 'jdbc:redshift://host:5439/database?user=xxx&password=xxx', isDataSource: true },
  { id: 'sqlite', name: 'SQLite', authType: 'File', scope: 'Personal', color: '#44A8B3', cat: 'database',
    logo: 'https://cdn.simpleicons.org/sqlite/44A8B3',
    docsUrl: 'https://www.sqlite.org/docs.html',
    desc: 'Connect to local SQLite database files for lightweight SQL queries.',
    connString: '/path/to/database.db', isDataSource: true },
  { id: 'github', name: 'GitHub', authType: 'OAuth2', scope: 'Personal', color: '#6e40c9', cat: 'devtools',
    logo: 'https://cdn.simpleicons.org/github/ffffff',
    docsUrl: 'https://docs.github.com/en/rest',
    connectUrl: 'https://github.com/settings/tokens',
    desc: 'Search repositories, issues, pull requests, and code across your GitHub organization.' },
  { id: 'linear', name: 'Linear', authType: 'OAuth2', scope: 'Personal', color: '#5E6AD2', cat: 'devtools',
    logo: 'https://cdn.simpleicons.org/linear/5E6AD2',
    docsUrl: 'https://developers.linear.app/docs/graphql/working-with-the-graphql-api',
    connectUrl: 'https://linear.app/settings/api',
    desc: 'Track issues, projects, and cycles. Query team velocity, backlog health, and sprint progress.' },
  { id: 'atlassian', name: 'Atlassian', authType: 'OAuth2', scope: 'Personal', color: '#0052CC', cat: 'devtools',
    logo: 'https://cdn.simpleicons.org/atlassian/2684FF',
    docsUrl: 'https://developer.atlassian.com/cloud/jira/platform/rest/v3/',
    connectUrl: 'https://id.atlassian.com',
    desc: 'Search and manage Jira issues and Confluence pages. Filter by project, sprint, or assignee.' },
  { id: 'salesforce', name: 'Salesforce', authType: 'OAuth2', scope: 'Shared', color: '#00A1E0', cat: 'business',
    logo: 'https://cdn.simpleicons.org/salesforce',
    docsUrl: 'https://developer.salesforce.com/docs/apis',
    connectUrl: 'https://login.salesforce.com/',
    desc: 'Access Salesforce CRM data — accounts, contacts, opportunities, and custom objects.' },
  { id: 'stripe', name: 'Stripe', authType: 'API Key', scope: 'Shared', color: '#635BFF', cat: 'business',
    logo: 'https://cdn.simpleicons.org/stripe/635BFF',
    docsUrl: 'https://docs.stripe.com/api',
    connectUrl: 'https://dashboard.stripe.com/apikeys',
    desc: 'Query payment data — charges, subscriptions, customers, invoices. Analyze revenue trends.',
    example: 'sk_live_xxxxxxxxxxxx' },
  { id: 'chargebee', name: 'Chargebee', authType: 'API Key', scope: 'Personal', color: '#FF6633', cat: 'business',
    logo: 'https://cdn.simpleicons.org/chargebee/FF6633',
    docsUrl: 'https://apidocs.chargebee.com/docs/api',
    connectUrl: 'https://app.chargebee.com/apikeys',
    desc: 'Query subscription data, invoices, and billing. Analyze MRR, churn, and revenue metrics.',
    example: 'live_abc123def456' },
  { id: 'intercom', name: 'Intercom', authType: 'API Key', scope: 'Personal', color: '#286EFA', cat: 'business',
    logo: 'https://cdn.simpleicons.org/intercom/286EFA',
    docsUrl: 'https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Introduction/',
    connectUrl: 'https://app.intercom.com/a/apps/_/developer-hub',
    desc: 'Query customer conversations, user profiles, and support metrics.',
    example: 'dG9rOmFiY2RlZjEyMzQ1Njc4OTA=' },
  { id: 'zendesk', name: 'Zendesk', authType: 'OAuth2', scope: 'Personal', color: '#03363D', cat: 'business',
    logo: 'https://cdn.simpleicons.org/zendesk/17494D',
    docsUrl: 'https://developer.zendesk.com/api-reference/',
    connectUrl: 'https://www.zendesk.com/login/',
    desc: 'Search support tickets, satisfaction ratings. Analyze volumes and resolution times.' },
  { id: 'marketo', name: 'Marketo', authType: 'OAuth2', scope: 'Shared', color: '#5C4C9F', cat: 'business',
    logo: "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 40 40'%3E%3Crect width='40' height='40' rx='8' fill='%235C4C9F'/%3E%3Ctext x='50%25' y='54%25' dominant-baseline='middle' text-anchor='middle' fill='white' font-family='Arial,sans-serif' font-weight='bold' font-size='22'%3EM%3C/text%3E%3C/svg%3E",
    docsUrl: 'https://developers.marketo.com/rest-api/',
    connectUrl: 'https://login.marketo.com/',
    desc: 'Manage leads, campaigns, and marketing automation. Track conversions and lead scoring.',
    hasSetupInstructions: true },
  { id: 'gmail', name: 'Gmail', authType: 'OAuth2', scope: 'Personal', color: '#EA4335', cat: 'communication',
    logo: 'https://cdn.simpleicons.org/gmail/EA4335',
    docsUrl: 'https://developers.google.com/gmail/api/reference/rest',
    connectUrl: 'https://console.cloud.google.com/apis/credentials',
    desc: 'Search and query your Gmail inbox. Ask about unread emails, filter by sender, date, or subject. Supports labels and attachments.' },
  { id: 'google_sheets', name: 'Google Sheets', authType: 'OAuth2', scope: 'Personal', color: '#34A853', cat: 'communication',
    logo: 'https://cdn.simpleicons.org/googlesheets/34A853',
    docsUrl: 'https://developers.google.com/sheets/api/reference/rest',
    connectUrl: 'https://console.cloud.google.com/apis/credentials',
    desc: 'Query your Google Sheets spreadsheets as SQL tables. Each tab becomes a table with auto-detected column types.' },
  { id: 'slack', name: 'Slack', authType: 'API Key', scope: 'Personal', color: '#611f69', cat: 'communication',
    logo: 'https://cdn.worldvectorlogo.com/logos/slack-new-logo.svg',
    docsUrl: 'https://api.slack.com/methods',
    connectUrl: 'https://api.slack.com/apps',
    desc: 'Search messages, channels, and user profiles. Analyze communication patterns.',
    example: 'xoxb-your-bot-token' },
  { id: 'notion', name: 'Notion', authType: 'OAuth2', scope: 'Personal', color: '#787878', cat: 'communication',
    logo: 'https://cdn.simpleicons.org/notion/ffffff',
    docsUrl: 'https://developers.notion.com/reference/intro',
    connectUrl: 'https://www.notion.so/my-integrations',
    desc: 'Access databases, pages, and workspace content. Query structured data from database views.' },
  { id: 'google_docs', name: 'Google Docs', authType: 'OAuth2', scope: 'Personal', color: '#4285F4', cat: 'communication',
    logo: 'https://cdn.simpleicons.org/googledocs/4285F4',
    docsUrl: 'https://developers.google.com/docs/api/reference/rest',
    connectUrl: 'https://console.cloud.google.com/apis/credentials',
    desc: 'Search and read Google Docs, Sheets, and Slides. Extract and analyze spreadsheet data.' },
  { id: 'airtable', name: 'Airtable', authType: 'API Key', scope: 'Personal', color: '#18BFFF', cat: 'communication',
    logo: 'https://cdn.simpleicons.org/airtable/18BFFF',
    docsUrl: 'https://airtable.com/developers/web/api/introduction',
    connectUrl: 'https://airtable.com/create/tokens',
    desc: 'Access bases and tables. Query records, filter by views, and analyze structured data.',
    example: 'pat1234567890.abcdef' },
  { id: 'google_analytics', name: 'Google Analytics', authType: 'OAuth2', scope: 'Shared', color: '#E37400', cat: 'analytics',
    logo: 'https://cdn.simpleicons.org/googleanalytics/E37400',
    docsUrl: 'https://developers.google.com/analytics/devguides/reporting/data/v1',
    connectUrl: 'https://analytics.google.com/',
    desc: 'Analyze website traffic, user behavior, and conversion funnels from GA4 properties.' },
  { id: 'cloudflare', name: 'Cloudflare', authType: 'API Key', scope: 'Personal', color: '#F48120', cat: 'analytics',
    logo: 'https://cdn.simpleicons.org/cloudflare/F48120',
    docsUrl: 'https://developers.cloudflare.com/api/',
    connectUrl: 'https://dash.cloudflare.com/profile/api-tokens',
    desc: 'Access analytics, DNS records, and security events. Monitor traffic and threats.',
    example: 'Bearer your-api-token' },
  { id: 'graphql', name: 'Hasura GraphQL', authType: 'API Key', scope: 'Shared', color: '#1EB4D4', cat: 'analytics',
    logo: 'https://cdn.simpleicons.org/hasura/1EB4D4',
    docsUrl: 'https://hasura.io/docs/latest/api-reference/graphql-api/index/',
    connectUrl: 'https://cloud.hasura.io/',
    desc: 'Connect to Hasura GraphQL Engine. Query and mutate data with full schema introspection.',
    example: 'your-hasura-admin-secret' },
  { id: 'glean', name: 'Glean', authType: 'API Key', scope: 'Shared', color: '#2ECC71', cat: 'analytics',
    logo: 'https://cdn.simpleicons.org/google/2ECC71',
    docsUrl: 'https://developers.glean.com/docs/client_api/overview/',
    connectUrl: 'https://app.glean.com/admin/setup',
    desc: 'Enterprise search across all connected workplace apps. Surface docs and knowledge.',
    example: 'glean-api-key-xxxxx' },
  { id: 'instantly', name: 'Instantly', authType: 'API Key', scope: 'Personal', color: '#6366F1', cat: 'communication',
    logo: 'https://cdn.simpleicons.org/minutemailer/6366F1',
    docsUrl: 'https://developer.instantly.ai/',
    connectUrl: 'https://app.instantly.ai/app/settings/integrations',
    desc: 'Access email campaign data, lead lists, and outreach analytics.',
    example: 'instantly-api-key-xxxxx' },
];

const CATEGORIES = [
  { id: 'all', label: 'All', icon: '✦' },
  { id: 'database', label: 'Databases', icon: '🗄' },
  { id: 'devtools', label: 'Dev Tools', icon: '⚙' },
  { id: 'business', label: 'Business', icon: '📊' },
  { id: 'communication', label: 'Collaboration', icon: '💬' },
  { id: 'analytics', label: 'Analytics', icon: '📈' },
];

function BrandIcon({ src, name, size = 20, fallback }) {
  const [err, setErr] = useState(false);
  if (err) return <span style={{ fontSize: size * 0.7, lineHeight: 1 }}>{fallback || name?.[0] || '?'}</span>;
  return <img src={src} alt={name} width={size} height={size} style={{ objectFit: 'contain' }} onError={() => setErr(true)} />;
}

// ── Add Data Source Modal ──
function AddDataSourceModal({ integration, onClose, onConnect }) {
  const [name, setName] = useState(integration?.id || '');
  const [connString, setConnString] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [connecting, setConnecting] = useState(false);

  const handleSubmit = () => {
    if (!connString.trim()) return;
    setConnecting(true);
    setTimeout(() => {
      onConnect({ ...integration, connectorName: name, connectionString: connString });
      setConnecting(false);
      onClose();
    }, 1500);
  };

  return (
    <div className="cx-overlay" onClick={onClose}>
      <div className="cx-modal" onClick={e => e.stopPropagation()}>
        <button className="cx-modal-close" onClick={onClose}>✕</button>
        <div className="cx-modal-badge">
          <div className="cx-modal-badge-icon" style={{ background: integration.color + '22' }}>
            <BrandIcon src={integration.logo} name={integration.name} size={28} />
          </div>
          <div>
            <h2 className="cx-modal-title">Connect {integration.name}</h2>
            <span className="cx-modal-tag">{integration.authType}</span>
          </div>
        </div>

        <div className="cx-field">
          <label className="cx-label">Connector Name</label>
          <input className="cx-input" value={name} onChange={e => setName(e.target.value)} placeholder={integration.id} />
          <span className="cx-hint">Unique identifier — cannot be changed later</span>
        </div>

        <div className="cx-field">
          <label className="cx-label">Connection String</label>
          <div className="cx-input-group">
            <input className="cx-input cx-input-stretch" type={showPassword ? 'text' : 'password'} value={connString} onChange={e => setConnString(e.target.value)} placeholder="Enter connection string..." />
            <button className="cx-input-addon" onClick={() => setShowPassword(!showPassword)}>{showPassword ? '🙈' : '👁'}</button>
          </div>
          {integration.connString && <span className="cx-hint cx-hint-mono">{integration.connString}</span>}
        </div>

        {integration.connectUrl && (
          <a href={integration.connectUrl} target="_blank" rel="noopener noreferrer" className="cx-ext-link">
            Get credentials from {integration.name} <span>↗</span>
          </a>
        )}

        <div className="cx-actions">
          <button className="cx-btn-ghost" onClick={onClose}>Cancel</button>
          <button className="cx-btn-primary" onClick={handleSubmit} disabled={!connString.trim() || connecting}>
            {connecting ? <><span className="cx-spinner" /> Connecting...</> : `Connect`}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Integration Detail Modal ──
function IntegrationDetailModal({ integration, onClose, onConnect }) {
  const [connecting, setConnecting] = useState(false);
  const [showSetup, setShowSetup] = useState(false);

  const handleConnect = () => {
    if (integration.connectUrl) window.open(integration.connectUrl, '_blank', 'noopener,noreferrer');
    setConnecting(true);
    setTimeout(() => { onConnect(integration); setConnecting(false); onClose(); }, 1500);
  };

  return (
    <div className="cx-overlay" onClick={onClose}>
      <div className="cx-modal cx-modal-wide" onClick={e => e.stopPropagation()}>
        <button className="cx-modal-close" onClick={onClose}>✕</button>

        <div className="cx-detail-hero" style={{ '--hero-color': integration.color }}>
          <div className="cx-detail-hero-icon">
            <BrandIcon src={integration.logo} name={integration.name} size={36} />
          </div>
          <div className="cx-detail-hero-info">
            <div className="cx-detail-hero-row">
              <h2 className="cx-detail-hero-name">{integration.name}</h2>
              <span className="cx-verified-badge">✓ Verified</span>
            </div>
            <p className="cx-detail-hero-sub">{integration.desc?.split('.')[0]}.</p>
          </div>
        </div>

        <div className="cx-detail-pills">
          <span className="cx-pill">{integration.authType}</span>
          <span className="cx-pill">{integration.scope}</span>
          <a href={integration.docsUrl} target="_blank" rel="noopener noreferrer" className="cx-pill cx-pill-link">Documentation ↗</a>
        </div>

        <p className="cx-detail-body">{integration.desc}</p>

        {integration.example && (
          <div className="cx-detail-key-hint">
            <span>API Key format</span>
            <code>{integration.example}</code>
          </div>
        )}

        {integration.hasSetupInstructions && (
          <details className="cx-detail-setup" open={showSetup}>
            <summary onClick={() => setShowSetup(!showSetup)}>Setup Instructions</summary>
            <div className="cx-detail-setup-body">
              <p>1. Open <a href={integration.connectUrl} target="_blank" rel="noopener noreferrer">{integration.name} settings</a></p>
              <p>2. Navigate to API / Integration settings</p>
              <p>3. Generate your {integration.authType === 'OAuth2' ? 'OAuth2 credentials' : 'API key'}</p>
              <p>4. Click "Connect" below to authorize DataQL</p>
            </div>
          </details>
        )}

        <div className="cx-actions">
          <button className="cx-btn-ghost" onClick={onClose}>Cancel</button>
          <button className="cx-btn-primary" onClick={handleConnect} disabled={connecting}>
            {connecting ? <><span className="cx-spinner" /> Connecting...</> : 'Connect'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Source Picker Modal ──
function SourcePickerModal({ onClose, onSelect }) {
  const dataSources = INTEGRATIONS.filter(i => i.isDataSource);
  const [search, setSearch] = useState('');
  const filtered = dataSources.filter(ds => ds.name.toLowerCase().includes(search.toLowerCase()));

  return (
    <div className="cx-overlay" onClick={onClose}>
      <div className="cx-modal cx-modal-picker" onClick={e => e.stopPropagation()}>
        <button className="cx-modal-close" onClick={onClose}>✕</button>
        <h2 className="cx-modal-title">Add a Data Source</h2>
        <p className="cx-modal-sub">Choose the database you'd like to connect.</p>

        <div className="cx-search-bar cx-search-bar-modal">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
          <input type="text" placeholder="Search databases..." value={search} onChange={e => setSearch(e.target.value)} autoFocus />
        </div>

        <div className="cx-picker-list">
          {filtered.map(ds => (
            <button key={ds.id} className="cx-picker-item" onClick={() => onSelect(ds)}>
              <div className="cx-picker-item-icon" style={{ background: ds.color + '18' }}>
                <BrandIcon src={ds.logo} name={ds.name} size={24} />
              </div>
              <div className="cx-picker-item-info">
                <span className="cx-picker-item-name">{ds.name}</span>
                <span className="cx-picker-item-auth">{ds.authType}</span>
              </div>
              <span className="cx-picker-arrow">→</span>
            </button>
          ))}
          {filtered.length === 0 && <p className="cx-empty-msg">No databases match "{search}"</p>}
        </div>
      </div>
    </div>
  );
}


// ── Main Page ──
export default function ConnectorsPage({ isOpen, onClose }) {
  const [connectors, setConnectors] = useState([]);
  const [search, setSearch] = useState('');
  const [category, setCategory] = useState('all');
  const [selectedIntegration, setSelectedIntegration] = useState(null);
  const [showAddSource, setShowAddSource] = useState(false);
  const [addSourceType, setAddSourceType] = useState(null);
  const [recentlyConnected, setRecentlyConnected] = useState(new Set());
  const gridRef = useRef(null);

  useEffect(() => {
    if (isOpen) {
      fetch(`${API}/api/connectors`).then(r => r.json()).then(d => setConnectors(d.connectors || [])).catch(() => {});
      setRecentlyConnected(new Set());
    }
  }, [isOpen]);

  const connected = connectors.filter(c => c.connected);

  const filteredIntegrations = useMemo(() => {
    let list = INTEGRATIONS;
    if (category !== 'all') list = list.filter(i => i.cat === category);
    if (search.trim()) {
      const q = search.toLowerCase();
      list = list.filter(i => i.name.toLowerCase().includes(q) || i.authType.toLowerCase().includes(q) || i.desc.toLowerCase().includes(q));
    }
    return list;
  }, [search, category]);

  const handleCardClick = (integration) => {
    if (integration.isDataSource) setAddSourceType(integration);
    else setSelectedIntegration(integration);
  };

  const handleConnected = (integration) => {
    setRecentlyConnected(prev => new Set([...prev, integration.id]));
  };

  const catCounts = useMemo(() => {
    const counts = { all: INTEGRATIONS.length };
    INTEGRATIONS.forEach(i => { counts[i.cat] = (counts[i.cat] || 0) + 1; });
    return counts;
  }, []);

  if (!isOpen) return null;

  return (
    <div className="cx-page-overlay">
      <div className="cx-page">
        {/* ─── Top Bar ─── */}
        <header className="cx-topbar">
          <div className="cx-topbar-left">
            <div className="cx-logo-mark">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" stroke="url(#cxg)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/><defs><linearGradient id="cxg" x1="2" y1="2" x2="22" y2="22"><stop stopColor="#818cf8"/><stop offset="1" stopColor="#6366f1"/></linearGradient></defs></svg>
            </div>
            <h1 className="cx-page-title">Connectors</h1>
            <span className="cx-page-count">{INTEGRATIONS.length} available</span>
          </div>
          <div className="cx-topbar-right">
            <div className="cx-security-chip">
              <span className="cx-security-dot" />
              Encrypted · SOC 2
            </div>
            <button className="cx-close" onClick={onClose}>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6L6 18M6 6l12 12"/></svg>
            </button>
          </div>
        </header>

        {/* ─── Connected Sources ─── */}
        <section className="cx-connected-strip">
          <div className="cx-connected-label">
            <span className="cx-pulse" />
            Active Connections
          </div>
          <div className="cx-connected-cards">
            {connected.map(c => {
              const match = INTEGRATIONS.find(i => c.name.toLowerCase().includes(i.name.split(' ')[0].toLowerCase()));
              return (
                <div key={c.source_id} className="cx-connected-chip">
                  <div className="cx-connected-chip-icon">
                    {match ? <BrandIcon src={match.logo} name={c.name} size={18} /> : <span>🗄</span>}
                  </div>
                  <div className="cx-connected-chip-info">
                    <span className="cx-connected-chip-name">{c.name}</span>
                    <span className="cx-connected-chip-status">● Connected</span>
                  </div>
                </div>
              );
            })}
            {connected.length === 0 && (
              <span className="cx-no-connections">No active connections yet</span>
            )}
          </div>
          <button className="cx-add-btn" onClick={() => setShowAddSource(true)}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M12 5v14M5 12h14"/></svg>
            Add Source
          </button>
        </section>

        {/* ─── Search + Filters ─── */}
        <div className="cx-toolbar">
          <div className="cx-search-bar">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
            <input type="text" placeholder="Search connectors..." value={search} onChange={e => setSearch(e.target.value)} />
            {search && <button className="cx-search-clear" onClick={() => setSearch('')}>✕</button>}
          </div>
          <div className="cx-cats">
            {CATEGORIES.map(cat => (
              <button
                key={cat.id}
                className={`cx-cat ${category === cat.id ? 'cx-cat-active' : ''}`}
                onClick={() => setCategory(cat.id)}
              >
                <span className="cx-cat-icon">{cat.icon}</span>
                {cat.label}
                <span className="cx-cat-count">{catCounts[cat.id] || 0}</span>
              </button>
            ))}
          </div>
        </div>

        {/* ─── Integration Grid ─── */}
        <div className="cx-grid" ref={gridRef}>
          {filteredIntegrations.map((intg, idx) => {
            const isConn = recentlyConnected.has(intg.id) || connectors.some(c => c.connected && c.name.toLowerCase().includes(intg.name.split(' ')[0].toLowerCase()));
            return (
              <div
                key={intg.id}
                className={`cx-card ${isConn ? 'cx-card-connected' : ''}`}
                style={{ '--card-color': intg.color, '--card-delay': `${idx * 30}ms` }}
                onClick={() => !isConn && handleCardClick(intg)}
              >
                <div className="cx-card-glow" />
                <div className="cx-card-content">
                  <div className="cx-card-top">
                    <div className="cx-card-icon">
                      <BrandIcon src={intg.logo} name={intg.name} size={24} />
                    </div>
                    {isConn && <span className="cx-card-connected-badge">✓ Connected</span>}
                  </div>
                  <h3 className="cx-card-name">{intg.name}</h3>
                  <p className="cx-card-desc">{intg.desc?.split('.')[0]}.</p>
                  <div className="cx-card-footer">
                    <div className="cx-card-tags">
                      <span className="cx-card-tag">{intg.authType}</span>
                      <span className="cx-card-tag">{intg.scope}</span>
                    </div>
                    <button
                      className={`cx-card-btn ${isConn ? 'cx-card-btn-done' : ''}`}
                      onClick={e => { e.stopPropagation(); if (!isConn) handleCardClick(intg); }}
                      disabled={isConn}
                    >
                      {isConn ? 'Active' : 'Connect'}
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {filteredIntegrations.length === 0 && (
          <div className="cx-empty">
            <span className="cx-empty-icon">🔍</span>
            <p>No connectors match your search</p>
            <button className="cx-btn-ghost" onClick={() => { setSearch(''); setCategory('all'); }}>Clear filters</button>
          </div>
        )}
      </div>

      {/* ─── Modals ─── */}
      {showAddSource && <SourcePickerModal onClose={() => setShowAddSource(false)} onSelect={ds => { setShowAddSource(false); setAddSourceType(ds); }} />}
      {addSourceType && <AddDataSourceModal integration={addSourceType} onClose={() => setAddSourceType(null)} onConnect={handleConnected} />}
      {selectedIntegration && <IntegrationDetailModal integration={selectedIntegration} onClose={() => setSelectedIntegration(null)} onConnect={handleConnected} />}
    </div>
  );
}
