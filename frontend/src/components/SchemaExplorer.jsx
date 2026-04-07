import { useState, useEffect } from 'react';

const API = 'http://localhost:8000';

export default function SchemaExplorer({ schema, isOpen, onClose, initialTab }) {
  const [collapsed, setCollapsed] = useState(new Set());
  const [connectors, setConnectors] = useState([]);
  const [sources, setSources] = useState([]);
  const [metadata, setMetadata] = useState(null);
  const [activeTab, setActiveTab] = useState(initialTab || 'connectors');
  const [isGenerating, setIsGenerating] = useState(false);

  // Sync tab when opened from sidebar nav
  useEffect(() => {
    if (isOpen && initialTab) {
      setActiveTab(initialTab);
    }
  }, [isOpen, initialTab]);

  useEffect(() => {
    if (isOpen) {
      fetch(`${API}/api/connectors`)
        .then(r => r.json())
        .then(data => setConnectors(data.connectors || []))
        .catch(() => {});

      fetch(`${API}/api/schema/sources`)
        .then(r => r.json())
        .then(data => setSources(data.sources || []))
        .catch(() => {});

      fetch(`${API}/api/metadata`)
        .then(r => r.json())
        .then(data => setMetadata(data))
        .catch(() => {});
    }
  }, [isOpen]);

  // Close on Escape key
  useEffect(() => {
    const handleKey = (e) => { if (e.key === 'Escape' && isOpen) onClose(); };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isOpen, onClose]);

  const toggleTable = (name) => {
    setCollapsed(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const handleAutoGenerate = async () => {
    setIsGenerating(true);
    try {
      const resp = await fetch(`${API}/api/metadata/generate`, { method: 'POST' });
      const data = await resp.json();
      setMetadata(data.metadata);
    } catch (err) {
      console.error('Failed to generate metadata:', err);
    } finally {
      setIsGenerating(false);
    }
  };

  const allTables = schema?.tables || [];

  return (
    <div className={`schema-drawer ${isOpen ? 'open' : ''}`}>
      <div className="schema-drawer-header">
        <button className="cx-back-btn" onClick={onClose} title="Back to Chat (Esc)">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M19 12H5"/><path d="M12 19l-7-7 7-7"/></svg>
          <span>Chat</span>
        </button>
        <h2>🗄️ Data Sources</h2>
        <button className="schema-close-btn" onClick={onClose}>×</button>
      </div>

      {/* Tabs */}
      <div className="schema-tabs">
        <button
          className={`schema-tab ${activeTab === 'connectors' ? 'active' : ''}`}
          onClick={() => setActiveTab('connectors')}
        >
          Connectors
        </button>
        <button
          className={`schema-tab ${activeTab === 'metadata' ? 'active' : ''}`}
          onClick={() => setActiveTab('metadata')}
        >
          Metadata
        </button>
        <button
          className={`schema-tab ${activeTab === 'schema' ? 'active' : ''}`}
          onClick={() => setActiveTab('schema')}
        >
          Schema
        </button>
      </div>

      <div className="schema-drawer-body">
        {/* ── Connectors Tab ── */}
        {activeTab === 'connectors' && (
          <div className="connectors-list">
            {connectors.length === 0 ? (
              <p style={{ color: 'var(--text-muted)' }}>Loading connectors...</p>
            ) : (
              connectors.map((c) => (
                <div key={c.source_id} className="connector-card">
                  <div className="connector-header">
                    <span className="connector-icon">{c.icon}</span>
                    <div className="connector-info">
                      <span className="connector-name">{c.name}</span>
                      <span className="connector-type">{c.type}</span>
                    </div>
                    <span className={`connector-status ${c.connected ? 'on' : 'off'}`}>
                      {c.connected ? '● Connected' : '○ Disconnected'}
                    </span>
                  </div>
                  <div className="connector-message">{c.message}</div>
                </div>
              ))
            )}

            <div className="connectors-help">
              <strong>How to add connectors</strong>
              <p>Set environment variables in <code>.env</code>:</p>
              <div className="connector-env-examples">
                <div className="env-item">
                  <span>🐘 PostgreSQL</span>
                  <code>POSTGRES_URL=postgresql://...</code>
                </div>
                <div className="env-item">
                  <span>⚡ ClickHouse</span>
                  <code>CLICKHOUSE_URL=clickhouse://...</code>
                </div>
                <div className="env-item">
                  <span>❄️ Snowflake</span>
                  <code>SNOWFLAKE_ACCOUNT + USER + PASSWORD + WAREHOUSE + DATABASE</code>
                </div>
                <div className="env-item">
                  <span>🧱 Databricks</span>
                  <code>DATABRICKS_HOST + HTTP_PATH + ACCESS_TOKEN</code>
                </div>
                <div className="env-item">
                  <span>🔴 Redshift</span>
                  <code>REDSHIFT_HOST + DATABASE + USER + PASSWORD</code>
                </div>
                <div className="env-item">
                  <span>📊 Google Analytics</span>
                  <code>GA_PROPERTY_ID + GA_CREDENTIALS_JSON</code>
                </div>
                <div className="env-item">
                  <span>☁️ Salesforce</span>
                  <code>SALESFORCE_INSTANCE_URL + ACCESS_TOKEN</code>
                </div>
                <div className="env-item">
                  <span>📧 Marketo</span>
                  <code>MARKETO_ENDPOINT + CLIENT_ID + SECRET</code>
                </div>
                <div className="env-item">
                  <span>📄 Google Docs</span>
                  <code>GOOGLE_DOCS_CREDENTIALS_JSON</code>
                </div>
                <div className="env-item">
                  <span>💬 Slack</span>
                  <code>SLACK_BOT_TOKEN=xoxb-...</code>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ── Metadata Tab ── */}
        {activeTab === 'metadata' && (
          <div className="metadata-panel">
            <div className="metadata-header-actions">
              <button
                className="metadata-action-btn"
                onClick={handleAutoGenerate}
                disabled={isGenerating}
              >
                {isGenerating ? '⏳ Generating...' : '🤖 Auto-Generate with AI'}
              </button>
            </div>

            {!metadata || !metadata.sources || Object.keys(metadata.sources).length === 0 ? (
              <div className="metadata-empty">
                <span className="metadata-empty-icon">🧠</span>
                <h3>No Semantic Metadata Yet</h3>
                <p>Metadata enriches your schema with business context, making the AI smarter about your data.</p>
                <button className="metadata-generate-btn" onClick={handleAutoGenerate} disabled={isGenerating}>
                  {isGenerating ? 'Generating...' : 'Generate Metadata with AI'}
                </button>
              </div>
            ) : (
              <>
                {/* Global rules */}
                {metadata.global_rules && metadata.global_rules.length > 0 && (
                  <div className="metadata-section">
                    <div className="metadata-section-title">⚡ Global Business Rules</div>
                    {metadata.global_rules.map((rule, i) => (
                      <div key={i} className="metadata-rule">• {rule}</div>
                    ))}
                  </div>
                )}

                {/* Global synonyms */}
                {metadata.global_synonyms && Object.keys(metadata.global_synonyms).length > 0 && (
                  <div className="metadata-section">
                    <div className="metadata-section-title">📖 Global Synonyms</div>
                    {Object.entries(metadata.global_synonyms).map(([term, def_]) => (
                      <div key={term} className="metadata-synonym">
                        <span className="synonym-term">"{term}"</span>
                        <span className="synonym-arrow">→</span>
                        <span className="synonym-def">{def_}</span>
                      </div>
                    ))}
                  </div>
                )}

                {/* Per-source metadata */}
                {Object.entries(metadata.sources).map(([sourceId, source]) => (
                  <div key={sourceId} className="metadata-source">
                    <div className="metadata-source-header">
                      {sourceId}
                      {source.description && (
                        <span className="metadata-source-desc">{source.description}</span>
                      )}
                    </div>

                    {/* Tables */}
                    {source.tables && Object.entries(source.tables).map(([tableName, table]) => (
                      <div key={tableName} className="metadata-table">
                        <div
                          className="metadata-table-name"
                          onClick={() => toggleTable(`meta-${tableName}`)}
                        >
                          <span>{collapsed.has(`meta-${tableName}`) ? '▶' : '▼'}</span>
                          {tableName}
                          {table.business_name && (
                            <span className="metadata-biz-name">{table.business_name}</span>
                          )}
                        </div>
                        {table.description && (
                          <div className="metadata-table-desc">{table.description}</div>
                        )}

                        {!collapsed.has(`meta-${tableName}`) && (
                          <div className="metadata-table-details">
                            {/* Column descriptions */}
                            {table.columns && Object.keys(table.columns).length > 0 && (
                              <div className="metadata-cols">
                                {Object.entries(table.columns).map(([col, desc]) => (
                                  <div key={col} className="metadata-col">
                                    <span className="metadata-col-name">{col}</span>
                                    <span className="metadata-col-desc">
                                      {typeof desc === 'string' ? desc : desc.description || ''}
                                    </span>
                                  </div>
                                ))}
                              </div>
                            )}

                            {/* Business rules */}
                            {table.business_rules && table.business_rules.length > 0 && (
                              <div className="metadata-rules">
                                <span className="metadata-rules-label">📋 Rules</span>
                                {table.business_rules.map((rule, i) => (
                                  <div key={i} className="metadata-rule">⚠️ {rule}</div>
                                ))}
                              </div>
                            )}

                            {/* Common queries */}
                            {table.common_queries && table.common_queries.length > 0 && (
                              <div className="metadata-queries">
                                <span className="metadata-rules-label">💡 Common Queries</span>
                                {table.common_queries.map((q, i) => (
                                  <div key={i} className="metadata-query">{q}</div>
                                ))}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    ))}

                    {/* Source synonyms */}
                    {source.synonyms && Object.keys(source.synonyms).length > 0 && (
                      <div className="metadata-section" style={{ marginTop: 12 }}>
                        <div className="metadata-section-title">📖 Synonyms</div>
                        {Object.entries(source.synonyms).map(([term, def_]) => (
                          <div key={term} className="metadata-synonym">
                            <span className="synonym-term">"{term}"</span>
                            <span className="synonym-arrow">→</span>
                            <span className="synonym-def">{def_}</span>
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Glossary */}
                    {source.glossary && Object.keys(source.glossary).length > 0 && (
                      <div className="metadata-section" style={{ marginTop: 12 }}>
                        <div className="metadata-section-title">📚 Glossary</div>
                        {Object.entries(source.glossary).map(([term, meaning]) => (
                          <div key={term} className="metadata-glossary-item">
                            <span className="glossary-term">{term}</span>
                            <span className="glossary-def">{meaning}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </>
            )}
          </div>
        )}

        {/* ── Schema Tab ── */}
        {activeTab === 'schema' && (
          <>
            {sources.length > 0 ? (
              sources.map((src) => (
                <div key={src.source_id} className="schema-source-group">
                  <div className="schema-source-header">
                    <span>{src.icon} {src.name}</span>
                    <span className="schema-source-badge">{src.type}</span>
                  </div>
                  {src.tables.length === 0 ? (
                    <p style={{ color: 'var(--text-muted)', fontSize: '12px', paddingLeft: '12px' }}>
                      No tables {src.error ? `(${src.error})` : ''}
                    </p>
                  ) : (
                    src.tables.map((table) => (
                      <div key={`${src.source_id}-${table.name}`} className="schema-table">
                        <div
                          className="schema-table-name"
                          onClick={() => toggleTable(`${src.source_id}-${table.name}`)}
                        >
                          <span className="table-icon">
                            {collapsed.has(`${src.source_id}-${table.name}`) ? '▶' : '▼'}
                          </span>
                          {table.name}
                          <span className="row-count">{table.row_count} rows</span>
                        </div>
                        {!collapsed.has(`${src.source_id}-${table.name}`) && (
                          <div className="schema-columns">
                            {table.columns.map((col) => (
                              <div key={col.name} className="schema-column">
                                <span>{col.name}</span>
                                <span className="col-type">{col.data_type}</span>
                                {col.is_primary_key && <span className="col-badge pk">PK</span>}
                                {col.is_foreign_key && <span className="col-badge fk">FK</span>}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    ))
                  )}
                </div>
              ))
            ) : (
              allTables.map((table) => (
                <div key={table.name} className="schema-table">
                  <div className="schema-table-name" onClick={() => toggleTable(table.name)}>
                    <span className="table-icon">
                      {collapsed.has(table.name) ? '▶' : '▼'}
                    </span>
                    {table.name}
                    <span className="row-count">{table.row_count} rows</span>
                  </div>
                  {!collapsed.has(table.name) && (
                    <div className="schema-columns">
                      {table.columns.map((col) => (
                        <div key={col.name} className="schema-column">
                          <span>{col.name}</span>
                          <span className="col-type">{col.data_type}</span>
                          {col.is_primary_key && <span className="col-badge pk">PK</span>}
                          {col.is_foreign_key && <span className="col-badge fk">FK</span>}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))
            )}
          </>
        )}
      </div>
    </div>
  );
}
