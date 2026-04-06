import { useState, useEffect } from 'react';

export default function ArtifactInspector({ artifacts }) {
  const [expandedIds, setExpandedIds] = useState(new Set());

  // Auto-expand all artifacts when they arrive
  useEffect(() => {
    if (artifacts && artifacts.length > 0) {
      setExpandedIds(new Set(artifacts.map(a => a.step_id)));
    }
  }, [artifacts]);

  const toggle = (id) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  if (!artifacts || artifacts.length === 0) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>📦 Artifacts</h2>
        </div>
        <div className="panel-body">
          <div className="artifacts-empty">
            <div className="artifacts-empty-icon">📦</div>
            <p><strong>Execution artifacts will appear here</strong></p>
            <p style={{ fontSize: '12px', marginTop: -4 }}>
              Each step produces a traceable output artifact
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>📦 Artifacts</h2>
        <span className="panel-badge">{artifacts.length}</span>
      </div>
      <div className="panel-body">
        <div className="artifacts-list">
          {artifacts.map((artifact) => (
            <div key={artifact.step_id} className="artifact-card">
              <div
                className="artifact-card-header"
                onClick={() => toggle(artifact.step_id)}
              >
                <h4>
                  <span className={`status-badge ${artifact.status}`}>
                    {artifact.status}
                  </span>
                  Step {artifact.step_id}: {artifact.description}
                </h4>
                <div className="artifact-card-meta">
                  {artifact.row_count != null && (
                    <span>{artifact.row_count} rows</span>
                  )}
                  <span>{artifact.execution_time_ms}ms</span>
                </div>
              </div>

              {expandedIds.has(artifact.step_id) && (
                <div className="artifact-card-body">
                  {artifact.error ? (
                    <div className="artifact-error">⚠️ {artifact.error}</div>
                  ) : artifact.columns && Array.isArray(artifact.data) ? (
                    <table className="artifact-table">
                      <thead>
                        <tr>
                          {artifact.columns.map((col, i) => (
                            <th key={i}>{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {artifact.data.slice(0, 50).map((row, ri) => (
                          <tr key={ri}>
                            {artifact.columns.map((col, ci) => (
                              <td key={ci}>
                                {row[col] != null ? String(row[col]) : '—'}
                              </td>
                            ))}
                          </tr>
                        ))}
                        {artifact.data.length > 50 && (
                          <tr>
                            <td
                              colSpan={artifact.columns.length}
                              style={{ textAlign: 'center', color: 'var(--text-muted)' }}
                            >
                              ... and {artifact.data.length - 50} more rows
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  ) : (
                    <div className="artifact-text">
                      {typeof artifact.data === 'string'
                        ? artifact.data
                        : JSON.stringify(artifact.data, null, 2)}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
