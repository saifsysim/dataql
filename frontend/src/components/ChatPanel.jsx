import { useState, useRef, useEffect } from 'react';

const API_BASE = 'http://localhost:8000';

const SUGGESTIONS = [
  { icon: '👥', text: "How many customers do we have?" },
  { icon: '💰', text: "What is the total revenue by product category?" },
  { icon: '🏆', text: "Show me the top 5 customers by total spending" },
  { icon: '📦', text: "Which products are running low on stock?" },
];

function ReliabilityBadge({ score }) {
  if (!score) return null;
  const colors = {
    high: { bg: 'rgba(52, 211, 153, 0.12)', color: '#34d399', icon: '●' },
    medium: { bg: 'rgba(251, 191, 36, 0.12)', color: '#fbbf24', icon: '●' },
    low: { bg: 'rgba(248, 113, 113, 0.12)', color: '#f87171', icon: '●' },
    unreliable: { bg: 'rgba(248, 113, 113, 0.15)', color: '#f87171', icon: '⚠' },
  };
  const c = colors[score.label] || colors.medium;
  return (
    <span className="reliability-badge" style={{ background: c.bg, color: c.color }}>
      {c.icon} Reliability: {score.score}/100
    </span>
  );
}

function InlineQueryPlan({ plan, isExpanded, onToggle }) {
  if (!plan) return null;
  return (
    <div className="inline-block plan-block">
      <div className="inline-block-header" onClick={onToggle}>
        <span className="inline-block-icon">💡</span>
        <span className="inline-block-title">Query Plan</span>
        <span className="inline-block-status">
          {plan.steps.length} steps
        </span>
        <span className={`inline-chevron ${isExpanded ? 'open' : ''}`}>▾</span>
      </div>
      {isExpanded && (
        <div className="inline-block-body">
          {plan.reasoning && (
            <div className="inline-reasoning">{plan.reasoning}</div>
          )}
          <div className="inline-steps">
            {plan.steps.map((step, i) => (
              <div key={step.step_id} className="inline-step">
                <div className={`inline-step-num ${step.status || 'completed'}`}>
                  {step.status === 'completed' ? '✓' : step.status === 'failed' ? '✗' : i + 1}
                </div>
                <div className="inline-step-content">
                  <span className="inline-step-desc">{step.description}</span>
                  <span className="inline-step-type">{step.step_type}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function InlineExecution({ artifacts, activeTab, onTabChange }) {
  if (!artifacts || artifacts.length === 0) return null;

  return (
    <div className="inline-block execution-block">
      <div className="inline-block-header">
        <span className="inline-block-icon">⚡</span>
        <span className="inline-block-title">Executed</span>
        <span className="inline-block-status" style={{ color: '#34d399' }}>
          Completed in {artifacts.reduce((s, a) => s + (a.execution_time_ms || 0), 0).toFixed(0)}ms
        </span>
      </div>

      <div className="execution-tabs">
        <button
          className={`execution-tab ${activeTab === 'code' ? 'active' : ''}`}
          onClick={() => onTabChange('code')}
        >
          &lt;/&gt; Code
        </button>
        <button
          className={`execution-tab ${activeTab === 'output' ? 'active' : ''}`}
          onClick={() => onTabChange('output')}
        >
          📊 Output
        </button>
      </div>

      <div className="execution-content">
        {activeTab === 'code' ? (
          <div className="execution-code">
            {artifacts.map((a, i) => (
              a.step_id && (
                <div key={i} className="code-block-wrapper">
                  <div className="code-block-label">Step {a.step_id}: {a.description}</div>
                  <pre className="code-block">
                    {artifacts
                      .filter(art => art.step_id === a.step_id)
                      .map(art => {
                        return art.columns
                          ? `-- Result: ${art.row_count || 0} rows, ${art.columns.length} columns\nSELECT ${art.columns.join(', ')}\n-- Executed in ${art.execution_time_ms}ms`
                          : `-- ${art.description}\n-- Executed in ${art.execution_time_ms}ms`;
                      })
                      .join('\n')}
                  </pre>
                </div>
              )
            ))}
          </div>
        ) : (
          <div className="execution-output">
            {artifacts.map((artifact, i) => (
              <div key={i} className="output-section">
                <div className="output-section-header">
                  <span className={`output-status ${artifact.status}`}>
                    {artifact.status}
                  </span>
                  <span className="output-label">
                    Step {artifact.step_id}: {artifact.description}
                  </span>
                  <span className="output-meta">
                    {artifact.row_count != null && `${artifact.row_count} rows · `}
                    {artifact.execution_time_ms}ms
                  </span>
                </div>
                {artifact.error ? (
                  <div className="output-error">⚠️ {artifact.error}</div>
                ) : artifact.columns && Array.isArray(artifact.data) ? (
                  <div className="output-table-wrap">
                    <table className="output-table">
                      <thead>
                        <tr>
                          {artifact.columns.map((col, ci) => (
                            <th key={ci}>{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {artifact.data.slice(0, 25).map((row, ri) => (
                          <tr key={ri}>
                            {artifact.columns.map((col, ci) => (
                              <td key={ci}>{row[col] != null ? String(row[col]) : '—'}</td>
                            ))}
                          </tr>
                        ))}
                        {artifact.data.length > 25 && (
                          <tr>
                            <td colSpan={artifact.columns.length} className="output-more">
                              ... and {artifact.data.length - 25} more rows
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="output-text">
                    {typeof artifact.data === 'string'
                      ? artifact.data
                      : JSON.stringify(artifact.data, null, 2)}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Live Query Building Process ──────────────────────────

function LiveQueryProcess({ streamState }) {
  if (!streamState) return null;

  const { phase, plan, steps, completedSteps, error } = streamState;

  return (
    <div className="live-process">
      {/* Phase indicator */}
      <div className="live-phases">
        <PhaseStep
          label="Schema"
          icon="🔍"
          status={phase === 'schema' ? 'active' : (phase !== 'idle' ? 'done' : 'pending')}
        />
        <div className="live-phase-connector" />
        <PhaseStep
          label="Planning"
          icon="🧠"
          status={phase === 'planning' ? 'active' : (['executing', 'summarizing', 'done'].includes(phase) ? 'done' : 'pending')}
        />
        <div className="live-phase-connector" />
        <PhaseStep
          label="Executing"
          icon="⚡"
          status={phase === 'executing' ? 'active' : (['summarizing', 'done'].includes(phase) ? 'done' : 'pending')}
        />
        <div className="live-phase-connector" />
        <PhaseStep
          label="Results"
          icon="✨"
          status={phase === 'summarizing' ? 'active' : (phase === 'done' ? 'done' : 'pending')}
        />
      </div>

      {/* Live reasoning */}
      {plan?.reasoning && (
        <div className="live-reasoning">
          <div className="live-reasoning-label">
            <span className="live-dot" />
            AI Reasoning
          </div>
          <p>{plan.reasoning}</p>
        </div>
      )}

      {/* Live steps */}
      {steps && steps.length > 0 && (
        <div className="live-steps">
          {steps.map((step) => {
            const completed = completedSteps?.find(c => c.step_id === step.step_id);
            const isRunning = !completed && phase === 'executing';
            const isPending = !completed && !isRunning;
            const stepStatus = completed
              ? (completed.status === 'failed' ? 'failed' : 'done')
              : isRunning ? 'running' : 'pending';

            return (
              <div key={step.step_id} className={`live-step live-step-${stepStatus}`}>
                <div className="live-step-indicator">
                  {stepStatus === 'done' ? (
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#34d399" strokeWidth="3"><path d="M20 6L9 17l-5-5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                  ) : stepStatus === 'failed' ? (
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#f87171" strokeWidth="3"><path d="M18 6L6 18M6 6l12 12" strokeLinecap="round"/></svg>
                  ) : stepStatus === 'running' ? (
                    <div className="live-step-spinner" />
                  ) : (
                    <div className="live-step-pending-dot" />
                  )}
                </div>
                <div className="live-step-body">
                  <div className="live-step-desc">{step.description}</div>
                  <div className="live-step-meta">
                    <span className="live-step-type">{step.step_type}</span>
                    {step.sql && <code className="live-step-sql">{step.sql}</code>}
                    {completed?.execution_time_ms != null && (
                      <span className="live-step-time">{completed.execution_time_ms}ms</span>
                    )}
                    {completed?.row_count != null && (
                      <span className="live-step-rows">{completed.row_count} rows</span>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {error && (
        <div className="live-error">❌ {error}</div>
      )}
    </div>
  );
}

function PhaseStep({ label, icon, status }) {
  return (
    <div className={`live-phase live-phase-${status}`}>
      <span className="live-phase-icon">{icon}</span>
      <span className="live-phase-label">{label}</span>
      {status === 'active' && <div className="live-phase-pulse" />}
    </div>
  );
}

// ── Main Chat Panel ──────────────────────────────────

export default function ChatPanel({ messages, onSend, isLoading, streamState }) {
  const [input, setInput] = useState('');
  const [expandedPlans, setExpandedPlans] = useState(new Set());
  const [executionTabs, setExecutionTabs] = useState({}); // msgIndex -> 'code' | 'output'
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamState]);

  useEffect(() => {
    if (!isLoading && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isLoading]);

  const togglePlan = (idx) => {
    setExpandedPlans(prev => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    onSend(input.trim());
    setInput('');
  };

  const handleSuggestion = (text) => {
    if (isLoading) return;
    onSend(text);
  };

  return (
    <div className="playground">
      {/* Toolbar */}
      <div className="playground-toolbar">
        <div className="playground-toolbar-left">
          <span className="playground-project-name">DataQL Playground</span>
          <span className="playground-project-tag">Interactive</span>
        </div>
        <div className="playground-toolbar-right">
          <button className="toolbar-btn" title="Share">
            ↗ Share
          </button>
        </div>
      </div>

      {/* Messages */}
      <div className="playground-messages">
        {messages.length === 0 && !isLoading ? (
          <div className="playground-empty">
            <div className="playground-empty-icon">⚡</div>
            <h2>Talk to your data</h2>
            <p>
              Ask a question in plain English. DataQL will plan, execute, and return
              results with full transparency into every step.
            </p>
            <div className="playground-suggestions">
              {SUGGESTIONS.map((s, i) => (
                <button
                  key={i}
                  className="playground-suggestion"
                  onClick={() => handleSuggestion(s.text)}
                >
                  <span className="suggestion-icon">{s.icon}</span>
                  {s.text}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="playground-thread">
            {messages.map((msg, i) => (
              <div key={i} className={`playground-msg ${msg.role}`}>
                <div className="playground-msg-avatar">
                  {msg.role === 'user' ? (
                    <div className="avatar-user">
                      <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
                        <path strokeLinecap="round" d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2" />
                        <circle cx="12" cy="7" r="4" />
                      </svg>
                    </div>
                  ) : (
                    <div className="avatar-ai">QL</div>
                  )}
                </div>
                <div className="playground-msg-body">
                  {msg.role === 'user' ? (
                    <div className="user-bubble">{msg.content}</div>
                  ) : (
                    <div className="ai-response">
                      {/* Query Plan (inline, collapsible) */}
                      {msg.plan && (
                        <InlineQueryPlan
                          plan={msg.plan}
                          isExpanded={expandedPlans.has(i)}
                          onToggle={() => togglePlan(i)}
                        />
                      )}

                      {/* Execution (inline, with Code/Output tabs) */}
                      {msg.artifacts && msg.artifacts.length > 0 && (
                        <InlineExecution
                          artifacts={msg.artifacts}
                          activeTab={executionTabs[i] || 'output'}
                          onTabChange={(tab) => setExecutionTabs(prev => ({ ...prev, [i]: tab }))}
                        />
                      )}

                      {/* Answer */}
                      <div className="ai-answer">{msg.content}</div>

                      {/* Footer: time + reliability */}
                      <div className="ai-footer">
                        {msg.meta?.time && (
                          <span className="ai-time">⏱ {msg.meta.time}ms</span>
                        )}
                        {msg.meta?.retries > 0 && (
                          <span className="ai-retry">🔄 {msg.meta.retries} retry</span>
                        )}
                        <ReliabilityBadge score={msg.meta?.reliability_score} />
                      </div>
                    </div>
                  )}
                </div>
              </div>
            ))}

            {/* Live streaming state */}
            {isLoading && streamState && (
              <div className="playground-msg assistant">
                <div className="playground-msg-avatar">
                  <div className="avatar-ai">QL</div>
                </div>
                <div className="playground-msg-body">
                  <div className="ai-response">
                    <LiveQueryProcess streamState={streamState} />
                  </div>
                </div>
              </div>
            )}

            {/* Fallback loading (no stream data yet) */}
            {isLoading && !streamState && (
              <div className="playground-msg assistant">
                <div className="playground-msg-avatar">
                  <div className="avatar-ai">QL</div>
                </div>
                <div className="playground-msg-body">
                  <div className="ai-response">
                    <div className="ai-thinking">
                      <div className="thinking-pulse"></div>
                      Connecting to DataQL...
                    </div>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>

      {/* Input */}
      <div className="playground-input-area">
        <form onSubmit={handleSubmit} className="playground-input-wrapper">
          <input
            ref={inputRef}
            className="playground-input"
            type="text"
            placeholder="Let's make your data do stuff..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={isLoading}
            autoFocus
          />
          <div className="playground-input-actions">
            <button
              type="submit"
              className="playground-send-btn"
              disabled={!input.trim() || isLoading}
            >
              <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2.5">
                <path strokeLinecap="round" strokeLinejoin="round" d="M5 12h14M12 5l7 7-7 7" />
              </svg>
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
