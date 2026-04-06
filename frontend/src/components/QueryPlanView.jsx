export default function QueryPlanView({ plan }) {
  if (!plan) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>📋 Query Plan</h2>
        </div>
        <div className="panel-body">
          <div className="plan-empty">
            <div className="plan-empty-icon">📋</div>
            <p><strong>Query plan will appear here</strong></p>
            <p style={{ fontSize: '12px', marginTop: -4 }}>
              The AI generates a step-by-step execution plan for each question
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>📋 Query Plan</h2>
        <span className="panel-badge">{plan.steps.length} steps</span>
      </div>
      <div className="panel-body">
        {plan.reasoning && (
          <div className="plan-reasoning">
            <strong>💭 AI Reasoning</strong>
            {plan.reasoning}
          </div>
        )}

        <div className="plan-steps">
          {plan.steps.map((step, index) => (
            <div
              key={step.step_id}
              className={`plan-step ${step.status === 'running' ? 'active' : ''}`}
            >
              <div className={`step-indicator ${step.status}`}>
                {step.status === 'completed' ? '✓' :
                 step.status === 'failed' ? '✗' :
                 step.status === 'running' ? '●' :
                 index + 1}
              </div>
              <div className="step-details">
                <div className="step-desc">{step.description}</div>
                <span className="step-type">{step.step_type}</span>
                {step.sql && (
                  <div className="step-sql">{step.sql}</div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
