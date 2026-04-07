import { useState, useEffect, useRef, useCallback } from 'react';
import ChatPanel from './components/ChatPanel';
import ThreadSidebar from './components/ThreadSidebar';
import SchemaExplorer from './components/SchemaExplorer';
import ConnectorsPage from './components/ConnectorsPage';
import './index.css';

const API_BASE = 'http://localhost:8000';

export default function App() {
  const [threads, setThreads] = useState([]);
  const [activeThreadId, setActiveThreadId] = useState(null);
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [schema, setSchema] = useState(null);
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [schemaTab, setSchemaTab] = useState('connectors');
  const [connectorsOpen, setConnectorsOpen] = useState(false);
  const [streamState, setStreamState] = useState(null);
  const abortRef = useRef(null);

  // Fetch schema on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/schema`)
      .then(res => res.json())
      .then(data => setSchema(data))
      .catch(err => console.error('Failed to fetch schema:', err));
  }, []);

  const createNewThread = () => {
    const id = crypto.randomUUID();
    const newThread = {
      id,
      title: 'New conversation',
      time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      messages: [],
    };
    setThreads(prev => [newThread, ...prev]);
    setActiveThreadId(id);
    setMessages([]);
  };

  const selectThread = (id) => {
    // Save current messages
    if (activeThreadId) {
      setThreads(prev => prev.map(t =>
        t.id === activeThreadId ? { ...t, messages } : t
      ));
    }
    const thread = threads.find(t => t.id === id);
    setActiveThreadId(id);
    setMessages(thread?.messages || []);
  };

  const deleteThread = (id) => {
    setThreads(prev => prev.filter(t => t.id !== id));
    if (activeThreadId === id) {
      setActiveThreadId(null);
      setMessages([]);
    }
  };

  const handleOpenPanel = (tab) => {
    if (tab === 'connectors') {
      setConnectorsOpen(true);
      return;
    }
    setSchemaTab(tab);
    setSchemaOpen(true);
  };

  const handleSend = useCallback(async (question) => {
    // Auto-create thread if none active
    let threadId = activeThreadId;
    if (!threadId) {
      const id = crypto.randomUUID();
      const newThread = {
        id,
        title: question.slice(0, 50) + (question.length > 50 ? '...' : ''),
        time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        messages: [],
      };
      setThreads(prev => [newThread, ...prev]);
      setActiveThreadId(id);
      threadId = id;
    }

    // Update thread title from first message
    if (messages.length === 0) {
      setThreads(prev => prev.map(t =>
        t.id === threadId
          ? { ...t, title: question.slice(0, 50) + (question.length > 50 ? '...' : '') }
          : t
      ));
    }

    setMessages(prev => [...prev, { role: 'user', content: question }]);
    setIsLoading(true);
    setStreamState({ phase: 'idle', plan: null, steps: [], completedSteps: [], error: null });

    try {
      // Use SSE streaming endpoint
      const res = await fetch(`${API_BASE}/api/query/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, thread_id: threadId }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Query failed');
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let finalPlan = null;
      let finalArtifacts = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        let eventType = '';
        let eventData = '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            eventData = line.slice(6).trim();
          } else if (line === '' && eventType && eventData) {
            // Process the event
            try {
              const data = JSON.parse(eventData);

              switch (eventType) {
                case 'phase':
                  setStreamState(prev => ({
                    ...prev,
                    phase: data.phase,
                    message: data.message,
                  }));
                  break;

                case 'plan':
                  finalPlan = data;
                  setStreamState(prev => ({
                    ...prev,
                    plan: data,
                    steps: data.steps,
                    phase: 'executing',
                  }));
                  break;

                case 'step_start':
                  setStreamState(prev => {
                    const updated = (prev.steps || []).map(s =>
                      s.step_id === data.step_id ? { ...s, status: 'running' } : s
                    );
                    return { ...prev, steps: updated };
                  });
                  break;

                case 'step_complete':
                  finalArtifacts.push(data);
                  setStreamState(prev => {
                    const updated = (prev.steps || []).map(s =>
                      s.step_id === data.step_id ? { ...s, status: data.status === 'failed' ? 'failed' : 'completed' } : s
                    );
                    return {
                      ...prev,
                      steps: updated,
                      completedSteps: [...(prev.completedSteps || []), data],
                    };
                  });
                  break;

                case 'result':
                  // Final result — add the assistant message
                  setStreamState(prev => ({ ...prev, phase: 'done' }));

                  // Build plan with completed statuses
                  const planForMessage = finalPlan ? {
                    ...finalPlan,
                    steps: finalPlan.steps.map(s => ({
                      ...s,
                      status: finalArtifacts.find(a => a.step_id === s.step_id)?.status || 'completed',
                    })),
                  } : null;

                  setMessages(prev => [
                    ...prev,
                    {
                      role: 'assistant',
                      content: data.answer,
                      plan: planForMessage,
                      artifacts: finalArtifacts,
                      meta: {
                        time: data.total_execution_time_ms,
                        retries: data.retries,
                        reliability_score: data.reliability_score,
                      },
                    },
                  ]);
                  break;

                case 'error':
                  setStreamState(prev => ({ ...prev, error: data.message }));
                  setMessages(prev => [
                    ...prev,
                    { role: 'assistant', content: `❌ Error: ${data.message}` },
                  ]);
                  break;
              }
            } catch (parseErr) {
              console.error('Failed to parse SSE data:', parseErr);
            }
            eventType = '';
            eventData = '';
          }
        }
      }
    } catch (err) {
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: `❌ Error: ${err.message}` },
      ]);
    } finally {
      setIsLoading(false);
      setStreamState(null);
    }
  }, [activeThreadId, messages.length]);

  // Sync messages back to thread on change
  useEffect(() => {
    if (activeThreadId && messages.length > 0) {
      setThreads(prev => prev.map(t =>
        t.id === activeThreadId ? { ...t, messages } : t
      ));
    }
  }, [messages, activeThreadId]);

  return (
    <div className="app-layout">
      <ThreadSidebar
        threads={threads}
        activeThreadId={activeThreadId}
        onSelect={selectThread}
        onNew={createNewThread}
        onDelete={deleteThread}
        onOpenPanel={handleOpenPanel}
      />

      <div className="app-main">
        <ChatPanel
          messages={messages}
          onSend={handleSend}
          isLoading={isLoading}
          streamState={streamState}
        />
      </div>

      <SchemaExplorer
        schema={schema}
        isOpen={schemaOpen}
        onClose={() => setSchemaOpen(false)}
        initialTab={schemaTab}
      />

      <ConnectorsPage
        isOpen={connectorsOpen}
        onClose={() => setConnectorsOpen(false)}
      />
    </div>
  );
}
