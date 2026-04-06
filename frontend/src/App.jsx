import { useState, useEffect } from 'react';
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

  const handleSend = async (question) => {
    // Auto-create thread if none active
    if (!activeThreadId) {
      const id = crypto.randomUUID();
      const newThread = {
        id,
        title: question.slice(0, 50) + (question.length > 50 ? '...' : ''),
        time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        messages: [],
      };
      setThreads(prev => [newThread, ...prev]);
      setActiveThreadId(id);
    }

    // Update thread title from first message
    if (messages.length === 0) {
      setThreads(prev => prev.map(t =>
        t.id === activeThreadId || (!activeThreadId && prev[0]?.id === t.id)
          ? { ...t, title: question.slice(0, 50) + (question.length > 50 ? '...' : '') }
          : t
      ));
    }

    setMessages(prev => [...prev, { role: 'user', content: question }]);
    setIsLoading(true);

    try {
      const res = await fetch(`${API_BASE}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question,
          thread_id: activeThreadId,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Query failed');
      }

      const data = await res.json();

      // Mark plan steps as completed
      if (data.plan && data.plan.steps) {
        data.plan.steps = data.plan.steps.map(s => ({
          ...s,
          status: data.artifacts.find(a => a.step_id === s.step_id)?.status || 'completed',
        }));
      }

      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: data.answer,
          plan: data.plan,
          artifacts: data.artifacts,
          meta: {
            time: data.total_execution_time_ms,
            retries: data.retries,
            reliability_score: data.reliability_score,
          },
        },
      ]);
    } catch (err) {
      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: `❌ Error: ${err.message}`,
        },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

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
