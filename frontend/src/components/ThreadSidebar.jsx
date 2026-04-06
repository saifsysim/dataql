import { useState } from 'react';

export default function ThreadSidebar({ threads, activeThreadId, onSelect, onNew, onDelete, onOpenPanel }) {
  const [searchQuery, setSearchQuery] = useState('');

  const filteredThreads = threads.filter(t =>
    t.title.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <div className="sidebar-logo">
          <div className="sidebar-logo-icon">QL</div>
          <span className="sidebar-logo-text">DataQL</span>
        </div>
        <button className="sidebar-new-btn" onClick={onNew} title="New Thread">
          <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
            <path strokeLinecap="round" d="M12 5v14M5 12h14" />
          </svg>
        </button>
      </div>

      {/* Quick Actions */}
      <div className="sidebar-nav">
        <button className="sidebar-nav-btn" onClick={() => onOpenPanel('connectors')}>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
            <path strokeLinecap="round" d="M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7C5 4 4 5 4 7z" />
            <path strokeLinecap="round" d="M9 4v16M4 9h5M4 15h5" />
          </svg>
          Connectors
        </button>
        <button className="sidebar-nav-btn" onClick={() => onOpenPanel('schema')}>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
            <path strokeLinecap="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
          </svg>
          Schema
        </button>
        <button className="sidebar-nav-btn" onClick={() => onOpenPanel('metadata')}>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
            <path strokeLinecap="round" d="M9.663 17h4.674M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
          </svg>
          Metadata
        </button>
      </div>

      <div className="sidebar-search">
        <input
          type="text"
          placeholder="Search threads..."
          value={searchQuery}
          onChange={e => setSearchQuery(e.target.value)}
          className="sidebar-search-input"
        />
      </div>

      <div className="sidebar-threads">
        {filteredThreads.length === 0 ? (
          <div className="sidebar-empty">
            <p>No conversations yet</p>
          </div>
        ) : (
          filteredThreads.map(thread => (
            <div
              key={thread.id}
              className={`sidebar-thread ${thread.id === activeThreadId ? 'active' : ''}`}
              onClick={() => onSelect(thread.id)}
            >
              <div className="sidebar-thread-icon">
                <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
                  <path strokeLinecap="round" d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" />
                </svg>
              </div>
              <div className="sidebar-thread-content">
                <div className="sidebar-thread-title">{thread.title}</div>
                <div className="sidebar-thread-time">{thread.time}</div>
              </div>
              <button
                className="sidebar-thread-delete"
                onClick={(e) => { e.stopPropagation(); onDelete(thread.id); }}
                title="Delete"
              >
                ×
              </button>
            </div>
          ))
        )}
      </div>

      <div className="sidebar-footer">
        <div className="sidebar-footer-item" onClick={() => onOpenPanel('connectors')}>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
            <path strokeLinecap="round" d="M10.325 4.317a1.724 1.724 0 013.35 0c.4.98 1.61 1.44 2.51.9a1.724 1.724 0 012.573 1.066c-.156 1.054.649 2.01 1.673 2.01h.005a1.724 1.724 0 011.035 2.532c-.6.89-.2 2.09.79 2.51a1.724 1.724 0 01-.34 3.35c-1.05.16-1.81 1.09-1.61 2.13a1.724 1.724 0 01-1.87 2.07c-1.06-.16-1.99.65-2.01 1.67a1.724 1.724 0 01-2.53 1.04c-.89-.6-2.09-.2-2.51.79a1.724 1.724 0 01-3.35-.34c-.16-1.05-1.09-1.81-2.13-1.61a1.724 1.724 0 01-2.07-1.87c.16-1.06-.65-1.99-1.67-2.01a1.724 1.724 0 01-1.04-2.53c.6-.89.2-2.09-.79-2.51a1.724 1.724 0 01.34-3.35" />
            <circle cx="12" cy="12" r="3" />
          </svg>
          Settings
        </div>
      </div>
    </div>
  );
}
