# DataQL Streaming Architecture — SSE Deep Dive

## Overview

DataQL uses **Server-Sent Events (SSE)** to stream the entire query lifecycle to the frontend in real-time. Instead of a single JSON response after all processing is complete, the backend emits granular events at every phase — giving users live visibility into schema introspection, AI planning, SQL execution, and result summarization.

---

## Why SSE?

| Property | REST (old) | SSE (current) | WebSocket |
|----------|-----------|---------------|-----------|
| **Latency perception** | User waits 3-8s for full response | Instant feedback per phase | Instant |
| **Protocol** | HTTP POST → JSON | HTTP POST → text/event-stream | Upgrade to ws:// |
| **Complexity** | Simple | Low — standard HTTP | Higher (connection mgmt) |
| **Directionality** | Request-response | Server → Client stream | Bidirectional |
| **Why not?** | Bad UX for long queries | ✅ Chosen | Overkill — we don't need client→server streaming |

**Key decision**: We use `fetch()` + `ReadableStream` instead of the browser's native `EventSource` API because `EventSource` only supports `GET` requests — we need `POST` with a JSON body.

---

## Backend: FastAPI SSE Endpoint

### `POST /api/query/stream`

**File**: `backend/main.py`

The endpoint returns a `StreamingResponse` with `media_type="text/event-stream"`. It uses an async generator that `yield`s SSE-formatted strings at each processing phase.

```python
@app.post("/api/query/stream")
async def query_stream(request: QueryRequest):
    async def event_generator():
        # Phase 1: Schema
        yield emit("phase", {"phase": "schema", "message": "Introspecting..."})
        _, schema_context = _get_schemas()

        # Phase 2: Planning
        yield emit("phase", {"phase": "planning", "message": "Building plan..."})
        plan = generate_query_plan(request.question, schema_context)
        yield emit("plan", plan_data)  # Full plan with reasoning + steps

        # Phase 3: Step-by-step execution
        for step in plan.steps:
            yield emit("step_start", {"step_id": step.step_id, ...})
            artifact = engine._execute_step(step, ...)
            yield emit("step_complete", {"step_id": ..., "status": ..., ...})

        # Phase 4: Final answer
        yield emit("result", {"answer": answer, "reliability_score": ...})

    return StreamingResponse(event_generator(), media_type="text/event-stream")
```

### SSE Frame Format

Each event follows the [W3C SSE specification](https://html.spec.whatwg.org/multipage/server-sent-events.html):

```
event: <event-type>\n
data: <json-payload>\n
\n
```

The double newline (`\n\n`) terminates each event frame. The `emit()` helper:

```python
def emit(event_type: str, data: dict):
    payload = json.dumps(data, default=str)
    return f"event: {event_type}\ndata: {payload}\n\n"
```

### Response Headers

```http
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no       # Disable nginx buffering if behind proxy
```

---

## Event Catalog

| Event | When | Payload |
|-------|------|---------|
| `phase` | Each processing phase starts | `{ phase, message }` |
| `plan` | AI query plan generated | `{ question, reasoning, steps[] }` |
| `step_start` | Individual step begins executing | `{ step_id, description, step_type, sql }` |
| `step_complete` | Step finishes (success or failure) | `{ step_id, status, execution_time_ms, row_count, columns, data, error }` |
| `result` | Final answer + scoring | `{ thread_id, answer, total_execution_time_ms, retries, reliability_score }` |
| `error` | Uncaught exception | `{ message }` |

### Phase Progression

```
idle → schema → schema_done → planning → executing → summarizing → done
```

---

## Frontend: ReadableStream Consumer

### `App.jsx` — SSE Stream Handler

The frontend POSTs to `/api/query/stream` and reads the response body as a stream:

```javascript
const res = await fetch(`${API_BASE}/api/query/stream`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ question, thread_id: threadId }),
});

const reader = res.body.getReader();
const decoder = new TextDecoder();
let buffer = '';

while (true) {
  const { done, value } = await reader.read();
  if (done) break;

  buffer += decoder.decode(value, { stream: true });
  const lines = buffer.split('\n');
  buffer = lines.pop() || '';  // Keep incomplete line in buffer

  // Parse SSE frames
  let eventType = '';
  let eventData = '';

  for (const line of lines) {
    if (line.startsWith('event: ')) {
      eventType = line.slice(7).trim();
    } else if (line.startsWith('data: ')) {
      eventData = line.slice(6).trim();
    } else if (line === '' && eventType && eventData) {
      const data = JSON.parse(eventData);
      // Dispatch to React state based on eventType
      switch (eventType) {
        case 'phase':    setStreamState(prev => ({ ...prev, phase: data.phase })); break;
        case 'plan':     setStreamState(prev => ({ ...prev, plan: data, steps: data.steps })); break;
        case 'step_start':   /* Mark step as running */ break;
        case 'step_complete': /* Mark step as done, store artifact */ break;
        case 'result':   /* Add assistant message with final answer */ break;
        case 'error':    /* Show error in chat */ break;
      }
      eventType = ''; eventData = '';
    }
  }
}
```

### Stream State Shape

```typescript
interface StreamState {
  phase: 'idle' | 'schema' | 'schema_done' | 'planning' | 'executing' | 'summarizing' | 'done';
  plan: null | { question: string; reasoning: string; steps: Step[] };
  steps: Step[];              // Steps with live status updates
  completedSteps: Artifact[]; // Completed artifacts with data
  error: null | string;
}
```

---

## Frontend: Live Visualization

### `ChatPanel.jsx` — `LiveQueryProcess` Component

This component renders the streaming state in real-time:

```
┌─────────────────────────────────────────────────────────────┐
│  🔍 Schema  ──── 🧠 Planning  ──── ⚡ Executing ──── ✨ Results  │
│     (done)        (active ◉)         (pending)        (pending)  │
├─────────────────────────────────────────────────────────────┤
│  ◉ AI REASONING                                             │
│  "I'll query the orders table and join with products..."    │
├─────────────────────────────────────────────────────────────┤
│  ✓ Query total orders                 sql_query   12.4ms    │
│  ◉ Join with products table           sql_query   [spinner] │
│  ○ Summarize the results              summarize   (pending) │
└─────────────────────────────────────────────────────────────┘
```

### Phase Indicator

Each phase pill lights up with a pulsing glow animation when active, transitions to green checkmark when done:

```jsx
<PhaseStep label="Planning" icon="🧠" status={phase === 'planning' ? 'active' : ...} />
```

CSS classes: `.live-phase-pending`, `.live-phase-active`, `.live-phase-done`

### Step Animation

Steps slide in with `animation: stepSlideIn 0.35s ease` and cycle through:
- **Pending**: gray dot, dimmed text
- **Running**: spinning indigo ring
- **Done**: green ✓ SVG checkmark with execution time badge
- **Failed**: red ✗ SVG with error details

---

## Data Flow Diagram

```
User types question
         │
         ▼
   App.handleSend()
         │
         ├─ setMessages([...prev, { role: 'user', content }])
         ├─ setIsLoading(true)
         ├─ setStreamState({ phase: 'idle', ... })
         │
         ▼
   fetch('/api/query/stream', { method: 'POST', body: ... })
         │
    ┌────┴────────────────────┐
    │  ReadableStream reader  │
    │  (async while loop)     │
    └────┬────────────────────┘
         │
     For each SSE event:
         │
    ┌────┴───────────────────────────────────────┐
    │ event: phase     → setStreamState.phase     │
    │ event: plan      → setStreamState.plan      │
    │ event: step_start → update step status      │
    │ event: step_complete → push to artifacts    │
    │ event: result    → setMessages + assistant  │
    │ event: error     → setMessages + error      │
    └─────────────────────────────────────────────┘
         │
         ▼
   setIsLoading(false)
   setStreamState(null)
```

---

## Navigation & State Preservation

The Connectors page opens as an overlay (`z-index: 200`) while the main chat and all React state stays mounted underneath. When the user clicks the **← Chat** back button or presses **Escape**, the overlay closes and the chat is exactly where they left it — including any in-progress SSE streaming.

Key design decisions:
- **No routing library** — panels are toggled via boolean state (`connectorsOpen`, `schemaOpen`)
- **No unmounting** — `ChatPanel` is always mounted, preserving scroll position, input state, and messages
- **Overlay approach** — `ConnectorsPage` renders on top with `position: fixed` but doesn't interfere with the chat's lifecycle

---

## Performance Considerations

1. **`asyncio.sleep(0)`** — Yields control in the async generator to ensure events are flushed immediately, not buffered
2. **`X-Accel-Buffering: no`** — Disables nginx/reverse proxy buffering for real SSE delivery
3. **TextDecoder with `{ stream: true }`** — Handles partial UTF-8 sequences across chunk boundaries
4. **Buffer management** — Incomplete SSE lines are held in `buffer` until the next chunk arrives
5. **React batching** — Each `setStreamState` call triggers a re-render; React 18's automatic batching ensures efficient updates
