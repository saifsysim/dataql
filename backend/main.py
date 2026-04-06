"""DataQL — FastAPI backend for natural language data querying with multi-source connectors."""

from __future__ import annotations
import os
import time
import uuid
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from models import (
    QueryRequest, QueryResponse, SchemaInfo, ErrorResponse,
    ThreadMessage, StepStatus,
)
from data_connectors import build_registry_from_env, ConnectorRegistry
from schema_introspector import introspect_schema, introspect_all, all_schemas_to_prompt_context
from query_planner import generate_query_plan
from execution_engine import ExecutionEngine
from self_correction import execute_with_correction
from semantic_metadata import MetadataStore, auto_generate_metadata
from ai_primitives import compute_reliability_score

# Load environment
load_dotenv()

# ── App Setup ─────────────────────────────────────────────────

app = FastAPI(
    title="DataQL",
    description="Natural language data query engine with multi-source connectors and semantic metadata",
    version="0.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── State ─────────────────────────────────────────────────────

registry = build_registry_from_env()
metadata_store = MetadataStore("metadata.yaml")
threads: dict[str, list[ThreadMessage]] = {}

# Cache
_schema_cache: Optional[tuple[list[dict], str]] = None


def _get_schemas() -> tuple[list[dict], str]:
    global _schema_cache
    if _schema_cache is None:
        sources = introspect_all(registry)
        raw_context = all_schemas_to_prompt_context(sources)
        # Enrich with semantic metadata
        enriched_context = metadata_store.enrich_prompt_context(raw_context)
        _schema_cache = (sources, enriched_context)
    return _schema_cache


# ── Routes ────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "connectors": len(registry.connectors)}


@app.get("/api/connectors")
def list_connectors():
    """List all registered connectors with their status."""
    return {"connectors": registry.list_all()}


@app.get("/api/schema")
def get_schema():
    """Return the combined schema from all connected data sources."""
    sources, _ = _get_schemas()
    all_tables = []
    db_type_parts = []
    for src in sources:
        schema = src["schema"]
        db_type_parts.append(src["name"])
        for table in schema.tables:
            all_tables.append(table)

    combined = SchemaInfo(
        database_type=" + ".join(db_type_parts),
        tables=all_tables,
    )
    return combined.model_dump()


@app.get("/api/schema/sources")
def get_schema_by_source():
    """Return schema grouped by data source."""
    sources, _ = _get_schemas()
    return {
        "sources": [
            {
                "source_id": s["source_id"],
                "name": s["name"],
                "icon": s["icon"],
                "type": s["type"],
                "tables": [t.model_dump() for t in s["schema"].tables],
                "error": s.get("error"),
            }
            for s in sources
        ]
    }


# ── Metadata API ──────────────────────────────────────────────

@app.get("/api/metadata")
def get_metadata():
    """Return the full semantic metadata configuration."""
    return metadata_store.get_metadata_dict()


@app.put("/api/metadata")
async def update_metadata(request: Request):
    """Update the semantic metadata configuration."""
    body = await request.json()
    metadata_store.update_from_dict(body)
    # Invalidate schema cache so next query uses updated metadata
    global _schema_cache
    _schema_cache = None
    return {"status": "updated", "metadata": metadata_store.get_metadata_dict()}


@app.post("/api/metadata/generate")
def generate_metadata():
    """Auto-generate semantic metadata from schema using LLM."""
    _, raw_context = _get_schemas()
    # Strip existing metadata enrichment for clean generation
    sources = introspect_all(registry)
    clean_context = all_schemas_to_prompt_context(sources)
    generated = auto_generate_metadata(clean_context)
    metadata_store.update_from_dict(generated)
    # Invalidate schema cache
    global _schema_cache
    _schema_cache = None
    return {"status": "generated", "metadata": metadata_store.get_metadata_dict()}


@app.post("/api/metadata/reload")
def reload_metadata():
    """Reload metadata from the YAML file on disk."""
    metadata_store.reload()
    global _schema_cache
    _schema_cache = None
    return {"status": "reloaded", "metadata": metadata_store.get_metadata_dict()}


# ── Query ─────────────────────────────────────────────────────

@app.post("/api/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """Process a natural language question against all connected data sources."""
    total_start = time.perf_counter()

    thread_id = request.thread_id or str(uuid.uuid4())
    if thread_id not in threads:
        threads[thread_id] = []

    threads[thread_id].append(ThreadMessage(role="user", content=request.question))

    try:
        _, schema_context = _get_schemas()

        # Generate plan (schema_context now includes semantic metadata)
        plan = generate_query_plan(request.question, schema_context)

        # Determine which connector to use
        connector = _pick_connector_for_plan(plan)
        engine = ExecutionEngine(connector)

        # Execute with self-correction
        final_plan, artifacts, retries = execute_with_correction(
            question=request.question,
            schema_context=schema_context,
            plan=plan,
            engine=engine,
        )

        # Build answer
        failed = [a for a in artifacts if a.status == StepStatus.FAILED]
        if failed:
            answer = f"I encountered an error: {failed[0].error}"
        else:
            summarize_artifacts = [
                a for a in artifacts
                if a.description.lower().startswith("summar") and a.data is not None
            ]
            if summarize_artifacts:
                answer = str(summarize_artifacts[-1].data)
            elif artifacts:
                last = artifacts[-1]
                if isinstance(last.data, list) and last.data:
                    if len(last.data) == 1:
                        answer = ", ".join(f"{k}: {v}" for k, v in last.data[0].items())
                    else:
                        answer = f"Found {len(last.data)} results."
                else:
                    answer = str(last.data) if last.data else "Query completed but returned no data."
            else:
                answer = "No results."

        total_ms = round((time.perf_counter() - total_start) * 1000, 2)

        # Compute reliability score
        failed_count = len([a for a in artifacts if a.status == StepStatus.FAILED])
        reliability = compute_reliability_score(
            retries=retries,
            total_steps=len(final_plan.steps),
            failed_steps=failed_count,
            execution_time_ms=total_ms,
            has_data=bool(artifacts and any(a.data for a in artifacts)),
        )

        response = QueryResponse(
            thread_id=thread_id,
            question=request.question,
            plan=final_plan,
            artifacts=artifacts,
            answer=answer,
            total_execution_time_ms=total_ms,
            retries=retries,
            reliability_score=reliability,
        )

        threads[thread_id].append(ThreadMessage(
            role="assistant", content=answer,
            plan=final_plan, artifacts=artifacts,
        ))

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _pick_connector_for_plan(plan) -> "BaseConnector":
    """Route queries to the right connector based on table names in the SQL."""
    sql_parts = []
    for step in plan.steps:
        if step.sql:
            sql_parts.append(step.sql.lower())
    combined_sql = " ".join(sql_parts)

    for connector in registry.get_all():
        if connector.source_id == "sqlite":
            continue
        try:
            tables = connector.get_tables()
            for table in tables:
                if table.lower() in combined_sql:
                    return connector
        except Exception:
            continue

    return registry.get_primary()


@app.get("/api/threads/{thread_id}")
def get_thread(thread_id: str):
    if thread_id not in threads:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {
        "thread_id": thread_id,
        "messages": [m.model_dump() for m in threads[thread_id]],
    }


@app.delete("/api/threads/{thread_id}")
def delete_thread(thread_id: str):
    if thread_id in threads:
        del threads[thread_id]
    return {"status": "deleted"}


@app.post("/api/schema/refresh")
def refresh_schema():
    """Force re-introspection of all data sources."""
    global _schema_cache
    _schema_cache = None
    for c in registry.get_all():
        if hasattr(c, 'force_refresh'):
            c.force_refresh()
    sources, _ = _get_schemas()
    all_tables = []
    for src in sources:
        all_tables.extend(src["schema"].tables)
    return SchemaInfo(
        database_type="all",
        tables=all_tables,
    ).model_dump()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
