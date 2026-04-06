"""Pydantic models for DataQL API requests, responses, query plans, and artifacts."""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


# ── Schema Models ──────────────────────────────────────────────

class ColumnInfo(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references: Optional[str] = None  # "table.column"


class TableInfo(BaseModel):
    name: str
    columns: list[ColumnInfo]
    row_count: Optional[int] = None


class SchemaInfo(BaseModel):
    database_type: str
    tables: list[TableInfo]


# ── Query Plan Models ─────────────────────────────────────────

class StepType(str, Enum):
    SQL_QUERY = "sql_query"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    JOIN = "join"
    SORT = "sort"
    LIMIT = "limit"
    SUMMARIZE = "summarize"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class PlanStep(BaseModel):
    step_id: int
    description: str
    step_type: StepType
    sql: Optional[str] = None
    depends_on: list[int] = Field(default_factory=list)
    status: StepStatus = StepStatus.PENDING


class QueryPlan(BaseModel):
    question: str
    reasoning: str
    steps: list[PlanStep]
    natural_language_answer_template: str = ""


# ── Artifact Models ───────────────────────────────────────────

class Artifact(BaseModel):
    step_id: int
    description: str
    data: Any  # rows, scalar, summary text
    row_count: Optional[int] = None
    columns: Optional[list[str]] = None
    execution_time_ms: float = 0
    status: StepStatus = StepStatus.COMPLETED
    error: Optional[str] = None


# ── API Models ────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    thread_id: Optional[str] = None


class QueryResponse(BaseModel):
    thread_id: str
    question: str
    plan: QueryPlan
    artifacts: list[Artifact]
    answer: str
    total_execution_time_ms: float
    retries: int = 0
    reliability_score: Optional[dict] = None


class ThreadMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    plan: Optional[QueryPlan] = None
    artifacts: Optional[list[Artifact]] = None


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
