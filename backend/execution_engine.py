"""Execution engine — runs a QueryPlan step-by-step, producing Artifacts."""

from __future__ import annotations
import time
from typing import Any

from models import QueryPlan, PlanStep, Artifact, StepType, StepStatus
from data_connectors import SQLiteConnector


class ExecutionEngine:
    """Executes a QueryPlan against a database connector, producing traceable Artifacts."""

    def __init__(self, connector: SQLiteConnector):
        self.connector = connector

    def execute_plan(self, plan: QueryPlan) -> list[Artifact]:
        """Execute all steps in a plan sequentially. Returns a list of Artifacts."""
        artifacts: list[Artifact] = []
        step_results: dict[int, Any] = {}  # step_id → result data

        for step in plan.steps:
            step.status = StepStatus.RUNNING
            artifact = self._execute_step(step, step_results, plan)
            artifacts.append(artifact)

            if artifact.status == StepStatus.FAILED:
                # Stop execution on first failure
                # Mark remaining steps as pending
                break

            step_results[step.step_id] = artifact.data
            step.status = StepStatus.COMPLETED

        return artifacts

    def _execute_step(
        self,
        step: PlanStep,
        previous_results: dict[int, Any],
        plan: QueryPlan,
    ) -> Artifact:
        """Execute a single step and return an Artifact."""
        start = time.perf_counter()

        try:
            if step.step_type == StepType.SUMMARIZE:
                return self._execute_summarize(step, previous_results, plan, start)
            elif step.step_type == StepType.SQL_QUERY:
                return self._execute_sql(step, start)
            elif step.step_type in (
                StepType.AGGREGATE, StepType.FILTER, StepType.JOIN,
                StepType.SORT, StepType.LIMIT, StepType.TRANSFORM,
            ):
                # These are all expressed as SQL in practice
                if step.sql:
                    return self._execute_sql(step, start)
                else:
                    return self._execute_passthrough(step, previous_results, start)
            else:
                return Artifact(
                    step_id=step.step_id,
                    description=step.description,
                    data=None,
                    status=StepStatus.FAILED,
                    error=f"Unknown step type: {step.step_type}",
                    execution_time_ms=self._elapsed(start),
                )
        except Exception as e:
            return Artifact(
                step_id=step.step_id,
                description=step.description,
                data=None,
                status=StepStatus.FAILED,
                error=str(e),
                execution_time_ms=self._elapsed(start),
            )

    def _execute_sql(self, step: PlanStep, start: float) -> Artifact:
        """Execute a SQL query step."""
        if not step.sql:
            return Artifact(
                step_id=step.step_id,
                description=step.description,
                data=None,
                status=StepStatus.FAILED,
                error="SQL step has no SQL query.",
                execution_time_ms=self._elapsed(start),
            )

        result = self.connector.execute_query(step.sql)
        return Artifact(
            step_id=step.step_id,
            description=step.description,
            data=result["rows"],
            columns=result["columns"],
            row_count=result["row_count"],
            execution_time_ms=result["execution_time_ms"],
            status=StepStatus.COMPLETED,
        )

    def _execute_summarize(
        self,
        step: PlanStep,
        previous_results: dict[int, Any],
        plan: QueryPlan,
        start: float,
    ) -> Artifact:
        """Generate a natural language summary from previous step results."""
        summary_parts = []

        for dep_id in step.depends_on:
            dep_data = previous_results.get(dep_id)
            if dep_data is not None:
                if isinstance(dep_data, list) and len(dep_data) > 0:
                    if len(dep_data) == 1:
                        # Single-row result — format as key-value
                        row = dep_data[0]
                        vals = ", ".join(f"{k}: {v}" for k, v in row.items())
                        summary_parts.append(vals)
                    else:
                        summary_parts.append(f"Result set with {len(dep_data)} rows")
                elif isinstance(dep_data, str):
                    summary_parts.append(dep_data)

        summary = "; ".join(summary_parts) if summary_parts else "No data returned."

        return Artifact(
            step_id=step.step_id,
            description=step.description,
            data=summary,
            status=StepStatus.COMPLETED,
            execution_time_ms=self._elapsed(start),
        )

    def _execute_passthrough(
        self,
        step: PlanStep,
        previous_results: dict[int, Any],
        start: float,
    ) -> Artifact:
        """Pass through data from a dependency (for non-SQL transform steps)."""
        dep_data = None
        if step.depends_on:
            dep_data = previous_results.get(step.depends_on[0])

        return Artifact(
            step_id=step.step_id,
            description=step.description,
            data=dep_data,
            status=StepStatus.COMPLETED,
            execution_time_ms=self._elapsed(start),
        )

    @staticmethod
    def _elapsed(start: float) -> float:
        return round((time.perf_counter() - start) * 1000, 2)
