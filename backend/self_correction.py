"""Self-correction loop — retries failed query plans up to MAX_RETRIES times."""

from __future__ import annotations
from models import QueryPlan, Artifact, StepStatus
from query_planner import generate_corrected_plan
from execution_engine import ExecutionEngine

MAX_RETRIES = 3


def execute_with_correction(
    question: str,
    schema_context: str,
    plan: QueryPlan,
    engine: ExecutionEngine,
) -> tuple[QueryPlan, list[Artifact], int]:
    """
    Execute a query plan with automatic self-correction on failure.

    Returns (final_plan, artifacts, retry_count).
    """
    current_plan = plan
    retries = 0

    while retries <= MAX_RETRIES:
        artifacts = engine.execute_plan(current_plan)

        # Check if any step failed
        failed = [a for a in artifacts if a.status == StepStatus.FAILED]
        if not failed:
            return current_plan, artifacts, retries

        if retries >= MAX_RETRIES:
            # Return the failed result as-is
            return current_plan, artifacts, retries

        # Get the first failed step's error
        failed_artifact = failed[0]
        error_msg = failed_artifact.error or "Unknown execution error"

        # Ask LLM for a corrected plan
        try:
            current_plan = generate_corrected_plan(
                question=question,
                schema_context=schema_context,
                previous_plan=current_plan,
                failed_step_id=failed_artifact.step_id,
                error_message=error_msg,
            )
            retries += 1
        except Exception as e:
            # If the LLM call itself fails, give up
            failed_artifact.error = f"Self-correction failed: {e}. Original error: {error_msg}"
            return current_plan, artifacts, retries

    return current_plan, artifacts, retries
