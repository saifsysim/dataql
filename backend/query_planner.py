"""LLM-powered query planner — takes natural language and produces a structured QueryPlan."""

from __future__ import annotations
import json
import os
from dotenv import load_dotenv
import anthropic

from models import QueryPlan, PlanStep, StepType

load_dotenv()

SYSTEM_PROMPT_TEMPLATE = """You are DataQL, an expert SQL query planner. Your job is to take a natural language question about a database and produce a structured query plan.

DATABASE SCHEMA:
{schema_context}

RULES:
1. You MUST respond with valid JSON only — no markdown, no explanation outside JSON.
2. Break complex questions into simple SQL steps.
3. Each step should be a single SQL query (SELECT only).
4. Use the EXACT table and column names from the schema above.
5. For SQLite: use date('now') for current date, strftime for date formatting.
6. Always include a final "summarize" step that describes how to present the answer in natural language.
7. Reference previous step results by their step_id in your reasoning.

RESPONSE FORMAT (strict JSON):
{{
  "question": "the original question",
  "reasoning": "your chain-of-thought about how to answer this",
  "steps": [
    {{
      "step_id": 1,
      "description": "what this step does",
      "step_type": "sql_query",
      "sql": "SELECT ...",
      "depends_on": []
    }},
    {{
      "step_id": 2,
      "description": "Summarize the results",
      "step_type": "summarize",
      "sql": null,
      "depends_on": [1]
    }}
  ],
  "natural_language_answer_template": "There are {{step_1_result}} orders."
}}

VALID step_type values: sql_query, transform, aggregate, filter, join, sort, limit, summarize
"""

RETRY_PROMPT_TEMPLATE = """The previous query plan failed during execution.

ORIGINAL QUESTION: {question}

PREVIOUS PLAN:
{previous_plan}

ERROR at step {failed_step_id}:
{error_message}

Please generate a CORRECTED query plan that avoids this error. Return strict JSON only.
"""


def _get_client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set. Add it to your .env file.")
    return anthropic.Anthropic(api_key=api_key)


def generate_query_plan(question: str, schema_context: str) -> QueryPlan:
    """Call Anthropic Claude to generate a structured query plan from a natural language question."""
    client = _get_client()

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(schema_context=schema_context)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=system_prompt,
        messages=[
            {"role": "user", "content": question}
        ],
    )

    response_text = message.content[0].text.strip()

    # Strip markdown code fences if present
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    plan_data = json.loads(response_text)

    # Parse into Pydantic models
    steps = []
    for step in plan_data["steps"]:
        steps.append(PlanStep(
            step_id=step["step_id"],
            description=step["description"],
            step_type=StepType(step["step_type"]),
            sql=step.get("sql"),
            depends_on=step.get("depends_on", []),
        ))

    return QueryPlan(
        question=plan_data["question"],
        reasoning=plan_data["reasoning"],
        steps=steps,
        natural_language_answer_template=plan_data.get("natural_language_answer_template", ""),
    )


def generate_corrected_plan(
    question: str,
    schema_context: str,
    previous_plan: QueryPlan,
    failed_step_id: int,
    error_message: str,
) -> QueryPlan:
    """Ask the LLM to fix a failed query plan."""
    client = _get_client()

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(schema_context=schema_context)
    retry_message = RETRY_PROMPT_TEMPLATE.format(
        question=question,
        previous_plan=previous_plan.model_dump_json(indent=2),
        failed_step_id=failed_step_id,
        error_message=error_message,
    )

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=system_prompt,
        messages=[
            {"role": "user", "content": retry_message}
        ],
    )

    response_text = message.content[0].text.strip()
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    plan_data = json.loads(response_text)

    steps = []
    for step in plan_data["steps"]:
        steps.append(PlanStep(
            step_id=step["step_id"],
            description=step["description"],
            step_type=StepType(step["step_type"]),
            sql=step.get("sql"),
            depends_on=step.get("depends_on", []),
        ))

    return QueryPlan(
        question=plan_data["question"],
        reasoning=plan_data["reasoning"],
        steps=steps,
        natural_language_answer_template=plan_data.get("natural_language_answer_template", ""),
    )
