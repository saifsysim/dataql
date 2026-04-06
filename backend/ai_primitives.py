"""AI Primitives — classify, summarize, extract functions for DataQL."""

from __future__ import annotations
import json
import os
from typing import Any

import anthropic
from dotenv import load_dotenv

load_dotenv()


def _get_client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set.")
    return anthropic.Anthropic(api_key=api_key)


def classify(text: str, categories: list[str]) -> dict[str, Any]:
    """Sort text into one or more dynamic categories via LLM."""
    client = _get_client()
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=512,
        system="You are a classification engine. Respond with strict JSON only.",
        messages=[{
            "role": "user",
            "content": f"""Classify the following text into one or more of these categories: {json.dumps(categories)}

Text: {text}

Respond with JSON: {{"category": "primary category", "confidence": 0.0-1.0, "all_matches": [{{"category": "...", "confidence": 0.0-1.0}}]}}"""
        }],
    )
    response = msg.content[0].text.strip()
    if response.startswith("```"):
        lines = response.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response = "\n".join(lines)
    return json.loads(response)


def summarize(data: Any, context: str = "") -> str:
    """Create a concise natural language summary of data."""
    client = _get_client()
    data_str = json.dumps(data, default=str) if not isinstance(data, str) else data
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system="You are a data analyst. Provide clear, concise summaries.",
        messages=[{
            "role": "user",
            "content": f"""Summarize the following data{' (context: ' + context + ')' if context else ''}:

{data_str[:4000]}

Provide a concise, insightful summary in 2-4 sentences."""
        }],
    )
    return msg.content[0].text.strip()


def extract(text: str, fields: list[str]) -> dict[str, Any]:
    """Pull structured data from unstructured text."""
    client = _get_client()
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system="You are a data extraction engine. Respond with strict JSON only.",
        messages=[{
            "role": "user",
            "content": f"""Extract the following fields from the text: {json.dumps(fields)}

Text: {text}

Respond with JSON mapping each field to its extracted value. Use null if not found."""
        }],
    )
    response = msg.content[0].text.strip()
    if response.startswith("```"):
        lines = response.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response = "\n".join(lines)
    return json.loads(response)


def compute_reliability_score(
    retries: int,
    total_steps: int,
    failed_steps: int,
    execution_time_ms: float,
    has_data: bool,
) -> dict[str, Any]:
    """Compute a reliability score (0-100) for a query response.
    
    Scoring factors:
    - Base score: 85
    - Retries penalty: -15 per retry
    - Failed steps: -20 per failure
    - No data penalty: -10
    - Speed bonus: +5 if < 3s, +10 if < 1s
    - Complexity bonus: +5 if multi-step executed cleanly
    """
    score = 85

    # Retry penalty
    score -= retries * 15

    # Failed steps
    score -= failed_steps * 20

    # No data
    if not has_data:
        score -= 10

    # Speed bonus
    if execution_time_ms < 1000:
        score += 10
    elif execution_time_ms < 3000:
        score += 5

    # Multi-step clean execution bonus
    if total_steps > 1 and failed_steps == 0:
        score += 5

    score = max(0, min(100, score))

    # Determine label
    if score >= 90:
        label = "high"
    elif score >= 70:
        label = "medium"
    elif score >= 50:
        label = "low"
    else:
        label = "unreliable"

    return {
        "score": score,
        "label": label,
        "factors": {
            "retries": retries,
            "failed_steps": failed_steps,
            "total_steps": total_steps,
            "execution_time_ms": round(execution_time_ms, 2),
            "has_data": has_data,
        },
    }
