"""Robust JSON extraction from AI responses.

AI models sometimes wrap JSON in markdown fences or include explanatory text
before/after the JSON object.  This utility handles all common cases without
ever raising — returning None on total failure so callers can log and skip.

Strategy order:
  1. Direct json.loads()                   — ideal, model followed instructions
  2. First { to last } substring           — preamble/postamble present
  3. Strip all markdown fences             — ```json ... ``` wrapping
  4. JSON inside a code block (regex)      — more exotic wrapping

Usage:
    from .json_parser import parse_json_response
    data = parse_json_response(raw_text)
    if data is None:
        log.warning("Failed to parse JSON from model response")
"""
from __future__ import annotations

import json
import logging
import re

log = logging.getLogger("dbscan")


def parse_json_response(text: str) -> dict | None:
    """Extract and parse the first JSON object from an AI response.

    Returns the parsed dict, or None on total failure (never raises).
    Logs the raw response (truncated) at WARNING level on failure.
    """
    if not text or not text.strip():
        return None

    # ── Strategy 1: direct parse ─────────────────────────────────────────────
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # ── Strategy 2: find outermost { ... } ──────────────────────────────────
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            result = json.loads(candidate)
            if isinstance(result, dict):
                return result
        except (json.JSONDecodeError, ValueError):
            pass

    # ── Strategy 3: strip all markdown fences ────────────────────────────────
    stripped = re.sub(r"```(?:json)?", "", text)
    stripped = stripped.replace("```", "").strip()
    try:
        result = json.loads(stripped)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # ── Strategy 4: extract from code block with regex ────────────────────────
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group(1))
            if isinstance(result, dict):
                return result
        except (json.JSONDecodeError, ValueError):
            pass

    # ── Total failure ─────────────────────────────────────────────────────────
    log.warning(
        "JSON parse failure — all strategies exhausted.\n"
        "Raw response (first 500 chars):\n%s",
        text[:500],
    )
    return None
