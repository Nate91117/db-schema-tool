"""Stage 3: Deep Inspection — Sonnet generates rich semantic annotations.

Pulls sample rows per high-value table and sends to Claude Sonnet for
detailed business-context annotation.

Key improvements over v1:
- Confirmed FK relationships (from Stage 1 DB query) included in prompt
- Primary keys explicitly called out in column list
- industry-specific system prompt
- max_tables parameter for cost control on large databases
- Sample rows are ephemeral — used for AI call only, never sent to portal
- 5-retry exponential backoff (10/20/40/80/120s) for transient errors
- Configurable batch_delay between table API calls
- Detailed error logging with full stack traces and error classification
"""
from __future__ import annotations

import json
import logging
import time
import traceback

from .connection import QueryExecutor
from .constants import get_stage3_system_prompt
from .types import ScoredTable, SemanticTable

try:
    from .memory import format_memory_for_stage3, MemoryContext
except ImportError:
    pass

SAMPLE_ROWS = 10
_RETRY_DELAYS = [10, 20, 40, 80, 120]  # seconds — 5 retries with exponential backoff

log = logging.getLogger("dbscan")


# ── Error classification ───────────────────────────────────────────────────────

def _classify_error(exc: Exception) -> str:
    """Classify an API exception into a human-readable category."""
    exc_str = str(exc)
    exc_type = type(exc).__name__

    if "429" in exc_str:
        return "RATE_LIMIT (429 Too Many Requests)"
    if "529" in exc_str:
        return "OVERLOADED (529 API Overloaded)"
    if any(c in exc_str for c in ["500", "502", "503", "504"]):
        return "SERVER_ERROR (5xx)"
    if any(c in exc_str for c in ["401", "403"]):
        return "AUTH_ERROR (401/403)"
    if "400" in exc_str:
        return "BAD_REQUEST (400)"
    if any(k in exc_str.lower() for k in ["timeout", "timed out"]):
        return "TIMEOUT"
    if any(k in exc_str.lower() for k in ["connection", "refused", "remotedisconnected",
                                            "connectionreset", "brokenpipe"]):
        return "CONNECTION_ERROR"
    if "ssl" in exc_str.lower():
        return "SSL_ERROR"
    return f"UNKNOWN ({exc_type})"


def _is_retryable(exc: Exception) -> bool:
    """Return True if the error is transient and worth retrying."""
    exc_str = str(exc)
    if any(code in exc_str for code in ["status_code=400", "status_code=401",
                                         "status_code=403", "status_code=404"]):
        return False
    return True


# ── Retry wrapper ──────────────────────────────────────────────────────────────

def _call_with_retry(
    client, model: str, system_prompt: str, prompt: str,
    max_tokens: int = 4096,
    table_name: str = "",
    table_num: int = 0,
    total_tables: int = 0,
) -> object:
    """Call the Anthropic API with exponential backoff on transient errors.

    Retries up to len(_RETRY_DELAYS) times (5 retries) with increasing delays:
    10s / 20s / 40s / 80s / 120s.

    Raises immediately on non-retryable 4xx client errors.
    """
    last_exc: Exception | None = None
    table_label = f"[{table_num}/{total_tables}] '{table_name}'"

    for attempt, delay in enumerate([0] + _RETRY_DELAYS):
        if attempt > 0:
            log.warning(
                "%s: Retry %d/%d in %ds (previous error: %s)",
                table_label, attempt, len(_RETRY_DELAYS), delay,
                _classify_error(last_exc),
            )
            print(f"    Retrying in {delay}s... (attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1})")
            time.sleep(delay)

        try:
            log.info(
                "%s: API call attempt %d",
                table_label, attempt + 1,
            )
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
            )
            log.info(
                "%s: API call succeeded (attempt %d)",
                table_label, attempt + 1,
            )
            return response

        except Exception as e:
            last_exc = e
            error_class = _classify_error(e)

            if not _is_retryable(e):
                log.error(
                    "%s: Non-retryable error — %s\n"
                    "  Exception: %s: %s\n"
                    "  Traceback:\n%s",
                    table_label, error_class,
                    type(e).__name__, e,
                    traceback.format_exc(),
                )
                raise

            if attempt < len(_RETRY_DELAYS):
                log.warning(
                    "%s: Transient error on attempt %d — %s: %s",
                    table_label, attempt + 1, error_class, str(e)[:200],
                )
                print(
                    f"    WARNING: API call failed for '{table_name}' "
                    f"(attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1}): "
                    f"{error_class}: {str(e)[:120]}"
                )

    # All attempts failed
    log.error(
        "%s: All %d attempts failed.\n"
        "  Last error class: %s\n"
        "  Exception: %s: %s\n"
        "  Full traceback:\n%s",
        table_label, len(_RETRY_DELAYS) + 1,
        _classify_error(last_exc),
        type(last_exc).__name__, last_exc,
        traceback.format_exc(),
    )
    print(
        f"    ERROR: All {len(_RETRY_DELAYS) + 1} attempts failed for '{table_name}': "
        f"{_classify_error(last_exc)}: {str(last_exc)[:200]}"
    )
    raise last_exc


# ── Public entry point ─────────────────────────────────────────────────────────

def run_stage3(
    high_value_tables: list[ScoredTable],
    query: QueryExecutor,
    client: object,
    model: str = "claude-sonnet-4-5",
    max_tables: int | None = None,
    industry: str = "biofuel",
    memory_context: str = "",
    batch_delay: float = 1.0,
) -> tuple[list[SemanticTable], dict, int]:
    """Generate rich semantic annotations for high-value tables.

    Args:
        high_value_tables: Tables that scored >= min_score in Stage 2
        query:             Audited query executor (connected to client DB)
        client:            anthropic.Anthropic() instance
        model:             Model ID for annotation (Sonnet)
        max_tables:        Limit to top N tables (cost control for large DBs)
        industry:          Industry context for the annotation prompt
        batch_delay:       Seconds to sleep between per-table API calls (default 1.0)

    Returns:
        (semantic_tables, semantic_layer_dict, total_tokens)
    """
    # Apply table limit (already sorted by score descending from Stage 2)
    tables_to_annotate = high_value_tables
    if max_tables and len(high_value_tables) > max_tables:
        tables_to_annotate = high_value_tables[:max_tables]
        print(
            f"\n  Stage 3: Limiting to top {max_tables} tables "
            f"(of {len(high_value_tables)} high-value, use --max-stage3-tables to adjust)"
        )

    total_tokens = 0
    semantic_tables: list[SemanticTable] = []

    # If memory_context is a MemoryContext object, generate per-table prompts.
    # If it's a string, use it as-is for all tables.
    _memory_obj = None
    _memory_str = ""
    if hasattr(memory_context, 'total_fetched'):
        _memory_obj = memory_context  # type: ignore[assignment]
    elif isinstance(memory_context, str):
        _memory_str = memory_context

    total_count = len(tables_to_annotate)

    for i, table in enumerate(tables_to_annotate, 1):
        print(f"\n  [{i}/{total_count}] Inspecting {table.name} "
              f"(score={table.score}, {table.row_count:,} rows)...")
        log.info(
            "[%d/%d] Stage 3 starting: table='%s' score=%d rows=%d",
            i, total_count, table.name, table.score, table.row_count,
        )

        # Pull sample rows (ephemeral — never sent to portal)
        sample_rows: list[dict] = []
        try:
            sample_rows = query.get_sample_rows(table.name, limit=SAMPLE_ROWS)
        except Exception as e:
            print(f"    WARNING: Could not get sample rows: {e}")
            log.warning("[%d/%d] '%s': sample rows failed: %s", i, total_count, table.name, e)

        prompt = _build_inspection_prompt(table, sample_rows)

        # Per-table memory context if available
        if _memory_obj is not None:
            table_mem = format_memory_for_stage3(_memory_obj, table.name)
        else:
            table_mem = _memory_str
        system_prompt = get_stage3_system_prompt(industry, memory_context=table_mem)

        try:
            response = _call_with_retry(
                client=client,
                model=model,
                system_prompt=system_prompt,
                prompt=prompt,
                max_tokens=4096,
                table_name=table.name,
                table_num=i,
                total_tables=total_count,
            )
        except Exception as e:
            error_class = _classify_error(e)
            print(f"    ERROR: Stage 3 failed for '{table.name}' after all retries "
                  f"({error_class}): {type(e).__name__}: {str(e)[:200]}")
            print(f"    (See log file for full stack trace)")
            log.error(
                "[%d/%d] '%s': FAILED after all retries — %s\n"
                "  Exception: %s: %s\n"
                "  Traceback:\n%s",
                i, total_count, table.name, error_class,
                type(e).__name__, e,
                traceback.format_exc(),
            )
            # Delay before next table even after failure
            if batch_delay > 0 and i < total_count:
                time.sleep(batch_delay)
            continue

        tokens = response.usage.input_tokens + response.usage.output_tokens
        total_tokens += tokens
        log.info(
            "[%d/%d] '%s': annotated — %d tokens used",
            i, total_count, table.name, tokens,
        )

        raw_text = response.content[0].text.strip()
        semantic = _parse_annotation(raw_text, table)

        if semantic:
            semantic_tables.append(semantic)
            print(f"    OK {semantic.business_concept}: {semantic.description[:80]}...")
        else:
            print(f"    WARNING: Failed to parse annotation for {table.name}")
            log.warning("[%d/%d] '%s': annotation parse failed", i, total_count, table.name)

        # Delay between table API calls
        if batch_delay > 0 and i < total_count:
            log.debug("Sleeping %.1fs between tables", batch_delay)
            time.sleep(batch_delay)

    # Build the semantic layer dict (what gets stored in the portal — no raw data)
    semantic_layer = _build_semantic_layer(semantic_tables)

    skipped = len(high_value_tables) - len(tables_to_annotate)
    print(f"\n  Stage 3: {len(semantic_tables)}/{len(tables_to_annotate)} tables annotated"
          + (f" ({skipped} skipped by --max-stage3-tables)" if skipped else ""))
    log.info(
        "Stage 3 complete: %d/%d tables annotated, %d tokens used",
        len(semantic_tables), len(tables_to_annotate), total_tokens,
    )

    return semantic_tables, semantic_layer, total_tokens


def _build_inspection_prompt(table: ScoredTable, sample_rows: list[dict]) -> str:
    """Build the enriched inspection prompt for a single table."""
    lines = [
        f"Annotate the following database table.\n",
        f"Table: {table.name}",
        f"Row count: {table.row_count:,}",
        f"AI relevance score: {table.score}/10",
        f"AI-assigned concept: {table.likely_concept}",
        f"AI scoring reason: {table.reason}",
        "",
    ]

    # Primary keys
    if table.primary_keys:
        lines.append(f"Primary keys: {', '.join(table.primary_keys)}")

    # Columns
    lines.append("Columns:")
    for col in table.columns:
        pk_flag = " [PK]" if col.name in table.primary_keys else ""
        fk_targets = [fk for fk in table.foreign_keys if fk["from_column"] == col.name]
        fk_flag = (
            f" [FK → {fk_targets[0]['to_table']}.{fk_targets[0]['to_column']}]"
            if fk_targets else ""
        )
        lines.append(f"  - {col.name} ({col.data_type}){pk_flag}{fk_flag}")

    # Confirmed FK relationships from DB schema
    if table.foreign_keys:
        lines.append("\nConfirmed foreign key relationships (from DB schema):")
        for fk in table.foreign_keys:
            lines.append(
                f"  {fk['from_column']} → {fk['to_table']}.{fk['to_column']}"
            )

    # Sample rows
    if sample_rows:
        lines.append(f"\nSample rows ({len(sample_rows)} of {table.row_count:,}):")
        for j, row in enumerate(sample_rows):
            # Truncate long values to keep prompt size reasonable
            truncated = {
                k: (str(v)[:50] + "..." if isinstance(v, str) and len(str(v)) > 50 else v)
                for k, v in row.items()
            }
            lines.append(f"  Row {j + 1}: {json.dumps(truncated, default=str)}")

    return "\n".join(lines)


def _parse_annotation(raw_text: str, table: ScoredTable) -> SemanticTable | None:
    """Parse Sonnet's JSON response into a SemanticTable."""
    text = raw_text
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        print(f"    WARNING: Failed to parse Sonnet response as JSON")
        print(f"    Raw (first 200 chars): {raw_text[:200]}")
        log.warning(
            "JSON parse failure for '%s' — raw response (first 500 chars):\n%s",
            table.name, raw_text[:500],
        )
        return None

    return SemanticTable(
        name=data.get("table_name", table.name),
        description=data.get("description", ""),
        business_concept=data.get("business_concept", table.likely_concept),
        columns=data.get("columns", []),
        relationships=data.get("relationships", []),
        score=table.score,
        row_count=table.row_count,
    )


def _build_semantic_layer(tables: list[SemanticTable]) -> dict:
    """Build the semantic layer dict — what gets stored in the portal.

    No sample rows, no raw data. Only descriptions and structure.
    """
    return {
        "version": "1.1",
        "table_count": len(tables),
        "tables": {
            t.name: {
                "description": t.description,
                "business_concept": t.business_concept,
                "row_count": t.row_count,
                "relevance_score": t.score,
                "columns": t.columns,
                "relationships": t.relationships,
            }
            for t in tables
        },
    }
