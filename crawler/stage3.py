"""Stage 3: Deep Inspection — generates rich semantic annotations per table.

Sends full column metadata + optional sample rows to the configured AI model
and produces a structured SemanticTable for each high-value table.

v0.3.0 changes:
- Accepts AIClient instead of a raw Anthropic client — provider-agnostic
- executor is optional (only used for sample rows; omit for standalone mode)
- Checkpoint/resume support (stage3_checkpoint.json by default)
- Uses json_parser.parse_json_response for robust JSON extraction
- Clean progress output: "Annotating table 3/75..."
- stage3.log written independently of cli.py
"""
from __future__ import annotations

import json
import logging
import time
import traceback
from typing import TYPE_CHECKING

from .ai_client import AIClient
from .constants import get_stage3_system_prompt
from .json_parser import parse_json_response
from .types import ColumnInfo, ScoredTable, SemanticTable

if TYPE_CHECKING:
    from .connection import QueryExecutor

try:
    from .memory import format_memory_for_stage3, MemoryContext
except ImportError:
    pass

SAMPLE_ROWS = 10
_RETRY_DELAYS = [10, 20, 40, 80, 120]

log = logging.getLogger("dbscan")


# ── Error classification ───────────────────────────────────────────────────────

def _classify_error(exc: Exception) -> str:
    exc_str = str(exc)
    exc_type = type(exc).__name__
    if "429" in exc_str:
        return "RATE_LIMIT (429)"
    if "529" in exc_str:
        return "OVERLOADED (529)"
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
    exc_str = str(exc)
    if any(marker in exc_str for marker in
           ["status_code=400", "status_code=401", "status_code=403", "status_code=404",
            "HTTP/1.1 400", "HTTP/1.1 401", "HTTP/1.1 403", "HTTP/1.1 404"]):
        return False
    return True


# ── Retry wrapper ──────────────────────────────────────────────────────────────

def _call_with_retry(
    ai_client: AIClient,
    system_prompt: str,
    prompt: str,
    max_tokens: int = 8192,
    table_name: str = "",
    table_num: int = 0,
    total_tables: int = 0,
) -> tuple[str, int]:
    """Call the AI with exponential backoff. Returns (text, tokens)."""
    last_exc: Exception | None = None
    label = f"[{table_num}/{total_tables}] '{table_name}'"

    for attempt, delay in enumerate([0] + _RETRY_DELAYS):
        if attempt > 0:
            log.warning(
                "%s: Retry %d/%d in %ds (previous: %s)",
                label, attempt, len(_RETRY_DELAYS), delay, _classify_error(last_exc),
            )
            print(f"    Retrying in {delay}s... (attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1})")
            time.sleep(delay)

        try:
            log.info("%s: API call attempt %d", label, attempt + 1)
            text, tokens = ai_client.complete(system_prompt, prompt, max_tokens=max_tokens)
            log.info("%s: succeeded (attempt %d)", label, attempt + 1)
            return text, tokens

        except Exception as e:
            last_exc = e
            error_class = _classify_error(e)

            if not _is_retryable(e):
                log.error(
                    "%s: Non-retryable — %s\n  %s: %s\n%s",
                    label, error_class, type(e).__name__, e, traceback.format_exc(),
                )
                raise

            if attempt < len(_RETRY_DELAYS):
                log.warning("%s: Transient error attempt %d — %s: %s",
                            label, attempt + 1, error_class, str(e)[:200])
                print(f"    WARNING: API call failed for '{table_name}' "
                      f"(attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1}): "
                      f"{error_class}: {str(e)[:120]}")

    log.error(
        "%s: All %d attempts failed. Last: %s\n  %s: %s\n%s",
        label, len(_RETRY_DELAYS) + 1, _classify_error(last_exc),
        type(last_exc).__name__, last_exc, traceback.format_exc(),
    )
    print(f"    ERROR: All {len(_RETRY_DELAYS) + 1} attempts failed for '{table_name}': "
          f"{_classify_error(last_exc)}: {str(last_exc)[:200]}")
    raise last_exc


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

def _semantic_table_to_dict(t: SemanticTable) -> dict:
    return {
        "name": t.name,
        "description": t.description,
        "business_concept": t.business_concept,
        "columns": t.columns,
        "relationships": t.relationships,
        "score": t.score,
        "row_count": t.row_count,
    }


def _semantic_table_from_dict(d: dict) -> SemanticTable:
    return SemanticTable(
        name=d["name"],
        description=d.get("description", ""),
        business_concept=d.get("business_concept", "unknown"),
        columns=d.get("columns", []),
        relationships=d.get("relationships", []),
        score=d.get("score", 0),
        row_count=d.get("row_count", 0),
    )


def _load_checkpoint(path: str) -> dict[str, SemanticTable]:
    """Load stage3 checkpoint. Returns {table_name: SemanticTable}."""
    try:
        with open(path) as f:
            data = json.load(f)
        result = {}
        for name, t in data.get("completed_tables", {}).items():
            result[name] = _semantic_table_from_dict(t)
        log.info("Stage3 checkpoint loaded from '%s': %d tables done", path, len(result))
        print(f"  Checkpoint: {len(result)} table(s) already annotated — resuming.")
        return result
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Could not load stage3 checkpoint '%s': %s — starting fresh", path, e)
        print(f"  WARNING: Could not read checkpoint '{path}': {e} — starting fresh.")
        return {}


def _save_checkpoint(path: str, completed: dict[str, SemanticTable]) -> None:
    try:
        data = {
            "version": "1",
            "completed_tables": {
                name: _semantic_table_to_dict(t)
                for name, t in completed.items()
            },
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        log.debug("Stage3 checkpoint saved to '%s' (%d tables)", path, len(completed))
    except Exception as e:
        log.warning("Could not save stage3 checkpoint '%s': %s", path, e)
        print(f"  WARNING: Could not save checkpoint: {e}")


# ── Public entry point ─────────────────────────────────────────────────────────

def run_stage3(
    high_value_tables: list[ScoredTable],
    ai_client: AIClient,
    industry: str = "general",
    max_tables: int | None = None,
    batch_delay: float = 1.0,
    checkpoint_file: str = "stage3_checkpoint.json",
    memory_context: str = "",
    executor: "QueryExecutor | None" = None,
) -> tuple[list[SemanticTable], dict, int]:
    """Generate rich semantic annotations for high-value tables.

    Args:
        high_value_tables: Tables that scored >= min_score in Stage 2
        ai_client:         Provider-agnostic AIClient instance
        industry:          Industry context for the annotation prompt
        max_tables:        Limit to top N tables (cost control)
        batch_delay:       Seconds to sleep between per-table API calls
        checkpoint_file:   Path to save/load per-table progress
        memory_context:    Optional SuperMemory context (str or MemoryContext)
        executor:          Optional QueryExecutor for sample rows (standalone: None)

    Returns:
        (semantic_tables, semantic_layer_dict, total_tokens)
    """
    # Apply table limit (already sorted by score descending from Stage 2)
    tables_to_annotate = high_value_tables
    if max_tables and len(high_value_tables) > max_tables:
        tables_to_annotate = high_value_tables[:max_tables]
        print(f"\n  Stage 3: Limiting to top {max_tables} tables "
              f"(of {len(high_value_tables)} high-value)")

    if executor is None:
        print("\n  Stage 3: Running in standalone mode — sample rows skipped (no DB connection)")

    # ── Load checkpoint ───────────────────────────────────────────────────────
    completed: dict[str, SemanticTable] = _load_checkpoint(checkpoint_file)

    total_tokens = 0
    semantic_tables: list[SemanticTable] = []

    # Add already-completed tables to results in original order
    for table in tables_to_annotate:
        if table.name in completed:
            semantic_tables.append(completed[table.name])

    # Determine memory handling
    _memory_obj = None
    _memory_str = ""
    if hasattr(memory_context, "total_fetched"):
        _memory_obj = memory_context
    elif isinstance(memory_context, str):
        _memory_str = memory_context

    total_count = len(tables_to_annotate)

    for i, table in enumerate(tables_to_annotate, 1):
        if table.name in completed:
            print(f"\n  Annotating table {i}/{total_count}: {table.name} — SKIPPED (checkpoint)")
            log.info("[%d/%d] '%s': skipped via checkpoint", i, total_count, table.name)
            continue

        print(f"\n  Annotating table {i}/{total_count}: {table.name} "
              f"(score={table.score}, {table.row_count:,} rows)...")
        log.info("[%d/%d] Stage 3 starting: table='%s' score=%d rows=%d",
                 i, total_count, table.name, table.score, table.row_count)

        # Pull sample rows if executor is available
        sample_rows: list[dict] = []
        if executor is not None:
            try:
                sample_rows = executor.get_sample_rows(table.name, limit=SAMPLE_ROWS)
            except Exception as e:
                print(f"    WARNING: Could not get sample rows: {e}")
                log.warning("[%d/%d] '%s': sample rows failed: %s", i, total_count, table.name, e)

        prompt = _build_inspection_prompt(table, sample_rows)

        # Per-table memory context
        if _memory_obj is not None:
            table_mem = format_memory_for_stage3(_memory_obj, table.name)
        else:
            table_mem = _memory_str
        system_prompt = get_stage3_system_prompt(industry, memory_context=table_mem)

        try:
            raw_text, tokens = _call_with_retry(
                ai_client=ai_client,
                system_prompt=system_prompt,
                prompt=prompt,
                max_tokens=8192,
                table_name=table.name,
                table_num=i,
                total_tables=total_count,
            )
        except Exception as e:
            error_class = _classify_error(e)
            print(f"    ERROR: Stage 3 failed for '{table.name}' after all retries "
                  f"({error_class}): {type(e).__name__}: {str(e)[:200]}")
            log.error(
                "[%d/%d] '%s': FAILED after all retries — %s\n  %s: %s\n%s",
                i, total_count, table.name, error_class,
                type(e).__name__, e, traceback.format_exc(),
            )
            if batch_delay > 0 and i < total_count:
                time.sleep(batch_delay)
            continue

        total_tokens += tokens
        log.info("[%d/%d] '%s': annotated — %d tokens", i, total_count, table.name, tokens)

        semantic = _parse_annotation(raw_text, table)

        if semantic:
            semantic_tables.append(semantic)
            completed[table.name] = semantic
            _save_checkpoint(checkpoint_file, completed)
            print(f"    OK {semantic.business_concept}: {semantic.description[:80]}...")
        else:
            print(f"    WARNING: Failed to parse annotation for {table.name}")
            log.warning("[%d/%d] '%s': annotation parse failed", i, total_count, table.name)

        if batch_delay > 0 and i < total_count:
            log.debug("Sleeping %.1fs between tables", batch_delay)
            time.sleep(batch_delay)

    semantic_layer = _build_semantic_layer(semantic_tables)

    skipped = len(high_value_tables) - len(tables_to_annotate)
    print(f"\n  Stage 3: {len(semantic_tables)}/{len(tables_to_annotate)} tables annotated"
          + (f" ({skipped} skipped by --max-tables)" if skipped else ""))
    log.info("Stage 3 complete: %d/%d annotated, %d tokens",
             len(semantic_tables), len(tables_to_annotate), total_tokens)

    return semantic_tables, semantic_layer, total_tokens


# ── Prompt builder ─────────────────────────────────────────────────────────────

def _build_inspection_prompt(table: ScoredTable, sample_rows: list[dict]) -> str:
    lines = [
        f"Annotate the following database table.\n",
        f"Table: {table.name}",
        f"Row count: {table.row_count:,}",
        f"AI relevance score: {table.score}/10",
        f"AI-assigned concept: {table.likely_concept}",
        f"AI scoring reason: {table.reason}",
        "",
    ]

    if table.primary_keys:
        lines.append(f"Primary keys: {', '.join(table.primary_keys)}")

    MAX_COLS = 50
    fk_col_names = {fk["from_column"] for fk in table.foreign_keys}
    priority_cols = [c for c in table.columns
                     if c.name in table.primary_keys or c.name in fk_col_names]
    other_cols = [c for c in table.columns
                  if c.name not in table.primary_keys and c.name not in fk_col_names]
    cols_to_show = (priority_cols + other_cols)[:MAX_COLS]
    omitted = len(table.columns) - len(cols_to_show)

    lines.append(
        f"Columns ({len(table.columns)} total"
        f"{f', showing first {MAX_COLS}' if omitted else ''}):"
    )
    for col in cols_to_show:
        pk_flag = " [PK]" if col.name in table.primary_keys else ""
        fk_targets = [fk for fk in table.foreign_keys if fk["from_column"] == col.name]
        fk_flag = (
            f" [FK → {fk_targets[0]['to_table']}.{fk_targets[0]['to_column']}]"
            if fk_targets else ""
        )
        lines.append(f"  - {col.name} ({col.data_type}){pk_flag}{fk_flag}")
    if omitted:
        lines.append(f"  ... and {omitted} more columns omitted for brevity")

    if table.foreign_keys:
        lines.append("\nConfirmed foreign key relationships (from DB schema):")
        for fk in table.foreign_keys:
            lines.append(f"  {fk['from_column']} → {fk['to_table']}.{fk['to_column']}")

    if sample_rows:
        lines.append(f"\nSample rows ({len(sample_rows)} of {table.row_count:,}):")
        for j, row in enumerate(sample_rows):
            truncated = {
                k: (str(v)[:50] + "..." if isinstance(v, str) and len(str(v)) > 50 else v)
                for k, v in row.items()
            }
            lines.append(f"  Row {j + 1}: {json.dumps(truncated, default=str)}")

    return "\n".join(lines)


# ── Response parser ────────────────────────────────────────────────────────────

def _parse_annotation(raw_text: str, table: ScoredTable) -> SemanticTable | None:
    data = parse_json_response(raw_text)
    if data is None:
        print(f"    WARNING: Failed to parse annotation response as JSON")
        print(f"    Raw (first 200 chars): {raw_text[:200]}")
        log.warning("JSON parse failure for '%s' — raw (first 500):\n%s",
                    table.name, raw_text[:500])
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
    """Build the semantic layer dict — schema metadata only, no raw data."""
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
