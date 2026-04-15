"""Stage 2: AI Batch Scoring — scores tables for business relevance.

Sends enriched table metadata to the configured AI model in batches.
The prompt includes primary keys, foreign key relationships, and optionally
column statistics (null %, distinct count, numeric range) gathered from the DB.

No raw data leaves the client environment — only schema and stats.

v0.3.0 changes:
- Accepts AIClient instead of a raw Anthropic client — provider-agnostic
- executor is optional (only needed for column stats; omit for standalone mode)
- Uses json_parser.parse_json_response for robust JSON extraction
- Checkpoint/resume built in (stage2_checkpoint.json by default)
- Configurable batch_delay
- stage2.log written independently of cli.py
"""
from __future__ import annotations

import json
import logging
import time
import traceback
from typing import TYPE_CHECKING

from .ai_client import AIClient
from .constants import get_stage2_system_prompt
from .json_parser import parse_json_response
from .types import CandidateTable, ColumnInfo, ScoredTable

if TYPE_CHECKING:
    from .connection import QueryExecutor

BATCH_SIZE = 20

# Retry schedule: 5 retries with exponential backoff
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
    if any(code in exc_str for code in ["400", "401", "403", "404"]):
        # Don't retry on definitive client errors
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
    max_tokens: int = 4096,
    batch_num: int = 0,
    total_batches: int = 0,
    table_names: list[str] | None = None,
) -> tuple[str, int]:
    """Call the AI with exponential backoff. Returns (text, tokens)."""
    last_exc: Exception | None = None
    batch_label = f"Batch {batch_num}/{total_batches}"
    tables_str = ", ".join(table_names or [])

    for attempt, delay in enumerate([0] + _RETRY_DELAYS):
        if attempt > 0:
            log.warning(
                "%s: Retry %d/%d in %ds (previous: %s)",
                batch_label, attempt, len(_RETRY_DELAYS), delay,
                _classify_error(last_exc),
            )
            print(f"    Retrying in {delay}s... (attempt {attempt + 1}/{len(_RETRY_DELAYS) + 1})")
            time.sleep(delay)

        try:
            log.info("%s: API call attempt %d — tables: [%s]", batch_label, attempt + 1, tables_str)
            text, tokens = ai_client.complete(system_prompt, prompt, max_tokens=max_tokens)
            log.info("%s: API call succeeded (attempt %d)", batch_label, attempt + 1)
            return text, tokens

        except Exception as e:
            last_exc = e
            error_class = _classify_error(e)

            if not _is_retryable(e):
                log.error(
                    "%s: Non-retryable error — %s\n  Tables: [%s]\n  %s: %s\n%s",
                    batch_label, error_class, tables_str,
                    type(e).__name__, e, traceback.format_exc(),
                )
                raise

            if attempt < len(_RETRY_DELAYS):
                log.warning(
                    "%s: Transient error attempt %d — %s: %s",
                    batch_label, attempt + 1, error_class, str(e)[:200],
                )

    log.error(
        "%s: All %d attempts failed.\n  Last: %s\n  Tables: [%s]\n  %s: %s\n%s",
        batch_label, len(_RETRY_DELAYS) + 1, _classify_error(last_exc),
        tables_str, type(last_exc).__name__, last_exc, traceback.format_exc(),
    )
    raise last_exc


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

def _scored_table_to_dict(t: ScoredTable) -> dict:
    return {
        "name": t.name,
        "score": t.score,
        "reason": t.reason,
        "likely_concept": t.likely_concept,
        "key_columns": t.key_columns,
        "row_count": t.row_count,
        "columns": [
            {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable}
            for c in t.columns
        ],
        "primary_keys": t.primary_keys,
        "foreign_keys": t.foreign_keys,
    }


def _scored_table_from_dict(d: dict) -> ScoredTable:
    return ScoredTable(
        name=d["name"],
        score=d["score"],
        reason=d["reason"],
        likely_concept=d["likely_concept"],
        key_columns=d.get("key_columns", []),
        row_count=d.get("row_count", 0),
        columns=[
            ColumnInfo(name=c["name"], data_type=c["data_type"],
                       is_nullable=c.get("is_nullable", True))
            for c in d.get("columns", [])
        ],
        primary_keys=d.get("primary_keys", []),
        foreign_keys=d.get("foreign_keys", []),
    )


def _load_checkpoint(path: str) -> dict[int, list[ScoredTable]]:
    try:
        with open(path) as f:
            data = json.load(f)
        result: dict[int, list[ScoredTable]] = {}
        for k, tables in data.get("completed_batches", {}).items():
            result[int(k)] = [_scored_table_from_dict(t) for t in tables]
        log.info("Checkpoint loaded from '%s': %d batches already done", path, len(result))
        print(f"  Checkpoint: {len(result)} batch(es) already done — resuming.")
        return result
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Could not load checkpoint '%s': %s — starting fresh", path, e)
        print(f"  WARNING: Could not read checkpoint '{path}': {e} — starting fresh.")
        return {}


def _save_checkpoint(path: str, completed: dict[int, list[ScoredTable]]) -> None:
    try:
        data = {
            "version": "1",
            "completed_batches": {
                str(k): [_scored_table_to_dict(t) for t in tables]
                for k, tables in completed.items()
            },
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        log.debug("Checkpoint saved to '%s' (%d batches)", path, len(completed))
    except Exception as e:
        log.warning("Could not save checkpoint '%s': %s", path, e)
        print(f"  WARNING: Could not save checkpoint: {e}")


# ── Public entry point ─────────────────────────────────────────────────────────

def run_stage2(
    candidates: list[CandidateTable],
    ai_client: AIClient,
    min_score: int = 7,
    industry: str = "general",
    memory_context: str = "",
    batch_delay: float = 1.0,
    checkpoint_file: str = "stage2_checkpoint.json",
    executor: "QueryExecutor | None" = None,
    skip_column_stats: bool = False,
) -> tuple[list[ScoredTable], int]:
    """Score candidate tables using the configured AI model.

    Args:
        candidates:        Tables that passed Stage 1 heuristic filter
        ai_client:         Provider-agnostic AIClient instance
        min_score:         Return only tables with score >= this (default 7)
        industry:          Industry context for the scoring prompt
        memory_context:    Optional SuperMemory context string
        batch_delay:       Seconds to sleep between API calls
        checkpoint_file:   Path to save/load batch progress
        executor:          Optional QueryExecutor for column stats (standalone: None)
        skip_column_stats: Skip column stats even if executor is provided

    Returns:
        (high_value_tables, total_tokens) — only tables scoring >= min_score
    """
    total_tokens = 0
    all_scored: list[ScoredTable] = []

    # ── Column stats (only when executor is available) ────────────────────────
    STATS_LIMIT = 500
    should_gather_stats = (
        executor is not None
        and not skip_column_stats
        and len(candidates) <= STATS_LIMIT
    )

    if should_gather_stats:
        print(f"\n  Stage 2: Gathering column stats for {len(candidates)} candidates...")
        for i, candidate in enumerate(candidates):
            try:
                stats = executor.get_column_stats(candidate.name, candidate.columns)
                candidate.column_stats = stats
                if (i + 1) % 10 == 0:
                    print(f"    Stats: {i + 1}/{len(candidates)} done")
            except Exception as e:
                log.warning("Column stats failed for %s: %s", candidate.name, e)
                candidate.column_stats = {}
    elif executor is None:
        print("\n  Stage 2: Running in standalone mode — column stats skipped (no DB connection)")
    else:
        reason = ("--skip-column-stats flag" if skip_column_stats
                  else f"{len(candidates)} candidates > limit {STATS_LIMIT}")
        print(f"\n  Stage 2: Column stats skipped ({reason})")

    # ── Load checkpoint ───────────────────────────────────────────────────────
    completed_batches: dict[int, list[ScoredTable]] = _load_checkpoint(checkpoint_file)
    for batch_scored in completed_batches.values():
        all_scored.extend(batch_scored)

    # ── Score in batches ──────────────────────────────────────────────────────
    system_prompt = get_stage2_system_prompt(industry, memory_context=memory_context)
    total_batches = (len(candidates) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i : i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE
        batch_num = batch_idx + 1
        table_names = [t.name for t in batch]

        if batch_idx in completed_batches:
            print(f"\n  Scoring table {i + 1}-{min(i + BATCH_SIZE, len(candidates))}/{len(candidates)}..."
                  f" SKIPPED (checkpoint — {len(completed_batches[batch_idx])} already scored)")
            log.info("Batch %d/%d: skipped via checkpoint", batch_num, total_batches)
            continue

        print(f"\n  Scoring table {i + 1}-{min(i + BATCH_SIZE, len(candidates))}/{len(candidates)}...")
        log.info("Batch %d/%d: starting — [%s]", batch_num, total_batches, ", ".join(table_names))

        prompt = _build_batch_prompt(batch)

        try:
            raw_text, tokens = _call_with_retry(
                ai_client=ai_client,
                system_prompt=system_prompt,
                prompt=prompt,
                max_tokens=4096,
                batch_num=batch_num,
                total_batches=total_batches,
                table_names=table_names,
            )
        except Exception as e:
            error_class = _classify_error(e)
            print(f"  ERROR: Batch {batch_num}/{total_batches} failed ({error_class}) — "
                  f"tables: {', '.join(table_names)}")
            print(f"  {type(e).__name__}: {str(e)[:200]}")
            log.error(
                "Batch %d/%d FAILED — %s\n  Tables: [%s]\n  %s: %s\n%s",
                batch_num, total_batches, error_class,
                ", ".join(table_names), type(e).__name__, e, traceback.format_exc(),
            )
            if batch_delay > 0 and i + BATCH_SIZE < len(candidates):
                time.sleep(batch_delay)
            continue

        total_tokens += tokens
        log.info("Batch %d/%d: success — %d tokens", batch_num, total_batches, tokens)

        scored = _parse_scores(raw_text, batch)
        all_scored.extend(scored)

        completed_batches[batch_idx] = scored
        _save_checkpoint(checkpoint_file, completed_batches)

        if batch_delay > 0 and i + BATCH_SIZE < len(candidates):
            log.debug("Sleeping %.1fs between batches", batch_delay)
            time.sleep(batch_delay)

    # ── Filter and sort ───────────────────────────────────────────────────────
    high_value = [t for t in all_scored if t.score >= min_score]
    high_value.sort(key=lambda t: t.score, reverse=True)

    print(f"\n  Stage 2: {len(all_scored)} scored, "
          f"{len(high_value)} high-value (score >= {min_score})")
    for t in high_value:
        fk_str = f", {len(t.foreign_keys)} FKs" if t.foreign_keys else ""
        print(f"    {t.name}: score={t.score}{fk_str}, concept={t.likely_concept} — {t.reason}")

    return high_value, total_tokens


# ── Prompt builder ─────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list[CandidateTable]) -> str:
    lines = ["Score the following database tables for business relevance.\n"]

    for table in batch:
        lines.append(f"### {table.name}")
        lines.append(f"Row count: {table.row_count:,}")

        if table.primary_keys:
            lines.append(f"Primary keys: {', '.join(table.primary_keys)}")
        else:
            lines.append("Primary keys: none detected")

        if table.foreign_keys:
            fk_parts = [
                f"{fk['from_column']} → {fk['to_table']}.{fk['to_column']}"
                for fk in table.foreign_keys[:8]
            ]
            lines.append(f"Foreign keys: {' | '.join(fk_parts)}")

        lines.append("Columns:")
        for col in table.columns:
            col_str = f"  {col.name} ({col.data_type})"
            if col.name in table.primary_keys:
                col_str += " [PK]"
            fk_targets = [fk for fk in table.foreign_keys if fk["from_column"] == col.name]
            if fk_targets:
                col_str += f" [FK → {fk_targets[0]['to_table']}]"
            if col.name in table.column_stats:
                s = table.column_stats[col.name]
                stat_parts = [f"null: {s['null_pct']}%", f"distinct: {s['distinct_count']:,}"]
                if "min" in s and "max" in s:
                    stat_parts.append(f"range: {s['min']} to {s['max']}")
                col_str += f"  |  {' | '.join(stat_parts)}"
            lines.append(col_str)

        if table.date_columns:
            lines.append(f"Date/time columns: {', '.join(table.date_columns)}")

        if table.sample_values:
            lines.append(f"Sample values (col 2): {', '.join(str(v) for v in table.sample_values)}")

        lines.append("")

    return "\n".join(lines)


# ── Response parser ────────────────────────────────────────────────────────────

def _parse_scores(raw_text: str, batch: list[CandidateTable]) -> list[ScoredTable]:
    """Parse the AI's JSON response into ScoredTable objects.

    Per-table error isolation: a bad value for one table doesn't kill the batch.
    """
    scores = parse_json_response(raw_text)
    if scores is None:
        print(f"  WARNING: Failed to parse AI response as JSON")
        print(f"  Raw (first 300 chars): {raw_text[:300]}")
        return []

    candidate_map = {c.name: c for c in batch}
    candidate_map_upper = {c.name.upper(): c for c in batch}

    scored: list[ScoredTable] = []
    for table_name, info in scores.items():
        if not isinstance(info, dict):
            print(f"  WARNING: Unexpected format for {table_name}, skipping")
            continue

        candidate = candidate_map.get(table_name) or candidate_map_upper.get(table_name.upper())

        try:
            scored.append(ScoredTable(
                name=table_name,
                score=int(info.get("score", 0)),
                reason=str(info.get("reason", "")),
                likely_concept=str(info.get("likely_concept", "unknown")),
                key_columns=list(info.get("key_columns", [])),
                row_count=candidate.row_count if candidate else 0,
                columns=candidate.columns if candidate else [],
                primary_keys=candidate.primary_keys if candidate else [],
                foreign_keys=candidate.foreign_keys if candidate else [],
            ))
        except Exception as e:
            print(f"  WARNING: Skipping {table_name} due to parse error: {e}")
            log.warning("Parse error for table '%s': %s", table_name, e)

    return scored
