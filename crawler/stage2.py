"""Stage 2: AI Batch Scoring — Haiku scores tables for business relevance.

Sends enriched table metadata to Claude Haiku in batches. The prompt now
includes primary keys, foreign key relationships, and column statistics
(null %, distinct count, numeric range) gathered directly from the DB.

No raw data leaves the client's environment — only schema and stats.

Key improvements over v1:
- Executor passed in for column stats (null%, distinct, range)
- Richer prompt: PK, FK, and column stats included
- Per-table error isolation — one bad AI response doesn't fail the batch
- Configurable min_score threshold
- Checkpoint/resume: saves progress after each batch so a mid-run failure
  doesn't throw away completed work
- Exponential backoff with 5 retries (10/20/40/80/120s) for transient errors
- Detailed error logging with full stack traces and error classification
"""
from __future__ import annotations

import json
import logging
import time
import traceback

from .connection import QueryExecutor
from .constants import get_stage2_system_prompt
from .types import CandidateTable, ScoredTable, ColumnInfo

BATCH_SIZE = 20  # Reduced from 25 — richer prompts are larger

# Retry schedule: 5 retries with exponential backoff
_RETRY_DELAYS = [10, 20, 40, 80, 120]  # seconds between attempts

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
        return f"SERVER_ERROR (5xx)"
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
    # Don't retry client errors
    if any(code in exc_str for code in ["status_code=400", "status_code=401",
                                         "status_code=403", "status_code=404"]):
        return False
    return True


# ── Retry wrapper ──────────────────────────────────────────────────────────────

def _call_with_retry(
    client, model: str, system_prompt: str, prompt: str,
    max_tokens: int = 4096,
    batch_num: int = 0, total_batches: int = 0,
    table_names: list[str] | None = None,
) -> object:
    """Call the Anthropic API with exponential backoff on transient errors.

    Retries up to len(_RETRY_DELAYS) times with increasing delays.
    Raises immediately on non-retryable 4xx client errors.
    """
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
            log.info(
                "%s: API call attempt %d — tables: [%s]",
                batch_label, attempt + 1, tables_str,
            )
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
            )
            log.info(
                "%s: API call succeeded (attempt %d)",
                batch_label, attempt + 1,
            )
            return response

        except Exception as e:
            last_exc = e
            error_class = _classify_error(e)

            if not _is_retryable(e):
                log.error(
                    "%s: Non-retryable error — %s\n"
                    "  Tables: [%s]\n"
                    "  Exception: %s: %s\n"
                    "  Traceback:\n%s",
                    batch_label, error_class,
                    tables_str,
                    type(e).__name__, e,
                    traceback.format_exc(),
                )
                raise

            if attempt < len(_RETRY_DELAYS):
                log.warning(
                    "%s: Transient error on attempt %d — %s: %s",
                    batch_label, attempt + 1, error_class, str(e)[:200],
                )
            # else: all retries exhausted — fall through to raise below

    # All attempts failed
    log.error(
        "%s: All %d attempts failed.\n"
        "  Last error class: %s\n"
        "  Tables: [%s]\n"
        "  Exception: %s: %s\n"
        "  Full traceback:\n%s",
        batch_label, len(_RETRY_DELAYS) + 1,
        _classify_error(last_exc),
        tables_str,
        type(last_exc).__name__, last_exc,
        traceback.format_exc(),
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
        "columns": [{"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable}
                    for c in t.columns],
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
    """Load a stage2 checkpoint file. Returns {batch_index: [ScoredTable]}."""
    try:
        with open(path) as f:
            data = json.load(f)
        result: dict[int, list[ScoredTable]] = {}
        for k, tables in data.get("completed_batches", {}).items():
            result[int(k)] = [_scored_table_from_dict(t) for t in tables]
        log.info("Checkpoint loaded from '%s': %d batches already done", path, len(result))
        print(f"  Checkpoint loaded: {len(result)} batch(es) already completed — skipping those.")
        return result
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Could not load checkpoint '%s': %s — starting fresh", path, e)
        print(f"  WARNING: Could not read checkpoint '{path}': {e} — starting fresh.")
        return {}


def _save_checkpoint(path: str, completed: dict[int, list[ScoredTable]]) -> None:
    """Save checkpoint to disk after each batch completes."""
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
    executor: QueryExecutor,
    client: object,
    model: str = "claude-haiku-4-5",
    min_score: int = 7,
    skip_column_stats: bool = False,
    industry: str = "biofuel",
    memory_context: str = "",
    batch_delay: float = 1.0,
    checkpoint_file: str = "stage2_checkpoint.json",
) -> tuple[list[ScoredTable], int]:
    """Score candidate tables using Claude Haiku.

    Args:
        candidates:         Tables that passed Stage 1 heuristic filter
        executor:           QueryExecutor — used to gather column stats
        client:             anthropic.Anthropic() instance
        model:              Model ID to use for scoring
        min_score:          Only return tables with score >= min_score (default 7)
        skip_column_stats:  Skip column stats (faster, lower token count)
        industry:           Industry context for the scoring prompt
        batch_delay:        Seconds to sleep between API calls (default 1.0)
        checkpoint_file:    Path to save/load progress checkpoint (default stage2_checkpoint.json)

    Returns:
        (scored_tables, total_tokens) — only tables scoring >= min_score
    """
    total_tokens = 0
    all_scored: list[ScoredTable] = []

    # ── Pre-fetch column stats for all candidates ─────────────────────────────
    STATS_CANDIDATE_LIMIT = 500
    should_gather_stats = (
        not skip_column_stats
        and len(candidates) <= STATS_CANDIDATE_LIMIT
    )

    if should_gather_stats:
        print(f"\n  Stage 2: Gathering column stats for {len(candidates)} candidates...")
        for i, candidate in enumerate(candidates):
            try:
                stats = executor.get_column_stats(candidate.name, candidate.columns)
                candidate.column_stats = stats
                if (i + 1) % 10 == 0:
                    print(f"    Stats: {i + 1}/{len(candidates)} tables done")
            except Exception as e:
                print(f"    WARNING: Stats failed for {candidate.name}: {e}")
                candidate.column_stats = {}
    else:
        if skip_column_stats:
            reason = "skipped by --skip-column-stats flag"
        else:
            reason = (f"{len(candidates)} candidates exceeds limit of {STATS_CANDIDATE_LIMIT} "
                      f"— use --skip-column-stats to suppress this message")
        print(f"\n  Stage 2: Per-column DB stats skipped ({reason})")
        print("  Stage 2: AI batch scoring will proceed without per-column stats.")

    # ── Load checkpoint (resume from previous run if available) ──────────────
    completed_batches: dict[int, list[ScoredTable]] = _load_checkpoint(checkpoint_file)

    # Populate all_scored from checkpoint so resumed runs get full results
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

        # Resume: skip batches already in checkpoint
        if batch_idx in completed_batches:
            print(f"\n  Batch {batch_num}/{total_batches}: SKIPPED (checkpoint) — "
                  f"{len(completed_batches[batch_idx])} tables already scored")
            log.info("Batch %d/%d: skipped via checkpoint", batch_num, total_batches)
            continue

        print(f"\n  Batch {batch_num}/{total_batches}: scoring {len(batch)} tables...")
        log.info(
            "Batch %d/%d: starting — tables: [%s]",
            batch_num, total_batches, ", ".join(table_names),
        )

        prompt = _build_batch_prompt(batch)

        try:
            response = _call_with_retry(
                client=client,
                model=model,
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
            print(f"  (See log file for full stack trace)")
            log.error(
                "Batch %d/%d FAILED — %s\n"
                "  Tables: [%s]\n"
                "  Exception: %s: %s\n"
                "  Traceback:\n%s",
                batch_num, total_batches, error_class,
                ", ".join(table_names),
                type(e).__name__, e,
                traceback.format_exc(),
            )
            # Delay before next batch even after failure
            if batch_delay > 0 and i + BATCH_SIZE < len(candidates):
                time.sleep(batch_delay)
            continue

        tokens = response.usage.input_tokens + response.usage.output_tokens
        total_tokens += tokens
        log.info(
            "Batch %d/%d: success — %d tokens used",
            batch_num, total_batches, tokens,
        )

        raw_text = response.content[0].text.strip()
        scored = _parse_scores(raw_text, batch)
        all_scored.extend(scored)

        # Save checkpoint after each successful batch
        completed_batches[batch_idx] = scored
        _save_checkpoint(checkpoint_file, completed_batches)

        # Delay between batches to avoid rate limiting
        if batch_delay > 0 and i + BATCH_SIZE < len(candidates):
            log.debug("Sleeping %.1fs between batches", batch_delay)
            time.sleep(batch_delay)

    # ── Filter by min_score ───────────────────────────────────────────────────
    high_value = [t for t in all_scored if t.score >= min_score]
    high_value.sort(key=lambda t: t.score, reverse=True)

    print(f"\n  Stage 2: {len(all_scored)} scored, {len(high_value)} high-value (score >= {min_score})")
    for t in high_value:
        fk_str = f", {len(t.foreign_keys)} FKs" if t.foreign_keys else ""
        print(f"    {t.name}: score={t.score}{fk_str}, concept={t.likely_concept} — {t.reason}")

    return high_value, total_tokens


def _build_batch_prompt(batch: list[CandidateTable]) -> str:
    """Build the enriched user prompt for a batch of tables."""
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
                for fk in table.foreign_keys[:8]  # Cap to keep prompt size sane
            ]
            lines.append(f"Foreign keys: {' | '.join(fk_parts)}")

        # Column listing with stats
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


def _parse_scores(raw_text: str, batch: list[CandidateTable]) -> list[ScoredTable]:
    """Parse Haiku's JSON response into ScoredTable objects.

    Per-table error isolation: a bad value for one table doesn't kill the batch.
    """
    # Strip markdown code fences
    text = raw_text
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        scores: dict = json.loads(text)
    except json.JSONDecodeError:
        print(f"  WARNING: Failed to parse Haiku response as JSON")
        print(f"  Raw (first 300 chars): {raw_text[:300]}")
        log.warning("JSON parse failure — raw response (first 500 chars):\n%s", raw_text[:500])
        return []

    # Build lookup for candidate metadata
    candidate_map = {c.name: c for c in batch}
    # Also case-insensitive fallback
    candidate_map_upper = {c.name.upper(): c for c in batch}

    scored: list[ScoredTable] = []

    for table_name, info in scores.items():
        if not isinstance(info, dict):
            print(f"  WARNING: Unexpected format for {table_name}, skipping")
            continue

        # Match to candidate (exact, then case-insensitive)
        candidate = candidate_map.get(table_name) or candidate_map_upper.get(table_name.upper())

        try:
            scored.append(
                ScoredTable(
                    name=table_name,
                    score=int(info.get("score", 0)),
                    reason=str(info.get("reason", "")),
                    likely_concept=str(info.get("likely_concept", "unknown")),
                    key_columns=list(info.get("key_columns", [])),
                    row_count=candidate.row_count if candidate else 0,
                    columns=candidate.columns if candidate else [],
                    primary_keys=candidate.primary_keys if candidate else [],
                    foreign_keys=candidate.foreign_keys if candidate else [],
                )
            )
        except Exception as e:
            print(f"  WARNING: Skipping {table_name} due to parse error: {e}")
            log.warning("Parse error for table '%s': %s", table_name, e)
            continue

    return scored
