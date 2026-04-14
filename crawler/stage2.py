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
"""
from __future__ import annotations

import json

from .connection import QueryExecutor
from .constants import get_stage2_system_prompt
from .types import CandidateTable, ScoredTable, ColumnInfo

BATCH_SIZE = 20  # Reduced from 25 — richer prompts are larger


def run_stage2(
    candidates: list[CandidateTable],
    executor: QueryExecutor,
    client: object,
    model: str = "claude-haiku-4-5",
    min_score: int = 7,
    skip_column_stats: bool = False,
    industry: str = "biofuel",
    memory_context: str = "",
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

    Returns:
        (scored_tables, total_tokens) — only tables scoring >= min_score
    """
    total_tokens = 0
    all_scored: list[ScoredTable] = []

    # ── Pre-fetch column stats for all candidates ─────────────────────────────
    # This adds significant context to the scoring prompt.
    # Skipped if --skip-column-stats or candidate count exceeds the performance guard.
    # NOTE: This guard only skips per-column DB stats. AI batch scoring always runs.
    STATS_CANDIDATE_LIMIT = 500  # Raised from 100 — Haiku is cheap, stats are the slow part
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

    # ── Score in batches ──────────────────────────────────────────────────────
    system_prompt = get_stage2_system_prompt(industry, memory_context=memory_context)

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(candidates) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"\n  Batch {batch_num}/{total_batches}: scoring {len(batch)} tables...")

        prompt = _build_batch_prompt(batch)

        try:
            response = client.messages.create(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"  ERROR: Haiku API call failed for batch {batch_num}: {e}")
            continue

        tokens = response.usage.input_tokens + response.usage.output_tokens
        total_tokens += tokens

        raw_text = response.content[0].text.strip()
        scored = _parse_scores(raw_text, batch)
        all_scored.extend(scored)

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
            continue

    return scored
