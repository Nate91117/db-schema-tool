"""Stage 1: Heuristic Filter — no AI, pure code.

Scans all tables in a database, pulls metadata, and filters down to
candidate tables worth AI-scoring in Stage 2.

Key improvements over v1:
- Bulk row count via sys.partitions (one query for ALL tables — critical for AX)
- Primary key and foreign key detection per candidate
- Noise prefix filtering (DEL_, TMP, SYS prefix tables auto-excluded)
- Extension table prefix bonus (Z, X, CUS, ISV)
"""
from __future__ import annotations

from .connection import QueryExecutor
from .constants import (
    RELEVANT_KEYWORDS,
    NOISE_KEYWORDS,
    NOISE_PREFIXES,
    EXTENSION_PREFIXES,
)
from .types import ColumnInfo, CandidateTable


def run_stage1(
    query: QueryExecutor,
    min_rows: int = 10,
) -> tuple[list[CandidateTable], dict]:
    """Run Stage 1 heuristic filter.

    Args:
        query:    Audited query executor (connected to client DB)
        min_rows: Minimum row count to consider a table

    Returns:
        (candidates, summary) where summary contains filter stats
    """
    tables = query.get_tables()
    total_tables = len(tables)
    print(f"\n  Stage 1: Found {total_tables} tables")

    # ── Bulk row counts (single query on MSSQL — dramatically faster for AX) ─
    print("  Stage 1: Fetching row counts...")
    bulk_counts = query.get_all_row_counts()
    if bulk_counts:
        print(f"  Stage 1: Got {len(bulk_counts)} row counts in one query (sys.partitions)")
    else:
        print("  Stage 1: Counting rows per-table (SQLite or sys.partitions unavailable)")

    candidates: list[CandidateTable] = []
    filtered_out: dict[str, str] = {}

    for table in tables:
        table_upper = table.upper()

        # ── Noise prefix filter (AX-specific) ──────────────────────────────
        if any(table_upper.startswith(pfx.upper()) for pfx in NOISE_PREFIXES):
            filtered_out[table] = "noise_prefix"
            continue

        # ── Noise keyword filter ────────────────────────────────────────────
        if any(kw in table_upper for kw in NOISE_KEYWORDS):
            filtered_out[table] = "noise_keyword"
            continue

        # ── Row count (use bulk if available) ──────────────────────────────
        if bulk_counts:
            row_count = bulk_counts.get(table, 0)
        else:
            try:
                row_count = query.get_fast_row_count(table)
            except Exception:
                filtered_out[table] = "count_error"
                continue

        if row_count < min_rows:
            filtered_out[table] = "too_few_rows"
            continue

        # ── Column metadata ────────────────────────────────────────────────
        try:
            col_dicts = query.get_columns(table)
        except Exception:
            filtered_out[table] = "column_error"
            continue

        if not col_dicts:
            filtered_out[table] = "no_columns"
            continue

        columns = [
            ColumnInfo(
                name=c["name"],
                data_type=c["data_type"],
                is_nullable=c.get("is_nullable", True),
            )
            for c in col_dicts
        ]

        # ── Date columns ───────────────────────────────────────────────────
        date_columns = [
            c.name for c in columns
            if any(kw in c.name.upper() for kw in ["DATE", "TIME", "DATETIME", "PERIOD"])
        ]

        # ── Sample values from second column ──────────────────────────────
        sample_values: list[str] = []
        if len(columns) > 1:
            try:
                sample_values = query.get_sample_values(table, columns[1].name, limit=5)
            except Exception:
                pass

        # ── Heuristic scoring ──────────────────────────────────────────────
        score = 0

        # Keyword match on table name
        if any(kw in table_upper for kw in RELEVANT_KEYWORDS):
            score += 3

        # Date columns suggest transactional data
        if date_columns:
            score += 2

        # More rows = more likely to be real data
        if row_count > 1_000:
            score += 1
        if row_count > 100_000:
            score += 1

        # Extension/custom tables — high priority in AX
        if any(table_upper.startswith(pfx.upper()) for pfx in EXTENSION_PREFIXES):
            score += 2

        # Many columns = richer table
        if len(columns) > 20:
            score += 1

        if score < 2:
            filtered_out[table] = "low_score"
            continue

        # ── PK and FK detection (cheap info-schema queries) ────────────────
        primary_keys: list[str] = []
        foreign_keys: list[dict] = []
        try:
            primary_keys = query.get_primary_keys(table)
        except Exception:
            pass
        try:
            foreign_keys = query.get_foreign_keys(table)
        except Exception:
            pass

        candidates.append(
            CandidateTable(
                name=table,
                row_count=row_count,
                columns=columns,
                has_date_columns=bool(date_columns),
                date_columns=date_columns,
                sample_values=sample_values,
                heuristic_score=score,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys,
            )
        )

    # Sort by heuristic score descending
    candidates.sort(key=lambda t: t.heuristic_score, reverse=True)

    filter_reasons = {
        reason: sum(1 for v in filtered_out.values() if v == reason)
        for reason in set(filtered_out.values())
    }

    summary = {
        "total_tables": total_tables,
        "candidates": len(candidates),
        "filtered_out": len(filtered_out),
        "filter_reasons": filter_reasons,
    }

    print(f"  Stage 1: {len(candidates)} candidates out of {total_tables} total")
    print(f"  Stage 1: Filter reasons: {filter_reasons}")
    for c in candidates[:20]:  # Show top 20 to avoid flooding output
        pk_str = f" [PK: {', '.join(c.primary_keys)}]" if c.primary_keys else ""
        fk_str = f" [{len(c.foreign_keys)} FKs]" if c.foreign_keys else ""
        print(f"    {c.name}: {c.row_count:,} rows, score={c.heuristic_score}{pk_str}{fk_str}")
    if len(candidates) > 20:
        print(f"    ... and {len(candidates) - 20} more candidates")

    return candidates, summary
