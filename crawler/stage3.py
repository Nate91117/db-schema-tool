"""Stage 3: Deep Inspection — Sonnet generates rich semantic annotations.

Pulls sample rows per high-value table and sends to Claude Sonnet for
detailed business-context annotation.

Key improvements over v1:
- Confirmed FK relationships (from Stage 1 DB query) included in prompt
- Primary keys explicitly called out in column list
- industry-specific system prompt
- max_tables parameter for cost control on large databases
- Sample rows are ephemeral — used for AI call only, never sent to portal
"""
from __future__ import annotations

import json

from .connection import QueryExecutor
from .constants import get_stage3_system_prompt
from .types import ScoredTable, SemanticTable

try:
    from .memory import format_memory_for_stage3, MemoryContext
except ImportError:
    pass

SAMPLE_ROWS = 10


def run_stage3(
    high_value_tables: list[ScoredTable],
    query: QueryExecutor,
    client: object,
    model: str = "claude-sonnet-4-5",
    max_tables: int | None = None,
    industry: str = "biofuel",
    memory_context: str = "",
) -> tuple[list[SemanticTable], dict, int]:
    """Generate rich semantic annotations for high-value tables.

    Args:
        high_value_tables: Tables that scored >= min_score in Stage 2
        query:             Audited query executor (connected to client DB)
        client:            anthropic.Anthropic() instance
        model:             Model ID for annotation (Sonnet)
        max_tables:        Limit to top N tables (cost control for large DBs)
        industry:          Industry context for the annotation prompt

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

    for i, table in enumerate(tables_to_annotate, 1):
        print(f"\n  [{i}/{len(tables_to_annotate)}] Inspecting {table.name} "
              f"(score={table.score}, {table.row_count:,} rows)...")

        # Pull sample rows (ephemeral — never sent to portal)
        sample_rows: list[dict] = []
        try:
            sample_rows = query.get_sample_rows(table.name, limit=SAMPLE_ROWS)
        except Exception as e:
            print(f"    WARNING: Could not get sample rows: {e}")

        prompt = _build_inspection_prompt(table, sample_rows)

        # Per-table memory context if available
        if _memory_obj is not None:
            table_mem = format_memory_for_stage3(_memory_obj, table.name)
        else:
            table_mem = _memory_str
        system_prompt = get_stage3_system_prompt(industry, memory_context=table_mem)

        try:
            response = client.messages.create(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"    ERROR: Sonnet API call failed: {e}")
            continue

        tokens = response.usage.input_tokens + response.usage.output_tokens
        total_tokens += tokens

        raw_text = response.content[0].text.strip()
        semantic = _parse_annotation(raw_text, table)

        if semantic:
            semantic_tables.append(semantic)
            print(f"    OK {semantic.business_concept}: {semantic.description[:80]}...")
        else:
            print(f"    WARNING: Failed to parse annotation for {table.name}")

    # Build the semantic layer dict (what gets stored in the portal — no raw data)
    semantic_layer = _build_semantic_layer(semantic_tables)

    skipped = len(high_value_tables) - len(tables_to_annotate)
    print(f"\n  Stage 3: {len(semantic_tables)}/{len(tables_to_annotate)} tables annotated"
          + (f" ({skipped} skipped by --max-stage3-tables)" if skipped else ""))

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
