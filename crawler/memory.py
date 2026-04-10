"""SuperMemory integration — fetches known patterns before AI scoring.

Pre-crawl: queries SuperMemory for relevant knowledge given the industry
and table names found in Stage 1. Formats results as prompt context
for Stage 2 (batch scoring) and Stage 3 (deep annotation).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

try:
    from supermemory import Supermemory
    HAS_SDK = True
except ImportError:
    HAS_SDK = False


CONTAINER_TAG = "db-schema-tool"
MAX_CONTEXT_CHARS = 6000  # ~2000 tokens worth of context


@dataclass
class MemoryContext:
    """Holds retrieved memories from SuperMemory."""
    table_memories: list[dict] = field(default_factory=list)
    pattern_memories: list[dict] = field(default_factory=list)
    total_fetched: int = 0
    fetch_duration_ms: int = 0


def fetch_memory_context(
    api_key: str,
    industry: str = "biofuel",
    table_names: Optional[list[str]] = None,
) -> MemoryContext:
    """Fetch relevant knowledge from SuperMemory before AI scoring.

    Makes 2 search calls:
      1. Industry + ERP patterns (general knowledge)
      2. Specific table names from Stage 1 candidates

    Args:
        api_key:      SuperMemory API key
        industry:     Industry context (biofuel, manufacturing, etc.)
        table_names:  Table names from Stage 1 candidates (top 20 used)

    Returns:
        MemoryContext with table and pattern memories
    """
    if not HAS_SDK:
        print("  Memory: supermemory SDK not installed (pip install --pre supermemory)")
        return MemoryContext()

    start = time.time()
    client = Supermemory(api_key=api_key)
    context = MemoryContext()
    seen_ids: set[str] = set()

    # Search 1: General industry + ERP patterns
    try:
        results = client.search.execute(
            q=f"Dynamics AX {industry} table scoring patterns business concepts",
            container_tag=CONTAINER_TAG,
            limit=10,
        )
        for doc in (results.results if hasattr(results, 'results') else []):
            doc_id = getattr(doc, 'id', None) or getattr(doc, 'custom_id', str(id(doc)))
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            content = getattr(doc, 'content', '')
            metadata = getattr(doc, 'metadata', {}) or {}
            score = getattr(doc, 'score', 0)

            mem_type = metadata.get('type', 'unknown')
            entry = {'content': content, 'metadata': metadata, 'score': score}

            if mem_type == 'pattern_knowledge':
                context.pattern_memories.append(entry)
            else:
                context.table_memories.append(entry)
    except Exception as e:
        print(f"  Memory: Search 1 failed: {e}")

    # Search 2: Specific table names (top 20)
    if table_names:
        names_query = " ".join(table_names[:20])
        try:
            results = client.search.execute(
                q=f"Dynamics AX tables {names_query}",
                container_tag=CONTAINER_TAG,
                limit=15,
            )
            for doc in (results.results if hasattr(results, 'results') else []):
                doc_id = getattr(doc, 'id', None) or getattr(doc, 'custom_id', str(id(doc)))
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                content = getattr(doc, 'content', '')
                metadata = getattr(doc, 'metadata', {}) or {}
                score = getattr(doc, 'score', 0)

                mem_type = metadata.get('type', 'unknown')
                entry = {'content': content, 'metadata': metadata, 'score': score}

                if mem_type == 'pattern_knowledge':
                    context.pattern_memories.append(entry)
                else:
                    context.table_memories.append(entry)
        except Exception as e:
            print(f"  Memory: Search 2 failed: {e}")

    context.total_fetched = len(context.table_memories) + len(context.pattern_memories)
    context.fetch_duration_ms = int((time.time() - start) * 1000)

    return context


def format_memory_for_stage2(memory: MemoryContext) -> str:
    """Format retrieved memories as a prompt section for Stage 2 scoring.

    Returns an empty string if no memories were found, so prompts remain
    unchanged when no prior knowledge exists.
    """
    if memory.total_fetched == 0:
        return ""

    lines: list[str] = [
        "",
        "=== KNOWN PATTERNS FROM PREVIOUS ENGAGEMENTS ===",
        "The following knowledge was gathered from reviewing previous client databases.",
        "Use this to calibrate your scoring — but always score based on the actual metadata you see.",
        "",
    ]

    total_chars = 0

    if memory.pattern_memories:
        lines.append("[General Patterns]")
        for mem in memory.pattern_memories:
            content = mem['content']
            if total_chars + len(content) > MAX_CONTEXT_CHARS:
                break
            lines.append(content)
            lines.append("")
            total_chars += len(content)

    if memory.table_memories:
        lines.append("[Known Table Knowledge]")
        # Sort by relevance score descending
        sorted_mems = sorted(memory.table_memories, key=lambda m: m.get('score', 0), reverse=True)
        for mem in sorted_mems:
            content = mem['content']
            if total_chars + len(content) > MAX_CONTEXT_CHARS:
                break
            lines.append(content)
            lines.append("")
            total_chars += len(content)

    lines.append("=== END KNOWN PATTERNS ===")
    lines.append("")

    return "\n".join(lines)


def format_memory_for_stage3(memory: MemoryContext, table_name: str) -> str:
    """Format memories relevant to a specific table for Stage 3 annotation.

    Filters table memories to only those matching or related to the given table.
    Always includes pattern memories (they're broadly applicable).
    """
    if memory.total_fetched == 0:
        return ""

    # Filter table memories to those that mention this table name
    table_upper = table_name.upper()
    relevant_table_mems = [
        m for m in memory.table_memories
        if table_upper in m['content'].upper()
    ]

    if not relevant_table_mems and not memory.pattern_memories:
        return ""

    lines: list[str] = [
        "",
        "=== KNOWN CONTEXT FROM PREVIOUS ENGAGEMENTS ===",
        "",
    ]

    total_chars = 0

    if relevant_table_mems:
        lines.append(f"[Previous knowledge about {table_name}]")
        for mem in relevant_table_mems:
            content = mem['content']
            if total_chars + len(content) > MAX_CONTEXT_CHARS // 2:
                break
            lines.append(content)
            lines.append("")
            total_chars += len(content)

    if memory.pattern_memories:
        lines.append("[General patterns]")
        for mem in memory.pattern_memories[:3]:  # Only top 3 patterns for stage 3
            content = mem['content']
            if total_chars + len(content) > MAX_CONTEXT_CHARS:
                break
            lines.append(content)
            lines.append("")
            total_chars += len(content)

    lines.append("Use this as context but annotate based on the actual data you see.")
    lines.append("=== END KNOWN CONTEXT ===")
    lines.append("")

    return "\n".join(lines)
