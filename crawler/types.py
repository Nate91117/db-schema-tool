from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool = True


@dataclass
class CandidateTable:
    """Stage 1 output — table that passed heuristic filter."""
    name: str
    row_count: int
    columns: list[ColumnInfo]
    has_date_columns: bool
    date_columns: list[str]
    sample_values: list[str]
    heuristic_score: int
    # New: structural metadata gathered in Stage 1
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)
    # New: column stats gathered in Stage 2 pre-processing
    column_stats: dict = field(default_factory=dict)


@dataclass
class ScoredTable:
    """Stage 2 output — table scored by AI."""
    name: str
    score: int
    reason: str
    likely_concept: str
    key_columns: list[str]
    # Carried from Stage 1
    row_count: int = 0
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)


@dataclass
class SemanticTable:
    """Stage 3 output — fully annotated table."""
    name: str
    description: str
    business_concept: str
    columns: list[dict]  # [{name, description, data_type, business_meaning}]
    relationships: list[dict]  # [{from_col, to_table, to_col, relationship_type}]
    score: int
    row_count: int


@dataclass
class AuditEntry:
    """Record of a query executed against client DB."""
    query_text: str
    query_type: str  # metadata | sample | count | report
    table_name: Optional[str] = None
    row_count_returned: Optional[int] = None
    duration_ms: Optional[int] = None
    executed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class CrawlResult:
    """Full pipeline output — sent to portal webhook."""
    status: str  # completed | failed
    total_tables: int
    candidate_tables: int
    scored_tables: int
    high_value_tables: int
    tokens_used: int
    duration_ms: int
    stage1_output: dict
    stage2_output: dict
    stage3_output: dict
    semantic_layer: dict
    audit_log: list[dict]
    error_stage: Optional[str] = None
    error_message: Optional[str] = None
