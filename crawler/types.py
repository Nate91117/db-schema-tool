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
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)
    column_stats: dict = field(default_factory=dict)
    contract_signature: dict = field(default_factory=dict)


@dataclass
class ScoredTable:
    """Stage 2 output — table scored by AI (or carried below the cap)."""
    name: str
    score: int
    reason: str
    likely_concept: str
    key_columns: list[str]
    row_count: int = 0
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)
    heuristic_score: int = 0
    contract_signature: dict = field(default_factory=dict)
    market_risk_score: int = 0
    market_risk_rationale: str = ""
    ai_scored: bool = True


@dataclass
class SemanticTable:
    """Stage 3 output — fully annotated table."""
    name: str
    description: str
    business_concept: str
    columns: list[dict]
    relationships: list[dict]
    score: int
    row_count: int
    market_risk_score: int = 0
    market_risk_rationale: str = ""
    contract_signature: dict = field(default_factory=dict)


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
