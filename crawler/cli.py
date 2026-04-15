"""Database schema discovery CLI — v0.3.0.

Subcommands:
  crawl     Stage 1 only. Requires DB connection. Outputs stage1.json.
  score     Stage 2 only. No DB needed. Reads stage1.json, outputs stage2.json.
  annotate  Stage 3 only. No DB needed. Reads stage2.json, outputs results.json.
  full      All 3 stages in sequence (legacy behaviour). Requires DB + AI key.

Examples:

  # Crawl a SQL Server DB (Windows auth), write stage1.json:
  dbscan crawl --db-type mssql --db-host SERVER\\INST --db-name YourDB \\
    --windows-auth --output stage1.json

  # Score with Anthropic Haiku (no DB needed):
  dbscan score stage1.json --provider anthropic --output stage2.json

  # Score with Google Gemini:
  dbscan score stage1.json --provider google --output stage2.json

  # Annotate top 25 tables (no DB needed):
  dbscan annotate stage2.json --provider anthropic --max-tables 25 \\
    --output results.json

  # Annotate with sample rows pulled from DB:
  dbscan annotate stage2.json --provider anthropic \\
    --db-type mssql --db-host SERVER --db-name YourDB --windows-auth \\
    --output results.json

  # Full pipeline (legacy):
  dbscan full --db-type mssql --db-host SERVER --db-name YourDB \\
    --windows-auth --provider anthropic --output results.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        pass

from .ai_client import AIClient, DEFAULT_MODELS, SUPPORTED_PROVIDERS, get_default_model
from .stage1 import run_stage1
from .stage2 import run_stage2
from .stage3 import run_stage3
from .types import CandidateTable, ColumnInfo, ScoredTable


# ── Logging setup ──────────────────────────────────────────────────────────────

def _setup_logging(log_file: str, logger_name: str = "dbscan") -> logging.Logger:
    """Configure the named logger to write to a file (DEBUG) + stderr (WARNING)."""
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except OSError as e:
        print(f"WARNING: Could not open log file '{log_file}': {e}", file=sys.stderr)

    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("Logging initialised — file: %s", log_file)
    return logger


# ── DB connection helpers ──────────────────────────────────────────────────────

def _build_executor(args) -> "QueryExecutor":
    """Build a QueryExecutor from parsed CLI args. Exits on failure."""
    from .connection import QueryExecutor

    try:
        if args.db_type == "sqlite":
            executor = QueryExecutor(db_type="sqlite", db_path=args.db_path)
            print(f"  Connected to SQLite: {args.db_path}")
            return executor

        if not args.db_host or not args.db_name:
            print("ERROR: MSSQL requires --db-host and --db-name")
            sys.exit(1)

        if args.windows_auth:
            executor = QueryExecutor(
                db_type="mssql",
                host=args.db_host,
                port=args.db_port,
                database=args.db_name,
                windows_auth=True,
                odbc_driver=args.odbc_driver,
            )
            print(f"  Connected to SQL Server: {args.db_host}/{args.db_name} (Windows Auth)")
        else:
            if not all([args.db_user, args.db_password]):
                print("ERROR: SQL auth requires --db-user and --db-password (or --windows-auth)")
                sys.exit(1)
            executor = QueryExecutor(
                db_type="mssql",
                host=args.db_host,
                port=args.db_port,
                database=args.db_name,
                user=args.db_user,
                password=args.db_password,
            )
            print(f"  Connected to SQL Server: {args.db_host}/{args.db_name} (SQL Auth)")

        return executor

    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)


def _db_args_provided(args) -> bool:
    """True if enough DB args are present to attempt a connection."""
    if getattr(args, "db_type", None) == "sqlite":
        return bool(getattr(args, "db_path", None))
    return bool(getattr(args, "db_host", None) and getattr(args, "db_name", None))


# ── Stage 1 JSON file helpers ──────────────────────────────────────────────────

def _load_stage1_file(path: str) -> tuple[list[CandidateTable], dict]:
    """Load Stage 1 candidates from a JSON file.

    Supports both:
      - New format (from 'dbscan crawl'): {"version":"1","candidates":[...],"summary":{}}
      - Old format (from old 'dbscan full'): {"crawl_summary":{"stage1_output":...}}
    """
    with open(path) as f:
        data = json.load(f)

    # New format
    if "candidates" in data:
        candidates_raw = data.get("candidates", [])
        summary = data.get("summary", {})
    # Old format
    elif "crawl_summary" in data:
        stage1_out = data["crawl_summary"].get("stage1_output", {})
        candidates_raw = stage1_out.get("candidates", [])
        summary = stage1_out.get("summary", {})
    else:
        raise ValueError(
            f"Unrecognised stage1 format in '{path}'. "
            "Expected 'candidates' key or 'crawl_summary.stage1_output' structure."
        )

    candidates: list[CandidateTable] = []
    for c in candidates_raw:
        columns = [
            ColumnInfo(
                name=col["name"],
                data_type=col["data_type"],
                is_nullable=col.get("is_nullable", True),
            )
            for col in c.get("columns", [])
        ]
        candidates.append(CandidateTable(
            name=c["name"],
            row_count=c.get("row_count", 0),
            columns=columns,
            has_date_columns=bool(c.get("date_columns")),
            date_columns=c.get("date_columns", []),
            sample_values=c.get("sample_values", []),
            heuristic_score=c.get("heuristic_score", 0),
            primary_keys=c.get("primary_keys", []),
            foreign_keys=c.get("foreign_keys", []),
        ))

    return candidates, summary


def _write_stage1_file(
    path: str,
    candidates: list[CandidateTable],
    summary: dict,
    meta: dict | None = None,
) -> None:
    """Write Stage 1 output in the new clean format."""
    output = {
        "version": "1",
        "meta": meta or {},
        "summary": summary,
        "candidates": [
            {
                "name": c.name,
                "row_count": c.row_count,
                "heuristic_score": c.heuristic_score,
                "primary_keys": c.primary_keys,
                "foreign_key_count": len(c.foreign_keys),
                "foreign_keys": c.foreign_keys,
                "columns": [
                    {"name": col.name, "data_type": col.data_type, "is_nullable": col.is_nullable}
                    for col in c.columns
                ],
                "date_columns": c.date_columns,
                "sample_values": c.sample_values,
            }
            for c in candidates
        ],
    }
    with open(path, "w") as f:
        json.dump(output, f, indent=2, default=str)


# ── Stage 2 JSON file helpers ──────────────────────────────────────────────────

def _load_stage2_file(path: str) -> list[ScoredTable]:
    """Load scored tables from a stage2.json file."""
    with open(path) as f:
        data = json.load(f)

    tables_raw = data.get("tables", [])
    tables: list[ScoredTable] = []
    for t in tables_raw:
        columns = [
            ColumnInfo(
                name=col["name"],
                data_type=col["data_type"],
                is_nullable=col.get("is_nullable", True),
            )
            for col in t.get("columns", [])
        ]
        tables.append(ScoredTable(
            name=t["name"],
            score=t.get("score", 0),
            reason=t.get("reason", ""),
            likely_concept=t.get("likely_concept", "unknown"),
            key_columns=t.get("key_columns", []),
            row_count=t.get("row_count", 0),
            columns=columns,
            primary_keys=t.get("primary_keys", []),
            foreign_keys=t.get("foreign_keys", []),
        ))
    return tables


def _write_stage2_file(
    path: str,
    all_scored: list[ScoredTable],
    high_value: list[ScoredTable],
    min_score: int,
    meta: dict | None = None,
) -> None:
    """Write all scored tables to stage2.json (sorted by score desc)."""
    all_sorted = sorted(all_scored, key=lambda t: t.score, reverse=True)
    output = {
        "version": "1",
        "meta": meta or {},
        "stats": {
            "scored_count": len(all_scored),
            "high_value_count": len(high_value),
            "min_score_threshold": min_score,
        },
        "tables": [
            {
                "name": t.name,
                "score": t.score,
                "reason": t.reason,
                "likely_concept": t.likely_concept,
                "key_columns": t.key_columns,
                "row_count": t.row_count,
                "primary_keys": t.primary_keys,
                "foreign_key_count": len(t.foreign_keys),
                "foreign_keys": t.foreign_keys,
                "columns": [
                    {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable}
                    for c in t.columns
                ],
            }
            for t in all_sorted
        ],
    }
    with open(path, "w") as f:
        json.dump(output, f, indent=2, default=str)


# ── Shared argument groups ─────────────────────────────────────────────────────

def _add_db_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("Database Connection (optional for annotate)")
    g.add_argument("--db-type", choices=["sqlite", "mssql"],
                   default=os.getenv("DB_TYPE", "mssql"))
    g.add_argument("--db-path", default=os.getenv("DB_PATH", "local.db"),
                   help="SQLite DB path")
    g.add_argument("--db-host", default=os.getenv("DB_HOST"),
                   help="SQL Server host")
    g.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "1433")))
    g.add_argument("--db-name", default=os.getenv("DB_NAME"))
    g.add_argument("--db-user", default=os.getenv("DB_USER"))
    g.add_argument("--db-password", default=os.getenv("DB_PASSWORD"))
    g.add_argument("--windows-auth", action="store_true",
                   default=os.getenv("DB_WINDOWS_AUTH", "").lower() in ("1", "true", "yes"))
    g.add_argument("--odbc-driver", default=os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server"))


def _add_ai_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("AI Model")
    g.add_argument(
        "--provider",
        choices=SUPPORTED_PROVIDERS,
        default=os.getenv("AI_PROVIDER", "anthropic"),
        help="AI provider (default: anthropic)",
    )
    g.add_argument(
        "--model",
        default=os.getenv("AI_MODEL", ""),
        metavar="MODEL_ID",
        help="Override the default model for the selected provider and stage",
    )
    g.add_argument(
        "--api-key",
        default=os.getenv("AI_API_KEY"),
        metavar="KEY",
        help="API key (falls back to provider-specific env var: ANTHROPIC_API_KEY / "
             "GOOGLE_API_KEY / OPENAI_API_KEY)",
    )


# ── Subcommand: crawl ─────────────────────────────────────────────────────────

def cmd_crawl(args) -> None:
    """Stage 1 only — enumerate tables, apply heuristic filter, write stage1.json."""
    _setup_logging("stage1.log")
    log = logging.getLogger("dbscan")

    print("=" * 60)
    print("  DB Schema Tool — CRAWL (Stage 1)")
    print("=" * 60)

    auth_mode = "Windows auth" if args.windows_auth else "SQL auth"
    print(f"\nConnecting to {args.db_type} ({auth_mode})...")
    executor = _build_executor(args)

    start = time.time()
    candidates, summary = run_stage1(executor, min_rows=args.min_rows)

    meta = {
        "db_type": args.db_type,
        "db_name": getattr(args, "db_name", None) or getattr(args, "db_path", None),
        "industry": args.industry,
        "min_rows": args.min_rows,
        "total_tables": summary.get("total_tables", 0),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    _write_stage1_file(args.output, candidates, summary, meta=meta)
    executor.close()

    duration_ms = int((time.time() - start) * 1000)
    print(f"\n  Crawl complete: {len(candidates)} candidates from "
          f"{summary.get('total_tables', 0)} tables ({duration_ms:,}ms)")
    print(f"  Output: {args.output}")
    log.info("Crawl complete: %d candidates, %dms, output=%s",
             len(candidates), duration_ms, args.output)


# ── Subcommand: score ─────────────────────────────────────────────────────────

def cmd_score(args) -> None:
    """Stage 2 only — AI batch scoring. No DB required."""
    _setup_logging("stage2.log")
    log = logging.getLogger("dbscan")

    print("=" * 60)
    print("  DB Schema Tool — SCORE (Stage 2)")
    print("=" * 60)

    # Load candidates from stage1 file
    stage1_path = args.input_file
    if not os.path.exists(stage1_path):
        print(f"ERROR: Input file not found: {stage1_path}")
        sys.exit(1)

    print(f"\nLoading candidates from {stage1_path}...")
    try:
        candidates, summary = _load_stage1_file(stage1_path)
    except Exception as e:
        print(f"ERROR: Could not load '{stage1_path}': {e}")
        sys.exit(1)
    print(f"  Loaded {len(candidates)} candidates")

    # Determine model
    stage_key = "score"
    model = args.model or get_default_model(args.provider, stage_key)
    if not model:
        print(f"ERROR: No default model for provider '{args.provider}'. Use --model.")
        sys.exit(1)

    print(f"\nAI provider: {args.provider} / model: {model}")
    print(f"Industry: {args.industry} | Min score: {args.min_score} | "
          f"Batch delay: {args.batch_delay}s")

    try:
        ai_client = AIClient(provider=args.provider, model=model, api_key=args.api_key)
    except Exception as e:
        print(f"ERROR: Could not initialise AI client: {e}")
        sys.exit(1)

    print("\n" + "-" * 40)
    print("  STAGE 2: AI Batch Scoring")
    print("-" * 40)

    start = time.time()
    high_value, tokens = run_stage2(
        candidates=candidates,
        ai_client=ai_client,
        min_score=args.min_score,
        industry=args.industry,
        batch_delay=args.batch_delay,
        checkpoint_file=args.checkpoint_file,
    )
    duration_ms = int((time.time() - start) * 1000)

    # Collect all_scored from checkpoint + current run
    # run_stage2 returns only high_value; for the output file we reconstruct all_scored
    # from the checkpoint file which holds every batch result regardless of score
    import json as _json
    all_scored_flat: list[ScoredTable] = []
    try:
        with open(args.checkpoint_file) as f:
            cp = _json.load(f)
        from .stage2 import _scored_table_from_dict
        for batch_tables in cp.get("completed_batches", {}).values():
            all_scored_flat.extend(_scored_table_from_dict(t) for t in batch_tables)
    except Exception:
        all_scored_flat = high_value  # fallback: at least write high-value tables

    meta = {
        "provider": args.provider,
        "model": model,
        "industry": args.industry,
        "min_score": args.min_score,
        "input_file": stage1_path,
        "tokens_used": tokens,
        "duration_ms": duration_ms,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    _write_stage2_file(
        path=args.output,
        all_scored=all_scored_flat,
        high_value=high_value,
        min_score=args.min_score,
        meta=meta,
    )

    print(f"\n  Score complete: {len(high_value)} high-value tables "
          f"(of {len(all_scored_flat)} scored) | {tokens:,} tokens | {duration_ms:,}ms")
    print(f"  Output: {args.output}")
    log.info("Score complete: %d/%d high-value, %d tokens, output=%s",
             len(high_value), len(all_scored_flat), tokens, args.output)


# ── Subcommand: annotate ──────────────────────────────────────────────────────

def cmd_annotate(args) -> None:
    """Stage 3 only — deep annotation. No DB required (optional for sample rows)."""
    _setup_logging("stage3.log")
    log = logging.getLogger("dbscan")

    print("=" * 60)
    print("  DB Schema Tool — ANNOTATE (Stage 3)")
    print("=" * 60)

    stage2_path = args.input_file
    if not os.path.exists(stage2_path):
        print(f"ERROR: Input file not found: {stage2_path}")
        sys.exit(1)

    print(f"\nLoading scored tables from {stage2_path}...")
    try:
        all_tables = _load_stage2_file(stage2_path)
    except Exception as e:
        print(f"ERROR: Could not load '{stage2_path}': {e}")
        sys.exit(1)

    # Filter by min-score
    high_value = [t for t in all_tables if t.score >= args.min_score]
    high_value.sort(key=lambda t: t.score, reverse=True)
    print(f"  Loaded {len(all_tables)} tables, {len(high_value)} above min-score {args.min_score}")

    if not high_value:
        print(f"\n  No tables with score >= {args.min_score}. "
              f"Lower --min-score or re-run score with lower threshold.")
        sys.exit(0)

    # Determine model
    stage_key = "annotate"
    model = args.model or get_default_model(args.provider, stage_key)
    if not model:
        print(f"ERROR: No default model for provider '{args.provider}'. Use --model.")
        sys.exit(1)

    print(f"\nAI provider: {args.provider} / model: {model}")
    print(f"Industry: {args.industry} | Batch delay: {args.batch_delay}s")
    if args.max_tables:
        print(f"Max tables: {args.max_tables}")

    try:
        ai_client = AIClient(provider=args.provider, model=model, api_key=args.api_key)
    except Exception as e:
        print(f"ERROR: Could not initialise AI client: {e}")
        sys.exit(1)

    # Optional DB connection for sample rows
    executor = None
    if _db_args_provided(args):
        print("\nDB connection args detected — connecting for sample rows...")
        executor = _build_executor(args)
    else:
        print("\nNo DB connection args — annotating without sample rows.")

    print("\n" + "-" * 40)
    print("  STAGE 3: Deep Annotation")
    print("-" * 40)

    start = time.time()
    semantic_tables, semantic_layer, tokens = run_stage3(
        high_value_tables=high_value,
        ai_client=ai_client,
        industry=args.industry,
        max_tables=args.max_tables,
        batch_delay=args.batch_delay,
        checkpoint_file=args.checkpoint_file,
        executor=executor,
    )
    duration_ms = int((time.time() - start) * 1000)

    if executor:
        executor.close()

    meta = {
        "provider": args.provider,
        "model": model,
        "industry": args.industry,
        "input_file": stage2_path,
        "tokens_used": tokens,
        "duration_ms": duration_ms,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    output = {
        "version": "1",
        "meta": meta,
        "stats": {
            "annotated_count": len(semantic_tables),
            "input_table_count": len(high_value),
        },
        "semantic_layer": semantic_layer,
        "tables": [
            {
                "name": t.name,
                "description": t.description,
                "business_concept": t.business_concept,
                "score": t.score,
                "row_count": t.row_count,
                "columns": t.columns,
                "relationships": t.relationships,
            }
            for t in semantic_tables
        ],
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n  Annotate complete: {len(semantic_tables)}/{len(high_value)} tables | "
          f"{tokens:,} tokens | {duration_ms:,}ms")
    print(f"  Output: {args.output}")
    log.info("Annotate complete: %d/%d annotated, %d tokens, output=%s",
             len(semantic_tables), len(high_value), tokens, args.output)


# ── Subcommand: full ──────────────────────────────────────────────────────────

def cmd_full(args) -> None:
    """Run all 3 stages in sequence — legacy behaviour. Requires DB + AI key."""
    for stage_log in ["stage1.log", "stage2.log", "stage3.log"]:
        _setup_logging(stage_log)
    log = logging.getLogger("dbscan")

    print("=" * 60)
    print("  DB Schema Tool — FULL PIPELINE")
    print(f"  Industry: {args.industry}")
    print("=" * 60)

    auth_mode = "Windows auth" if args.windows_auth else "SQL auth"
    print(f"\nConnecting to {args.db_type} ({auth_mode})...")
    executor = _build_executor(args)

    start_time = time.time()
    total_tokens = 0

    # ── Stage 1 ───────────────────────────────────────────────────────────────
    stage1_loaded = False
    candidates, stage1_summary = [], {}

    if args.skip_stage1:
        stage1_file = args.skip_stage1
        if os.path.exists(stage1_file):
            print(f"\n  Loading Stage 1 from '{stage1_file}' (--skip-stage1)...")
            try:
                candidates, stage1_summary = _load_stage1_file(stage1_file)
                stage1_loaded = True
                print(f"  Loaded {len(candidates)} candidates (skipping DB enumeration)")
            except Exception as e:
                print(f"  WARNING: Could not load '{stage1_file}': {e}")
                print("  Falling back to Stage 1 from scratch...")
        else:
            print(f"  WARNING: --skip-stage1 specified but '{stage1_file}' not found. "
                  "Running Stage 1.")

    if not stage1_loaded:
        print("\n" + "-" * 40)
        print("  STAGE 1: Heuristic Filter")
        print("-" * 40)
        candidates, stage1_summary = run_stage1(executor, min_rows=args.min_rows)

        # Write stage1.json as a side-effect
        _write_stage1_file(
            "stage1.json", candidates, stage1_summary,
            meta={"db_name": getattr(args, "db_name", None), "industry": args.industry},
        )

    if not candidates:
        print("\n  No candidate tables found.")
        _finish_full(executor, start_time, total_tokens, stage1_summary, candidates,
                     None, None, None, "failed", args)
        return

    # SuperMemory (optional)
    memory_ctx = None
    memory_str = ""
    if args.supermemory_key:
        from .memory import fetch_memory_context, format_memory_for_stage2
        print("\n  Fetching knowledge from SuperMemory...")
        table_names = [c.name for c in candidates]
        memory_ctx = fetch_memory_context(
            api_key=args.supermemory_key,
            industry=args.industry,
            table_names=table_names,
        )
        if memory_ctx.total_fetched > 0:
            memory_str = format_memory_for_stage2(memory_ctx)
            print(f"  Memory: {memory_ctx.total_fetched} patterns fetched "
                  f"({memory_ctx.fetch_duration_ms}ms)")
        else:
            print("  Memory: no prior knowledge found")

    # ── Stage 2 ───────────────────────────────────────────────────────────────
    stage_key = "score"
    model_score = args.model or get_default_model(args.provider, stage_key)
    if not model_score:
        print(f"\n  ERROR: No default score model for provider '{args.provider}'. Use --model.")
        _finish_full(executor, start_time, total_tokens, stage1_summary, candidates,
                     None, None, None, "failed", args)
        return

    try:
        ai_score_client = AIClient(provider=args.provider, model=model_score, api_key=args.api_key)
    except Exception as e:
        print(f"\n  ERROR: Could not initialise AI client for scoring: {e}")
        _finish_full(executor, start_time, total_tokens, stage1_summary, candidates,
                     None, None, None, "failed", args)
        return

    print("\n" + "-" * 40)
    print(f"  STAGE 2: AI Batch Scoring ({args.provider}/{model_score})")
    print(f"  Min score: {args.min_score} | Industry: {args.industry}")
    print("-" * 40)

    high_value, stage2_tokens = run_stage2(
        candidates=candidates,
        ai_client=ai_score_client,
        min_score=args.min_score,
        industry=args.industry,
        memory_context=memory_str,
        batch_delay=args.batch_delay,
        checkpoint_file=args.checkpoint_file,
        executor=executor,
        skip_column_stats=args.skip_column_stats,
    )
    total_tokens += stage2_tokens

    stage2_output = {
        "scored_count": len(candidates),
        "high_value_count": len(high_value),
        "min_score_threshold": args.min_score,
        "tables": [
            {
                "name": t.name, "score": t.score, "reason": t.reason,
                "likely_concept": t.likely_concept, "key_columns": t.key_columns,
                "primary_keys": t.primary_keys, "foreign_key_count": len(t.foreign_keys),
            }
            for t in high_value
        ],
    }

    if not high_value:
        print(f"\n  No tables scored >= {args.min_score}. Stopping.")
        _finish_full(executor, start_time, total_tokens, stage1_summary, candidates,
                     stage2_output, None, None, "completed", args)
        return

    # ── Stage 3 ───────────────────────────────────────────────────────────────
    model_annotate = args.model or get_default_model(args.provider, "annotate")
    if not model_annotate:
        print(f"\n  ERROR: No default annotate model for provider '{args.provider}'. Use --model.")
        _finish_full(executor, start_time, total_tokens, stage1_summary, candidates,
                     stage2_output, None, None, "failed", args)
        return

    # For annotate stage, prefer a capable model — if --model was set use it for both
    try:
        ai_annotate_client = AIClient(
            provider=args.provider, model=model_annotate, api_key=args.api_key
        )
    except Exception as e:
        print(f"\n  ERROR: Could not initialise AI client for annotation: {e}")
        _finish_full(executor, start_time, total_tokens, stage1_summary, candidates,
                     stage2_output, None, None, "failed", args)
        return

    print("\n" + "-" * 40)
    print(f"  STAGE 3: Deep Annotation ({args.provider}/{model_annotate})")
    if args.max_stage3_tables:
        print(f"  Limit: top {args.max_stage3_tables} tables")
    print("-" * 40)

    semantic_tables, semantic_layer, stage3_tokens = run_stage3(
        high_value_tables=high_value,
        ai_client=ai_annotate_client,
        industry=args.industry,
        max_tables=args.max_stage3_tables,
        batch_delay=args.batch_delay,
        checkpoint_file="stage3_checkpoint.json",
        memory_context=memory_ctx if memory_ctx else "",
        executor=executor,
    )
    total_tokens += stage3_tokens

    stage3_output = {
        "annotated_count": len(semantic_tables),
        "tables": [
            {"name": t.name, "description": t.description, "business_concept": t.business_concept}
            for t in semantic_tables
        ],
    }

    _finish_full(
        executor=executor,
        start_time=start_time,
        total_tokens=total_tokens,
        stage1_summary=stage1_summary,
        candidates=candidates,
        stage2_output=stage2_output,
        stage3_output=stage3_output,
        semantic_layer=semantic_layer,
        final_status="completed",
        args=args,
    )


def _finish_full(
    executor,
    start_time: float,
    total_tokens: int,
    stage1_summary: dict,
    candidates: list,
    stage2_output,
    stage3_output,
    semantic_layer,
    final_status: str,
    args,
) -> None:
    """Wrap up full pipeline: print summary and write the combined output file."""
    duration_ms = int((time.time() - start_time) * 1000)

    result = {
        "status": final_status,
        "mode": "windows_auth" if getattr(args, "windows_auth", False) else "sql_auth",
        "industry": getattr(args, "industry", "general"),
        "provider": getattr(args, "provider", "anthropic"),
        "total_tables": stage1_summary.get("total_tables", 0),
        "candidate_tables": stage1_summary.get("candidates", 0),
        "scored_tables": stage2_output.get("scored_count", 0) if stage2_output else 0,
        "high_value_tables": stage2_output.get("high_value_count", 0) if stage2_output else 0,
        "tokens_used": total_tokens,
        "duration_ms": duration_ms,
        "stage1_output": {
            "summary": stage1_summary,
            "candidates": [
                {
                    "name": c.name,
                    "row_count": c.row_count,
                    "heuristic_score": c.heuristic_score,
                    "primary_keys": c.primary_keys,
                    "foreign_key_count": len(c.foreign_keys),
                    "foreign_keys": c.foreign_keys,
                    "columns": [{"name": col.name, "data_type": col.data_type}
                                for col in c.columns],
                    "date_columns": c.date_columns,
                    "sample_values": c.sample_values,
                }
                for c in candidates
            ],
        },
        "stage2_output": stage2_output,
        "stage3_output": stage3_output,
    }

    audit_log = executor.get_audit_log_dicts() if executor else []
    if executor:
        executor.close()

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Status:     {final_status}")
    print(f"  Tables:     {result['total_tables']} total → "
          f"{result['candidate_tables']} candidates → "
          f"{result['high_value_tables']} high-value")
    print(f"  Tokens:     {total_tokens:,}")
    print(f"  Duration:   {duration_ms:,}ms ({duration_ms // 1000}s)")
    if audit_log:
        print(f"  Queries:    {len(audit_log)} executed against the database")

    output = {
        "crawl_summary": result,
        "semantic_layer": semantic_layer or {},
        "audit_log": audit_log,
    }
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  Results saved to {args.output}")


# ── Main parser ────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()

    root = argparse.ArgumentParser(
        prog="dbscan",
        description="Database schema discovery tool — v0.3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run 'dbscan <subcommand> --help' for subcommand options.",
    )
    root.add_argument("--version", action="version", version="dbscan 0.3.0")

    sub = root.add_subparsers(dest="command", required=True)

    # ──────────────────────────────────────────────────────────────────────────
    # crawl
    # ──────────────────────────────────────────────────────────────────────────
    p_crawl = sub.add_parser(
        "crawl",
        help="Stage 1 only — enumerate tables and apply heuristic filter (requires DB)",
        description="Connects to the database, enumerates all tables, applies heuristic "
                    "scoring, and writes candidate tables to stage1.json.",
    )
    _add_db_args(p_crawl)
    crawl_opts = p_crawl.add_argument_group("Crawl Options")
    crawl_opts.add_argument("--min-rows", type=int, default=10,
                            help="Min row count to keep a table (default: 10)")
    crawl_opts.add_argument(
        "--industry",
        choices=["biofuel", "manufacturing", "food_processing", "chemicals", "general"],
        default=os.getenv("INDUSTRY", "general"),
    )
    p_crawl.add_argument("--output", default="stage1.json",
                         help="Output file (default: stage1.json)")

    # ──────────────────────────────────────────────────────────────────────────
    # score
    # ──────────────────────────────────────────────────────────────────────────
    p_score = sub.add_parser(
        "score",
        help="Stage 2 only — AI batch scoring (no DB needed)",
        description="Reads stage1.json, scores every candidate table with AI, "
                    "writes stage2.json.  No database connection required.",
    )
    p_score.add_argument("input_file", metavar="STAGE1_JSON",
                         help="Path to stage1.json produced by 'dbscan crawl'")
    _add_ai_args(p_score)
    score_opts = p_score.add_argument_group("Score Options")
    score_opts.add_argument("--min-score", type=int, default=7,
                            help="Tables scoring >= this are 'high-value' (default: 7)")
    score_opts.add_argument(
        "--industry",
        choices=["biofuel", "manufacturing", "food_processing", "chemicals", "general"],
        default=os.getenv("INDUSTRY", "general"),
    )
    score_opts.add_argument("--batch-delay", type=float, default=1.0, metavar="SECONDS",
                            help="Seconds between API calls (default: 1.0)")
    score_opts.add_argument("--checkpoint-file", default="stage2_checkpoint.json",
                            help="Checkpoint file for resume (default: stage2_checkpoint.json)")
    p_score.add_argument("--output", default="stage2.json",
                         help="Output file (default: stage2.json)")

    # ──────────────────────────────────────────────────────────────────────────
    # annotate
    # ──────────────────────────────────────────────────────────────────────────
    p_ann = sub.add_parser(
        "annotate",
        help="Stage 3 only — deep annotation (no DB needed by default)",
        description="Reads stage2.json, generates rich semantic annotations per table. "
                    "DB connection is optional — provide DB args to include sample rows.",
    )
    p_ann.add_argument("input_file", metavar="STAGE2_JSON",
                       help="Path to stage2.json produced by 'dbscan score'")
    _add_ai_args(p_ann)
    _add_db_args(p_ann)
    ann_opts = p_ann.add_argument_group("Annotate Options")
    ann_opts.add_argument("--min-score", type=int, default=7,
                          help="Only annotate tables with score >= this (default: 7)")
    ann_opts.add_argument("--max-tables", type=int, default=None, metavar="N",
                          help="Limit to top N tables (cost control)")
    ann_opts.add_argument(
        "--industry",
        choices=["biofuel", "manufacturing", "food_processing", "chemicals", "general"],
        default=os.getenv("INDUSTRY", "general"),
    )
    ann_opts.add_argument("--batch-delay", type=float, default=1.0, metavar="SECONDS",
                          help="Seconds between API calls (default: 1.0)")
    ann_opts.add_argument("--checkpoint-file", default="stage3_checkpoint.json",
                          help="Checkpoint file for resume (default: stage3_checkpoint.json)")
    p_ann.add_argument("--output", default="results.json",
                       help="Output file (default: results.json)")

    # ──────────────────────────────────────────────────────────────────────────
    # full  (legacy)
    # ──────────────────────────────────────────────────────────────────────────
    p_full = sub.add_parser(
        "full",
        help="Run all 3 stages in sequence — legacy behaviour (requires DB + AI key)",
        description="Connects to the DB, runs Stage 1 heuristic filter, Stage 2 AI scoring, "
                    "and Stage 3 deep annotation, then writes a combined results file.",
    )
    _add_db_args(p_full)
    _add_ai_args(p_full)
    full_opts = p_full.add_argument_group("Pipeline Options")
    full_opts.add_argument("--min-rows", type=int, default=10)
    full_opts.add_argument("--min-score", type=int, default=7)
    full_opts.add_argument("--max-stage3-tables", type=int, default=None, metavar="N")
    full_opts.add_argument("--skip-column-stats", action="store_true")
    full_opts.add_argument(
        "--skip-stage1", nargs="?", const="stage1.json", default=None, metavar="FILE",
        help="Skip Stage 1 and load candidates from FILE (default: stage1.json)",
    )
    full_opts.add_argument(
        "--industry",
        choices=["biofuel", "manufacturing", "food_processing", "chemicals", "general"],
        default=os.getenv("INDUSTRY", "general"),
    )
    full_opts.add_argument("--batch-delay", type=float, default=1.0)
    full_opts.add_argument("--checkpoint-file", default="stage2_checkpoint.json")
    full_opts.add_argument("--supermemory-key", default=os.getenv("SUPERMEMORY_API_KEY"))
    p_full.add_argument("--output", required=True, help="Write combined results to this JSON file")

    # ── Dispatch ───────────────────────────────────────────────────────────────
    args = root.parse_args()

    dispatch = {
        "crawl": cmd_crawl,
        "score": cmd_score,
        "annotate": cmd_annotate,
        "full": cmd_full,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
