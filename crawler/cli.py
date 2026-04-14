"""Database schema discovery CLI — schema discovery + semantic layer builder.

Usage examples:

  # Against a local SQLite DB:
  python -m crawler.cli --db-type sqlite --db-path local.db --output results.json

  # SQL Server with username/password:
  python -m crawler.cli --db-type mssql --db-host 192.168.1.10 --db-name YourDB \
    --db-user readonly_user --db-password secret --output results.json

  # SQL Server with Windows Authentication (domain-joined Windows PC):
  python -m crawler.cli --db-type mssql --db-host SERVER\\INSTANCE \
    --db-name YourDB --windows-auth --output results.json

  # Full run with industry context and table limit:
  python -m crawler.cli --db-type mssql --windows-auth --db-host SERVER \
    --db-name YourDB --industry general --max-stage3-tables 25 --output results.json
"""

import argparse
import json
import os
import sys
import time

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        pass

from .connection import QueryExecutor
from .stage1 import run_stage1
from .stage2 import run_stage2
from .stage3 import run_stage3
from .memory import fetch_memory_context, format_memory_for_stage2


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Database schema discovery tool — AI-assisted schema discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── Database connection ───────────────────────────────────────────────────
    db_group = parser.add_argument_group("Database Connection")
    db_group.add_argument(
        "--db-type",
        choices=["sqlite", "mssql"],
        default=os.getenv("DB_TYPE", "sqlite"),
        help="Database type (default: from .env or sqlite)",
    )
    db_group.add_argument("--db-path", default=os.getenv("DB_PATH", "local.db"),
                          help="SQLite DB path")
    db_group.add_argument("--db-host", default=os.getenv("DB_HOST"),
                          help="SQL Server host (e.g. 192.168.1.10 or SERVER\\INSTANCE)")
    db_group.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "1433")),
                          help="SQL Server port")
    db_group.add_argument("--db-name", default=os.getenv("DB_NAME"),
                          help="SQL Server database name")
    db_group.add_argument("--db-user", default=os.getenv("DB_USER"),
                          help="SQL Server username (SQL auth only)")
    db_group.add_argument("--db-password", default=os.getenv("DB_PASSWORD"),
                          help="SQL Server password (SQL auth only)")
    db_group.add_argument(
        "--windows-auth", action="store_true",
        default=os.getenv("DB_WINDOWS_AUTH", "").lower() in ("1", "true", "yes"),
        help="Use Windows/domain authentication (requires pyodbc + ODBC Driver 17)",
    )
    db_group.add_argument(
        "--odbc-driver",
        default=os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server"),
        help="ODBC driver name for Windows auth (default: 'ODBC Driver 17 for SQL Server')",
    )

    # ── Crawler behavior ──────────────────────────────────────────────────────
    crawl_group = parser.add_argument_group("Crawler Options")
    crawl_group.add_argument("--stage", type=int, choices=[1, 2, 3], default=None,
                              help="Run only up to this stage (default: all stages)")
    crawl_group.add_argument("--min-rows", type=int, default=10,
                              help="Min rows for Stage 1 filter (default: 10)")
    crawl_group.add_argument("--min-score", type=int, default=7,
                              help="Min AI score to pass Stage 2 filter (default: 7, range 1-10)")
    crawl_group.add_argument(
        "--max-stage3-tables", type=int, default=None, metavar="N",
        help="Limit Stage 3 deep inspection to top N tables (cost control for large DBs)",
    )
    crawl_group.add_argument(
        "--skip-column-stats", action="store_true",
        help="Skip column statistics in Stage 2 (faster but lower scoring quality)",
    )
    crawl_group.add_argument(
        "--skip-stage1", nargs="?", const="stage1.json", default=None, metavar="FILE",
        help="Skip Stage 1 and load candidates from FILE (default: stage1.json). "
             "If FILE does not exist, Stage 1 runs normally.",
    )
    crawl_group.add_argument(
        "--industry",
        choices=["biofuel", "manufacturing", "food_processing", "chemicals", "general"],
        default=os.getenv("INDUSTRY", "general"),
        help="Industry context for AI prompts (default: general)",
    )

    # ── AI models ─────────────────────────────────────────────────────────────
    model_group = parser.add_argument_group("AI Models")
    model_group.add_argument("--anthropic-key", default=os.getenv("ANTHROPIC_API_KEY"),
                              help="Anthropic API key")
    model_group.add_argument("--haiku-model", default="claude-haiku-4-5",
                              help="Model for Stage 2 scoring (default: claude-haiku-4-5)")
    model_group.add_argument("--sonnet-model", default="claude-sonnet-4-5",
                              help="Model for Stage 3 inspection (default: claude-sonnet-4-5)")
    model_group.add_argument("--supermemory-key", default=os.getenv("SUPERMEMORY_API_KEY"),
                              help="SuperMemory API key for pre-crawl knowledge fetch (optional)")

    # ── Output ────────────────────────────────────────────────────────────────
    out_group = parser.add_argument_group("Output")
    out_group.add_argument("--output", required=True,
                           help="Write results to a local JSON file (required)")

    args = parser.parse_args()

    print("=" * 60)
    print("  Database Schema Discovery Tool")
    print(f"  Industry: {args.industry}")
    print("=" * 60)

    # ── Connect to database ───────────────────────────────────────────────────
    start_time = time.time()
    auth_mode = "Windows auth" if args.windows_auth else "SQL auth"
    print(f"\nConnecting to {args.db_type} ({auth_mode})...")

    try:
        if args.db_type == "sqlite":
            executor = QueryExecutor(db_type="sqlite", db_path=args.db_path)
            print(f"  Connected to SQLite: {args.db_path}")

        elif args.db_type == "mssql":
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
                    print("ERROR: SQL auth requires --db-user and --db-password")
                    print("       Or use --windows-auth for domain authentication")
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

    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        sys.exit(1)

    total_tokens = 0

    # ── Stage 1: Heuristic Filter ─────────────────────────────────────────────
    print("\n" + "-" * 40)
    print("  STAGE 1: Heuristic Filter")
    print("-" * 40)

    stage1_loaded_from_file = False
    candidates, stage1_summary = [], {}

    if args.skip_stage1:
        stage1_file = args.skip_stage1
        if os.path.exists(stage1_file):
            print(f"  Loading Stage 1 results from '{stage1_file}' (--skip-stage1)...")
            try:
                candidates, stage1_summary = _load_stage1_from_file(stage1_file)
                stage1_loaded_from_file = True
                print(f"  Loaded {len(candidates)} candidates from cache "
                      f"(skipping DB enumeration)")
            except Exception as e:
                print(f"  WARNING: Could not load '{stage1_file}': {e}")
                print("  Falling back to running Stage 1 from scratch...")
        else:
            print(f"  WARNING: --skip-stage1 specified but '{stage1_file}' not found. "
                  "Running Stage 1 from scratch.")

    if not stage1_loaded_from_file:
        candidates, stage1_summary = run_stage1(executor, min_rows=args.min_rows)

    if args.stage == 1 or not candidates:
        if not candidates:
            print("\n  No candidate tables found. Nothing to score.")
        _finish(
            executor=executor, start_time=start_time, total_tokens=total_tokens,
            stage1_summary=stage1_summary, candidates=candidates,
            stage2_output=None, stage3_output=None, semantic_layer=None,
            final_status="completed" if args.stage == 1 else "failed",
            args=args,
        )
        return

    # ── Pre-crawl memory fetch ────────────────────────────────────────────────
    memory_ctx = None
    memory_str = ""
    if args.supermemory_key:
        print("\n  Fetching knowledge from SuperMemory...")
        table_names = [c.name for c in candidates]
        memory_ctx = fetch_memory_context(
            api_key=args.supermemory_key,
            industry=args.industry,
            table_names=table_names,
        )
        if memory_ctx.total_fetched > 0:
            memory_str = format_memory_for_stage2(memory_ctx)
            print(f"  Memory: {memory_ctx.total_fetched} known patterns fetched "
                  f"({len(memory_ctx.table_memories)} table, "
                  f"{len(memory_ctx.pattern_memories)} pattern) "
                  f"in {memory_ctx.fetch_duration_ms}ms")
        else:
            print("  Memory: no prior knowledge found (this is normal for first crawl)")

    # ── Stage 2: AI Batch Scoring ─────────────────────────────────────────────
    run_stage2_flag = args.stage is None or args.stage >= 2
    stage2_output = None
    high_value: list = []

    if run_stage2_flag:
        if not args.anthropic_key:
            print("\n  ERROR: Stage 2 requires --anthropic-key or ANTHROPIC_API_KEY env var")
            _finish(
                executor=executor, start_time=start_time, total_tokens=total_tokens,
                stage1_summary=stage1_summary, candidates=candidates,
                stage2_output=None, stage3_output=None, semantic_layer=None,
                final_status="failed", args=args,
            )
            return

        import anthropic
        ai_client = anthropic.Anthropic(api_key=args.anthropic_key)

        print("\n" + "-" * 40)
        print("  STAGE 2: AI Batch Scoring (Haiku)")
        print(f"  Min score: {args.min_score} | Industry: {args.industry}")
        print("-" * 40)

        high_value, stage2_tokens = run_stage2(
            candidates=candidates,
            executor=executor,
            client=ai_client,
            model=args.haiku_model,
            min_score=args.min_score,
            skip_column_stats=args.skip_column_stats,
            industry=args.industry,
            memory_context=memory_str,
        )
        total_tokens += stage2_tokens

        stage2_output = {
            "scored_count": len(candidates),
            "high_value_count": len(high_value),
            "min_score_threshold": args.min_score,
            "tables": [
                {
                    "name": t.name,
                    "score": t.score,
                    "reason": t.reason,
                    "likely_concept": t.likely_concept,
                    "key_columns": t.key_columns,
                    "primary_keys": t.primary_keys,
                    "foreign_key_count": len(t.foreign_keys),
                }
                for t in high_value
            ],
        }

        if args.stage == 2 or not high_value:
            if not high_value:
                print(f"\n  No tables scored >= {args.min_score}. Stopping.")
                print(f"  Try lowering --min-score (current: {args.min_score})")
            _finish(
                executor=executor, start_time=start_time, total_tokens=total_tokens,
                stage1_summary=stage1_summary, candidates=candidates,
                stage2_output=stage2_output, stage3_output=None, semantic_layer=None,
                final_status="completed" if args.stage == 2 else "failed", args=args,
            )
            return

    # ── Stage 3: Deep Inspection ──────────────────────────────────────────────
    run_stage3_flag = args.stage is None or args.stage >= 3
    stage3_output = None
    semantic_layer = None

    if run_stage3_flag and high_value:
        print("\n" + "-" * 40)
        print("  STAGE 3: Deep Inspection (Sonnet)")
        if args.max_stage3_tables:
            print(f"  Limit: top {args.max_stage3_tables} tables")
        print("-" * 40)

        semantic_tables, semantic_layer, stage3_tokens = run_stage3(
            high_value_tables=high_value,
            query=executor,
            client=ai_client,
            model=args.sonnet_model,
            max_tables=args.max_stage3_tables,
            industry=args.industry,
            memory_context=memory_ctx if memory_ctx else "",
        )
        total_tokens += stage3_tokens

        stage3_output = {
            "annotated_count": len(semantic_tables),
            "tables": [
                {
                    "name": t.name,
                    "description": t.description,
                    "business_concept": t.business_concept,
                }
                for t in semantic_tables
            ],
        }

    _finish(
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


def _load_stage1_from_file(path: str):
    """Load Stage 1 candidates from a previously saved JSON output file.

    Returns (candidates, summary) matching the shape returned by run_stage1().
    Preserves full FK data if the file was written by a recent version of this tool
    (which saves foreign_keys, not just foreign_key_count).
    """
    from .types import CandidateTable, ColumnInfo

    with open(path, "r") as f:
        data = json.load(f)

    stage1_out = data.get("crawl_summary", {}).get("stage1_output", {})
    summary = stage1_out.get("summary", {})
    candidate_dicts = stage1_out.get("candidates", [])

    candidates = []
    for c in candidate_dicts:
        columns = [
            ColumnInfo(
                name=col["name"],
                data_type=col["data_type"],
                is_nullable=col.get("is_nullable", True),
            )
            for col in c.get("columns", [])
        ]
        candidates.append(
            CandidateTable(
                name=c["name"],
                row_count=c["row_count"],
                columns=columns,
                has_date_columns=bool(c.get("date_columns")),
                date_columns=c.get("date_columns", []),
                sample_values=c.get("sample_values", []),
                heuristic_score=c.get("heuristic_score", 0),
                primary_keys=c.get("primary_keys", []),
                # 'foreign_keys' is saved by newer versions; older files only have
                # 'foreign_key_count' — in that case we get no FK detail (acceptable).
                foreign_keys=c.get("foreign_keys", []),
            )
        )

    return candidates, summary


def _finish(
    executor: QueryExecutor,
    start_time: float,
    total_tokens: int,
    stage1_summary: dict,
    candidates: list,
    stage2_output,
    stage3_output,
    semantic_layer,
    final_status: str,
    args,
):
    """Wrap up: print summary and save results to a local JSON file."""
    duration_ms = int((time.time() - start_time) * 1000)

    result = {
        "status": final_status,
        "mode": "windows_auth" if getattr(args, "windows_auth", False) else "sql_auth",
        "industry": getattr(args, "industry", "general"),
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
                    "columns": [
                        {"name": col.name, "data_type": col.data_type}
                        for col in c.columns
                    ],
                    "date_columns": c.date_columns,
                    "sample_values": c.sample_values,
                }
                for c in candidates
            ],
        },
        "stage2_output": stage2_output,
        "stage3_output": stage3_output,
    }

    audit_log = executor.get_audit_log_dicts()
    executor.close()

    print("\n" + "=" * 60)
    print("  CRAWL COMPLETE")
    print("=" * 60)
    print(f"  Status:     {final_status}")
    print(f"  Tables:     {result['total_tables']} total -> "
          f"{result['candidate_tables']} candidates -> "
          f"{result['high_value_tables']} high-value")
    print(f"  Tokens:     {total_tokens:,}")
    print(f"  Duration:   {duration_ms:,}ms ({duration_ms // 1000}s)")
    print(f"  Queries:    {len(audit_log)} executed against the database")

    output = {
        "crawl_summary": result,
        "semantic_layer": semantic_layer or {},
        "audit_log": audit_log,
    }
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  Results saved to {args.output}")


if __name__ == "__main__":
    main()
