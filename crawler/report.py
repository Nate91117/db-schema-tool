"""Report Builder CLI — executes a SQL query against the database.

Accepts SQL directly, executes it via the audited read-only query executor,
and prints the results (optionally writing them to a JSON file).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        pass

from .connection import QueryExecutor


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Report Builder — execute a read-only SQL query")
    parser.add_argument("--db-type", choices=["sqlite", "mssql"], default=os.getenv("DB_TYPE", "sqlite"))
    parser.add_argument("--db-path", default=os.getenv("DB_PATH", "local.db"))
    parser.add_argument("--db-host", default=os.getenv("DB_HOST"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("DB_PORT", "1433")))
    parser.add_argument("--db-name", default=os.getenv("DB_NAME"))
    parser.add_argument("--db-user", default=os.getenv("DB_USER"))
    parser.add_argument("--db-password", default=os.getenv("DB_PASSWORD"))
    parser.add_argument("--sql", required=True, help="SQL query to execute")
    parser.add_argument("--output", help="Save results to JSON file")
    args = parser.parse_args()

    sql = args.sql

    # Connect to database
    try:
        if args.db_type == "sqlite":
            executor = QueryExecutor(db_type="sqlite", db_path=args.db_path)
        else:
            executor = QueryExecutor(
                db_type="mssql",
                host=args.db_host,
                port=args.db_port,
                database=args.db_name,
                user=args.db_user,
                password=args.db_password,
            )
    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        sys.exit(1)

    # Execute
    print(f"Executing query against {args.db_type}...")
    try:
        rows = executor.execute(sql, query_type="report")
        print(f"  {len(rows)} rows returned")

        # Display results
        if rows:
            headers = list(rows[0].keys())
            print(f"  Columns: {', '.join(headers)}")
            for i, row in enumerate(rows[:20]):
                print(f"  [{i+1}] {json.dumps(row, default=str)}")
            if len(rows) > 20:
                print(f"  ... and {len(rows) - 20} more rows")

        # Save to file
        if args.output:
            with open(args.output, "w") as f:
                json.dump({"rows": rows, "row_count": len(rows)}, f, indent=2, default=str)
            print(f"\n  Results saved to {args.output}")

    except Exception as e:
        print(f"ERROR: Query failed: {e}")
        sys.exit(1)
    finally:
        executor.close()


if __name__ == "__main__":
    main()
