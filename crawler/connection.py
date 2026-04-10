"""Audited, read-only query executor.

Every query is validated (SELECT-only) and logged to an audit trail.
Supports:
  - SQLite         (mock / local dev)
  - SQL Server     with SQL Server Authentication  (pymssql)
  - SQL Server     with Windows Authentication     (pyodbc — work PCs on domain)

Windows Auth is the recommended mode for corporate Dynamics AX environments
where service accounts are restricted and the work PC is domain-joined.
"""
from __future__ import annotations

import re
import sqlite3
import time
from typing import Optional

from .types import AuditEntry

# Patterns that are NOT allowed in queries
FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|EXEC|EXECUTE|CREATE|TRUNCATE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Query must start with SELECT (after optional whitespace/comments)
SELECT_PATTERN = re.compile(r"^\s*SELECT\b", re.IGNORECASE)

# Valid identifier: alphanumeric + underscore, must start with letter or underscore
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class QueryExecutor:
    """Audited, read-only query executor.

    Args:
        db_type:      "sqlite" | "mssql"
        db_path:      SQLite only — path to .db file
        host:         MSSQL — server hostname or IP
        port:         MSSQL — port (default 1433)
        database:     MSSQL — database name
        user:         MSSQL SQL auth — username (omit for Windows auth)
        password:     MSSQL SQL auth — password (omit for Windows auth)
        windows_auth: MSSQL — use Windows/domain authentication via pyodbc
        odbc_driver:  MSSQL Windows auth — ODBC driver name
                      (default: "ODBC Driver 17 for SQL Server")
    """

    def __init__(self, db_type: str = "sqlite", **kwargs):
        self.db_type = db_type
        self.audit_log: list[AuditEntry] = []

        if db_type == "sqlite":
            db_path = kwargs.get("db_path", "mock_plant.db")
            self._conn = sqlite3.connect(db_path)
            self._conn.row_factory = sqlite3.Row
            self._driver = "sqlite3"

        elif db_type == "mssql":
            host = kwargs["host"]
            port = kwargs.get("port", 1433)
            database = kwargs["database"]

            if kwargs.get("windows_auth", False):
                # ── Windows / domain authentication ───────────────────────────
                try:
                    import pyodbc
                except ImportError:
                    raise ImportError(
                        "pyodbc is required for Windows Authentication.\n"
                        "  pip install pyodbc\n"
                        "Also install: Microsoft ODBC Driver 17 for SQL Server\n"
                        "  https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
                    )
                driver = kwargs.get("odbc_driver", "ODBC Driver 17 for SQL Server")
                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={host},{port};"
                    f"DATABASE={database};"
                    f"Trusted_Connection=yes;"
                    f"TrustServerCertificate=yes;"
                    f"Connection Timeout=15;"
                    f"APP=db-schema-tool;"
                )
                self._conn = pyodbc.connect(conn_str)
                self._driver = "pyodbc"

            else:
                # ── SQL Server authentication ──────────────────────────────────
                try:
                    import pymssql
                except ImportError:
                    raise ImportError(
                        "pymssql is required for SQL Server Authentication.\n"
                        "  pip install pymssql"
                    )
                self._conn = pymssql.connect(
                    server=host,
                    port=port,
                    user=kwargs["user"],
                    password=kwargs["password"],
                    database=database,
                    login_timeout=15,
                    timeout=60,
                    appname="db-schema-tool",
                )
                self._driver = "pymssql"

        else:
            raise ValueError(f"Unsupported db_type: {db_type}")

    # ── Core execution ────────────────────────────────────────────────────────

    def execute(
        self,
        query: str,
        query_type: str = "metadata",
        table_name: Optional[str] = None,
    ) -> list[dict]:
        """Execute a read-only query with audit logging.

        Args:
            query:      SQL query — must be SELECT-only
            query_type: metadata | sample | count | report
            table_name: optional, used for audit log

        Returns:
            List of row dicts

        Raises:
            PermissionError: if query contains forbidden SQL
            RuntimeError:    on query execution failure
        """
        if not SELECT_PATTERN.match(query):
            raise PermissionError(f"Query must start with SELECT. Got: {query[:80]}")
        if FORBIDDEN_PATTERNS.search(query):
            match = FORBIDDEN_PATTERNS.search(query)
            raise PermissionError(
                f"Forbidden SQL keyword: {match.group() if match else 'unknown'}"
            )

        start = time.time()
        try:
            if self._driver == "sqlite3":
                cursor = self._conn.cursor()
                cursor.execute(query)
                rows = [dict(row) for row in cursor.fetchall()]

            elif self._driver == "pymssql":
                cursor = self._conn.cursor(as_dict=True)
                cursor.execute(query)
                rows = [dict(row) for row in cursor.fetchall()]

            elif self._driver == "pyodbc":
                cursor = self._conn.cursor()
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

            else:
                rows = []

            duration_ms = int((time.time() - start) * 1000)
            self.audit_log.append(AuditEntry(
                query_text=query,
                query_type=query_type,
                table_name=table_name,
                row_count_returned=len(rows),
                duration_ms=duration_ms,
            ))
            return rows

        except PermissionError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.audit_log.append(AuditEntry(
                query_text=query,
                query_type=query_type,
                table_name=table_name,
                row_count_returned=0,
                duration_ms=duration_ms,
            ))
            raise RuntimeError(f"Query failed on {table_name or 'unknown'}: {e}") from e

    # ── Schema discovery ──────────────────────────────────────────────────────

    def get_tables(self) -> list[str]:
        """Return all user table names in the database."""
        if self.db_type == "sqlite":
            rows = self.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
                query_type="metadata",
            )
            return [r["name"] for r in rows]
        elif self.db_type == "mssql":
            rows = self.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' "
                "ORDER BY TABLE_NAME",
                query_type="metadata",
            )
            return [r["TABLE_NAME"] for r in rows]
        return []

    def get_all_row_counts(self) -> dict[str, int]:
        """Return {table_name: row_count} for ALL tables in a single query.

        For MSSQL: uses sys.partitions — orders of magnitude faster than COUNT(*)
        on large Dynamics AX databases with thousands of tables.
        For SQLite: returns empty dict (Stage 1 will fall back to per-table counts).
        """
        if self.db_type == "mssql":
            try:
                rows = self.execute(
                    "SELECT t.name as table_name, SUM(p.rows) as row_count "
                    "FROM sys.tables t "
                    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                    "JOIN sys.partitions p ON t.object_id = p.object_id "
                    "WHERE s.name = 'dbo' AND p.index_id IN (0, 1) "
                    "GROUP BY t.name",
                    query_type="count",
                )
                return {r["table_name"]: int(r["row_count"] or 0) for r in rows}
            except Exception as e:
                print(f"  WARNING: Bulk row count failed ({e}), will count per-table")
                return {}
        return {}  # SQLite: caller falls back to per-table counts

    def get_fast_row_count(self, table: str) -> int:
        """Approximate row count for a single table.

        MSSQL: sys.partitions (fast, ~instant even on 100M-row tables).
        Falls back to COUNT(*) if sys.partitions is unavailable.
        """
        if not SAFE_IDENTIFIER.match(table):
            raise ValueError(f"Invalid table name: {table}")

        if self.db_type == "mssql":
            try:
                rows = self.execute(
                    f"SELECT SUM(p.rows) as cnt "
                    f"FROM sys.tables t "
                    f"JOIN sys.schemas s ON t.schema_id = s.schema_id "
                    f"JOIN sys.partitions p ON t.object_id = p.object_id "
                    f"WHERE t.name = '{table}' AND s.name = 'dbo' "
                    f"  AND p.index_id IN (0, 1)",
                    query_type="count",
                    table_name=table,
                )
                if rows and rows[0]["cnt"] is not None:
                    return int(rows[0]["cnt"])
            except Exception:
                pass

        return self.get_row_count(table)

    def get_row_count(self, table: str) -> int:
        """Exact row count via COUNT(*). Slow on large tables."""
        if not SAFE_IDENTIFIER.match(table):
            raise ValueError(f"Invalid table name: {table}")
        rows = self.execute(
            f"SELECT COUNT(*) as cnt FROM [{table}]",
            query_type="count",
            table_name=table,
        )
        return rows[0]["cnt"] if rows else 0

    def get_columns(self, table: str) -> list[dict]:
        """Return column info for a table: name, data_type, is_nullable."""
        if not SAFE_IDENTIFIER.match(table):
            raise ValueError(f"Invalid table name: {table}")

        if self.db_type == "sqlite":
            rows = self.execute(
                f"SELECT name, type, \"notnull\" FROM pragma_table_info('{table}')",
                query_type="metadata",
                table_name=table,
            )
            return [
                {"name": r["name"], "data_type": r["type"], "is_nullable": not r["notnull"]}
                for r in rows
            ]
        elif self.db_type == "mssql":
            rows = self.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                f"FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = 'dbo' "
                f"ORDER BY ORDINAL_POSITION",
                query_type="metadata",
                table_name=table,
            )
            return [
                {
                    "name": r["COLUMN_NAME"],
                    "data_type": r["DATA_TYPE"],
                    "is_nullable": r["IS_NULLABLE"] == "YES",
                }
                for r in rows
            ]
        return []

    def get_primary_keys(self, table: str) -> list[str]:
        """Return primary key column names for a table (empty list on failure)."""
        if not SAFE_IDENTIFIER.match(table):
            return []
        try:
            if self.db_type == "sqlite":
                rows = self.execute(
                    f"SELECT name FROM pragma_table_info('{table}') WHERE pk > 0 ORDER BY pk",
                    query_type="metadata",
                    table_name=table,
                )
                return [r["name"] for r in rows]
            elif self.db_type == "mssql":
                rows = self.execute(
                    f"SELECT kcu.COLUMN_NAME "
                    f"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                    f"JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
                    f"  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                    f"  AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
                    f"WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                    f"  AND tc.TABLE_NAME = '{table}' "
                    f"  AND tc.TABLE_SCHEMA = 'dbo' "
                    f"ORDER BY kcu.ORDINAL_POSITION",
                    query_type="metadata",
                    table_name=table,
                )
                return [r["COLUMN_NAME"] for r in rows]
        except Exception:
            pass
        return []

    def get_foreign_keys(self, table: str) -> list[dict]:
        """Return FK relationships: [{from_column, to_table, to_column}] (empty on failure)."""
        if not SAFE_IDENTIFIER.match(table):
            return []
        try:
            if self.db_type == "sqlite":
                rows = self.execute(
                    f"SELECT \"table\" as to_table, \"from\" as from_col, \"to\" as to_col "
                    f"FROM pragma_foreign_key_list('{table}')",
                    query_type="metadata",
                    table_name=table,
                )
                return [
                    {"from_column": r["from_col"], "to_table": r["to_table"], "to_column": r["to_col"]}
                    for r in rows
                ]
            elif self.db_type == "mssql":
                rows = self.execute(
                    f"SELECT fk_col.COLUMN_NAME as from_column, "
                    f"  pk_tab.TABLE_NAME as to_table, "
                    f"  pk_col.COLUMN_NAME as to_column "
                    f"FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc "
                    f"JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_col "
                    f"  ON rc.CONSTRAINT_NAME = fk_col.CONSTRAINT_NAME "
                    f"  AND fk_col.TABLE_SCHEMA = 'dbo' "
                    f"JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk_tc "
                    f"  ON rc.UNIQUE_CONSTRAINT_NAME = pk_tc.CONSTRAINT_NAME "
                    f"JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_col "
                    f"  ON pk_tc.CONSTRAINT_NAME = pk_col.CONSTRAINT_NAME "
                    f"JOIN INFORMATION_SCHEMA.TABLES pk_tab "
                    f"  ON pk_tc.TABLE_NAME = pk_tab.TABLE_NAME "
                    f"  AND pk_tab.TABLE_SCHEMA = 'dbo' "
                    f"WHERE fk_col.TABLE_NAME = '{table}' "
                    f"ORDER BY fk_col.ORDINAL_POSITION",
                    query_type="metadata",
                    table_name=table,
                )
                return [
                    {
                        "from_column": r["from_column"],
                        "to_table": r["to_table"],
                        "to_column": r["to_column"],
                    }
                    for r in rows
                ]
        except Exception:
            pass
        return []

    def get_column_stats(
        self, table: str, columns: list, sample_limit: int = 2000
    ) -> dict[str, dict]:
        """Get null%, distinct count, and numeric range for up to 10 key columns.

        Uses TOP/LIMIT sampling — never reads the whole table.
        WITH (NOLOCK) on MSSQL to avoid blocking production AX transactions.
        Results enrich the Stage 2 AI scoring prompt.
        """
        if not SAFE_IDENTIFIER.match(table):
            return {}

        NUMERIC = {"int", "bigint", "smallint", "tinyint", "float", "real",
                   "decimal", "numeric", "money", "smallmoney", "integer",
                   "number", "double"}
        INTERESTING = NUMERIC | {"date", "datetime", "datetime2", "smalldatetime",
                                  "varchar", "nvarchar", "char", "nchar"}

        cols_to_analyze = [
            c for c in columns[:25]
            if any(t in c.data_type.lower() for t in INTERESTING)
        ][:10]

        nolock = "WITH (NOLOCK)" if self.db_type == "mssql" else ""
        stats: dict[str, dict] = {}

        for col in cols_to_analyze:
            col_name = col.name
            if not SAFE_IDENTIFIER.match(col_name):
                continue
            try:
                if self.db_type == "sqlite":
                    q = (
                        f"SELECT COUNT(*) as total, COUNT([{col_name}]) as non_null, "
                        f"COUNT(DISTINCT [{col_name}]) as distinct_count "
                        f"FROM (SELECT [{col_name}] FROM [{table}] LIMIT {sample_limit})"
                    )
                else:
                    q = (
                        f"SELECT COUNT(*) as total, COUNT([{col_name}]) as non_null, "
                        f"COUNT(DISTINCT [{col_name}]) as distinct_count "
                        f"FROM (SELECT TOP {sample_limit} [{col_name}] FROM [{table}] {nolock}) t"
                    )
                rows = self.execute(q, query_type="metadata", table_name=table)
                if not rows:
                    continue
                r = rows[0]
                total = r["total"] or 1
                null_pct = round(100.0 * (1.0 - (r["non_null"] or 0) / total), 1)
                stats[col_name] = {
                    "null_pct": null_pct,
                    "distinct_count": r["distinct_count"] or 0,
                }

                # Numeric range (min/max)
                if any(t in col.data_type.lower() for t in NUMERIC):
                    try:
                        rq = (
                            f"SELECT MIN([{col_name}]) as min_val, MAX([{col_name}]) as max_val "
                            f"FROM [{table}] {nolock}"
                        )
                        rr = self.execute(rq, query_type="metadata", table_name=table)
                        if rr and rr[0]["min_val"] is not None:
                            stats[col_name]["min"] = rr[0]["min_val"]
                            stats[col_name]["max"] = rr[0]["max_val"]
                    except Exception:
                        pass

            except Exception:
                pass  # Skip individual column failures — don't abort whole table

        return stats

    # ── Sample data ───────────────────────────────────────────────────────────

    def get_sample_values(self, table: str, column: str, limit: int = 5) -> list[str]:
        """Get distinct non-null sample values from a column."""
        if not SAFE_IDENTIFIER.match(table):
            raise ValueError(f"Invalid table name: {table}")
        if not SAFE_IDENTIFIER.match(column):
            raise ValueError(f"Invalid column name: {column}")

        nolock = "WITH (NOLOCK)" if self.db_type == "mssql" else ""
        if self.db_type == "sqlite":
            query = f"SELECT DISTINCT [{column}] FROM [{table}] LIMIT {limit}"
        else:
            query = f"SELECT DISTINCT TOP {limit} [{column}] FROM [{table}] {nolock}"

        rows = self.execute(query, query_type="sample", table_name=table)
        return [str(r[column]) for r in rows if r[column] is not None]

    def get_sample_rows(self, table: str, limit: int = 10) -> list[dict]:
        """Get sample rows from a table (used in Stage 3 deep inspection)."""
        if not SAFE_IDENTIFIER.match(table):
            raise ValueError(f"Invalid table name: {table}")

        nolock = "WITH (NOLOCK)" if self.db_type == "mssql" else ""
        if self.db_type == "sqlite":
            query = f"SELECT * FROM [{table}] LIMIT {limit}"
        else:
            query = f"SELECT TOP {limit} * FROM [{table}] {nolock}"

        return self.execute(query, query_type="sample", table_name=table)

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def close(self):
        if self._conn:
            self._conn.close()

    def get_audit_log_dicts(self) -> list[dict]:
        return [
            {
                "query_text": e.query_text,
                "query_type": e.query_type,
                "table_name": e.table_name,
                "row_count_returned": e.row_count_returned,
                "duration_ms": e.duration_ms,
                "executed_at": e.executed_at,
            }
            for e in self.audit_log
        ]
