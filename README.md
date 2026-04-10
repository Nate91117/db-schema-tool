# Database Schema Discovery Tool

A read-only AI-assisted schema discovery tool for SQL Server and SQLite. Connects to a
database, walks the schema using a 3-stage pipeline, and produces a JSON file describing
the high-value tables and their semantic meaning.

## How It Works

```
Stage 1 — Heuristic Filter (no AI, fast)
  Get all tables → bulk row counts → filter noise → score by keywords, structure, FK count
  Output: candidate tables with PK/FK metadata

Stage 2 — AI Batch Scoring (Haiku, cheap)
  Gather column stats (null%, distinct count, ranges) → score each table 1–10
  Output: high-value tables (score >= 7 by default)

Stage 3 — Deep Inspection (Sonnet, rich)
  Pull sample rows → generate full semantic annotation per table
  Output: semantic layer written to a local JSON file
```

## Setup

### Prerequisites

- Python 3.10+
- Network access to the SQL Server (or a local SQLite file)
- An Anthropic API key

### Installation

```bash
git clone https://github.com/Nate91117/db-schema-tool.git
cd db-schema-tool
pip install -e .
```

### Windows Authentication Setup (recommended for Windows PCs)

Windows Authentication lets you connect using your domain credentials — no service account
needed.

**Step 1: Install Microsoft ODBC Driver 17 for SQL Server**

Download from Microsoft:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

Run the installer (requires admin rights). If you have SQL Server Management Studio
already installed, the driver may already be present.

**Step 2: Verify the driver is installed**

```powershell
# In PowerShell or cmd:
odbcad32
# Click "Drivers" tab — look for "ODBC Driver 17 for SQL Server"
```

**Step 3: Create your .env file**

```bash
cp .env.example .env
# Edit .env with your server name and database
```

```env
DB_TYPE=mssql
DB_HOST=YOUR-SERVER-NAME\INSTANCE
DB_NAME=YourDatabase
DB_WINDOWS_AUTH=true
ANTHROPIC_API_KEY=sk-ant-...
INDUSTRY=general
```

**Step 4: Run**

```bash
# Stage 1 only (fast, no AI tokens — good first test):
dbscan --stage 1 --output stage1_results.json

# Full pipeline, save locally:
dbscan --output results.json
```

### SQL Server Authentication (service account)

If you have a read-only SQL account instead of Windows auth:

```env
DB_TYPE=mssql
DB_HOST=192.168.1.100
DB_NAME=YourDatabase
DB_USER=readonly_user
DB_PASSWORD=your-password
ANTHROPIC_API_KEY=sk-ant-...
```

```bash
dbscan --output results.json
```

### SQLite (for local testing)

```env
DB_TYPE=sqlite
DB_PATH=local.db
ANTHROPIC_API_KEY=sk-ant-...
```

```bash
dbscan --output results.json
```

## All CLI Options

```
Database Connection:
  --db-type          sqlite | mssql
  --db-host          SQL Server hostname or IP (e.g. SERVER\INSTANCE or 192.168.1.10)
  --db-name          Database name
  --db-user          SQL Server username (SQL auth only)
  --db-password      SQL Server password (SQL auth only)
  --windows-auth     Use Windows/domain authentication (pyodbc)
  --odbc-driver      ODBC driver name (default: 'ODBC Driver 17 for SQL Server')

Crawler Options:
  --stage            Stop after stage 1, 2, or 3 (default: run all)
  --min-rows         Minimum row count to consider a table (default: 10)
  --min-score        Minimum AI score to pass Stage 2 (default: 7, range 1-10)
  --max-stage3-tables  Limit Stage 3 to top N tables (cost control)
  --skip-column-stats  Skip column stats in Stage 2 (faster, lower quality)
  --industry         biofuel | manufacturing | food_processing | chemicals | general

AI Models:
  --anthropic-key    Anthropic API key
  --haiku-model      Stage 2 model (default: claude-haiku-4-5)
  --sonnet-model     Stage 3 model (default: claude-sonnet-4-5)

Output:
  --output           Save results to a local JSON file (required)
```

## Running Against a Large Database

Enterprise databases can have 5,000–10,000+ tables. The crawler handles this:

- **Stage 1** uses `sys.partitions` to get all row counts in a single query (vs 8,000+
  COUNT(*) calls)
- `DEL_`, `SYS`, `TMP` prefix tables are excluded immediately (system tables)
- Column stats are skipped automatically if candidates exceed 100 tables

Recommended approach for a first run:

```bash
# Run Stage 1 only to see what candidates look like — no AI cost:
dbscan --db-type mssql --windows-auth --db-host SERVER --db-name YourDB \
  --stage 1 --output stage1.json

# Review stage1.json, then run full pipeline with a table limit:
dbscan --db-type mssql --windows-auth --db-host SERVER --db-name YourDB \
  --max-stage3-tables 30 --output results.json
```

## Required DB Permissions

The crawler only executes read-only SELECT queries. The database user needs:

```sql
-- Minimum permissions:
GRANT SELECT ON SCHEMA::dbo TO readonly_user;
GRANT VIEW DATABASE STATE TO readonly_user;  -- For sys.partitions row counts
```

For Windows auth, these permissions should be granted to your domain account or
an AD group. Ask your DBA.

## Security

- Only SELECT queries are permitted (enforced by query validation before execution)
- All queries are logged to a local audit trail and saved alongside the JSON results
- Sample rows used in Stage 3 are ephemeral — used for the AI call only, never persisted
- Only schema descriptions and column statistics leave the local environment (to the
  Anthropic API for Stage 2/3 scoring)
- `WITH (NOLOCK)` hints on all MSSQL queries — the crawler never takes locks on
  production data
