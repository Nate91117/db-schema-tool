# How To Run — Quick Start

Three double-click batch files handle everything on Windows. Zero commands to memorize.

---

## First-time setup (once per machine)

### 1. Install Python 3.10 or newer

Download: https://www.python.org/downloads/
**Important**: check the box "Add Python to PATH" during install.

### 2. Install Microsoft ODBC Driver 17 for SQL Server

Download: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
Needs admin rights. If SQL Server Management Studio is already installed, the driver may already be there.

### 3. Get the code

```
git clone https://github.com/Nate91117/db-schema-tool.git
```

Or download the ZIP from GitHub and unzip it. **Location doesn't matter** — the batch files find themselves no matter how deep you nest the folder.

### 4. Double-click `setup.bat`

This runs `pip install -e .` for you. Takes 30–60 seconds. Shows success or error at the end.

### 5. Create your `.env` file

Copy `.env.example` to `.env` in the same folder, then edit it:

```env
DB_TYPE=mssql
DB_HOST=YOUR-SERVER\INSTANCE
DB_NAME=YourDatabase
DB_WINDOWS_AUTH=true
ANTHROPIC_API_KEY=sk-ant-...
INDUSTRY=general
```

Replace `YOUR-SERVER\INSTANCE` and `YourDatabase` with the real values. Ask your DBA if you don't know them.

---

## Running it

### Smoke test — `run-stage1.bat` (free, no AI tokens)

Double-click. This is your "does it even connect?" check. Takes 10–60 seconds depending on DB size.

Output: `stage1.json` in the same folder. Contains every table, its row count, and column metadata. No AI involved.

**If this fails**, the problem is connection/permissions — fix that before running the full pipeline.

### Full pipeline — `run-full.bat` (uses API tokens)

Double-click. Runs all three stages:
- Stage 1 — heuristic filter (free)
- Stage 2 — Haiku scores each candidate table 1–10 (cheap, ~$0.01–0.10)
- Stage 3 — Sonnet deeply annotates the **top 15 tables** (main cost, ~$0.50–2.00)

Output: `results.json` in the same folder. Ready to review, email, or feed to another system.

The 15-table cap is there so a first run doesn't burn tokens. Edit the number in `run-full.bat` later if you want more.

---

## Troubleshooting

### "dbscan is not recognized as an internal or external command"

`setup.bat` didn't finish, or the terminal opened before PATH refreshed. Close all terminals, then re-run `setup.bat`. Still broken? Run it from an **Admin** command prompt.

### "pip is not recognized"

Python isn't on PATH. Reinstall Python and check the "Add Python to PATH" box. Or find it manually — usually `C:\Users\YOU\AppData\Local\Programs\Python\Python3XX\Scripts\pip.exe`.

### ODBC / connection errors

Either (a) ODBC Driver 17 isn't installed, or (b) the DB host/name in `.env` is wrong, or (c) your Windows account doesn't have SELECT permission on the DB. The error message usually tells you which. Ask your DBA for:

```sql
GRANT SELECT ON SCHEMA::dbo TO [YourDomain\YourUser];
GRANT VIEW DATABASE STATE TO [YourDomain\YourUser];
```

### "The .bat file just flashed and closed"

It hit an error before reaching `pause`. Open Command Prompt, `cd` into the folder, and run the `.bat` from there — error messages will stay visible.

### `stage1.json` is huge / empty / weird

Open it in a text editor — it's plain JSON. For a Dynamics AX DB expect 1,000–8,000 tables in there. Most are system tables that get filtered out in later stages. If you want to see just the candidates that would go to Stage 2, look for the `"candidates"` array in the JSON.

---

## What the output files contain

| File | Contents | Stage |
|------|----------|-------|
| `stage1.json` | All tables, row counts, column metadata, heuristic scores | Stage 1 only |
| `results.json` | Candidates + AI scores + semantic annotations for top tables | Full pipeline |

Both are safe to email or share — no raw data rows are included, only schema and statistics.

---

## Running it again

- To re-run stage 1: double-click `run-stage1.bat` again. It overwrites `stage1.json`.
- To re-run full: double-click `run-full.bat`. It overwrites `results.json`.
- If you pulled new code from GitHub: double-click `setup.bat` again to pick up any dependency changes.
