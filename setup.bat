@echo off
REM One-time setup: installs the dbscan CLI.
REM Run this once after cloning. Re-run anytime you pull new code.

cd /d "%~dp0"

echo Installing db-schema-tool...
pip install -e .

if errorlevel 1 (
    echo.
    echo ERROR: pip install failed. See message above.
    echo Common fixes:
    echo   - Make sure Python 3.10+ is installed and on PATH
    echo   - If pyodbc fails, install Microsoft ODBC Driver 17 for SQL Server
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  Setup complete. Next steps:
echo    1. Copy .env.example to .env and fill it in
echo    2. Double-click run-stage1.bat to test
echo ============================================
echo.
pause
