@echo off
REM Full pipeline: Stage 1 + Stage 2 (Haiku) + Stage 3 (Sonnet, top 15 tables only).
REM Uses Anthropic API tokens — make sure ANTHROPIC_API_KEY is set in .env.
REM Writes results.json next to this file.
REM
REM Stage 1 is skipped automatically if stage1.json already exists in this directory.
REM Run run-stage1.bat first to pre-generate stage1.json, then run this for Stage 2+3 only.

cd /d "%~dp0"

echo Running full pipeline (caps Stage 3 at 15 tables for cost control)...
if exist stage1.json (
    echo Stage 1 cache found ^(stage1.json^) — skipping DB enumeration.
) else (
    echo No stage1.json found — Stage 1 will run from scratch.
)
echo.
dbscan --max-stage3-tables 15 --skip-stage1 --output results.json

echo.
if exist results.json (
    echo ============================================
    echo  Done. Results saved to:
    echo    %~dp0results.json
    echo ============================================
) else (
    echo ERROR: results.json was not created. See messages above.
)
echo.
pause
