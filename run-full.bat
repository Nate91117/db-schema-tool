@echo off
REM Full pipeline: Stage 1 + Stage 2 (Haiku) + Stage 3 (Sonnet, top 15 tables only).
REM Uses Anthropic API tokens — make sure ANTHROPIC_API_KEY is set in .env.
REM Writes results.json next to this file.

cd /d "%~dp0"

echo Running full pipeline (caps Stage 3 at 15 tables for cost control)...
echo.
dbscan --max-stage3-tables 15 --output results.json

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
