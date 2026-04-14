@echo off
REM Stage 1 smoke test — no AI tokens used.
REM Gets table list, row counts, column metadata. Writes stage1.json next to this file.

cd /d "%~dp0"

echo Running Stage 1 (no AI, free)...
echo.
dbscan --stage 1 --output stage1.json

echo.
if exist stage1.json (
    echo ============================================
    echo  Done. Results saved to:
    echo    %~dp0stage1.json
    echo ============================================
) else (
    echo ERROR: stage1.json was not created. See messages above.
)
echo.
pause
