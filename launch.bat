@echo off
setlocal
cd /d "%~dp0"

echo ============================================
echo   Binance Futures Pipeline + Dashboard
echo ============================================

echo.
echo [0/4] Ensuring Redis is running in WSL...
wsl -e sudo service redis-server start
if errorlevel 1 (
  echo    ^! Could not auto-start Redis.
  echo    ^! Open WSL and run:  sudo service redis-server start
)

echo.
echo [1/4] Starting data pipeline ^(backfill then live^)...
start "Backfill_Live_Orchestrator" cmd /k python Backfill_Live_Orchestrator.py

echo.
echo [2/4] Starting dashboard server on http://localhost:8000 ...
start "Dashboard" cmd /k python -m uvicorn dashboard_server:app --port 8000

echo.
echo [3/4] Starting data pipeline ^(backfill then live^)...
start "Attributes" cmd /k python Attributes.py

echo.
echo [4/4] Opening dashboard in browser...
timeout /t 4 >nul
start "" http://localhost:8000

echo.
echo Launched. 3 windows opened: "Tick_Data" and "Dashboard" and "Moving Average".
echo   Dashboard  - http://localhost:8000           (add up to 12 symbols, updates over a websocket)
echo   WS stats   - http://localhost:8000/api/ws-stats
echo Closing THIS window is fine; the other two keep running.
echo.
pause
endlocal
