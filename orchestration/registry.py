SERVICES = {
    "Live Binance": ["python", "services/data/live_binance.py"],
    "Backfill":     ["python", "services/data/backfill_binance.py"],
    "Server":       ["python", "-m", "uvicorn", "services.dashboard_server.server:app", "--host", "0.0.0.0", "--port", "8000"],
    "Metrics": ["python", "services/metrics/run_all.py"],
}