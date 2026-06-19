SERVICES = {
    "Live Binance": ["python", "Services/Data/Live_Binance.py"],
    "Backfill":     ["python", "Services/Data/Backfill_Binance.py"],
    "Server":       ["python", "-m", "uvicorn", "Services.Dashboard_Server.server:app", "--host", "0.0.0.0", "--port", "8000"],
    "Metrics": ["python", "Services/Metrics/run_all.py"],
}