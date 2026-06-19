import time
import webbrowser
from Managers.ProcessManager import ProcessManager

pm = ProcessManager()

pm.start("Live Binance")
pm.start("Backfill")
pm.start("Server")
time.sleep(2)
webbrowser.open("http://localhost:8000")
pm.start("Metrics")

print("Pipeline running. Press Ctrl+C to stop.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down...")
    pm.stop_all()
