import logging
import time
import asyncio
import psutil

log = logging.getLogger("dashboard.stats")


class Stats:

    def __init__(self):
        self.msgs_in    = 0
        self.msgs_out   = 0
        self.bytes_in   = 0
        self.bytes_out  = 0
        self.window_start = time.time()
        self.last_rates = {
            "msgs_in_per_sec": 0, "msgs_out_per_sec": 0,
            "bytes_in_per_sec": 0, "bytes_out_per_sec": 0,
        }

    def snapshot_and_reset(self):
        elapsed = max(time.time() - self.window_start, 0.001)
        self.last_rates = {
            "msgs_in_per_sec":   round(self.msgs_in  / elapsed, 1),
            "msgs_out_per_sec":  round(self.msgs_out / elapsed, 1),
            "bytes_in_per_sec":  round(self.bytes_in  / elapsed, 1),
            "bytes_out_per_sec": round(self.bytes_out / elapsed, 1),
        }
        self.msgs_in = self.msgs_out = self.bytes_in = self.bytes_out = 0
        self.window_start = time.time()
        return self.last_rates


def get_system_stats(r):
    cpu_pct  = psutil.cpu_percent(interval=None)
    ram      = psutil.virtual_memory()
    swap     = psutil.swap_memory()
    try:
        redis_gb = round(r.info("memory").get("used_memory", 0) / (1024 ** 3), 3)
    except Exception:
        redis_gb = 0.0
    return {
        "cpu_pct":  cpu_pct,
        "ram_pct":  ram.percent,
        "ram_gb":   round(ram.used / (1024 ** 3), 2),
        "swap_gb":  round(swap.used / (1024 ** 3), 2),
        "redis_gb": redis_gb,
    }


async def stats_loop(r, stats: Stats, manager, send_fn, interval: float):
    while True:
        await asyncio.sleep(interval)
        try:
            rates    = stats.snapshot_and_reset()
            hw_stats = await asyncio.to_thread(get_system_stats, r)
            payload  = {
                "type":             "stats",
                "clients_connected": manager.client_count(),
                "active_symbols":   len(manager.active_symbols()),
                **rates,
                "hardware": hw_stats,
            }
            for ws in list(manager.all_clients):
                await send_fn(ws, payload)
        except Exception as e:
            log.error("stats loop error: %s", e)
