import logging
import time
from fastapi import WebSocket
from services.dashboard_server.mappers import (
    _tick, _ma, _metric_mapper, OVERLAY_METRICS,
    ticker_stream, ma_stream,
)

log = logging.getLogger("dashboard.history")


async def _read_quick_history(ar, key: str, count: int, mapper):
    try:
        entries = await ar.xrevrange(key, "+", "-", count=count)
    except Exception as e:
        log.error("quick-history %s: %s", key, e)
        return []
    points = []
    for entry_id, fields in reversed(entries):
        try:
            points.append(mapper(entry_id, fields))
        except (KeyError, ValueError, TypeError):
            continue
    return points


async def _read_window_history(ar, key: str, window_ms: int, mapper, chunk: int):
    since_ms   = int(time.time() * 1000) - window_ms
    collected  = []
    cursor_max = "+"
    try:
        while True:
            entries = await ar.xrevrange(key, cursor_max, since_ms, count=chunk)
            if not entries:
                break
            collected.extend(entries)
            if len(entries) < chunk:
                break
            last_id    = entries[-1][0]
            ts, seq    = last_id.split("-")
            seq        = int(seq)
            cursor_max = f"{ts}-{seq - 1}" if seq > 0 else f"{int(ts) - 1}-18446744073709551615"
    except Exception as e:
        log.error("window-history %s: %s", key, e)
    points = []
    for entry_id, fields in reversed(collected):
        try:
            points.append(mapper(entry_id, fields))
        except (KeyError, ValueError, TypeError):
            continue
    return points


async def load_full_window(ar, sym: str, window: str, window_ms: int, chunk: int, send_fn, clients_fn):
    ticks = await _read_window_history(ar, ticker_stream(sym), window_ms, _tick, chunk)
    await send_fn(clients_fn(sym), {"type": "history", "symbol": sym, "ticks": ticks, "complete": True, "window": window})

    ma_pts = await _read_window_history(ar, ma_stream(sym), window_ms, _ma, chunk)
    if ma_pts:
        await send_fn(clients_fn(sym), {"type": "ma-history", "symbol": sym, "points": ma_pts, "complete": True})

    for mkey, cfg in OVERLAY_METRICS.items():
        pts = await _read_window_history(ar, cfg["stream"](sym), window_ms, _metric_mapper(cfg["fields"]), chunk)
        if pts:
            await send_fn(clients_fn(sym), {"type": "metric-history", "key": mkey, "symbol": sym, "points": pts, "complete": True})
