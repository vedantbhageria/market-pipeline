import os
import sys
import json
import asyncio
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from Managers.RedisConnectionPool import ConnectionManager
from Managers.SubscriptionManager import SubscriptionManager
from Services.Dashboard_Server.mappers import (
    _tick, _ma, _metric_mapper, OVERLAY_METRICS, ticker_stream, ma_stream,
)
from Services.Dashboard_Server.history import _read_quick_history, _read_window_history, load_full_window
from Services.Dashboard_Server.stats import Stats, stats_loop

log = logging.getLogger("dashboard")

# ── Redis ──────────────────────────────────────────────────────────────────
_cm = ConnectionManager("dashboard")
ar  = _cm.get_async("dashboard-async", socket_timeout=30)
r   = _cm.get_sync("dashboard-sync")

# ── Config ─────────────────────────────────────────────────────────────────
MAX_SYMBOLS_PER_CLIENT = 12
STATS_INTERVAL         = 2.0
QUICK_HISTORY_COUNT    = 250
XREAD_BLOCK_MS         = 5000
XREAD_COUNT            = 500
HISTORY_CHUNK          = 5000

HISTORY_WINDOWS_MS = {
    "2m":  2  * 60_000,
    "10m": 10 * 60_000,
    "30m": 30 * 60_000,
    "1h":  60 * 60_000,
}
DEFAULT_WINDOW = "10m"

HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "Frontend", "dashboard.html")

# ── App ────────────────────────────────────────────────────────────────────
app   = FastAPI()
stats = Stats()


# ── Helpers ────────────────────────────────────────────────────────────────
async def _send(ws_or_list, payload: dict):
    msg = json.dumps(payload)
    targets = ws_or_list if isinstance(ws_or_list, list) else [ws_or_list]
    for ws in targets:
        try:
            await ws.send_text(msg)
            stats.msgs_out += 1
            stats.bytes_out += len(msg)
        except Exception:
            pass


async def _tail_symbol(symbol: str, mgr: SubscriptionManager):
    handlers = {
        ticker_stream(symbol): ("ticks",  _tick, None),
        ma_stream(symbol):     ("mas",    _ma,   None),
    }
    for mkey, cfg in OVERLAY_METRICS.items():
        handlers[cfg["stream"](symbol)] = ("metric", _metric_mapper(cfg["fields"]), mkey)

    cursors = {k: "$" for k in handlers}
    while True:
        try:
            results = await ar.xread(cursors, count=XREAD_COUNT, block=XREAD_BLOCK_MS)
            if not results:
                continue
            for key, entries in results:
                if not entries:
                    continue
                cursors[key] = entries[-1][0]
                msg_type, mapper, mkey = handlers[key]
                points = []
                for entry_id, fields in entries:
                    try:
                        points.append(mapper(entry_id, fields))
                    except (KeyError, ValueError, TypeError):
                        continue
                if not points:
                    continue
                frame_obj = {"type": msg_type, "symbol": symbol, "points": points}
                if mkey is not None:
                    frame_obj["key"] = mkey
                frame = json.dumps(frame_obj)
                for ws in mgr.clients_for(symbol):
                    try:
                        await ws.send_text(frame)
                        stats.msgs_out += 1
                        stats.bytes_out += len(frame)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error("tail %s error: %s", symbol, e)
            await asyncio.sleep(1)


manager = SubscriptionManager(listener_fn=_tail_symbol)


# ── Startup ────────────────────────────────────────────────────────────────
def _keep_awake():
    if sys.platform != "win32":
        return
    try:
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001 | 0x00000002)
        log.info("sleep inhibited")
    except Exception as e:
        log.warning("could not inhibit sleep: %s", e)


@app.on_event("startup")
async def _startup():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    _keep_awake()
    asyncio.create_task(stats_loop(r, stats, manager, _send, STATS_INTERVAL))


# ── WebSocket ──────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    await manager.connect(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            stats.msgs_in  += 1
            stats.bytes_in += len(raw)
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "subscribe":
                syms             = [s.upper() for s in msg.get("symbols", []) if isinstance(s, str)]
                requested_window = msg.get("window")

                for sym in syms:
                    ok = await manager.subscribe(websocket, sym, MAX_SYMBOLS_PER_CLIENT)
                    if not ok:
                        await _send(websocket, {"type": "error", "symbol": sym,
                                                "message": f"limit of {MAX_SYMBOLS_PER_CLIENT} symbols reached"})
                        continue

                    window = requested_window if requested_window in HISTORY_WINDOWS_MS else manager.get_window(sym, DEFAULT_WINDOW)
                    manager.set_window(sym, window)

                    quick_ticks = await _read_quick_history(ar, ticker_stream(sym), QUICK_HISTORY_COUNT, _tick)
                    await _send(websocket, {"type": "history", "symbol": sym, "ticks": quick_ticks,
                                            "complete": False, "window": window})

                    quick_ma = await _read_quick_history(ar, ma_stream(sym), QUICK_HISTORY_COUNT, _ma)
                    if quick_ma:
                        await _send(websocket, {"type": "ma-history", "symbol": sym,
                                                "points": quick_ma, "complete": False})

                    for mkey, cfg in OVERLAY_METRICS.items():
                        qpts = await _read_quick_history(ar, cfg["stream"](sym), QUICK_HISTORY_COUNT,
                                                         _metric_mapper(cfg["fields"]))
                        if qpts:
                            await _send(websocket, {"type": "metric-history", "key": mkey,
                                                    "symbol": sym, "points": qpts, "complete": False})

                    asyncio.create_task(load_full_window(
                        ar, sym, window, HISTORY_WINDOWS_MS[window], HISTORY_CHUNK,
                        lambda clients, p: _send(clients, p),
                        manager.clients_for,
                    ))

            elif action == "set_window":
                sym    = msg.get("symbol")
                window = msg.get("window")
                if isinstance(sym, str) and window in HISTORY_WINDOWS_MS:
                    sym = sym.upper()
                    if sym in manager.symbol_to_clients:
                        manager.set_window(sym, window)
                        asyncio.create_task(load_full_window(
                            ar, sym, window, HISTORY_WINDOWS_MS[window], HISTORY_CHUNK,
                            lambda clients, p: _send(clients, p),
                            manager.clients_for,
                        ))

            elif action == "unsubscribe":
                for sym in [s.upper() for s in msg.get("symbols", []) if isinstance(s, str)]:
                    await manager.unsubscribe(websocket, sym)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.error("ws error: %s", e)
    finally:
        await manager.disconnect(websocket)


# ── REST endpoints ─────────────────────────────────────────────────────────
@app.get("/api/symbols")
async def symbols():
    out = []
    async for key in ar.scan_iter(match="LiveTicker:*", count=1000):
        out.append(key.split(":", 1)[-1])
    return JSONResponse(sorted(out))


@app.get("/api/history/{symbol}")
async def history(symbol: str, window: str = DEFAULT_WINDOW):
    window = window if window in HISTORY_WINDOWS_MS else DEFAULT_WINDOW
    pts    = await _read_window_history(ar, ticker_stream(symbol.upper()), HISTORY_WINDOWS_MS[window], _tick, HISTORY_CHUNK)
    return JSONResponse(pts)


@app.get("/api/ma-history/{symbol}")
async def ma_history(symbol: str, window: str = DEFAULT_WINDOW):
    window = window if window in HISTORY_WINDOWS_MS else DEFAULT_WINDOW
    pts    = await _read_window_history(ar, ma_stream(symbol.upper()), HISTORY_WINDOWS_MS[window], _ma, HISTORY_CHUNK)
    return JSONResponse(pts)


@app.get("/api/metric-history/{metric}/{symbol}")
async def metric_history(metric: str, symbol: str, window: str = DEFAULT_WINDOW):
    cfg = OVERLAY_METRICS.get(metric)
    if not cfg:
        return JSONResponse({"error": f"unknown metric '{metric}'"}, status_code=404)
    window = window if window in HISTORY_WINDOWS_MS else DEFAULT_WINDOW
    pts    = await _read_window_history(ar, cfg["stream"](symbol.upper()), HISTORY_WINDOWS_MS[window],
                                        _metric_mapper(cfg["fields"]), HISTORY_CHUNK)
    return JSONResponse(pts)


@app.get("/api/ws-stats")
async def ws_stats():
    return JSONResponse({
        "clients_connected": manager.client_count(),
        "active_symbols":    manager.active_symbols(),
        **stats.last_rates,
    })


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(HTML_PATH, "r", encoding="utf-8") as fh:
        return HTMLResponse(fh.read())
