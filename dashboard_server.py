

import os
import sys
import json
import time
import asyncio
from collections import defaultdict


def keep_awake():
    """Ask Windows to keep the system (and display) awake for as long as this
    process is alive. ES_CONTINUOUS makes the request persistent; combined with
    ES_SYSTEM_REQUIRED it blocks sleep, and ES_DISPLAY_REQUIRED keeps the screen
    on (you're watching a live chart). It auto-reverts when the process exits --
    nothing to undo, no permanent power-plan changes. No-op off Windows."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        ES_CONTINUOUS       = 0x80000000
        ES_SYSTEM_REQUIRED  = 0x00000001
        ES_DISPLAY_REQUIRED = 0x00000002
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
        )
        print("[power] sleep/display-off inhibited while server runs")
    except Exception as e:
        print(f"[power] could not inhibit sleep: {e}")

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import redis.asyncio as aioredis

ar = aioredis.Redis(host="localhost", port=6379, db=0, decode_responses=True,
                    socket_timeout=30, socket_connect_timeout=5,
                    health_check_interval=30)

app = FastAPI()

HTML_PATH = os.path.join(os.path.dirname(__file__), "dashboard.html")

MAX_SYMBOLS_PER_CLIENT = 12
STATS_INTERVAL         = 2.0     # seconds between stats pushes to clients
QUICK_HISTORY_COUNT    = 250     # shown instantly, before the full window finishes
XREAD_BLOCK_MS         = 5000    # how long each XREAD parks waiting for new data
XREAD_COUNT            = 500     # max entries pulled per XREAD wakeup
HISTORY_CHUNK          = 5000    # per-round-trip size when paginating a full window

HISTORY_WINDOWS_MS = {
    "2m":  2  * 60_000,
    "10m": 10 * 60_000,
    "30m": 30 * 60_000,
    "1h":  60 * 60_000,
}
DEFAULT_WINDOW = "10m"


def ticker_stream(sym):
    return f"LiveTicker:{sym}"


def ma_stream(sym):
    return f"MovingAverage:{sym}"


def _tick(entry_id, f):
    """Map a LiveTicker entry to a chart point. Timestamp comes from the ID,
    not a field. Chart works in seconds (float, sub-second resolution kept)."""
    t_ms = int(entry_id.split("-")[0])
    return {
        "t":     t_ms / 1000.0,
        "price": float(f["price"]),
        "qty":   float(f.get("quantity", 0) or 0),
    }


def _ma(entry_id, f):
    """Map a MovingAverage entry to an MA point. Same deal -- ID carries time."""
    t_ms = int(entry_id.split("-")[0])
    return {
        "t":  t_ms / 1000.0,
        "ma": float(f["avg_price"]),
        "n":  int(float(f.get("window_ticks", 0) or 0)),
    }


# --------------------------------------------------------------------------
# Overlay metrics (everything except price + MA, which have their own fast
# paths above). Each maps a Redis stream to the numeric fields we forward,
# renamed for the client: single-value metrics expose "v", Bollinger exposes
# upper/lower/mid. Streams that aren't being produced just yield nothing, so
# it's safe to list metrics here before you enable them in Attributes.py.
# --------------------------------------------------------------------------
OVERLAY_METRICS = {
    "vwma":     {"stream": lambda s: f"VWMA:{s}",        "fields": {"vwma": "v"}},
    "ema":      {"stream": lambda s: f"EMA:{s}",         "fields": {"ema": "v"}},
    "boll":     {"stream": lambda s: f"Bollinger:{s}",   "fields": {"upper": "upper", "lower": "lower", "mid": "mid"}},
    "high":     {"stream": lambda s: f"RollingHigh:{s}", "fields": {"high": "v"}},
    "low":      {"stream": lambda s: f"RollingLow:{s}",  "fields": {"low": "v"}},
    "momentum": {"stream": lambda s: f"Momentum:{s}",    "fields": {"pct": "v"}},
    "volume":   {"stream": lambda s: f"Volume:{s}",      "fields": {"volume": "v"}},
}


def _metric_mapper(field_map):
    """Build a mapper(entry_id, fields) -> {"t": secs, <renamed fields>} for one
    overlay metric. Time comes from the entry ID, like ticks/MA."""
    def m(entry_id, f):
        pt = {"t": int(entry_id.split("-")[0]) / 1000.0}
        for src, dst in field_map.items():
            if src in f:
                pt[dst] = float(f[src])
        return pt
    return m


# --------------------------------------------------------------------------
# Inflow / outflow visibility (unchanged from the candle dashboard)
# --------------------------------------------------------------------------
class Stats:
    def __init__(self):
        self.msgs_in = 0
        self.msgs_out = 0
        self.bytes_in = 0
        self.bytes_out = 0
        self.window_start = time.time()
        self.last_rates = {"msgs_in_per_sec": 0, "msgs_out_per_sec": 0,
                            "bytes_in_per_sec": 0, "bytes_out_per_sec": 0}

    def snapshot_and_reset(self):
        elapsed = max(time.time() - self.window_start, 0.001)
        self.last_rates = {
            "msgs_in_per_sec":   round(self.msgs_in / elapsed, 1),
            "msgs_out_per_sec":  round(self.msgs_out / elapsed, 1),
            "bytes_in_per_sec":  round(self.bytes_in / elapsed, 1),
            "bytes_out_per_sec": round(self.bytes_out / elapsed, 1),
        }
        self.msgs_in = self.msgs_out = self.bytes_in = self.bytes_out = 0
        self.window_start = time.time()
        return self.last_rates


stats = Stats()


# --------------------------------------------------------------------------
# Subscription manager -- same lifecycle as before (0->1 subscribers starts
# a tailer task for the symbol, 1->0 cancels it), plus per-symbol window
# memory. Window is global per symbol (last requester wins), not per-client
# -- fine for a single-developer dashboard, would need rethinking for
# multiple people watching the same symbol with different windows.
# --------------------------------------------------------------------------
class SubscriptionManager:
    def __init__(self):
        self.all_clients = set()
        self.symbol_to_clients = defaultdict(set)
        self.client_to_symbols = defaultdict(set)
        self.symbol_tasks = {}
        self.sym_windows = {}
        self.lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        async with self.lock:
            self.all_clients.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            self.all_clients.discard(ws)
            symbols = self.client_to_symbols.pop(ws, set())
            for sym in symbols:
                self.symbol_to_clients[sym].discard(ws)
                if not self.symbol_to_clients[sym]:
                    self.symbol_to_clients.pop(sym, None)
                    self._stop_listener(sym)

    async def subscribe(self, ws: WebSocket, symbol: str) -> bool:
        async with self.lock:
            current = self.client_to_symbols[ws]
            if symbol not in current and len(current) >= MAX_SYMBOLS_PER_CLIENT:
                return False
            is_new = symbol not in self.symbol_to_clients
            self.symbol_to_clients[symbol].add(ws)
            current.add(symbol)
            if is_new:
                self._start_listener(symbol)
            return True

    async def unsubscribe(self, ws: WebSocket, symbol: str):
        async with self.lock:
            self.symbol_to_clients[symbol].discard(ws)
            if not self.symbol_to_clients[symbol]:
                self.symbol_to_clients.pop(symbol, None)
                self._stop_listener(symbol)
            self.client_to_symbols[ws].discard(symbol)

    def _start_listener(self, symbol):
        if symbol not in self.symbol_tasks:
            self.symbol_tasks[symbol] = asyncio.create_task(_tail_symbol(symbol, self))

    def _stop_listener(self, symbol):
        task = self.symbol_tasks.pop(symbol, None)
        if task:
            task.cancel()

    def get_window(self, symbol):
        return self.sym_windows.get(symbol, DEFAULT_WINDOW)

    def set_window(self, symbol, window):
        self.sym_windows[symbol] = window

    def active_symbols(self):
        return list(self.symbol_to_clients.keys())

    def clients_for(self, symbol):
        return list(self.symbol_to_clients.get(symbol, ()))

    def client_count(self):
        return len(self.all_clients)


manager = SubscriptionManager()

import psutil
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

import time

def get_system_stats():

    cpu_pct = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory()
    ram_pct = ram.percent
    ram_gb = round(ram.used / (1024 ** 3), 2)

    swap = psutil.swap_memory()
    swap_used = round(swap.used / (1024 ** 3), 2)

    try:
        r_info = r.info('memory')
        redis_bytes = r_info.get('used_memory', 0)
        redis_gb = round(redis_bytes / (1024 ** 3), 3)
    except Exception:
        redis_gb = 0.0

    return {
        "cpu_pct": cpu_pct,  
        "ram_pct": ram_pct,
        "ram_gb": ram_gb,
        "swap_gb": swap_used,
        "redis_gb": redis_gb,
    }

async def _send(ws: WebSocket, payload: dict):
    msg = json.dumps(payload)
    try:
        await ws.send_text(msg)
        stats.msgs_out += 1
        stats.bytes_out += len(msg)
    except Exception:
        pass


async def _send_text(ws: WebSocket, msg: str):
    """Send a pre-serialized frame. Used by the tailer so a batch is JSON-
    encoded exactly once and reused across every client watching the symbol."""
    try:
        await ws.send_text(msg)
        stats.msgs_out += 1
        stats.bytes_out += len(msg)
    except Exception:
        pass


async def _tail_symbol(symbol: str, mgr: "SubscriptionManager"):
    """Runs for as long as at least one client watches `symbol`. Tails both
    the tick stream and the MA stream with a single XREAD BLOCK over both
    keys, forwarding each new entry to whichever clients currently want the
    symbol. Cursors start at '$' (only entries that arrive after we begin
    tailing) -- history is delivered separately on subscribe. Retries on
    Redis error, exits cleanly on cancel (last subscriber left)."""
    tick_key = ticker_stream(symbol)
    ma_key = ma_stream(symbol)
    # Every stream we tail -> how to turn an entry into a frame. Ticks and MA
    # keep their own message types; overlay metrics share one "metric" type
    # tagged with the metric key. Non-existent streams just never wake us.
    handlers = {
        tick_key: ("ticks", _tick, None),
        ma_key:   ("mas",   _ma,   None),
    }
    for mkey, cfg in OVERLAY_METRICS.items():
        handlers[cfg["stream"](symbol)] = ("metric", _metric_mapper(cfg["fields"]), mkey)
    cursors = {k: "$" for k in handlers}   # '$' = only new entries from now on
    while True:
        try:
            results = await ar.xread(cursors, count=XREAD_COUNT, block=XREAD_BLOCK_MS)
            if not results:
                continue  # block timed out with no data; loop and wait again
            for key, entries in results:
                if not entries:
                    continue
                cursors[key] = entries[-1][0]   # advance cursor past what we just read

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
                for ws in mgr.clients_for(symbol):  # encode once, fan out
                    await _send_text(ws, frame)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"[tail] {symbol} error: {e}; retrying")
            await asyncio.sleep(1)


async def stats_loop():
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        try:
            rates = stats.snapshot_and_reset()
            hw_stats = await asyncio.to_thread(get_system_stats) 

            payload = {
                "type": "stats",
                "clients_connected": manager.client_count(),
                "active_symbols": len(manager.active_symbols()),
                "msgs_in_per_sec": rates["msgs_in_per_sec"],
                "msgs_out_per_sec": rates["msgs_out_per_sec"],
                "bytes_in_per_sec": rates["bytes_in_per_sec"],
                "bytes_out_per_sec": rates["bytes_out_per_sec"],
                "hardware": hw_stats,
            }
            for ws in list(manager.all_clients):
                await _send(ws, payload)
        except Exception as e:
            print(f"⚠️ [STATS LOOP ERROR] {e}")
        # the old unconditional resend loop that lived here after the
        # except block is gone -- see note below


@app.on_event("startup")
async def _startup():
    keep_awake()
    asyncio.create_task(stats_loop())


# --------------------------------------------------------------------------
# History reads
# --------------------------------------------------------------------------

async def _read_quick_history(key: str, count: int, mapper):
    """Most recent `count` entries, regardless of how far back in time that
    reaches -- a single bounded call, fast enough to feel instant."""
    try:
        entries = await ar.xrevrange(key, "+", "-", count=count)
    except Exception as e:
        print(f"[quick-history] {key}: {e}")
        return []
    points = []
    for entry_id, fields in reversed(entries):  # oldest -> newest
        try:
            points.append(mapper(entry_id, fields))
        except (KeyError, ValueError, TypeError):
            continue
    return points


async def _read_window_history(key: str, window_ms: int, mapper):
    """Every entry in the trailing `window_ms`, oldest -> newest. Paginated
    via the id cursor (HISTORY_CHUNK per round trip) so a long window on a
    busy stream can't block Redis long enough to time out in one call."""
    since_ms = int(time.time() * 1000) - window_ms
    collected = []
    cursor_max = "+"
    try:
        while True:
            entries = await ar.xrevrange(key, cursor_max, since_ms, count=HISTORY_CHUNK)
            if not entries:
                break
            collected.extend(entries)
            if len(entries) < HISTORY_CHUNK:
                break  # exhausted the window
            last_id = entries[-1][0]
            ts, seq = last_id.split("-")
            seq = int(seq)
            cursor_max = f"{ts}-{seq - 1}" if seq > 0 else f"{int(ts) - 1}-18446744073709551615"
    except Exception as e:
        print(f"[window-history] {key}: {e}")
    points = []
    for entry_id, fields in reversed(collected):  # oldest -> newest
        try:
            points.append(mapper(entry_id, fields))
        except (KeyError, ValueError, TypeError):
            continue
    return points


async def _load_full_window(sym: str, window: str, mgr: "SubscriptionManager"):
    """Background task: read the full selected window and broadcast it to
    every client currently watching `sym`, replacing whatever quick slice
    (or previous window) they had."""
    window_ms = HISTORY_WINDOWS_MS[window]

    ticks = await _read_window_history(ticker_stream(sym), window_ms, _tick)
    payload = {"type": "history", "symbol": sym, "ticks": ticks, "complete": True, "window": window}
    for ws in mgr.clients_for(sym):
        await _send(ws, payload)

    ma_pts = await _read_window_history(ma_stream(sym), window_ms, _ma)
    if ma_pts:
        payload = {"type": "ma-history", "symbol": sym, "points": ma_pts, "complete": True}
        for ws in mgr.clients_for(sym):
            await _send(ws, payload)

    for mkey, cfg in OVERLAY_METRICS.items():
        pts = await _read_window_history(cfg["stream"](sym), window_ms, _metric_mapper(cfg["fields"]))
        if pts:
            payload = {"type": "metric-history", "key": mkey, "symbol": sym, "points": pts, "complete": True}
            for ws in mgr.clients_for(sym):
                await _send(ws, payload)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    await manager.connect(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            stats.msgs_in += 1
            stats.bytes_in += len(raw)
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "subscribe":
                syms = [s.upper() for s in msg.get("symbols", []) if isinstance(s, str)]
                requested_window = msg.get("window")

                for sym in syms:
                    ok = await manager.subscribe(websocket, sym)
                    if not ok:
                        await _send(websocket, {
                            "type": "error", "symbol": sym,
                            "message": f"limit of {MAX_SYMBOLS_PER_CLIENT} symbols reached",
                        })
                        continue

                    window = requested_window if requested_window in HISTORY_WINDOWS_MS else manager.get_window(sym)
                    manager.set_window(sym, window)

                    # phase 1: instant, count-bounded slice
                    quick_ticks = await _read_quick_history(ticker_stream(sym), QUICK_HISTORY_COUNT, _tick)
                    await _send(websocket, {
                        "type": "history", "symbol": sym, "ticks": quick_ticks,
                        "complete": False, "window": window,
                    })

                    quick_ma = await _read_quick_history(ma_stream(sym), QUICK_HISTORY_COUNT, _ma)
                    if quick_ma:
                        await _send(websocket, {
                            "type": "ma-history", "symbol": sym, "points": quick_ma, "complete": False,
                        })

                    for mkey, cfg in OVERLAY_METRICS.items():
                        qpts = await _read_quick_history(cfg["stream"](sym), QUICK_HISTORY_COUNT, _metric_mapper(cfg["fields"]))
                        if qpts:
                            await _send(websocket, {
                                "type": "metric-history", "key": mkey, "symbol": sym,
                                "points": qpts, "complete": False,
                            })

                    # phase 2: the real window, filled in the background
                    asyncio.create_task(_load_full_window(sym, window, manager))

            elif action == "set_window":
                sym = msg.get("symbol")
                window = msg.get("window")
                if isinstance(sym, str) and window in HISTORY_WINDOWS_MS:
                    sym = sym.upper()
                    if sym in manager.symbol_to_clients:
                        manager.set_window(sym, window)
                        asyncio.create_task(_load_full_window(sym, window, manager))

            elif action == "unsubscribe":
                syms = [s.upper() for s in msg.get("symbols", []) if isinstance(s, str)]
                for sym in syms:
                    await manager.unsubscribe(websocket, sym)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] error: {e}")
    finally:
        await manager.disconnect(websocket)


@app.get("/api/symbols")
async def symbols():
    """Every symbol that currently has a tick stream."""
    out = []
    async for key in ar.scan_iter(match="LiveTicker:*", count=1000):
        out.append(key.split(":", 1)[-1])
    return JSONResponse(sorted(out))


@app.get("/api/history/{symbol}")
async def history(symbol: str, window: str = DEFAULT_WINDOW):
    """Tick points for the given window (2m/10m/30m/1h), oldest -> newest."""
    window = window if window in HISTORY_WINDOWS_MS else DEFAULT_WINDOW
    pts = await _read_window_history(ticker_stream(symbol.upper()), HISTORY_WINDOWS_MS[window], _tick)
    return JSONResponse(pts)


@app.get("/api/ma-history/{symbol}")
async def ma_history(symbol: str, window: str = DEFAULT_WINDOW):
    """MA points for the given window, oldest -> newest."""
    window = window if window in HISTORY_WINDOWS_MS else DEFAULT_WINDOW
    pts = await _read_window_history(ma_stream(symbol.upper()), HISTORY_WINDOWS_MS[window], _ma)
    return JSONResponse(pts)


@app.get("/api/ws-stats")
async def ws_stats():
    return JSONResponse({
        "clients_connected": manager.client_count(),
        "active_symbols": manager.active_symbols(),
        **stats.last_rates,
    })


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(HTML_PATH, "r", encoding="utf-8") as fh:
        return HTMLResponse(fh.read())
