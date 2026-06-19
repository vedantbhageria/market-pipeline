import asyncio
import json
import os
import re
import threading
from datetime import datetime, timezone
import numpy as np
import redis
import redis.asyncio as aredis
import websockets
from binance.client import Client
from QueueManager import RequestQueueManager

SYMBOLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "symbols_futures.txt")
NUM_QUEUES = 6
WEIGHT_AGGTRADES = 20  
SHARD_SIZE = 190        
BAN_MS_RE = re.compile(r"banned until (\d+)")

backfill_events: dict[str, asyncio.Event] = {}
live_buffer: dict[str, list[dict]] = {}
live_direct: dict[str, bool] = {}

r = redis.Redis(
    host="localhost", port=6379, decode_responses=True,
    socket_connect_timeout=5, socket_timeout=15,
)
ar = aredis.Redis(
    host="localhost", port=6379, decode_responses=True,
    socket_connect_timeout=5, socket_timeout=15,
)

def _symbol_done(queue_id, request_queues):
    if request_queues.size(queue_id) == 0:
        request_queues.put(queue_id, [None])

def stream_key(sym):
    return f"LiveTicker:{sym}"

def _is_rate_limit_error(e):
    return "-1003" in str(e) or getattr(e, "code", None) == -1003

def _report_rate_limit(e, request_queues):
    msg = str(e)
    m = BAN_MS_RE.search(msg)
    if m:
        until_ms = int(m.group(1))
        print(f"[rate-limit] IP banned until {until_ms} -- pausing all backfill workers")
        request_queues.report_rate_limit(until_ms=until_ms)
    else:
        print("[rate-limit] too many requests -- pausing all backfill workers for 15s")
        request_queues.report_rate_limit()

def aggregate_trades_call(queue_id, client, request_queues, msg):

    kwargs = {}
    kwargs["symbol"] = msg["symbol"]

    if msg["from_id"] is None:
        kwargs["startTime"] = msg["start_ms"]
        kwargs["endTime"] = msg["end_ms"]
    else:
        kwargs["fromId"] = msg["from_id"]

    try:
        return client.futures_aggregate_trades(**kwargs, limit = 1000)
    except Exception as e:
        if _is_rate_limit_error(e):
            _report_rate_limit(e, request_queues)
            request_queues.put(queue_id, [msg])
            return 0
        raise

def aggregate_to_seconds(trades):

    bars = {}

    for t in trades:

        sec_ms = (int(t["T"]) // 1000) * 1000
        bar = bars.get(sec_ms)
        if bar is None:
            bars[sec_ms] = {"open_time": sec_ms, "price": t["p"], "quantity": float(t["q"])}
        else:
            bar["price"] = t["p"]
            bar["quantity"] += float(t["q"])

    return [bars[k] for k in sorted(bars)]


def backfill_agent(queue_id, request_queues, client, loop):

    while True:
        msg = request_queues.get(queue_id, weight = WEIGHT_AGGTRADES)
        if msg is None:
            break

        sym = msg["symbol"]
        end_ms = msg["end_ms"]

        try:

            batch = aggregate_trades_call(queue_id, client, request_queues, msg)

            if batch == 0:
                continue

            trades = msg["trades"]
            if batch:
                trades.extend(batch)

            needs_more = bool(batch) and len(batch) == 1000 and int(batch[-1]["T"]) < end_ms

            if needs_more:
                request_queues.put(queue_id, [{
                    "symbol": sym,
                    "start_ms": msg["start_ms"],
                    "end_ms": end_ms,
                    "from_id": int(batch[-1]["a"]) + 1,
                    "trades": trades,
                }])
                print(f"[Backfill] Reinput {sym} for next pass as {len(batch)} limit met")
                continue  

            trades = [t for t in trades if int(t["T"]) < end_ms]
            bars = aggregate_to_seconds(trades)

            pipe = r.pipeline()
            for bar in bars:
                pipe.xadd(
                    stream_key(sym),
                    {"price": bar["price"], "quantity": bar["quantity"]},
                    id=f"{bar['open_time']}-0",
                    maxlen= 36000
                )
            pipe.execute(raise_on_error=False)
            print(f"[backfill] {sym}: {len(bars)} second-ticks stored (from {len(trades)} raw trades)")
            loop.call_soon_threadsafe(backfill_events[sym].set)
            print(f"Ban Lifted for {sym}")

        except Exception as e:
            print(f"[backfill] {sym} failed: {e}")

        _symbol_done(queue_id, request_queues)

async def handle_tick(sym, tick):
    if backfill_events[sym].is_set():
        if not live_direct.get(sym):
            buffered = live_buffer.pop(sym, [])
            if buffered:
                pipe = ar.pipeline()
                for b in buffered:
                    pipe.xadd(
                        stream_key(sym),
                        {"price": b["price"], "quantity": b["quantity"]},
                        id=f"{b['T']}-{b['a']}",
                        maxlen=36000,
                    )
                try:
                    results = await pipe.execute(raise_on_error=False)
                    failures = sum(1 for r in results if isinstance(r, Exception))
                    print(f"[live] flushed {len(buffered)-failures}/{len(buffered)} buffered ticks for {sym}")
                except Exception as e:
                    print(f"[live] flush failed for {sym}: {e}")
            live_direct[sym] = True

        try:
            await ar.xadd(
                stream_key(sym),
                {"price": tick["price"], "quantity": tick["quantity"]},
                id=f"{tick['T']}-{tick['a']}",
                maxlen=36000,
            )
        except Exception as e:
            print(f"[live] direct write failed for {sym}: {e}")
    else:
        live_buffer.setdefault(sym, []).append(tick)
    

async def live_socket(streams):
    url = "wss://fstream.binance.com/market/stream?streams=" + "/".join(streams)

    async for ws in websockets.connect(url):  
        print("[WS] Made Connection With Websocket")

        try:
            async for raw in ws:

                try:
                    msg = json.loads(raw)
                    d = msg.get("data", msg)
                    if d.get("e") != "aggTrade":
                        continue

                    await handle_tick(d["s"], {
                        "T": int(d["T"]), "a": int(d["a"]),
                        "price": d["p"], "quantity": d["q"],
                    })

                except Exception as e:
                    print(f"[live] [{d["s"]}] tick processing error: {e}")

        except websockets.ConnectionClosed:
            continue


async def live_listener(symbols):

    streams = [f"{s.lower()}@aggTrade" for s in symbols]
    tasks = [
        asyncio.create_task(live_socket(streams[i:i + SHARD_SIZE]))
        for i in range(0, len(streams), SHARD_SIZE)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            print(f"[live] a shard task exited with an error: {res}")

def _resume_start_times(symbols, default_start_ms, now_ms):
    pipe = r.pipeline()
    for sym in symbols:
        pipe.xrevrange(stream_key(sym), max="+", min="-", count=1)
    results = pipe.execute(raise_on_error=False)

    start_times = {}
    for sym, latest in zip(symbols, results):
        if isinstance(latest, Exception) or not latest:
            start_times[sym] = default_start_ms
            continue
        latest_ts = int(latest[0][0].split("-")[0])
        if now_ms - latest_ts > 60 * 60 * 1000:
            start_times[sym] = default_start_ms
        else:
            start_times[sym] = ((latest_ts // 1000) + 1) * 1000
    return start_times


async def main():
    if not os.path.exists(SYMBOLS_FILE):
        raise FileNotFoundError(f"symbols.txt not found at {SYMBOLS_FILE}")
    with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
        symbols = [line.strip().upper() for line in f if line.strip()]
    print(f"[load] {len(symbols)} symbols")

    for sym in symbols:
        backfill_events[sym] = asyncio.Event() #Create Backfill Indicator

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    default_start_ms = now_ms - 1000
    end_ms = now_ms 

    resume_start_ms = _resume_start_times(symbols, default_start_ms, now_ms)

    request_queues = RequestQueueManager(NUM_QUEUES)

    shards = np.array_split(np.array(symbols), NUM_QUEUES)
    for i, shard in enumerate(shards):
        if len(shard) == 0:
            request_queues.put(i, [None])
            continue
        messages = [
            {
                "symbol": str(s),
                "start_ms": resume_start_ms[str(s)],
                "end_ms": end_ms,
                "from_id": None,
                "trades": [],
            }
            for s in shard
        ]
        request_queues.put(i, messages)

    client = Client()                                       #Binance API Client
    loop = asyncio.get_running_loop()

    live_task = asyncio.create_task(live_listener(symbols))
    
    workers = [
        threading.Thread(
            target=backfill_agent,
            args=(i, request_queues, client, loop),
            daemon=True,
        )
        for i in range(NUM_QUEUES)
    ]
    for w in workers:
       w.start()
    

    await asyncio.gather(*(asyncio.to_thread(w.join) for w in workers))
    
    print("[backfill] all shards complete, per-symbol live handoff completed as each one finished")

    await live_task


if __name__ == "__main__":
    asyncio.run(main())