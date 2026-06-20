import asyncio
import json
import logging
import websockets
from shared.redis_pool import ConnectionManager
from shared.symbols import load_symbols
from shared.redis_streams import ticker_stream
from shared.constants import LIVE_WS_SHARD_SIZE, STREAM_MAXLEN
from shared.logging_setup import setup_logging

log = logging.getLogger("live")

cm = ConnectionManager("live")
ar = cm.get_async("live-async")

backfill_events: dict[str, asyncio.Event] = {}
live_buffer: dict[str, list[dict]] = {}
live_direct: dict[str, bool] = {}


async def handle_tick(sym, tick):
    if backfill_events[sym].is_set():
        if not live_direct.get(sym):
            buffered = live_buffer.pop(sym, [])
            if buffered:
                pipe = ar.pipeline()
                for b in buffered:
                    pipe.xadd(
                        ticker_stream(sym),
                        {"price": b["price"], "quantity": b["quantity"]},
                        id=f"{b['T']}-{b['a']}",
                        maxlen=STREAM_MAXLEN,
                    )
                try:
                    results = await pipe.execute(raise_on_error=False)
                    failures = sum(1 for r in results if isinstance(r, Exception))
                    log.info("flushed %d/%d buffered ticks for %s", len(buffered) - failures, len(buffered), sym)
                except Exception as e:
                    log.error("flush failed for %s: %s", sym, e)
            live_direct[sym] = True

        try:
            await ar.xadd(
                ticker_stream(sym),
                {"price": tick["price"], "quantity": tick["quantity"]},
                id=f"{tick['T']}-{tick['a']}",
                maxlen=STREAM_MAXLEN,
            )
        except Exception as e:
            log.error("direct write failed for %s: %s", sym, e)
    else:
        live_buffer.setdefault(sym, []).append(tick)


async def listen_for_backfill():
    pubsub = ar.pubsub()
    await pubsub.subscribe("backfill:complete")
    async for message in pubsub.listen():
        if message["type"] == "message":
            sym = message["data"]
            if sym in backfill_events:
                backfill_events[sym].set()
                log.info("backfill complete signal received for %s", sym)


async def live_socket(streams):
    url = "wss://fstream.binance.com/market/stream?streams=" + "/".join(streams)

    async for ws in websockets.connect(url):
        log.info("websocket connected (%d streams)", len(streams))
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
                    log.error("tick processing error [%s]: %s", d.get("s"), e)
        except websockets.ConnectionClosed:
            log.warning("websocket connection closed, reconnecting")
            continue


async def main():
    setup_logging()

    symbols = load_symbols("crypto_futures")
    log.info("loaded %d symbols", len(symbols))

    for sym in symbols:
        backfill_events[sym] = asyncio.Event()

    streams = [f"{s.lower()}@aggTrade" for s in symbols]

    tasks = [asyncio.create_task(listen_for_backfill())] + [
        asyncio.create_task(live_socket(streams[i:i + LIVE_WS_SHARD_SIZE]))
        for i in range(0, len(streams), LIVE_WS_SHARD_SIZE)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, Exception):
            log.error("shard task exited with error: %s", res)


if __name__ == "__main__":
    asyncio.run(main())
