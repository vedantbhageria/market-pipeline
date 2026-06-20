import re
import logging
import threading
from datetime import datetime, timezone
import numpy as np
from binance.client import Client
from services.data.request_queue import RequestQueueManager
from shared.redis_pool import ConnectionManager
from shared.symbols import load_symbols
from shared.redis_streams import ticker_stream, latest_key
from shared.constants import BACKFILL_NUM_WORKERS, BACKFILL_AGG_TRADE_WEIGHT, BACKFILL_LOOKBACK_MS, STREAM_MAXLEN
from shared.logging_setup import setup_logging
from services.data.data_aggregation import aggregate_to_seconds

log = logging.getLogger("backfill")

cm = ConnectionManager("backfill")
r  = cm.get_sync("backfill-sync")

BAN_MS_RE = re.compile(r"banned until (\d+)")


def _report_rate_limit(e, request_queues):
    msg = str(e)
    m = BAN_MS_RE.search(msg)
    if m:
        until_ms = int(m.group(1))
        log.warning("IP banned until %s — pausing all backfill workers", until_ms)
        request_queues.report_rate_limit(until_ms=until_ms)
    else:
        log.warning("rate limit hit — pausing all backfill workers for 15s")
        request_queues.report_rate_limit()


def aggregate_trades_call(queue_id, client, request_queues, msg):
    kwargs = {"symbol": msg["symbol"]}

    if msg["from_id"] is None:
        kwargs["startTime"] = msg["start_ms"]
        kwargs["endTime"]   = msg["end_ms"]
    else:
        kwargs["fromId"] = msg["from_id"]

    try:
        return client.futures_aggregate_trades(**kwargs, limit=1000)
    except Exception as e:
        if "-1003" in str(e) or getattr(e, "code", None) == -1003:
            _report_rate_limit(e, request_queues)
            request_queues.put(queue_id, [msg])
            return 0
        raise


def backfill_agent(queue_id, request_queues, client):
    while True:
        msg = request_queues.get(queue_id, weight=BACKFILL_AGG_TRADE_WEIGHT)
        if msg is None:
            break

        sym    = msg["symbol"]
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
                    "symbol":   sym,
                    "start_ms": msg["start_ms"],
                    "end_ms":   end_ms,
                    "from_id":  int(batch[-1]["a"]) + 1,
                    "trades":   trades,
                }])
                log.debug("reinput %s — limit of %d hit", sym, len(batch))
                continue

            trades = [t for t in trades if int(t["T"]) < end_ms]
            bars   = aggregate_to_seconds(trades)

            pipe = r.pipeline()
            for bar in bars:
                pipe.xadd(
                    ticker_stream(sym),
                    {"price": bar["price"], "quantity": bar["quantity"]},
                    id=f"{bar['open_time']}-0",
                    maxlen=STREAM_MAXLEN,
                )
            pipe.execute(raise_on_error=False)
            log.info("%s: stored %d second-ticks from %d raw trades", sym, len(bars), len(trades))

            r.publish("backfill:complete", sym)
            log.info("%s: published complete", sym)

        except Exception as e:
            log.error("%s failed: %s", sym, e)

        if request_queues.size(queue_id) == 0:
            request_queues.put(queue_id, [None])


def main():
    setup_logging()

    symbols = load_symbols("crypto_futures")
    log.info("loaded %d symbols", len(symbols))

    end_ms            = int(datetime.now(timezone.utc).timestamp() * 1000)
    default_start_ms  = end_ms - BACKFILL_LOOKBACK_MS
    resume_start_ms   = latest_key(r, symbols, default_start_ms, end_ms, BACKFILL_LOOKBACK_MS, ticker_stream)

    request_queues = RequestQueueManager(BACKFILL_NUM_WORKERS, 100, 2200)

    shards = np.array_split(np.array(symbols), BACKFILL_NUM_WORKERS)
    for i, shard in enumerate(shards):
        if len(shard) == 0:
            request_queues.put(i, [None])
            continue
        request_queues.put(i, [
            {"symbol": str(s), "start_ms": resume_start_ms[str(s)],
             "end_ms": end_ms, "from_id": None, "trades": []}
            for s in shard
        ])

    client  = Client()
    workers = [
        threading.Thread(target=backfill_agent, args=(i, request_queues, client), daemon=True)
        for i in range(BACKFILL_NUM_WORKERS)
    ]
    for w in workers:
        w.start()

    for w in workers:
        w.join()
    log.info("all workers complete")


if __name__ == "__main__":
    main()
