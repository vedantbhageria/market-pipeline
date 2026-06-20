# Market Pipeline

A real-time cryptocurrency market-data engine. It ingests live Binance USD-M futures trades over
WebSocket, backfills recent history over REST, computes rolling technical indicators, stores everything
in Redis Streams, and serves it to a live web dashboard.

> 📖 **Full documentation:** open [`docs/index.html`](docs/index.html) in any browser (single-file, offline).

## Architecture at a glance

One launcher (`pipeline.py`) spins up four independent services, each in its own console. They never call
each other directly — they communicate only through **Redis Streams** (data) and **Redis Pub/Sub** (signals).

| Service | Model | Role |
|---|---|---|
| **Live Ingestion** | asyncio | Streams live `aggTrade` events, writes the tick stream |
| **Backfill** | threads | Fetches recent history over REST, aggregates to 1-second bars |
| **Metric Workers** | threads → processes | 8 indicators (SMA, VWMA, EMA, Bollinger, High, Low, Momentum, Volume) |
| **Dashboard** | asyncio | FastAPI + WebSocket server feeding the browser UI |

```
Binance ──▶ Live / Backfill ──▶ Redis ──▶ Metric Workers ──▶ Redis
                                  │                              │
                                  └────────▶ Dashboard ◀─────────┘ ──▶ Browser
```

## Quick start

**Prerequisites:** Python 3.10+ and a running Redis server (default `localhost:6379`).

```bash
# from the project root — installs deps and registers the packages
pip install -e .

# run the whole pipeline (opens consoles + the dashboard at :8000)
python pipeline.py
```

Press **Ctrl+C** in the launcher to stop everything.

## Run a single service

```bash
python services/data/live_binance.py
python services/data/backfill_binance.py
python services/metrics/run_all.py
python -m uvicorn services.dashboard_server.server:app --port 8000
```

## Project layout

```
pipeline.py            entry point — starts everything
orchestration/         process launching (ProcessManager + service registry)
services/
  data/                live ingestion, backfill, aggregation, rate limiter
  metrics/             rolling-window engine + 8 indicator workers
  dashboard_server/    FastAPI app, WebSocket fan-out, history, stats
shared/                Redis pool, stream keys, symbols, constants, logging
frontend/              dashboard.html (single-page UI)
scripts/               flush_redis.py (dev helper)
docs/                  index.html (full documentation)
```

## Configuration

Tunables live in [`shared/constants.py`](shared/constants.py). Redis location is set via the
`REDIS_HOST` / `REDIS_PORT` environment variables (default `localhost:6379`).

See [`docs/index.html`](docs/index.html) for the full reference — Redis data model, WebSocket protocol,
the rolling-metric engine, and the cross-process connection registry.
