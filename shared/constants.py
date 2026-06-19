# Backfill
BACKFILL_NUM_WORKERS      = 6      # number of parallel backfill threads / request queues
BACKFILL_AGG_TRADE_WEIGHT = 20     # API weight cost per aggregate trades call
BACKFILL_LOOKBACK_MS      = 1000   # how far back to backfill on a fresh start (ms)

# Live
LIVE_WS_SHARD_SIZE = 190   # max streams per websocket connection

# Metrics
METRICS_WINDOW_MS = 30000  # rolling window size for all metric workers (ms)
