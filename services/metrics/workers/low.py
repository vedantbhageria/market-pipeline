from services.metrics.rolling_metric import RollingMetricWorker
from shared.redis_streams import ticker_stream, rolling_low_stream
from shared.constants import METRICS_WINDOW_MS


def rolling_min(window, popped, appended, prev, qty_sum):
    price = appended[1]
    # prev == min over (window ∪ popped). If the old min is still in the window
    # (i.e. it wasn't among the popped entries), the new min is just min(prev, price).
    if prev is not None and min((p for _, p, _ in popped), default=float("inf")) > prev:
        return prev if prev < price else price
    # the old min expired out of the window — rescan what remains
    m = price
    for _, p, _ in window:
        if p < m:
            m = p
    return m


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=rolling_low_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=rolling_min,
    value_field="low",
    name="low",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
