from services.metrics.rolling_metric import RollingMetricWorker
from shared.redis_streams import ticker_stream, rolling_high_stream
from shared.constants import METRICS_WINDOW_MS


def rolling_max(window, popped, appended, prev, qty_sum):
    price = appended[1]
    # prev == max over (window ∪ popped). If the old max is still in the window
    # (i.e. it wasn't among the popped entries), the new max is just max(prev, price).
    if prev is not None and max((p for _, p, _ in popped), default=float("-inf")) < prev:
        return prev if prev > price else price
    # the old max expired out of the window — rescan what remains
    m = price
    for _, p, _ in window:
        if p > m:
            m = p
    return m


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=rolling_high_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=rolling_max,
    value_field="high",
    name="high",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
