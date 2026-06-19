from Services.Metrics.RollingMetric import RollingMetricWorker
from shared.redis_streams import ticker_stream, ma_stream
from shared.constants import METRICS_WINDOW_MS

def _mean_price(window, price):
    return (price + sum(p for _, p, _ in window)) / (len(window) + 1)


def sma(window, popped, appended, prev, qty_sum):
    price = appended[1]
    n_new = len(window) + 1
    if prev is None:
        return _mean_price(window, price)
    n_prev  = len(window) + len(popped)
    sum_prev = prev * n_prev
    pop_sum  = sum(p for _, p, _ in popped)
    return (sum_prev - pop_sum + price) / n_new


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=ma_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=sma,
    value_field="avg_price",
    name="sma",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
