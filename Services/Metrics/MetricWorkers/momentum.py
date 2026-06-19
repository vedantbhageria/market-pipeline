from Services.Metrics.RollingMetric import RollingMetricWorker
from shared.redis_streams import ticker_stream, momentum_stream
from shared.constants import METRICS_WINDOW_MS


def momentum(window, popped, appended, prev, qty_sum):
    price = appended[1]
    if not window:
        return 0.0
    first = window[0][1]
    if first == 0:
        return 0.0
    return (price - first) / first * 100.0


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=momentum_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=momentum,
    value_field="pct",
    name="momentum",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
