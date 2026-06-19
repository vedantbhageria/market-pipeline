from Services.Metrics.RollingMetric import RollingMetricWorker
from shared.redis_streams import ticker_stream, rolling_low_stream
from shared.constants import METRICS_WINDOW_MS


def rolling_min(window, popped, appended, prev, qty_sum):
    m = appended[1]
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
