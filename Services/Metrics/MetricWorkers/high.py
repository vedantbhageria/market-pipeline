from Services.Metrics.RollingMetric import RollingMetricWorker
from shared.redis_streams import ticker_stream, rolling_high_stream
from shared.constants import METRICS_WINDOW_MS


def rolling_max(window, popped, appended, prev, qty_sum):
    m = appended[1]
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
