from services.metrics.rolling_metric import RollingMetricWorker
from shared.redis_streams import ticker_stream, volume_stream
from shared.constants import METRICS_WINDOW_MS


def total_volume(window, popped, appended, prev, qty_sum):
    return qty_sum + appended[2]


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=volume_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=total_volume,
    value_field="volume",
    name="volume",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
