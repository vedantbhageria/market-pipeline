from Services.Metrics.RollingMetric import RollingMetricWorker
from shared.redis_streams import ticker_stream, ema_stream
from shared.constants import METRICS_WINDOW_MS


def ema(window, popped, appended, prev, qty_sum, alpha=0.2):
    price = appended[1]
    if prev is None:
        return price
    return alpha * price + (1 - alpha) * prev


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=ema_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=ema,
    value_field="ema",
    name="ema",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
