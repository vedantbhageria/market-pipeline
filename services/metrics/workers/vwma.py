from services.metrics.rolling_metric import RollingMetricWorker
from shared.redis_streams import ticker_stream, vwma_stream
from shared.constants import METRICS_WINDOW_MS


def _mean_price(window, price):
    return (price + sum(p for _, p, _ in window)) / (len(window) + 1)


def vwma(window, popped, appended, prev, qty_sum):
    v, qa   = appended[1], appended[2]
    den_new = qty_sum + qa
    if prev is None:
        num = v * qa + sum(p * q for _, p, q in window)
        return num / den_new if den_new > 0 else _mean_price(window, v)
    den_prev = qty_sum + sum(q for _, _, q in popped)
    num_prev = prev * den_prev
    num_new  = num_prev - sum(p * q for _, p, q in popped) + v * qa
    if den_new <= 0:
        return _mean_price(window, v)
    return num_new / den_new


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=vwma_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=vwma,
    value_field="vwma",
    name="vwma",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
