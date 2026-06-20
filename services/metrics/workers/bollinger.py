import math
from services.metrics.rolling_metric import RollingMetricWorker
from shared.redis_streams import ticker_stream, bollinger_stream
from shared.constants import METRICS_WINDOW_MS


def bollinger(window, popped, appended, prev, qty_sum, k=2.0):
    price = appended[1]
    n_new = len(window) + 1
    if prev is None:
        prices = [p for _, p, _ in window] + [price]
        mid    = sum(prices) / n_new
        var    = sum((p - mid) ** 2 for p in prices) / n_new
        std    = math.sqrt(var)
    else:
        n_prev             = len(window) + len(popped)
        mid_prev, std_prev = prev["mid"], prev["std"]
        sum_x  = mid_prev * n_prev
        sum_x2 = (std_prev ** 2 + mid_prev ** 2) * n_prev
        for _, p, _ in popped:
            sum_x  -= p
            sum_x2 -= p * p
        sum_x  += price
        sum_x2 += price * price
        mid = sum_x / n_new
        var = max(sum_x2 / n_new - mid * mid, 0.0)  # clamp float error
        std = math.sqrt(var)
    return {
        "mid":   mid,
        "upper": mid + k * std,
        "lower": mid - k * std,
        "std":   std,
    }


worker = RollingMetricWorker(
    source_stream_fn=ticker_stream,
    output_stream_fn=bollinger_stream,
    window_ms=METRICS_WINDOW_MS,
    compute_fn=bollinger,
    name="bollinger",
)

if __name__ == "__main__":
    from shared.symbols import load_symbols
    worker.run(load_symbols("crypto_futures"))
