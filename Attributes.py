import math
from RollingMetric import RollingMetricWorker

def ticker_stream(sym):
    return f"LiveTicker:{sym}"


def ma_stream(sym):
    return f"MovingAverage:{sym}"


def vwma_stream(sym):
    return f"VWMA:{sym}"


def ema_stream(sym):
    return f"EMA:{sym}"


def bollinger_stream(sym):
    return f"Bollinger:{sym}"


def rolling_high_stream(sym):
    return f"RollingHigh:{sym}"


def rolling_low_stream(sym):
    return f"RollingLow:{sym}"


def momentum_stream(sym):
    return f"Momentum:{sym}"


def volume_stream(sym):
    return f"Volume:{sym}"


def load_symbols(path):
    import os
    if not os.path.exists(path):
        raise FileNotFoundError(f"symbols file not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().upper() for line in f if line.strip()]


def _mean_price(window, price):
    return (price + sum(p for _, p, _ in window)) / (len(window) + 1)


def sma(window, popped, appended, prev, qty_sum):

    price = appended[1]
    n_new = len(window) + 1
    if prev is None:
        return _mean_price(window, price)
    n_prev = len(window) + len(popped)
    sum_prev = prev * n_prev
    pop_sum = sum(p for _, p, _ in popped)
    return (sum_prev - pop_sum + price) / n_new


def vwma(window, popped, appended, prev, qty_sum):
  
    v, qa = appended[1], appended[2]
    den_new = qty_sum + qa
    if prev is None:
        num = v * qa + sum(p * q for _, p, q in window)
        return num / den_new if den_new > 0 else _mean_price(window, v)
    den_prev = qty_sum + sum(q for _, _, q in popped)
    num_prev = prev * den_prev
    num_new = num_prev - sum(p * q for _, p, q in popped) + v * qa
    if den_new <= 0:
        return _mean_price(window, v)
    return num_new / den_new


def ema(window, popped, appended, prev, qty_sum, alpha=0.2):

    price = appended[1]
    if prev is None:
        return price
    return alpha * price + (1 - alpha) * prev


def bollinger(window, popped, appended, prev, qty_sum, k=2.0):

    price = appended[1]
    n_new = len(window) + 1
    if prev is None:
        prices = [p for _, p, _ in window]
        prices.append(price)
        mid = sum(prices) / n_new
        var = sum((p - mid) ** 2 for p in prices) / n_new
        std = math.sqrt(var)
    else:
        n_prev = len(window) + len(popped)
        mid_prev, std_prev = prev["mid"], prev["std"]
        sum_x = mid_prev * n_prev
        sum_x2 = (std_prev * std_prev + mid_prev * mid_prev) * n_prev
        for _, p, _ in popped:
            sum_x -= p
            sum_x2 -= p * p
        sum_x += price
        sum_x2 += price * price
        mid = sum_x / n_new
        var = sum_x2 / n_new - mid * mid
        if var < 0.0:            # tiny negatives from float error
            var = 0.0
        std = math.sqrt(var)
    return {
        "mid": mid,
        "upper": mid + k * std,
        "lower": mid - k * std,
        "std": std,
    }


def rolling_max(window, popped, appended, prev, qty_sum):
    m = appended[1]
    for _, p, _ in window:
        if p > m:
            m = p
    return m


def rolling_min(window, popped, appended, prev, qty_sum):
    m = appended[1]
    for _, p, _ in window:
        if p < m:
            m = p
    return m


def momentum(window, popped, appended, prev, qty_sum):
    #(newest - oldest) / oldest * 100.
    price = appended[1]
    if not window:
        return 0.0
    first = window[0][1]
    if first == 0:
        return 0.0
    return (price - first) / first * 100.0


def total_volume(window, popped, appended, prev, qty_sum):
    return qty_sum + appended[2]


if __name__ == "__main__":
    import os
    SYMBOLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "symbols_futures.txt")
    symbols = load_symbols(SYMBOLS_FILE)
    print(f"[load] {len(symbols)} symbols")

    WINDOW_MS = 30000

    workers = [
        RollingMetricWorker(
            source_stream_fn=ticker_stream, output_stream_fn=ma_stream,
            window_ms=WINDOW_MS, compute_fn=sma, value_field="avg_price", name="sma",
        ),
        RollingMetricWorker(
            source_stream_fn=ticker_stream, output_stream_fn=vwma_stream,
            window_ms=WINDOW_MS, compute_fn=vwma, value_field="vwma", name="vwma",
        ),
        RollingMetricWorker(
            source_stream_fn=ticker_stream, output_stream_fn=ema_stream,
            window_ms=WINDOW_MS, compute_fn=ema, value_field="ema", name="ema",
        ),
        RollingMetricWorker(
            source_stream_fn=ticker_stream, output_stream_fn=bollinger_stream,
            window_ms=WINDOW_MS, compute_fn=bollinger, name="bollinger",
        ),
        RollingMetricWorker(ticker_stream, rolling_high_stream, WINDOW_MS, rolling_max,   value_field="high",   name="high"),
        RollingMetricWorker(ticker_stream, rolling_low_stream,  WINDOW_MS, rolling_min,   value_field="low",    name="low"),
        RollingMetricWorker(ticker_stream, momentum_stream,     WINDOW_MS, momentum,      value_field="pct",    name="momentum"),
        RollingMetricWorker(ticker_stream, volume_stream,       WINDOW_MS, total_volume,  value_field="volume", name="volume"),
    ]

    processes = []
    for w in workers:
        processes.extend(w.start(symbols))

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("[attributes] shutting down...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
