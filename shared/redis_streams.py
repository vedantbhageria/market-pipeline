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


def latest_key(r, symbols, default_start_ms, now_ms, max_lookback, stream_key_fn):
    pipe = r.pipeline()
    for sym in symbols:
        pipe.xrevrange(stream_key_fn(sym), max="+", min="-", count=1)
    results = pipe.execute(raise_on_error=False)

    start_times = {}
    for sym, latest in zip(symbols, results):
        if isinstance(latest, Exception) or not latest:
            start_times[sym] = default_start_ms
            continue
        latest_ts = int(latest[0][0].split("-")[0])
        if now_ms - latest_ts > max_lookback:
            start_times[sym] = default_start_ms
        else:
            start_times[sym] = ((latest_ts // 1000) + 1) * 1000
    return start_times
