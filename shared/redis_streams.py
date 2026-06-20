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


def parse_ts_ms(entry_id):
    return int(entry_id.split("-")[0])


def last_entry_per_symbol(r, symbols, stream_key_fn):
    """Pipelined xrevrange count=1 over all symbols. Returns {sym: latest_entry_or_exception}."""
    pipe = r.pipeline()
    for sym in symbols:
        pipe.xrevrange(stream_key_fn(sym), max="+", min="-", count=1)
    return dict(zip(symbols, pipe.execute(raise_on_error=False)))


def latest_key(r, symbols, default_start_ms, now_ms, max_lookback, stream_key_fn):
    start_times = {}
    for sym, latest in last_entry_per_symbol(r, symbols, stream_key_fn).items():
        if isinstance(latest, Exception) or not latest:
            start_times[sym] = default_start_ms
            continue
        latest_ts = parse_ts_ms(latest[0][0])
        if now_ms - latest_ts > max_lookback:
            start_times[sym] = default_start_ms
        else:
            start_times[sym] = ((latest_ts // 1000) + 1) * 1000
    return start_times
