from shared.redis_streams import (
    ticker_stream, ma_stream, vwma_stream, ema_stream, bollinger_stream,
    rolling_high_stream, rolling_low_stream, momentum_stream, volume_stream,
    parse_ts_ms,
)


def _tick(entry_id, f):
    return {
        "t":     parse_ts_ms(entry_id) / 1000.0,
        "price": float(f["price"]),
        "qty":   float(f.get("quantity", 0) or 0),
    }


def _ma(entry_id, f):
    return {
        "t":  parse_ts_ms(entry_id) / 1000.0,
        "ma": float(f["avg_price"]),
        "n":  int(float(f.get("window_ticks", 0) or 0)),
    }


def _metric_mapper(field_map):
    def m(entry_id, f):
        pt = {"t": parse_ts_ms(entry_id) / 1000.0}
        for src, dst in field_map.items():
            if src in f:
                pt[dst] = float(f[src])
        return pt
    return m


OVERLAY_METRICS = {
    "vwma":     {"stream": vwma_stream,         "fields": {"vwma":   "v"}},
    "ema":      {"stream": ema_stream,           "fields": {"ema":    "v"}},
    "boll":     {"stream": bollinger_stream,     "fields": {"upper": "upper", "lower": "lower", "mid": "mid"}},
    "high":     {"stream": rolling_high_stream,  "fields": {"high":   "v"}},
    "low":      {"stream": rolling_low_stream,   "fields": {"low":    "v"}},
    "momentum": {"stream": momentum_stream,      "fields": {"pct":    "v"}},
    "volume":   {"stream": volume_stream,        "fields": {"volume": "v"}},
}
