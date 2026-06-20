def aggregate_to_seconds(trades):

    bars = {}

    for t in trades:

        sec_ms = (int(t["T"]) // 1000) * 1000
        bar = bars.get(sec_ms)
        if bar is None:
            bars[sec_ms] = {"open_time": sec_ms, "price": t["p"], "quantity": float(t["q"])}
        else:
            bar["price"] = t["p"]
            bar["quantity"] += float(t["q"])

    return [bars[k] for k in sorted(bars)]
