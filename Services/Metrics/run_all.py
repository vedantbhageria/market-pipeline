import threading
from shared.symbols import load_symbols

from Services.Metrics.MetricWorkers.sma       import worker as sma
from Services.Metrics.MetricWorkers.vwma      import worker as vwma
from Services.Metrics.MetricWorkers.ema       import worker as ema
from Services.Metrics.MetricWorkers.bollinger import worker as bollinger
from Services.Metrics.MetricWorkers.high      import worker as high
from Services.Metrics.MetricWorkers.low       import worker as low
from Services.Metrics.MetricWorkers.momentum  import worker as momentum
from Services.Metrics.MetricWorkers.volume    import worker as volume

WORKERS = [sma, vwma, ema, bollinger, high, low, momentum, volume]

if __name__ == "__main__":
    symbols = load_symbols("crypto_futures")
    threads = [threading.Thread(target=w.run, args=(symbols,), daemon=True) for w in WORKERS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
