import threading
from shared.symbols import load_symbols

from services.metrics.workers.sma       import worker as sma
from services.metrics.workers.vwma      import worker as vwma
from services.metrics.workers.ema       import worker as ema
from services.metrics.workers.bollinger import worker as bollinger
from services.metrics.workers.high      import worker as high
from services.metrics.workers.low       import worker as low
from services.metrics.workers.momentum  import worker as momentum
from services.metrics.workers.volume    import worker as volume

WORKERS = [sma, vwma, ema, bollinger, high, low, momentum, volume]

if __name__ == "__main__":
    symbols = load_symbols("crypto_futures")
    threads = [threading.Thread(target=w.run, args=(symbols,), daemon=True) for w in WORKERS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
