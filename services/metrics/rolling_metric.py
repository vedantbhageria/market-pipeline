import time
import logging
import multiprocessing as mp
from collections import deque, defaultdict
from shared.redis_pool import ConnectionManager
from shared.logging_setup import setup_logging
from shared.redis_streams import parse_ts_ms, last_entry_per_symbol
from shared.constants import STREAM_MAXLEN


class RollingMetricWorker:

    def __init__(self, source_stream_fn, output_stream_fn, window_ms, compute_fn,
                 value_field="value", num_workers=4, name="worker",
                 xread_block_ms=5000, xread_count=1000, output_maxlen=STREAM_MAXLEN,
                 pipeline_flush=4000):

        self.source_stream_fn = source_stream_fn
        self.output_stream_fn = output_stream_fn
        self.window_ms        = window_ms
        self.compute_fn       = compute_fn
        self.value_field      = value_field
        self.num_workers      = num_workers
        self.name             = name
        self.xread_block_ms   = xread_block_ms
        self.xread_count      = xread_count
        self.output_maxlen    = output_maxlen
        self.pipeline_flush   = pipeline_flush

    def _connect(self, worker_id):
        cm = ConnectionManager(f"metrics-{self.name}")
        return cm.get_sync(f"metrics-{self.name}-{worker_id}", socket_timeout=30)

    def resume_cursors(self, r, symbols, worker_id):
        log = logging.getLogger(f"{self.name}-{worker_id}")
        latest_map = last_entry_per_symbol(r, symbols, self.output_stream_fn)

        cursors = {}
        fresh, resumed = 0, 0
        for sym, latest in latest_map.items():
            stream = self.source_stream_fn(sym)
            if isinstance(latest, Exception) or not latest:
                cursors[stream] = "0"
                fresh += 1
                continue
            last_ts = parse_ts_ms(latest[0][0])
            cursors[stream] = f"{max(last_ts - self.window_ms, 0)}-0"
            resumed += 1

        log.info("%d resumed, %d starting fresh", resumed, fresh)
        return cursors

    def _worker(self, worker_id, symbols):
        setup_logging()
        log = logging.getLogger(f"{self.name}-{worker_id}")

        r          = self._connect(worker_id)
        windows    = defaultdict(deque)
        prev_results = {}
        qty_sums   = defaultdict(float)

        log.info("tracking %d symbols", len(symbols))
        cursors = self.resume_cursors(r, symbols, worker_id)

        while True:
            try:
                response = r.xread(cursors, count=self.xread_count, block=self.xread_block_ms)
                if not response:
                    continue

                pipe    = r.pipeline()
                pending = 0
                for stream_name, entries in response:
                    sym    = stream_name.split(":", 1)[1]
                    window = windows[sym]

                    for entry_id, fields in entries:
                        cursors[stream_name] = entry_id
                        ts_ms  = parse_ts_ms(entry_id)
                        price  = float(fields["price"])
                        qty    = float(fields.get("quantity", 0) or 0)
                        appended = (ts_ms, price, qty)

                        qty_sum = qty_sums[sym]
                        cutoff  = ts_ms - self.window_ms
                        popped  = []
                        while window and window[0][0] < cutoff:
                            old = window.popleft()
                            popped.append(old)
                            qty_sum -= old[2]

                        result = self.compute_fn(window, popped, appended, prev_results.get(sym), qty_sum)
                        prev_results[sym]  = result
                        window.append(appended)
                        qty_sums[sym] = qty_sum + qty

                        out_fields = result if isinstance(result, dict) else {self.value_field: result}
                        out_fields["window_ticks"] = len(window)

                        pipe.xadd(self.output_stream_fn(sym), out_fields, id=entry_id, maxlen=self.output_maxlen)
                        pending += 1
                        if pending >= self.pipeline_flush:
                            pipe.execute(raise_on_error=False)
                            pipe    = r.pipeline()
                            pending = 0

                if pending:
                    pipe.execute(raise_on_error=False)

            except Exception as e:
                log.error("loop error: %s", e)
                time.sleep(1)

    def start(self, symbols):
        chunks    = [symbols[i::self.num_workers] for i in range(self.num_workers)]
        processes = []
        for worker_id, chunk in enumerate(chunks):
            if not chunk:
                continue
            p = mp.Process(target=self._worker, args=(worker_id, chunk))
            p.start()
            processes.append(p)
        return processes

    def run(self, symbols):
        log = logging.getLogger(self.name)
        processes = self.start(symbols)
        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            log.info("shutting down")
            for p in processes:
                p.terminate()
            for p in processes:
                p.join()
