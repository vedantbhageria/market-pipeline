import queue
import time
import threading
import numpy as np

class RequestQueueManager:

    def __init__(self, num_queues, budget_per_sec, budget_per_min):

        self.gate = threading.Condition()
        self.queues = [queue.Queue() for _ in range(num_queues)]

        self.capacity = budget_per_sec  
        self.refill_rate_per_sec = budget_per_min / 60.0
        self.tokens = self.capacity     

        self.paused_until = 0.0  
        self.running = True
        self.approver_thread = threading.Thread(target=self.approver, daemon=True)
        self.approver_thread.start()

    def approver(self):
        while self.running:
            time.sleep(1)

            with self.gate:
                if time.time() < self.paused_until:
                    continue  
                self.tokens = min(self.capacity, self.tokens + self.refill_rate_per_sec)
                self.gate.notify_all()

    def report_rate_limit(self, until_ms=None, default_pause_s=15):

        with self.gate:
            until_s = (until_ms / 1000.0) if until_ms else (time.time() + default_pause_s)
            if until_s > self.paused_until:
                self.paused_until = until_s
            self.tokens = 0
            self.gate.notify_all()

    def put(self, queue_id, items: list):
        if queue_id >= len(self.queues):
            raise ValueError("Queue ID does not exist")

        for item in items:
            self.queues[queue_id].put(item)

        print(f"[QUEUE {queue_id}]; Holding {self.queues[queue_id].qsize()}")

    def get(self, queue_id, weight=20):

        if queue_id >= len(self.queues):
            raise ValueError("Queue ID does not exist")

        with self.gate:
            while self.tokens < weight:
                self.gate.wait(timeout=1)  

            self.tokens -= weight

        item = self.queues[queue_id].get()

        return item
    
    def size(self, queue_id):
        return self.queues[queue_id].qsize()


    def stop(self):
        self.running = False
        with self.gate:
            self.gate.notify_all()
    
    def put_randomize(self, msg):
        id = np.random.randint(0, len(self.queues))
        try:
            self.put(id, msg)
        except:
            self.put_randomize(msg)