import os
import threading
import redis
import redis.asyncio as aredis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

class ConnectionManager:

    def __init__(self, name):
        self._lock = threading.Lock()
        self._connections: dict[str, redis.Redis | aredis.Redis] = {}
        self.r = redis.Redis(
                    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=5, socket_timeout= 15,
                )
        self.name = name

    def get_sync(self, name: str, socket_timeout=15):
        with self._lock:
            if name not in self._connections:
                client = redis.Redis(
                    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=5, socket_timeout=socket_timeout,
                )
                self._connections[name] = client
                print(f"[ConnectionManager] registered sync:{name}")
                self.r.hset(f"ConnectionManager:{self.name}", name, "sync")
            return self._connections[name]

    def get_async(self, name: str, socket_timeout=15):
        with self._lock:
            if name not in self._connections:
                client = aredis.Redis(
                    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=5, socket_timeout=socket_timeout,
                )
                self._connections[name] = client
                print(f"[ConnectionManager] registered async:{name}")
                self.r.hset(f"ConnectionManager:{self.name}", name, "async")
            return self._connections[name]

    def list(self):
        with self._lock:
            return list(self._connections.keys())

    def close(self, name: str):
        with self._lock:
            client = self._connections.pop(name, None)
            if client:
                client.close()
                print(f"[ConnectionManager] closed {name}")
                self.r.hdel(f"ConnectionManager:{self.name}", name)

    def close_all(self):
        with self._lock:
            for name, client in self._connections.items():
                client.close()
                print(f"[ConnectionManager] closed {name}")
            self._connections.clear()
            self.r.delete(f"ConnectionManager:{self.name}")

