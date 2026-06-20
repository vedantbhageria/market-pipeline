import os
import json
import threading
import redis
import redis.asyncio as aredis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

_monitor_r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5, socket_timeout=15)

class ConnectionManager:

    def __init__(self, name):
        self._lock = threading.Lock()
        self._connections: dict[str, redis.Redis | aredis.Redis] = {}
        self.name = name
        self.r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
            socket_connect_timeout=5, socket_timeout=15,
        )
        self._connections[f"{name}-managerial"] = self.r
        self._publish({"manager": self.name, "connection": f"{name}-managerial", "type": "sync", "action": "add"})

    def _publish(self, payload: dict):
        try:
            self.r.publish("Connections", json.dumps(payload))
        except Exception:
            pass

    def get_sync(self, name: str, socket_timeout=15):
        with self._lock:
            if name not in self._connections:
                client = redis.Redis(
                    host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=5, socket_timeout=socket_timeout,
                )
                self._connections[name] = client
                print(f"[ConnectionManager] registered sync:{name}")
                self._publish({"manager": self.name, "connection": name, "type": "sync", "action": "add"})
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
                self._publish({"manager": self.name, "connection": name, "type": "async", "action": "add"})
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
                self._publish({"manager": self.name, "connection": name, "action": "del"})

    def close_all(self):
        with self._lock:
            for name, client in self._connections.items():
                client.close()
                print(f"[ConnectionManager] closed {name}")
            self._connections.clear()
            self._publish({"manager": self.name, "action": "delall"})
    

def ManageAllConnections():
    import json as _json
    connections: dict[str, list] = {"Monitor": [("RedisConnectionMonitor", "sync")]}
    sub = _monitor_r.pubsub()
    sub.subscribe("Connections")
    for raw in sub.listen():
        if raw["type"] != "message":
            continue
        try:
            msg = _json.loads(raw["data"])
        except Exception:
            continue
        manager = msg.get("manager", "unknown")
        action  = msg.get("action")
        if action == "add":
            connections.setdefault(manager, [])
            connections[manager].append((msg.get("connection"), msg.get("type")))
        elif action == "del":
            entry = (msg.get("connection"), msg.get("type"))
            if manager in connections:
                connections[manager] = [c for c in connections[manager] if c != entry]
        elif action == "delall":
            connections.pop(manager, None)
        _monitor_r.hset("Connections", mapping={k: _json.dumps(v) for k, v in connections.items()})
    


