import asyncio
from collections import defaultdict
from fastapi import WebSocket


class SubscriptionManager:

    def __init__(self, listener_fn):
        self._listener_fn       = listener_fn
        self.all_clients        = set()
        self.symbol_to_clients  = defaultdict(set)
        self.client_to_symbols  = defaultdict(set)
        self.symbol_tasks       = {}
        self.sym_windows        = {}
        self.lock               = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        async with self.lock:
            self.all_clients.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            self.all_clients.discard(ws)
            symbols = self.client_to_symbols.pop(ws, set())
            for sym in symbols:
                self.symbol_to_clients[sym].discard(ws)
                if not self.symbol_to_clients[sym]:
                    self.symbol_to_clients.pop(sym, None)
                    self._stop_listener(sym)

    async def subscribe(self, ws: WebSocket, symbol: str, max_symbols: int) -> bool:
        async with self.lock:
            current = self.client_to_symbols[ws]
            if symbol not in current and len(current) >= max_symbols:
                return False
            is_new = symbol not in self.symbol_to_clients
            self.symbol_to_clients[symbol].add(ws)
            current.add(symbol)
            if is_new:
                self._start_listener(symbol)
            return True

    async def unsubscribe(self, ws: WebSocket, symbol: str):
        async with self.lock:
            self.symbol_to_clients[symbol].discard(ws)
            if not self.symbol_to_clients[symbol]:
                self.symbol_to_clients.pop(symbol, None)
                self._stop_listener(symbol)
            self.client_to_symbols[ws].discard(symbol)

    def _start_listener(self, symbol):
        if symbol not in self.symbol_tasks:
            self.symbol_tasks[symbol] = asyncio.create_task(self._listener_fn(symbol, self))

    def _stop_listener(self, symbol):
        task = self.symbol_tasks.pop(symbol, None)
        if task:
            task.cancel()

    def get_window(self, symbol, default):
        return self.sym_windows.get(symbol, default)

    def set_window(self, symbol, window):
        self.sym_windows[symbol] = window

    def active_symbols(self):
        return list(self.symbol_to_clients.keys())

    def clients_for(self, symbol):
        return list(self.symbol_to_clients.get(symbol, ()))

    def client_count(self):
        return len(self.all_clients)
