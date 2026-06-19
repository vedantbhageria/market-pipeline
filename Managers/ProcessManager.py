import subprocess
import threading
import sys
from Services.registry import SERVICES

class ProcessManager:

    def __init__(self):
        self._processes: dict[str, subprocess.Popen] = {}
        self._lock = threading.Lock()

    def start(self, name: str):
        with self._lock:
            if name in self._processes and self._processes[name].poll() is None:
                print(f"[PM] {name} already running")
                return
            flags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0
            proc = subprocess.Popen(SERVICES[name], creationflags=flags)
            self._processes[name] = proc
            print(f"[PM] started {name} (pid {proc.pid})")

    def stop(self, name: str):
        with self._lock:
            proc = self._processes.pop(name, None)
            if proc:
                if sys.platform == "win32":
                    subprocess.call(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    proc.terminate()
                proc.wait()
                print(f"[PM] stopped {name}")

    def status(self) -> dict[str, str]:
        with self._lock:
            return {
                name: "running" if proc.poll() is None else "dead"
                for name, proc in self._processes.items()
            }

    def start_all(self):
        for name in SERVICES:
            self.start(name)

    def stop_all(self):
        for name in list(self._processes):
            self.stop(name)