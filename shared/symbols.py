import os

def load_symbols(instrument: str) -> list[str]:
    path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{instrument}.txt"))
    if not os.path.exists(path):
        raise FileNotFoundError(f"symbols file not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().upper() for line in f if line.strip()]
