"""
backend/services/cache_service.py
SHA-256 file hash cache — same SOW+PO bytes = instant result, no GPU call.
TTL: 30 minutes per entry.
"""
import hashlib, time, threading
from utils.logger import get_logger

log      = get_logger("backend.cache")
_LOCK    = threading.Lock()
_CACHE:  dict = {}
CACHE_TTL = 1800   # 30 minutes


def make_key(sow_bytes: bytes, po_bytes: bytes) -> str:
    return hashlib.sha256(sow_bytes + po_bytes).hexdigest()


def get(key: str) -> dict | None:
    with _LOCK:
        entry = _CACHE.get(key)
        if entry and (time.time() - entry["cached_at"]) < CACHE_TTL:
            log.info(f"Cache HIT  → {key[:12]}…")
            return entry["state"]
        if entry:
            del _CACHE[key]
    return None


def set(key: str, state: dict):
    with _LOCK:
        _CACHE[key] = {"state": state, "cached_at": time.time()}
        log.info(f"Cache SET  → {key[:12]}… (entries={len(_CACHE)})")


def size() -> int:
    return len(_CACHE)


def clear():
    with _LOCK:
        _CACHE.clear()
    log.info("Cache cleared")
