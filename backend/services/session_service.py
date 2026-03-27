"""
backend/services/session_service.py
Per-user session management — UUID cookie keyed, thread-safe.
Supports 20 concurrent users with automatic TTL eviction.
"""
import time, uuid, threading
from fastapi import Request
from utils.logger import get_logger

log = get_logger("backend.session")

_LOCK        = threading.Lock()
_SESSIONS:   dict = {}
SESSION_TTL  = 7200    # 2 hours
MAX_SESSIONS = 200


def _new_session() -> dict:
    return {
        "state":       None,
        "sow_name":    None,
        "po_name":     None,
        "job_id":      None,
        "run_count":   0,
        "last_access": time.time(),
    }


def get_or_create(request: Request) -> tuple:
    """Return (session_id, session_dict). Thread-safe."""
    sid = request.cookies.get("session_id")
    with _LOCK:
        if sid and sid in _SESSIONS:
            _SESSIONS[sid]["last_access"] = time.time()
            return sid, _SESSIONS[sid]
        sid = str(uuid.uuid4())
        _SESSIONS[sid] = _new_session()
        log.info(f"Session created: {sid[:8]} (active={len(_SESSIONS)})")
        return sid, _SESSIONS[sid]


def update(sid: str, **kwargs):
    """Update session fields by key."""
    with _LOCK:
        if sid in _SESSIONS:
            _SESSIONS[sid].update(kwargs)
            _SESSIONS[sid]["last_access"] = time.time()


def set_cookie(response, sid: str):
    response.set_cookie(
        "session_id", sid,
        httponly=True, samesite="lax", max_age=SESSION_TTL
    )


def evict_expired():
    """Remove sessions older than TTL. Call periodically."""
    now = time.time()
    with _LOCK:
        expired = [s for s, v in _SESSIONS.items()
                   if now - v.get("last_access", 0) > SESSION_TTL]
        for s in expired:
            del _SESSIONS[s]
        if expired:
            log.info(f"Evicted {len(expired)} sessions (active={len(_SESSIONS)})")


def active_count() -> int:
    return len(_SESSIONS)
