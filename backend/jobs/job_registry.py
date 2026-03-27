"""
backend/jobs/job_registry.py
In-memory job registry — tracks status of every agent run.
Thread-safe. Jobs auto-expire after 1 hour.
"""
import uuid, time, threading
from utils.logger import get_logger

log      = get_logger("backend.jobs")
_LOCK    = threading.Lock()
_JOBS:   dict = {}
JOB_TTL  = 3600   # 1 hour


class JobStatus:
    PENDING = "pending"
    RUNNING = "running"
    DONE    = "done"
    FAILED  = "failed"


def create() -> str:
    """Create a new job entry. Returns job_id."""
    job_id = str(uuid.uuid4())
    with _LOCK:
        _JOBS[job_id] = {
            "status":       JobStatus.PENDING,
            "error":        None,
            "started_at":   None,
            "finished_at":  None,
            "_finished_ts": None,
        }
    return job_id


def set_running(job_id: str):
    with _LOCK:
        if job_id in _JOBS:
            _JOBS[job_id]["status"]     = JobStatus.RUNNING
            _JOBS[job_id]["started_at"] = _now()


def set_done(job_id: str):
    with _LOCK:
        if job_id in _JOBS:
            _JOBS[job_id]["status"]       = JobStatus.DONE
            _JOBS[job_id]["finished_at"]  = _now()
            _JOBS[job_id]["_finished_ts"] = time.time()


def set_failed(job_id: str, error: str):
    with _LOCK:
        if job_id in _JOBS:
            _JOBS[job_id]["status"]       = JobStatus.FAILED
            _JOBS[job_id]["error"]        = error
            _JOBS[job_id]["finished_at"]  = _now()
            _JOBS[job_id]["_finished_ts"] = time.time()


def get(job_id: str) -> dict | None:
    return _JOBS.get(job_id)


def is_running(job_id: str) -> bool:
    job = _JOBS.get(job_id)
    return job is not None and job["status"] == JobStatus.RUNNING


def running_count() -> int:
    return sum(1 for j in _JOBS.values() if j["status"] == JobStatus.RUNNING)


def pending_count() -> int:
    return sum(1 for j in _JOBS.values() if j["status"] == JobStatus.PENDING)


def evict_old():
    """Remove finished jobs older than TTL."""
    now = time.time()
    with _LOCK:
        dead = [j for j, v in _JOBS.items()
                if v.get("_finished_ts") and now - v["_finished_ts"] > JOB_TTL]
        for j in dead:
            del _JOBS[j]


def _now() -> str:
    from datetime import datetime
    return datetime.now().isoformat()
