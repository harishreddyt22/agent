"""
backend/routes/api.py
JSON API routes — job polling, health check, session state, GPU check.
"""
import asyncio
import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from fastapi import Request
from datetime import datetime

from utils.logger  import get_logger
from backend.jobs  import job_registry
from backend.services import session_service, cache_service

log    = get_logger("backend.routes.api")
router = APIRouter(prefix="/api")


def _worker_capacity() -> int:
    cpu_count = os.cpu_count() or 1
    return int(os.getenv("MAX_WORKERS", str(max(1, min(2, cpu_count)))))


@router.get("/job/{job_id}")
async def job_status(job_id: str):
    """
    Poll this to check job progress.
    Frontend polls every 2s until status == 'done' or 'failed'.
    """
    job = job_registry.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse({
        "job_id":      job_id,
        "status":      job["status"],
        "error":       job.get("error"),
        "started_at":  job.get("started_at"),
        "finished_at": job.get("finished_at"),
    })


@router.get("/health")
async def health():
    """
    Load balancer / cloud health check.
    Returns active sessions, running jobs, pool capacity.
    """
    return JSONResponse({
        "status":          "ok",
        "active_sessions": session_service.active_count(),
        "running_jobs":    job_registry.running_count(),
        "queued_jobs":     job_registry.pending_count(),
        "worker_capacity": _worker_capacity(),
        "cache_entries":   cache_service.size(),
        "timestamp":       datetime.now().isoformat(),
    })


@router.get("/gpu-check")
async def api_gpu_check():
    """GPU health — returns JSON."""
    from utils.gpu_client import check_gpu_health
    ok = await asyncio.get_event_loop().run_in_executor(None, check_gpu_health)
    return JSONResponse({"gpu_online": ok})


@router.get("/session/state")
async def session_state(request: Request):
    """
    Returns current session's agent state summary as JSON.
    Useful for REST clients or future React frontend.
    """
    _, session = session_service.get_or_create(request)
    state = session.get("state")
    if not state:
        return JSONResponse({"ready": False})
    return JSONResponse({
        "ready":         True,
        "validation_ok": bool(state.get("validation_ok")),
        "issues_count":  len(state.get("validation_issues", [])),
        "has_schedule9": bool(state.get("has_schedule9")),
        "sow_name":      session.get("sow_name"),
        "po_name":       session.get("po_name"),
        "run_count":     session.get("run_count", 0),
    })
