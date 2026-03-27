"""
backend/jobs/worker.py
Async job runner — submits agent work to a ProcessPoolExecutor.
Each user's job runs in its own OS process (zero blocking between users).
"""
import os, sys, asyncio, tempfile, traceback
from concurrent.futures import ProcessPoolExecutor
from typing import Optional
from datetime import datetime

from utils.logger  import get_logger
from utils.db      import save_run
from backend.jobs  import job_registry
from backend.services import session_service, cache_service

log = get_logger("backend.worker")

_MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
_pool: Optional[ProcessPoolExecutor] = None


def get_pool() -> ProcessPoolExecutor:
    global _pool
    if _pool is None:
        import multiprocessing
        _pool = ProcessPoolExecutor(
            max_workers = _MAX_WORKERS,
            mp_context  = multiprocessing.get_context("spawn"),
        )
        log.info(f"Process pool ready — {_MAX_WORKERS} workers")
    return _pool


def shutdown_pool():
    global _pool
    if _pool:
        _pool.shutdown(wait=False, cancel_futures=True)
        _pool = None
        log.info("Process pool shut down")


# ── Top-level worker function (must be picklable) ─────────────

def _run_agent_process(sow_path: str, po_path: str) -> dict:
    """Runs in a child process. Returns AgentState dict."""
    import sys, os
    for candidate in [os.getcwd()]:
        if os.path.exists(os.path.join(candidate, "src")):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            break
    from src.agent.graph import run_agent
    return run_agent(
        sow_path, po_path,
        sow_upload_dt=datetime.now(),
        po_upload_dt=datetime.now(),
    )


# ── Async orchestrator ────────────────────────────────────────

async def submit(
    job_id:       str,
    session_id:   str,
    sow_path:     str,
    po_path:      str,
    sow_filename: str,
    po_filename:  str,
    sow_bytes:    bytes,
    po_bytes:     bytes,
    sow_upload_id: Optional[int],
    po_upload_id:  Optional[int],
    cache_key:    str,
):
    """
    Fire-and-forget async task.
    Runs agent in process pool → updates job + session when done.
    The FastAPI event loop is never blocked.
    """
    job_registry.set_running(job_id)
    log.info(f"[Job {job_id[:8]}] Started | session={session_id[:8]}")

    try:
        loop  = asyncio.get_event_loop()
        state = await loop.run_in_executor(
            get_pool(), _run_agent_process, sow_path, po_path
        )

        # Cache result
        cache_service.set(cache_key, state)

        # Persist to SQLite
        try:
            save_run(sow_upload_id, po_upload_id, state)
        except Exception as e:
            log.warning(f"[Job {job_id[:8]}] DB save failed: {e}")

        # Update session
        session_service.update(
            session_id,
            state    = state,
            sow_name = sow_filename,
            po_name  = po_filename,
            job_id   = None,
            run_count= session_service._SESSIONS.get(session_id, {}).get("run_count", 0) + 1,
        )

        job_registry.set_done(job_id)
        log.info(f"[Job {job_id[:8]}] ✅ Complete")

    except Exception as e:
        tb = traceback.format_exc()
        log.error(f"[Job {job_id[:8]}] ❌ Failed: {e}\n{tb}")
        job_registry.set_failed(job_id, str(e))
        session_service.update(session_id, job_id=None)

    finally:
        for p in [sow_path, po_path]:
            try: os.unlink(p)
            except: pass


def write_tmp(data: bytes) -> str:
    """Write bytes to a temp file and return its path."""
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".docx")
    f.write(data); f.flush(); f.close()
    return f.name
