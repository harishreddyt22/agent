"""
backend/routes/pages.py
HTML page routes — index, results, history, gpu-check.
"""
import asyncio
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os

from utils.db      import save_upload, list_runs, list_uploads
from utils.logger  import get_logger
from backend.jobs  import job_registry
from backend.services import session_service, cache_service, render_service
from backend.jobs.worker import submit, write_tmp

log       = get_logger("backend.routes.pages")
router    = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "..", "templates")
)


def _render(request, session, error=None, success_msg=None):
    state  = session.get("state")
    tables = render_service.build_tables(state) if state else {}
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request":     request,
            "state":       state,
            "tables":      tables,
            "error":       error,
            "success_msg": success_msg,
            "sow_name":    session.get("sow_name"),
            "po_name":     session.get("po_name"),
        },
    )


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    sid, session = session_service.get_or_create(request)
    resp = _render(request, session)
    session_service.set_cookie(resp, sid)
    return resp


@router.post("/run", response_class=HTMLResponse)
async def run(
    request:  Request,
    sow_file: UploadFile = File(...),
    po_file:  UploadFile = File(...),
):
    session_service.evict_expired()
    job_registry.evict_old()
    sid, session = session_service.get_or_create(request)

    # Block double-submit
    existing = session.get("job_id")
    if existing and job_registry.is_running(existing):
        resp = _render(request, session,
                       error="⏳ Your job is still running. Please wait.")
        session_service.set_cookie(resp, sid)
        return resp

    # Read uploads concurrently
    sow_bytes, po_bytes = await asyncio.gather(
        sow_file.read(), po_file.read()
    )
    if not sow_bytes or not po_bytes:
        resp = _render(request, session, error="❌ Empty file uploaded.")
        session_service.set_cookie(resp, sid)
        return resp

    # Cache check — instant result
    key    = cache_service.make_key(sow_bytes, po_bytes)
    cached = cache_service.get(key)
    if cached:
        session_service.update(sid,
            state=cached, sow_name=sow_file.filename, po_name=po_file.filename)
        # Cache hit — update session then return results page
        session_service.update(sid,
            state=cached, sow_name=sow_file.filename, po_name=po_file.filename)
        resp = _render(request, session_service._SESSIONS.get(sid, {}),
                       success_msg="⚡ Instant result from cache!")
        session_service.set_cookie(resp, sid)
        return resp

    # Write temp files
    loop    = asyncio.get_event_loop()
    sow_tmp = await loop.run_in_executor(None, write_tmp, sow_bytes)
    po_tmp  = await loop.run_in_executor(None, write_tmp, po_bytes)

    # Save upload records
    sow_uid = po_uid = None
    try:
        sow_uid = save_upload(sow_file.filename, "SOW")
        po_uid  = save_upload(po_file.filename,  "PO")
    except Exception as e:
        log.warning(f"DB upload save: {e}")

    # Create job + fire async task
    job_id = job_registry.create()
    session_service.update(sid,
        job_id=job_id,
        sow_name=sow_file.filename,
        po_name=po_file.filename)

    asyncio.create_task(submit(
        job_id, sid,
        sow_tmp, po_tmp,
        sow_file.filename, po_file.filename,
        sow_bytes, po_bytes,
        sow_uid, po_uid, key,
    ))
    log.info(f"[Session {sid[:8]}] Job {job_id[:8]} queued "
             f"(running={job_registry.running_count()})")

    resp = templates.TemplateResponse(
        request,
        "waiting.html",
        {
            "request":  request,
            "job_id":   job_id,
            "sow_name": sow_file.filename,
            "po_name":  po_file.filename,
        },
    )
    session_service.set_cookie(resp, sid)
    return resp


@router.get("/results", response_class=HTMLResponse)
async def results(request: Request):
    sid, session = session_service.get_or_create(request)
    resp = _render(request, session,
        success_msg="✅ Agent completed!" if session.get("state") else None)
    session_service.set_cookie(resp, sid)
    return resp


@router.get("/history", response_class=HTMLResponse)
async def history(request: Request):
    sid, session = session_service.get_or_create(request)
    runs = uploads = []
    try: runs    = list_runs(limit=20)
    except Exception as e: log.warning(f"list_runs: {e}")
    try: uploads = list_uploads()
    except Exception as e: log.warning(f"list_uploads: {e}")
    resp = templates.TemplateResponse(
        request,
        "history.html",
        {
            "request": request,
            "runs": runs,
            "uploads": uploads,
        },
    )
    session_service.set_cookie(resp, sid)
    return resp


@router.get("/gpu-check", response_class=HTMLResponse)
async def gpu_check(request: Request):
    sid, session = session_service.get_or_create(request)
    from utils.gpu_client import check_gpu_health
    ok  = await asyncio.get_event_loop().run_in_executor(None, check_gpu_health)
    msg = "✅ GPU server online." if ok else "❌ GPU unreachable."
    resp = _render(request, session,
                   success_msg=msg if ok else None,
                   error=None if ok else msg)
    session_service.set_cookie(resp, sid)
    return resp
