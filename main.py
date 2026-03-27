"""
main.py — Application entry point
===================================
Assembles the FastAPI app from clean backend modules.
Run:  python main.py
      uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
"""

import sys, os

_HERE = os.path.abspath(os.path.dirname(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# ── Load config + logging first ───────────────────────────────
import config

import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

from utils.db    import init_db
from utils.logger import get_logger

from backend.middleware.cors     import add_cors
from backend.middleware.security import add_security_headers
from backend.routes.pages        import router as pages_router
from backend.routes.api          import router as api_router
from backend.routes.downloads    import router as downloads_router
from backend.jobs.worker         import get_pool, shutdown_pool

log = get_logger("main")


def _worker_capacity() -> int:
    cpu_count = os.cpu_count() or 1
    return int(os.getenv("MAX_WORKERS", str(max(1, min(2, cpu_count)))))


# ══════════════════════════════════════════════════════════════
# LIFESPAN — startup + shutdown (replaces deprecated on_event)
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────
    try:
        init_db()
        log.info("SQLite DB ready")
    except Exception as e:
        log.warning(f"DB init failed: {e}")
    get_pool()   # warm up process pool
    log.info(f"🚀 Procurement Agent started — worker capacity {_worker_capacity()}")

    yield   # app runs here

    # ── Shutdown ──────────────────────────────────────────────
    log.info("Shutting down process pool…")
    shutdown_pool()
    log.info("Shutdown complete")


# ══════════════════════════════════════════════════════════════
# BUILD APP
# ══════════════════════════════════════════════════════════════

app = FastAPI(
    title       = "SOW & PO Procurement Agent",
    description = "Andor Tech — LangGraph procurement validation",
    version     = "3.0.0",
    docs_url    = "/docs",
    redoc_url   = "/redoc",
    lifespan    = lifespan,           # modern lifespan handler
)

# ── Middleware ────────────────────────────────────────────────
add_cors(app)
add_security_headers(app)

# ── Routers ───────────────────────────────────────────────────
app.include_router(pages_router)
app.include_router(api_router)
app.include_router(downloads_router)


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    log.info(f"Starting on port {port}")
    uvicorn.run(
        "main:app",
        host      = "0.0.0.0",
        port      = port,
        workers   = 1,
        reload    = False,
        access_log= True,
    )
