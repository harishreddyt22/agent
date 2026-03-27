"""
utils/db.py
SQLite database layer — zero setup, no server required.
Same interface as the old PostgreSQL db.py.

Tables:
  uploads    — filename, document_type (SOW/PO), file_format (Word/PDF), uploaded_at
  agent_runs — run results + validation_result text
"""
import sys, os, sqlite3
from datetime import datetime

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.logger import get_logger
log = get_logger("utils.db")

# ── DB path from config (fallback to data/procurement.db) ─────
try:
    from config import SETTINGS
    _DB_PATH = os.path.join(_ROOT, SETTINGS["database"]["path"])
except Exception:
    _DB_PATH = os.path.join(_ROOT, "data", "procurement.db")

os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # safe concurrent access
    return conn


# ══════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════

def init_db():
    """Create tables if they do not exist."""
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            filename      TEXT    NOT NULL,
            document_type TEXT    NOT NULL,
            file_format   TEXT    NOT NULL,
            uploaded_at   TEXT    NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS agent_runs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            sow_upload_id     INTEGER REFERENCES uploads(id),
            po_upload_id      INTEGER REFERENCES uploads(id),
            ran_at            TEXT    NOT NULL,
            company_name      TEXT,
            validation_ok     INTEGER,
            issues_count      INTEGER,
            validation_result TEXT,
            audit_report      TEXT,
            error             TEXT
        );
    """)

    conn.commit()
    conn.close()
    log.info(f"SQLite DB ready → {_DB_PATH}")


# ══════════════════════════════════════════════════════════════
# UPLOADS
# ══════════════════════════════════════════════════════════════

def _detect_format(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    if ext in (".docx", ".doc"): return "Word"
    if ext == ".pdf":            return "PDF"
    return "Word"


def save_upload(filename: str, document_type: str) -> int:
    file_format = _detect_format(filename)
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO uploads (filename, document_type, file_format, uploaded_at) "
        "VALUES (?, ?, ?, ?)",
        (filename, document_type.upper(), file_format,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    log.info(f"Upload saved: {document_type} '{filename}' ({file_format}) → id={new_id}")
    return new_id


def list_uploads(document_type: str = None) -> list:
    conn = get_conn()
    cur  = conn.cursor()
    if document_type:
        cur.execute(
            "SELECT id, filename, document_type, file_format, uploaded_at "
            "FROM uploads WHERE document_type=? ORDER BY uploaded_at DESC",
            (document_type.upper(),)
        )
    else:
        cur.execute(
            "SELECT id, filename, document_type, file_format, uploaded_at "
            "FROM uploads ORDER BY uploaded_at DESC"
        )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ══════════════════════════════════════════════════════════════
# AGENT RUNS
# ══════════════════════════════════════════════════════════════

def _df_to_text(df) -> str:
    if df is None or df.empty: return "No data"
    try:    return df.to_string(index=True)
    except: return "Could not serialize"


def save_run(sow_upload_id: int, po_upload_id: int, state: dict) -> int:
    df_meta      = state.get("df_metadata")
    company_name = None
    if df_meta is not None and not df_meta.empty:
        company_name = str(df_meta.iloc[0].get("company_name", ""))

    issues            = state.get("validation_issues", [])
    validation_ok     = 1 if state.get("validation_ok") else 0
    audit_report      = state.get("audit_report", "")
    error             = state.get("error")
    validation_result = _df_to_text(state.get("df_validation"))

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO agent_runs "
        "(sow_upload_id, po_upload_id, ran_at, company_name, "
        " validation_ok, issues_count, validation_result, audit_report, error) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (sow_upload_id, po_upload_id,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
         company_name, validation_ok, len(issues),
         validation_result, audit_report,
         str(error) if error else None)
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    log.info(f"Run saved → agent_runs.id={run_id}")
    return run_id


def list_runs(limit: int = 20) -> list:
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            r.id, r.ran_at, r.company_name,
            r.validation_ok, r.issues_count,
            r.validation_result, r.audit_report, r.error,
            s.filename    AS sow_filename,
            s.file_format AS sow_format,
            s.id          AS sow_upload_id,
            p.filename    AS po_filename,
            p.file_format AS po_format,
            p.id          AS po_upload_id
        FROM  agent_runs r
        LEFT JOIN uploads s ON s.id = r.sow_upload_id
        LEFT JOIN uploads p ON p.id = r.po_upload_id
        ORDER BY r.ran_at DESC LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows
