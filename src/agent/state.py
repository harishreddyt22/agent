"""
agent/state.py
Defines shared state flowing through every LangGraph node.
"""

from typing import TypedDict, Optional, Any
from datetime import datetime


class AgentState(TypedDict):
    # ── Input paths ──────────────────────────────────────────
    sow_path: str
    po_path:  str

    # ── Upload timestamps (set in app.py at upload time) ─────
    sow_upload_dt: Optional[Any]       # datetime
    po_upload_dt:  Optional[Any]       # datetime

    # ── Extracted DataFrames ─────────────────────────────────
    df_metadata:   Optional[Any]       # pd.DataFrame
    df_schedule1:  Optional[Any]       # pd.DataFrame
    df_schedule9:  Optional[Any]       # pd.DataFrame
    df_po:         Optional[Any]       # pd.DataFrame
    df_validation: Optional[Any]       # pd.DataFrame

    # ── Retry tracking ───────────────────────────────────────
    metadata_retries: int
    schedule_retries: int
    po_retries:       int
    max_retries:      int

    # ── Agent decisions & memory ─────────────────────────────
    has_schedule9:     bool
    extraction_notes:  list[str]
    validation_issues: list[str]
    audit_report:      str

    # ── Status flags ─────────────────────────────────────────
    metadata_ok:   bool
    schedule_ok:   bool
    po_ok:         bool
    validation_ok: bool
    error:         Optional[str]
