"""
src/agent/graph.py
Parallel LangGraph agent — targets <60 seconds via concurrent extraction.

PARALLEL EXECUTION PLAN:
  Sequential (old): metadata→schedules→PO = ~80s GPU time
  Parallel   (new): metadata + schedules + PO run at the SAME TIME = ~25s

  Timeline:
    T=0   → GPU health check
    T=2   → [metadata] + [schedules] + [PO] all start simultaneously
    T=25  → all 3 done → decide_schedule9 → validate → audit_report
    T=45  → DONE  (target: <60s)
"""

import sys, os, time
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *

import concurrent.futures
from langgraph.graph import StateGraph, END
from src.agent.state import AgentState
from src.agent.nodes import (
    node_check_gpu_health,
    node_extract_metadata,
    node_extract_schedules,
    node_decide_schedule9,
    node_extract_po,
    node_run_validation,
    node_generate_audit_report,
)
from utils.logger import get_logger

log = get_logger("agent.graph")


# ══════════════════════════════════════════════════════════════
# PARALLEL EXTRACTION NODE
# Runs metadata + schedules + PO concurrently using threads.
# All 3 call the GPU server independently — GPU handles them
# as separate HTTP requests in parallel.
# ══════════════════════════════════════════════════════════════

def node_extract_all_parallel(state: AgentState) -> dict:
    """
    Runs metadata, SOW schedules, and PO extraction simultaneously.
    Uses ThreadPoolExecutor(3) — each thread makes its own HTTP call
    to the Colab GPU server. Total time = slowest of the 3 (not sum).
    """
    log.info("⚡ [Parallel] Starting metadata + schedules + PO simultaneously")
    t0 = time.time()

    results = {
        "metadata":  None,
        "schedules": None,
        "po":        None,
        "errors":    [],
    }

    def run_metadata():
        try:
            return ("metadata", node_extract_metadata(state))
        except Exception as e:
            return ("metadata_error", str(e))

    def run_schedules():
        try:
            return ("schedules", node_extract_schedules(state))
        except Exception as e:
            return ("schedules_error", str(e))

    def run_po():
        try:
            # PO doesn't need schedule9 at this stage — pass None
            po_state = dict(state)
            po_state["df_schedule9"] = None
            return ("po", node_extract_po(po_state))
        except Exception as e:
            return ("po_error", str(e))

    # Fire all 3 in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(run_metadata),
            pool.submit(run_schedules),
            pool.submit(run_po),
        ]
        completed = [f.result() for f in concurrent.futures.as_completed(futures)]

    # Merge all results into a single state update
    merged = {}
    notes  = list(state.get("extraction_notes", []))

    for key, result in completed:
        if key == "metadata":
            merged.update(result)
            log.info("✅ [Parallel] Metadata done")
        elif key == "schedules":
            merged.update(result)
            log.info("✅ [Parallel] Schedules done")
        elif key == "po":
            merged.update(result)
            log.info("✅ [Parallel] PO done")
        elif key.endswith("_error"):
            err_msg = f"⚠️ {key}: {result}"
            notes.append(err_msg)
            log.warning(f"[Parallel] {err_msg}")

    elapsed = round(time.time() - t0, 1)
    notes.append(f"⚡ Parallel extraction completed in {elapsed}s")
    log.info(f"⚡ [Parallel] All 3 extractions done in {elapsed}s")

    merged["extraction_notes"] = notes
    return merged


# ══════════════════════════════════════════════════════════════
# ROUTING
# ══════════════════════════════════════════════════════════════

def route_after_health(state: AgentState) -> str:
    if state.get("error"):
        return END
    return "extract_all_parallel"


def route_after_parallel(state: AgentState) -> str:
    """After parallel extraction, check if core data is available."""
    if not state.get("metadata_ok"):
        retries = state.get("metadata_retries", 0)
        max_r   = state.get("max_retries", 2)
        if retries < max_r:
            log.info(f"↻ Metadata retry {retries}/{max_r}")
            return "extract_all_parallel"
        log.warning("✗ Max retries reached — stopping")
        return END

    if not state.get("schedule_ok"):
        retries = state.get("schedule_retries", 0)
        max_r   = state.get("max_retries", 2)
        if retries < max_r:
            log.info(f"↻ Schedule retry {retries}/{max_r}")
            return "extract_all_parallel"
        log.warning("✗ Max schedule retries — stopping")
        return END

    if not state.get("po_ok"):
        retries = state.get("po_retries", 0)
        max_r   = state.get("max_retries", 2)
        if retries < max_r:
            log.info(f"↻ PO retry {retries}/{max_r}")
            return "extract_all_parallel"
        log.warning("✗ Max PO retries — stopping")
        return END

    return "decide_schedule9"


# ══════════════════════════════════════════════════════════════
# BUILD GRAPH
# ══════════════════════════════════════════════════════════════

def build_agent():
    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("check_gpu_health",      node_check_gpu_health)
    graph.add_node("extract_all_parallel",  node_extract_all_parallel)   # NEW
    graph.add_node("decide_schedule9",      node_decide_schedule9)
    graph.add_node("run_validation",        node_run_validation)
    graph.add_node("generate_audit_report", node_generate_audit_report)

    # Entry
    graph.set_entry_point("check_gpu_health")

    # Edges
    graph.add_conditional_edges("check_gpu_health",     route_after_health)
    graph.add_conditional_edges("extract_all_parallel", route_after_parallel)
    graph.add_edge("decide_schedule9",      "run_validation")
    graph.add_edge("run_validation",        "generate_audit_report")
    graph.add_edge("generate_audit_report", END)

    return graph.compile()


def run_agent(
    sow_path: str,
    po_path:  str,
    sow_upload_dt=None,
    po_upload_dt=None,
) -> AgentState:

    agent = build_agent()

    initial_state: AgentState = {
        "sow_path":          sow_path,
        "po_path":           po_path,
        "sow_upload_dt":     sow_upload_dt,
        "po_upload_dt":      po_upload_dt,
        "df_metadata":       None,
        "df_schedule1":      None,
        "df_schedule9":      None,
        "df_po":             None,
        "df_validation":     None,
        "metadata_retries":  0,
        "schedule_retries":  0,
        "po_retries":        0,
        "max_retries":       2,
        "has_schedule9":     False,
        "extraction_notes":  [],
        "validation_issues": [],
        "audit_report":      "",
        "metadata_ok":       False,
        "schedule_ok":       False,
        "po_ok":             False,
        "validation_ok":     False,
        "error":             None,
    }

    t0 = time.time()
    log.info("=" * 55)
    log.info("🤖 Procurement Agent — START (parallel mode)")
    log.info("=" * 55)

    final_state = agent.invoke(initial_state)

    elapsed = round(time.time() - t0, 1)
    log.info("=" * 55)
    log.info(f"🤖 Procurement Agent — DONE in {elapsed}s")
    log.info("=" * 55)

    return final_state
