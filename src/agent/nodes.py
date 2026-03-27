"""
agent/nodes.py
"""

import sys, os

# Path fix — ensures extractors/ and utils/ are always findable
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *  # fix broken torch DLL on Windows

# ── Torch DLL workaround for Windows ─────────────────────────
# Torch is not needed locally (inference runs on Colab GPU).
# Pre-emptively mock it so broken torch installs don't crash the app.
import types
if "torch" not in sys.modules:
    try:
        import torch  # try loading normally first
    except Exception:
        # Create a minimal mock so any "import torch" downstream won't crash
        _torch_mock = types.ModuleType("torch")
        _torch_mock.cuda = types.SimpleNamespace(
            is_available=lambda: False,
            empty_cache=lambda: None,
            memory_allocated=lambda: 0
        )
        _torch_mock.device = lambda x: x
        _torch_mock.no_grad = lambda: __import__('contextlib').nullcontext()
        sys.modules["torch"] = _torch_mock
        sys.modules["torch.cuda"] = _torch_mock.cuda
        print("⚠️  torch not available locally — using mock (inference runs on Colab GPU)")

import traceback
import pandas as pd
from src.agent.state import AgentState
from utils.gpu_client import call_gpu, check_gpu_health as _check_health
from src.extractors.extract_metadata import extract_metadata
from src.extractors.extract_sow_schedules import extract_sow_schedules
from src.extractors.extract_po import extract_po
from src.extractors.validate_procurement import run_validation
from src.prompts.audit_prompt import build_audit_prompt


# ──────────────────────────────────────────────────────────────
# NODE 1 — Health check
# ──────────────────────────────────────────────────────────────
def node_check_gpu_health(state: AgentState) -> dict:
    print("\n🔍 [Agent] Checking GPU server health...")
    # Clear doc cache so each run parses fresh
    try:
        from utils.doc_cache import clear as _clear_doc_cache
        _clear_doc_cache()
    except Exception:
        pass
    alive = _check_health()
    if not alive:
        return {"error": "❌ Colab GPU server is unreachable. Check your .env COLAB_GPU_URL and ensure Cell 4 is running in Colab."}
    print("✅ [Agent] GPU server is live.")
    return {"error": None}


# ──────────────────────────────────────────────────────────────
# NODE 2 — Extract metadata
# ──────────────────────────────────────────────────────────────
def node_extract_metadata(state: AgentState) -> dict:
    print("\n📑 [Agent] Extracting company metadata...")
    try:
        df = extract_metadata(
            state["sow_path"],
            sow_upload_dt=state.get("sow_upload_dt"),
            po_upload_dt=state.get("po_upload_dt")
        )
        if df.empty:
            raise ValueError("Metadata DataFrame is empty")
        notes = state.get("extraction_notes", [])
        notes.append(f"Metadata extracted: company={df.iloc[0].get('company_name','?')}")
        return {"df_metadata": df, "metadata_ok": True, "extraction_notes": notes, "metadata_retries": 0}
    except Exception as e:
        retries = state.get("metadata_retries", 0)
        print(f"⚠️  [Agent] Metadata extraction failed (attempt {retries+1}): {e}")
        return {"metadata_ok": False, "metadata_retries": retries + 1,
                "error": str(e), "df_metadata": pd.DataFrame()}


# ──────────────────────────────────────────────────────────────
# NODE 3 — Extract SOW schedules
# ──────────────────────────────────────────────────────────────
def node_extract_schedules(state: AgentState) -> dict:
    print("\n📌 [Agent] Extracting Schedule 1 & Schedule 9...")
    try:
        df1, df9 = extract_sow_schedules(state["sow_path"])
        notes = state.get("extraction_notes", [])
        notes.append(f"Schedule 1: {len(df1)} rows | Schedule 9: {len(df9)} rows")
        return {
            "df_schedule1": df1,
            "df_schedule9": df9,
            "schedule_ok": True,
            "extraction_notes": notes,
            "schedule_retries": 0
        }
    except Exception as e:
        retries = state.get("schedule_retries", 0)
        print(f"⚠️  [Agent] Schedule extraction failed (attempt {retries+1}): {e}")
        return {
            "schedule_ok": False,
            "schedule_retries": retries + 1,
            "error": str(e),
            "df_schedule1": pd.DataFrame(),
            "df_schedule9": pd.DataFrame()
        }


# ──────────────────────────────────────────────────────────────
# NODE 4 — Agent decides if Schedule 9 is usable
# ──────────────────────────────────────────────────────────────
def node_decide_schedule9(state: AgentState) -> dict:
    print("\n🤔 [Agent] Deciding if Schedule 9 is usable...")
    df9 = state.get("df_schedule9")
    notes = state.get("extraction_notes", [])

    # ── DEBUG — print exactly what was extracted ──────────────
    if df9 is not None:
        print(f"   [DEBUG] df9 shape: {df9.shape}")
        print(f"   [DEBUG] df9 columns: {list(df9.columns)}")
        print(f"   [DEBUG] df9 empty: {df9.empty}")
        if not df9.empty:
            print(f"   [DEBUG] df9 head:\n{df9.head().to_string()}")
    else:
        print("   [DEBUG] df9 is None")
    # ─────────────────────────────────────────────────────────

    if df9 is None or df9.empty:
        notes.append("⚠️ Schedule 9 not found — validation will be limited to Schedule 1 vs PO only.")
        print("   → Schedule 9 missing, will proceed without it.")
        return {"has_schedule9": False, "extraction_notes": notes}

    # Check if ANY data at all exists in the rows (milestone text is enough)
    has_milestones = (
        df9["Deliverable_or_Milestone"].notna().values.any()
        if "Deliverable_or_Milestone" in df9.columns else False
    )
    has_dates   = df9["sow_date"].notna().values.any() if "sow_date" in df9.columns else False
    has_amounts = df9["sow_amt"].notna().values.any()  if "sow_amt" in df9.columns else False

    # Accept Schedule 9 if it has milestone text OR dates OR amounts
    if not (has_milestones or has_dates or has_amounts):
        notes.append("⚠️ Schedule 9 rows are completely empty — agent skipping.")
        print("   → Schedule 9 completely empty, skipping.")
        return {"has_schedule9": False, "extraction_notes": notes}

    if not has_dates:
        notes.append("⚠️ Schedule 9 has milestones but dates could not be parsed — showing table without date validation.")
    if not has_amounts:
        notes.append("⚠️ Schedule 9 has milestones but amounts are empty.")

    notes.append(f"✅ Schedule 9 usable: {len(df9)} rows extracted.")
    print(f"   → Schedule 9 confirmed usable ({len(df9)} rows).")
    return {"has_schedule9": True, "extraction_notes": notes}


# ──────────────────────────────────────────────────────────────
# NODE 5 — Extract PO
# ──────────────────────────────────────────────────────────────
def node_extract_po(state: AgentState) -> dict:
    print("\n🧾 [Agent] Extracting Purchase Order...")
    try:
        df9 = state.get("df_schedule9")
        df = extract_po(state["po_path"], df9=df9)
        if df.empty:
            raise ValueError("PO DataFrame is empty")
        notes = state.get("extraction_notes", [])
        notes.append(f"PO extracted: {len(df)} line items")
        return {"df_po": df, "po_ok": True, "extraction_notes": notes, "po_retries": 0}
    except Exception as e:
        retries = state.get("po_retries", 0)
        print(f"⚠️  [Agent] PO extraction failed (attempt {retries+1}): {e}")
        return {"po_ok": False, "po_retries": retries + 1,
                "error": str(e), "df_po": pd.DataFrame()}


# ──────────────────────────────────────────────────────────────
# NODE 6 — Run validation
# ──────────────────────────────────────────────────────────────
def node_run_validation(state: AgentState) -> dict:
    print("\n🔍 [Agent] Running procurement flow validation...")
    try:
        df1   = state["df_schedule1"]
        df9   = state["df_schedule9"]
        df_po = state["df_po"]

        df_val = run_validation(df1, df9, df_po)

        issues = []
        for idx, row in df_val.iterrows():
            status = str(row.get("All_Conditions_Passed", ""))
            if "❌" in status or "Invalid" in status:
                issues.append(f"{idx}: {status}")

        notes = state.get("extraction_notes", [])
        notes.append(f"Validation complete: {len(df_val)} milestones checked, {len(issues)} issues found.")

        return {
            "df_validation": df_val,
            "validation_ok": len(issues) == 0,
            "validation_issues": issues,
            "extraction_notes": notes
        }
    except Exception as e:
        print(f"❌ [Agent] Validation failed: {e}")
        return {"validation_ok": False, "error": str(e),
                "df_validation": pd.DataFrame(), "validation_issues": [str(e)]}


# ──────────────────────────────────────────────────────────────
# NODE 7 — Generate audit report
# ──────────────────────────────────────────────────────────────
def node_generate_audit_report(state: AgentState) -> dict:
    print("\n📝 [Agent] Generating audit report via GPU...")

    notes   = state.get("extraction_notes", [])
    issues  = state.get("validation_issues", [])
    df_val  = state.get("df_validation")
    df_meta = state.get("df_metadata")

    company = "Unknown"
    if df_meta is not None and not df_meta.empty:
        company = df_meta.iloc[0].get("company_name", "Unknown")

    # Only send pass/fail summary — not full table (saves GPU tokens)
    if df_val is not None and not df_val.empty:
        key_col = "All_Conditions_Passed" if "All_Conditions_Passed" in df_val.columns else "Conditions_Passed"
        val_summary = df_val[[key_col]].to_string()[:1500]
    else:
        val_summary = "No data"
    issues_text  = "\n".join(issues) if issues else "None — all milestones passed."
    notes_text   = "\n".join(notes)

    prompt = build_audit_prompt(company, notes_text, issues_text, val_summary)

    try:
        report = call_gpu(prompt, max_new_tokens=600)
        print("✅ [Agent] Audit report generated.")
    except Exception as e:
        report = f"Audit report generation failed: {e}\n\nRaw issues:\n{issues_text}"

    return {"audit_report": report}
