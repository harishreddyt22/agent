"""
extractors/validate_procurement.py
"""
import sys, os, re
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *  # fix broken torch DLL on Windows

import pandas as pd
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# ROBUST DATE PARSER — handles all formats from extractors
# ══════════════════════════════════════════════════════════════

_MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,
    "november":11,"december":12,
}

_SEP = r"\s*[\-./]\s*"

_DATE_PATTERNS = [
    # dd-Mon-yyyy (any separator + optional spaces)
    (r"(\d{1,2})" + _SEP + r"([A-Za-z]{3,9})" + _SEP + r"(\d{4})", "dmy_text"),
    # dd-mm-yyyy (numeric month)
    (r"(\d{1,2})" + _SEP + r"(\d{1,2})" + _SEP + r"(\d{4})", "dmy_num"),
    # yyyy-mm-dd
    (r"(\d{4})" + _SEP + r"(\d{1,2})" + _SEP + r"(\d{1,2})", "ymd"),
    # dd Month yyyy (space separated)
    (r"(\d{1,2})\s+([A-Za-z]{3,9}),?\s+(\d{4})", "dmy_space"),
    # Month dd, yyyy
    (r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", "mdy_space"),
]


def _parse_date(value) -> pd.Timestamp:
    """
    Parse any date string format → pd.Timestamp.
    Returns pd.NaT if unparseable.
    """
    if not isinstance(value, str):
        return pd.NaT
    v = value.strip()
    if not v or v in ("-", "—", "N/A", "null", "None", "Not Found", ""):
        return pd.NaT

    for pattern, fmt in _DATE_PATTERNS:
        m = re.search(pattern, v, re.IGNORECASE)
        if not m:
            continue
        try:
            g = m.groups()
            if fmt == "dmy_text":
                day, mon_str, year = g
                mo = _MONTH_MAP.get(mon_str.lower())
                if mo:
                    return pd.Timestamp(datetime(int(year), mo, int(day)))

            elif fmt == "dmy_num":
                day, mon_num, year = g
                d, mo = int(day), int(mon_num)
                if mo > 12 and d <= 12:
                    d, mo = mo, d
                if 1 <= mo <= 12:
                    return pd.Timestamp(datetime(int(year), mo, d))

            elif fmt == "ymd":
                year, mon_num, day = g
                mo = int(mon_num)
                if 1 <= mo <= 12:
                    return pd.Timestamp(datetime(int(year), mo, int(day)))

            elif fmt == "dmy_space":
                day, mon_str, year = g
                mo = _MONTH_MAP.get(mon_str.lower())
                if mo:
                    return pd.Timestamp(datetime(int(year), mo, int(day)))

            elif fmt == "mdy_space":
                mon_str, day, year = g
                mo = _MONTH_MAP.get(mon_str.lower())
                if mo:
                    return pd.Timestamp(datetime(int(year), mo, int(day)))

        except Exception:
            continue

    return pd.NaT


# ══════════════════════════════════════════════════════════════
# MAIN VALIDATION
# ══════════════════════════════════════════════════════════════

def run_validation(df1: pd.DataFrame, df9: pd.DataFrame, df_po: pd.DataFrame) -> pd.DataFrame:

    # Find common milestone indexes
    rows = df1.index.intersection(df9.index).intersection(df_po.index)

    positional_mode = False
    if len(rows) == 0 and not df1.empty and not df9.empty and not df_po.empty:
        # Fallback to positional matching when index labels do not align
        n = min(len(df1), len(df9), len(df_po))
        rows = list(range(n))
        positional_mode = True

    # Build result row by row — no joins, no column conflicts
    records = []
    for idx, ms in enumerate(rows):
        if positional_mode:
            r1   = df1.iloc[ms]
            r9   = df9.iloc[ms]
            r_po = df_po.iloc[ms]
            ms_label = f"Milestone {ms+1}"
        else:
            r1   = df1.loc[ms]
            r9   = df9.loc[ms]
            r_po = df_po.loc[ms]
            ms_label = ms
        # Get scalar values safely (handles both Series and scalar)
        def v(row, col):
            val = row[col] if col in row.index else None
            if isinstance(val, pd.Series):
                val = val.iloc[0]
            return val

        svc     = v(r1,   "Services_Deliverables")
        due_str = v(r1,   "Deliverable_Due_Date")
        rev_str = v(r1,   "Review_Completion_Date")
        sow_str = v(r9,   "sow_date")
        sow_amt = v(r9,   "sow_amt")
        po_str  = v(r_po, "po_delivery_date")
        po_amt  = v(r_po, "po_amt")

        # Parse all dates with the robust parser
        due    = _parse_date(due_str)
        rev    = _parse_date(rev_str)
        sow_ms = _parse_date(sow_str)
        po_del = _parse_date(po_str)

        # Determine validation status
        missing = []
        if pd.isna(due):    missing.append("Due Date")
        if pd.isna(rev):    missing.append("Review Date")
        if pd.isna(sow_ms): missing.append("SOW Date")
        if pd.isna(po_del): missing.append("PO Date")

        po_vs_sow = bool(po_del >= sow_ms) if not pd.isna(po_del) and not pd.isna(sow_ms) else False
        amount_matches = False

        if not pd.isna(sow_amt) and not pd.isna(po_amt):
            sa = re.sub(r"[₹,\s]", "", str(sow_amt)).strip()
            pa = re.sub(r"[₹,\s]", "", str(po_amt)).strip()
            amount_matches = (sa == pa)

        if missing:
            status = "Invalid"
        else:
            c1 = bool(rev > due)           # Review > Due Date
            c2 = bool(sow_ms >= rev)       # SOW Milestone >= Review
            c3 = po_vs_sow                 # PO Date >= SOW Milestone
            c4 = amount_matches            # PO/ST amount match

            if c1 and c2 and c3 and c4:
                status = "Valid"
            else:
                errs = []
                if not c1: errs.append("Review <= Due Date")
                if not c2: errs.append("SOW < Review")
                if not c3: errs.append("PO < SOW")
                if not c4: errs.append("Amount Mismatch")
                status = f"Invalid ({', '.join(errs)})"

        amount_valid = "Valid" if amount_matches else "Invalid"

        records.append({
            "Milestone":                 ms_label,
            "Services_Deliverables":     svc,
            "Deliverable_Due_Date":      due_str,
            "Review_Completion_Date":    rev_str,
            "SOW_Milestone_Date":        sow_str,
            "po_delivery_date":          po_str,
            "SOW_Amount":                sow_amt,
            "PO_Amount":                 po_amt,
            "PO ≥ SOW":                  "Yes" if po_vs_sow else "No",
            "Amount_Valid":              amount_valid,
            "Conditions_Passed":         (int(c1) + int(c2) + int(c3) + int(c4)) if not missing else 0,
            "Validation_Status":         status,
            "All_Conditions_Passed":     status,
        })

    return pd.DataFrame(records, index=rows)


def get_display_columns() -> list:
    return [
        "Milestone",
        "Services_Deliverables",
        "Deliverable_Due_Date",
        "Review_Completion_Date",
        "SOW_Milestone_Date",
        "po_delivery_date",
        "SOW_Amount",
        "PO_Amount",
        "PO ≥ SOW",
        "Amount_Valid",
        "Conditions_Passed",
        "Validation_Status",
    ]
