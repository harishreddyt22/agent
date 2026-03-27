"""
backend/services/render_service.py
HTML table rendering helpers — converts DataFrames to HTML tables.
"""
import pandas as pd
from utils.logger import get_logger

log = get_logger("backend.render")


def format_rupees(value) -> str:
    """Format amount with rupee symbol in Indian number format: ₹1,23,000"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        # Try to convert to float if it's a string
        if isinstance(value, str):
            # Remove any existing rupee symbols or commas
            clean = value.replace("₹", "").replace(",", "").strip()
            amount = float(clean)
        else:
            amount = float(value)
        
        # Format with Indian numbering: rightmost 3 digits, then pairs
        if amount >= 0:
            amount_str = str(int(amount))
            # Reverse to group from right
            if len(amount_str) <= 3:
                return f"₹{amount_str}"
            else:
                # Split: rightmost 3 digits + rest
                rest = amount_str[:-3]
                last_three = amount_str[-3:]
                # Group rest by pairs from right
                pairs = []
                for i in range(len(rest), 0, -2):
                    start = max(0, i - 2)
                    pairs.insert(0, rest[start:i])
                return f"₹{','.join(pairs)},{last_three}"
        else:
            return "—"
    except (ValueError, TypeError, AttributeError):
        return "—"


def cell_class(val: str) -> str:
    return ""
    return ""


def df_to_html(df) -> str:
    if df is None or (hasattr(df, "empty") and df.empty):
        return '<div class="empty-state">No data available.</div>'
    rows = ""
    for idx, row in df.iterrows():
        cells = f'<td class="idx">{idx}</td>'
        for col, val in row.items():
            # Format amounts with rupee symbol for Schedule 9 and PO
            if col in ["sow_amt", "po_amt", "Amount", "SOW_Amount", "PO_Amount"]:
                text = format_rupees(val)
            else:
                text = str(val) if val is not None and str(val) != "nan" else "—"
            cls   = cell_class(text)
            cells += f'<td class="{cls}">{text}</td>'
        rows += f"<tr>{cells}</tr>"
    heads = "".join(f"<th>{c}</th>" for c in [""] + list(df.columns))
    return (
        f'<div class="table-wrap"><table>'
        f'<thead><tr>{heads}</tr></thead>'
        f'<tbody>{rows}</tbody>'
        f'</table></div>'
    )


def build_tables(state: dict) -> dict:
    from src.extractors.validate_procurement import get_display_columns
    t = {}
    t["meta"]   = df_to_html(state.get("df_metadata"))
    t["sched1"] = df_to_html(state.get("df_schedule1"))
    t["sched9"] = df_to_html(state.get("df_schedule9"))
    t["po"]     = df_to_html(state.get("df_po"))
    df_val = state.get("df_validation")
    if df_val is not None and not df_val.empty:
        cols     = [c for c in get_display_columns() if c in df_val.columns]
        t["val"] = df_to_html(df_val[cols])
    else:
        t["val"] = '<div class="empty-state">No validation data.</div>'
    return t
