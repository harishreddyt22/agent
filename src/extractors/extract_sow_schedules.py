"""
extractors/extract_sow_schedules.py
Extract Schedule 1 and Schedule 9 from SOW document.
"""
import sys, os
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *  # fix broken torch DLL on Windows

import concurrent.futures
import json, re
import pandas as pd
from utils.logger import get_logger
from utils.gpu_client import call_gpu


def get_doc_markdown_schedule1(file_path: str) -> str:
    import re as _re
    from utils.doc_cache import get_markdown
    md = get_markdown(file_path)

    sched1 = _re.search(r'schedule\s*1', md, _re.IGNORECASE)

    # If schedule 1 exists, extract from the first hit to the end (more robust for long files)
    if sched1:
        start = max(0, sched1.start() - 300)
        return md[start:]

    # Fallback to a general marker and longer window
    sched_match = _re.search(
        r'(schedule\s*1|services.*deliverable|milestone)',
        md, _re.IGNORECASE
    )
    if sched_match:
        start = max(0, sched_match.start() - 300)
        return md[start:]

    return md[-30000:] if len(md) > 30000 else md


def get_doc_markdown_schedule9(file_path: str) -> str:
    import re as _re
    from utils.doc_cache import get_markdown
    md = get_markdown(file_path)

    sched9 = _re.search(r'schedule\s*9', md, _re.IGNORECASE)

    # If schedule 9 exists, extract from the first hit to the end (more robust for long files)
    if sched9:
        start = max(0, sched9.start() - 300)
        return md[start:]

    # Fallback to a general marker and longer window
    sched_match = _re.search(
        r'(schedule\s*9|fixed\.fee)',
        md, _re.IGNORECASE
    )
    if sched_match:
        start = max(0, sched_match.start() - 300)
        return md[start:]

    return md[-30000:] if len(md) > 30000 else md


def build_prompt_schedule1(full_content: str) -> str:
    return f"""
EXTRACT Schedule 1 data from the markdown below.

MANDATORY RULES:
1. Scan EVERY cell in the table for data.
2. Extract ALL rows with deliverables/services.
3. EXCLUDE totals, sums, or summary rows.
4. Format dates as DD-MMM-YYYY (e.g., 15-Jan-2025).
5. Return ONLY valid JSON.

SOURCE MARKDOWN:
{full_content}

OUTPUT FORMAT:
{{
 "schedule_1_services_milestones": [
  {{
   "Services_Deliverables": "description",
   "Deliverable_Due_Date": "DD-MMM-YYYY",
   "Review_Completion_Date": "DD-MMM-YYYY"
  }}
 ]
}}
"""


def build_prompt_schedule9(full_content: str) -> str:
    return f"""
EXTRACT Schedule 9 data from the markdown below.

MANDATORY RULES:
1. Scan EVERY cell in the table for data.
2. Extract ALL rows with deliverables/milestones.
3. EXCLUDE totals, sums, or summary rows.
4. Format dates as DD-MMM-YYYY (e.g., 15-Jan-2025).
5. Return ONLY valid JSON.

SOURCE MARKDOWN:
{full_content}

OUTPUT FORMAT:
{{
 "schedule_9_fixed_fee_engagement": [
  {{
   "Deliverable_or_Milestone": "description",
   "Date": "DD-MMM-YYYY",
   "Amount": "amount"
  }}
 ]
}}
"""


def set_milestone_index(df):
    if not df.empty:
        df.index = [f"Milestone {i+1}" for i in range(len(df))]
    return df


# Accept multiple date formats and normalise to DD-MMM-YYYY
_MONTH_MAP = {
    # short
    "jan":"Jan","feb":"Feb","mar":"Mar","apr":"Apr","may":"May","jun":"Jun",
    "jul":"Jul","aug":"Aug","sep":"Sep","oct":"Oct","nov":"Nov","dec":"Dec",
    # full
    "january":"Jan","february":"Feb","march":"Mar","april":"Apr","june":"Jun",
    "july":"Jul","august":"Aug","september":"Sep","october":"Oct",
    "november":"Nov","december":"Dec",
    # numeric
    "1":"Jan","2":"Feb","3":"Mar","4":"Apr","5":"May","6":"Jun",
    "7":"Jul","8":"Aug","9":"Sep","10":"Oct","11":"Nov","12":"Dec",
    "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun",
    "07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec",
}

# SEP = separator: hyphen, dot, slash, or spaces around any of those
# e.g.  12-Jan-2025  |  12 - Jan - 2025  |  12.Jan.2025  |  12/Jan/2025
_SEP = r"\s*[\-./]\s*"

_DATE_PATTERNS = [
    # dd-Mon-yyyy  (any separator, with optional spaces)
    # e.g. 12-Jan-2025 | 12 - Jan - 2025 | 12.Jan.2025
    (r"(\d{1,2})" + _SEP + r"([A-Za-z]{3,9})" + _SEP + r"(\d{4})", "dmy_text"),

    # dd-mm-yyyy  (numeric month)
    # e.g. 12-01-2025 | 12.01.2025 | 12/01/2025
    (r"(\d{1,2})" + _SEP + r"(\d{1,2})" + _SEP + r"(\d{4})", "dmy_num"),

    # yyyy-mm-dd
    (r"(\d{4})" + _SEP + r"(\d{1,2})" + _SEP + r"(\d{1,2})", "ymd"),

    # dd Month yyyy  (space separated, full or short month name)
    # e.g. 12 January 2025 | 12 Jan 2025
    (r"(\d{1,2})\s+([A-Za-z]{3,9}),?\s+(\d{4})", "dmy_space"),

    # Month dd, yyyy  or  Month dd yyyy
    # e.g. January 12, 2025
    (r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", "mdy_space"),

    # ddth/st/rd Mon yyyy (ordinal dates)
    # e.g. 15th Jan 2025 | 1st Feb 2025
    (r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\s+(\d{4})", "ordinal_mon_year"),

    # Mon dd, yyyy with comma
    # e.g. Jan 15, 2025
    (r"([A-Za-z]{3,9})\s+(\d{1,2}),\s+(\d{4})", "mon_dd_comma_year"),

    # Already in DD-MMM-YYYY format
    (r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", "already_formatted"),
]


def validate_date(value):
    """
    Parse a date string in any common format and return DD-Mon-YYYY.
    Returns the original string if a year is found but format is unrecognised.
    Returns None if no date-like content found.
    """
    if not isinstance(value, str):
        return None
    v = value.strip()
    if not v or v in ("-", "—", "N/A", "null", "None", ""):
        return None

    for pattern, fmt in _DATE_PATTERNS:
        m = re.search(pattern, v, re.IGNORECASE)
        if not m:
            continue
        try:
            g = m.groups()
            if fmt == "dmy_text":
                day, mon_str, year = g
                mon = _MONTH_MAP.get(mon_str.lower(), mon_str[:3].capitalize())
                return f"{int(day):02d}-{mon}-{year}"

            elif fmt == "dmy_num":
                day, mon_num, year = g
                # Disambiguate: if mon_num > 12 it is actually the day — swap
                d, mo = int(day), int(mon_num)
                if mo > 12 and d <= 12:
                    d, mo = mo, d
                mon = _MONTH_MAP.get(str(mo), f"{mo:02d}")
                return f"{d:02d}-{mon}-{year}"

            elif fmt == "ymd":
                year, mon_num, day = g
                mon = _MONTH_MAP.get(str(int(mon_num)), f"{int(mon_num):02d}")
                return f"{int(day):02d}-{mon}-{year}"

            elif fmt == "dmy_space":
                day, mon_str, year = g
                mon = _MONTH_MAP.get(mon_str.lower(), mon_str[:3].capitalize())
                return f"{int(day):02d}-{mon}-{year}"

            elif fmt == "mdy_space":
                mon_str, day, year = g
                mon = _MONTH_MAP.get(mon_str.lower(), mon_str[:3].capitalize())
                return f"{int(day):02d}-{mon}-{year}"

            elif fmt == "ordinal_mon_year":
                day, mon_str, year = g
                mon = _MONTH_MAP.get(mon_str.lower(), mon_str[:3].capitalize())
                return f"{int(day):02d}-{mon}-{year}"

            elif fmt == "mon_dd_comma_year":
                mon_str, day, year = g
                mon = _MONTH_MAP.get(mon_str.lower(), mon_str[:3].capitalize())
                return f"{int(day):02d}-{mon}-{year}"

            elif fmt == "already_formatted":
                day, mon, year = g
                # Already in correct format, just validate
                try:
                    int(day), int(year)
                    return f"{int(day):02d}-{mon}-{year}"
                except:
                    continue

        except Exception:
            continue

    # Last resort — preserve the raw value if it contains a 4-digit year
    return v if re.search(r"\d{4}", v) else None


# ── Schedule 1 validation ─────────────────────────────────────
# Rule: Deliverable_Due_Date < Review_Completion_Date
def validate_schedule1(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    t_due = pd.to_datetime(df["Deliverable_Due_Date"],   format="%d-%b-%Y", errors="coerce")
    t_rev = pd.to_datetime(df["Review_Completion_Date"], format="%d-%b-%Y", errors="coerce")

    def check(i):
        if pd.isna(t_due.iloc[i]) or pd.isna(t_rev.iloc[i]):
            return "Invalid (Missing Date)"
        return "Valid" if t_due.iloc[i] < t_rev.iloc[i] else "Invalid (Due Date >= Review Date)"

    df["Deliverable_Date < Review_Completion_Date"] = [check(i) for i in range(len(df))]
    return df


# ── Schedule 9 validation ─────────────────────────────────────
# Rule: sow_date >= Review_Completion_Date (from Schedule 1 by position)
def validate_schedule9(df9: pd.DataFrame, df1: pd.DataFrame) -> pd.DataFrame:
    if df9.empty:
        return df9

    t_ms = pd.to_datetime(df9["sow_date"], format="%d-%b-%Y", errors="coerce")
    t_rv = pd.Series([pd.NaT] * len(df9))

    if not df1.empty and "Review_Completion_Date" in df1.columns:
        rv = pd.to_datetime(df1["Review_Completion_Date"].reset_index(drop=True),
                            format="%d-%b-%Y", errors="coerce")
        for i in range(min(len(df9), len(rv))):
            t_rv.iloc[i] = rv.iloc[i]

    def check(i):
        ms = t_ms.iloc[i]
        rv = t_rv.iloc[i]
        if pd.isna(ms):
            original_date = df9["sow_date"].iloc[i] if i < len(df9) else "N/A"
            print(f"   [DEBUG] Invalid date at row {i}: sow_date='{original_date}' -> parsed as NaT")
            return "Invalid"
        if pd.isna(rv):
            return "Invalid (Missing Review Date)"
        return "Valid" if ms >= rv else "Invalid (Fees Date < Review Date)"

    df9["sow_date >= Review_Completion_Date"] = [check(i) for i in range(len(df9))]
    return df9


def validate_amount(value):
    """Validate that amount is a proper numeric value."""
    if not isinstance(value, str):
        return "Invalid (Not String)"
    v = value.strip().replace(",", "").replace(" ", "")
    if not v:
        return "Invalid (Empty)"
    try:
        # Try to parse as float
        float(v)
        return "Valid"
    except ValueError:
        return "Invalid (Not Numeric)"


def extract_schedule_from_markdown_schedule1(md: str) -> list:
    """Fallback parser: read a markdown table for schedule 1.
    """
    lines = [line.strip() for line in md.splitlines() if line.strip()]
    header_phrases = ["services_deliverables", "deliverable_due_date", "review_completion_date"]

    start = None
    headers = []
    rows = []

    for i, line in enumerate(lines):
        lower = line.lower()
        if line.startswith("|") and all(p in lower for p in header_phrases):
            start = i
            headers = [cell.strip() for cell in line.strip("|").split("|")]
            break

    if start is None:
        return []

    for line in lines[start+1:]:
        if not line.startswith("|"):
            break
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != len(headers):
            continue
        raw = dict(zip(headers, cells))
        rows.append({
            "Services_Deliverables": raw.get("Services_Deliverables", raw.get("services_deliverables", "")),
            "Deliverable_Due_Date": raw.get("Deliverable_Due_Date", raw.get("deliverable_due_date", "")),
            "Review_Completion_Date": raw.get("Review_Completion_Date", raw.get("review_completion_date", "")),
        })

    # Filter out total/summary rows
    filtered_rows = []
    for row in rows:
        desc = str(row.get("Services_Deliverables", "")).lower()
        if "total" not in desc:
            filtered_rows.append(row)

    return filtered_rows


def extract_schedule_from_markdown_schedule9(md: str) -> list:
    """Fallback parser: read a markdown table for schedule 9.
    """
    lines = [line.strip() for line in md.splitlines() if line.strip()]
    header_phrases = ["deliverable_or_milestone", "date", "amount"]

    start = None
    headers = []
    rows = []

    for i, line in enumerate(lines):
        lower = line.lower()
        if line.startswith("|") and all(p in lower for p in header_phrases):
            start = i
            headers = [cell.strip() for cell in line.strip("|").split("|")]
            break

    if start is None:
        return []

    for line in lines[start+1:]:
        if not line.startswith("|"):
            break
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != len(headers):
            continue
        raw = dict(zip(headers, cells))
        rows.append({
            "Deliverable_or_Milestone": raw.get("Deliverable_or_Milestone", raw.get("deliverable_or_milestone", "")),
            "Date": raw.get("Date", raw.get("date", "")),
            "Amount": raw.get("Amount", raw.get("amount", "")),
        })

    # Filter out total/summary rows
    filtered_rows = []
    for row in rows:
        desc = str(row.get("Deliverable_or_Milestone", "")).lower()
        if "total" not in desc:
            filtered_rows.append(row)

    return filtered_rows


def extract_schedule1(file_path: str):
    full_content = get_doc_markdown_schedule1(file_path)
    prompt = build_prompt_schedule1(full_content)
    log = get_logger("extractors.schedules") # Ensure logger is available
    log.info("GPU: extracting Schedule 1...")
    response = call_gpu(prompt, max_new_tokens=1024)

    raw1 = []
    try:
        clean = re.sub(r'```json\s*|```', '', response).strip()
        json_str = clean[clean.find('{'):clean.rfind('}')+1]
        data = json.loads(json_str)

        # schedule key fallback list
        schedule1_keys = ["schedule_1_services_milestones", "schedule_1", "schedule_1_services"]

        def _get_schedule_data(keys):
            for k in keys:
                if isinstance(data.get(k), list):
                    return data.get(k)
            return []

        raw1 = _get_schedule_data(schedule1_keys)

        # ── DEBUG ──────────────────────────────────────────────────
        print(f"   [DEBUG] schedule_1 raw rows count: {len(raw1)}")
        if not raw1: # Use log.debug instead of print
            log.debug("schedule_1 is EMPTY in JSON response")
            log.debug(f"JSON keys found: {list(data.keys())}")
        # ───────────────────────────────────────────────────────────
    except Exception as e:
        log.debug(f"JSON parsing failed: {e}, falling back to markdown")

    if not raw1:
        raw1 = extract_schedule_from_markdown_schedule1(full_content)

    df1 = pd.DataFrame(raw1)

    # Filter out total/summary rows
    if not df1.empty and "Services_Deliverables" in df1.columns:
        df1 = df1[~df1["Services_Deliverables"].astype(str).str.lower().str.contains("total")]

    for col in ["Services_Deliverables", "Deliverable_Due_Date", "Review_Completion_Date"]:
        if col not in df1.columns:
            df1[col] = None
    df1 = set_milestone_index(df1)

    if not df1.empty:
        df1["Deliverable_Due_Date"]   = df1["Deliverable_Due_Date"].apply(validate_date)
        df1["Review_Completion_Date"] = df1["Review_Completion_Date"].apply(validate_date)

    df1 = validate_schedule1(df1)

    log.info("Schedule 1 extracted")
    return df1


def extract_schedule9(file_path: str):
    full_content = get_doc_markdown_schedule9(file_path)
    prompt = build_prompt_schedule9(full_content)
    log = get_logger("extractors.schedules") # Ensure logger is available
    log.info("GPU: extracting Schedule 9...")
    response = call_gpu(prompt, max_new_tokens=1024)

    raw9 = []
    try:
        clean = re.sub(r'```json\s*|```', '', response).strip()
        json_str = clean[clean.find('{'):clean.rfind('}')+1]
        data = json.loads(json_str)

        # schedule key fallback list
        schedule9_keys = ["schedule_9_fixed_fee_engagement", "schedule_9", "schedule_9_fixed_fee"]

        def _get_schedule_data(keys):
            for k in keys:
                if isinstance(data.get(k), list):
                    return data.get(k)
            return []

        raw9 = _get_schedule_data(schedule9_keys)

        # ── DEBUG ──────────────────────────────────────────────────
        print(f"   [DEBUG] schedule_9 raw rows count: {len(raw9)}")
        if not raw9: # Use log.debug instead of print
            log.debug("schedule_9 is EMPTY in JSON response")
            log.debug(f"JSON keys found: {list(data.keys())}")
        # ───────────────────────────────────────────────────────────
    except Exception as e:
        log.debug(f"JSON parsing failed: {e}, falling back to markdown")

    if not raw9:
        raw9 = extract_schedule_from_markdown_schedule9(full_content)

    df9 = pd.DataFrame(raw9)

    # Filter out total/summary rows
    if not df9.empty and "Deliverable_or_Milestone" in df9.columns:
        df9 = df9[~df9["Deliverable_or_Milestone"].astype(str).str.lower().str.contains("total")]

    for col in ["Deliverable_or_Milestone", "Date", "Amount"]:
        if col not in df9.columns:
            df9[col] = None
    # Rename to simple internal names
    df9 = df9.rename(columns={"Date": "sow_date", "Amount": "sow_amt"})
    df9 = set_milestone_index(df9)

    if not df9.empty:
        df9["sow_date"] = df9["sow_date"].apply(validate_date)
        log.debug(f"Schedule 9 dates after validation: {df9['sow_date'].tolist()}")

    log.info("Schedule 9 extracted")
    return df9


def extract_sow_schedules(file_path: str): # No change needed here, just ensuring logger is imported
    # Run extractions in parallel for speed
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(extract_schedule1, file_path)
        future9 = executor.submit(extract_schedule9, file_path)

        df1 = future1.result()
        df9 = future9.result()

    # Now validate schedule 9 with df1
    df9 = validate_schedule9(df9, df1)

    log = get_logger("extractors.schedules") # Ensure logger is available
    log.info("SOW schedules extracted")
    return df1, df9


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "SOW Template .docx"
    df1, df9 = extract_sow_schedules(path)
    print(df1.to_string())
    print(df9.to_string())
