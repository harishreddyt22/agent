"""
extractors/extract_metadata.py
DO NOT MODIFY THE PROMPT.
"""

import sys, os
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *  # fix broken torch DLL on Windows

import json, re
import pandas as pd
from datetime import datetime
from utils.gpu_client import call_gpu


def get_doc_markdown(file_path: str) -> str:
    from utils.doc_cache import get_markdown
    md = get_markdown(file_path)
    # Do NOT truncate — Section 8 (Entire Agreement) is near the end of the document
    return md


from src.prompts.metadata_prompt import build_metadata_prompt as build_prompt


def _parse_date(raw: str) -> datetime:
    """Parse a date string trying many formats. Returns datetime or None."""
    if not raw or not isinstance(raw, str):
        return None

    # Strip ordinal suffixes: 1st→1, 2nd→2, 3rd→3, 4th→4, 29th→29, etc.
    clean = re.sub(r'(\d+)\s*(?:st|nd|rd|th)', r'\1', raw.strip())
    # Normalise separators: dots, slashes, spaces-around-hyphens → single hyphen
    clean = re.sub(r'\s*[./]\s*', '-', clean)
    clean = re.sub(r'\s*-\s*', '-', clean)
    # Replace multiple spaces with single space
    clean = re.sub(r'\s+', ' ', clean).strip()

    print(f"  🔍 Parsing: '{raw}' → '{clean}'")

    formats = [
        "%d-%b-%Y", "%d-%B-%Y",
        "%d %b %Y", "%d %B %Y",
        "%B %d, %Y", "%b %d, %Y",
        "%B %d %Y",  "%b %d %Y",
        "%d-%m-%Y",  "%d %m %Y",
        "%Y-%m-%d",  "%m-%d-%Y",
        "%d %b, %Y", "%d %B, %Y",
        "%d-%m %Y", "%d %m-%Y",  # Handle mixed separators
    ]

    # Try on clean version
    for fmt in formats:
        try:
            result = datetime.strptime(clean, fmt)
            print(f"  ✅ Matched fmt '{fmt}' → {result.strftime('%d-%b-%Y')}")
            return result
        except ValueError:
            continue

    # Try replacing hyphens with spaces (covers "01 January-2026" oddities)
    clean2 = clean.replace('-', ' ')
    for fmt in formats:
        try:
            result = datetime.strptime(clean2, fmt)
            print(f"  ✅ Matched fmt '{fmt}' (spaced) → {result.strftime('%d-%b-%Y')}")
            return result
        except ValueError:
            continue

    # Nuclear fallback — extract day, month name/number and year individually
    month_map = {
        'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,
        'july':7,'august':8,'september':9,'october':10,'november':11,'december':12,
        'jan':1,'feb':2,'mar':3,'apr':4,'jun':6,'jul':7,'aug':8,
        'sep':9,'oct':10,'nov':11,'dec':12
    }
    year_m = re.search(r'\b(20\d{2})\b', clean)
    day_m  = re.search(r'\b(\d{1,2})\b', clean)
    mon_m  = re.search(r'[A-Za-z]{3,}', clean)
    if year_m and day_m and mon_m:
        mon_str = mon_m.group(0).lower()
        if mon_str in month_map:
            try:
                result = datetime(int(year_m.group(1)), month_map[mon_str], int(day_m.group(1)))
                print(f"   Nuclear fallback → {result.strftime('%d-%b-%Y')}")
                return result
            except ValueError:
                pass

    print(f"   All parsing failed for: '{raw}'")
    return None


def _extract_dates_from_text(text: str):
    """
    Extract all date-like substrings from a text and return parsed datetimes.
    Handles: '01-Jan-2026', '1st January 2026', '1 st January 2026', etc.
    """
    patterns = [
        r'\d{1,2}\s+(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+\d{4}',     # 1 st January 2026 or 1st January 2026
        r'\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+\d{4}',       # 1st January 2026 (no space before ordinal)
        r'\d{1,2}\s*(?:st|nd|rd|th)?\s*[\-./]\s*[A-Za-z]{3,9}\s*[\-./]\s*\d{4}',  # 1st - Jan - 2026
        r'\d{1,2}\s*-\s*[A-Za-z]{3,9}\s*-\s*\d{4}',              # 01 - Jan - 2026
        r'\d{1,2}-[A-Za-z]{3,9}-\d{4}',                              # 01-Jan-2026
        r'\d{1,2}\.[A-Za-z]{3,9}\.\d{4}',                          # 01.Jan.2026
        r'\d{1,2}/[A-Za-z]{3,9}/\d{4}',                              # 01/Jan/2026
        r'\d{1,2}\s*[\-./]\s*\d{1,2}\s*[\-./]\s*\d{4}',       # 01-01-2026 | 01.01.2026 | 01 - 01 - 2026
        r'\d{1,2}/\d{1,2}/\d{4}',                                   # 01/01/2026
        r'\d{1,2}\.\d{1,2}\.\d{4}',                               # 01.01.2026
        r'\d{4}-\d{1,2}-\d{1,2}',                                   # 2026-01-01
    ]
    found = []
    for pattern in patterns:
        found.extend(re.findall(pattern, text, re.IGNORECASE))

    parsed = []
    seen = set()
    for f in found:
        if f not in seen:
            seen.add(f)
            dt = _parse_date(f)
            if dt:
                parsed.append(dt)
    return parsed


def extract_metadata(
    file_path: str,
    sow_upload_dt: datetime = None,
    po_upload_dt:  datetime = None
) -> pd.DataFrame:

    full_content = get_doc_markdown(file_path)
    prompt = build_prompt(full_content)
    print("🧠 GPU: extracting metadata...")
    response = call_gpu(prompt, max_new_tokens=1024)

    json_match = re.search(r"\{.*\}", response, re.DOTALL)
    data = json.loads(json_match.group(0))
    meta = data.get("document_metadata", {})

    # Flatten parties list
    if isinstance(meta.get("parties_in_entire_agreement"), list):
        meta["parties_in_entire_agreement"] = ", ".join(meta["parties_in_entire_agreement"])

    # ── Parse purchase_module_term — extract dates only ───────
    term_raw = meta.get("purchase_module_term", "")
    print(f"\n📅 Raw purchase_module_term: '{term_raw}'")

    term_start_dt = None
    term_end_dt   = None

    # First, try to extract dates using range separators (to, till, through, until, and, -, etc.)
    # More flexible range pattern that handles various date formats
    range_match = re.search(
        r'(\d{1,2}[^\d]*?(?:st|nd|rd|th)?[^\d]*?[A-Za-z]{3,9}[^\d]*?\d{4})\s*(?:to|till|through|until|and|-)\s*(\d{1,2}[^\d]*?(?:st|nd|rd|th)?[^\d]*?[A-Za-z]{3,9}[^\d]*?\d{4})',
        term_raw,
        re.IGNORECASE
    )
    
    if range_match:
        # Found a date range pattern
        start_str = range_match.group(1).strip()
        end_str = range_match.group(2).strip()
        print(f"  Found range pattern: '{start_str}' → '{end_str}'")
        
        start_dt = _parse_date(start_str)
        end_dt = _parse_date(end_str)
        
        if start_dt and end_dt:
            term_start_dt = start_dt
            term_end_dt = end_dt
            meta["purchase_module_term"] = (
                f"{term_start_dt.strftime('%d-%b-%Y')} to {term_end_dt.strftime('%d-%b-%Y')}"
            )
        else:
            # Try generic date extraction as fallback
            parsed_dates = _extract_dates_from_text(term_raw)
            print(f"  Extracted {len(parsed_dates)} date(s) from term")
            if len(parsed_dates) >= 2:
                term_start_dt = parsed_dates[0]
                term_end_dt = parsed_dates[1]
                meta["purchase_module_term"] = (
                    f"{term_start_dt.strftime('%d-%b-%Y')} to {term_end_dt.strftime('%d-%b-%Y')}"
                )
            elif len(parsed_dates) == 1:
                term_start_dt = parsed_dates[0]
                meta["purchase_module_term"] = term_start_dt.strftime('%d-%b-%Y')
            else:
                meta["purchase_module_term"] = term_raw or "Not Found"
    else:
        # No explicit range pattern found, try generic extraction
        parsed_dates = _extract_dates_from_text(term_raw)
        print(f"  Extracted {len(parsed_dates)} date(s) from term")

        if len(parsed_dates) >= 2:
            term_start_dt = parsed_dates[0]
            term_end_dt = parsed_dates[1]
            meta["purchase_module_term"] = (
                f"{term_start_dt.strftime('%d-%b-%Y')} to {term_end_dt.strftime('%d-%b-%Y')}"
            )
        elif len(parsed_dates) == 1:
            term_start_dt = parsed_dates[0]
            meta["purchase_module_term"] = term_start_dt.strftime('%d-%b-%Y')
        else:
            # Final fallback: try parsing with "to" split
            if " to " in term_raw.lower():
                parts = re.split(r'\s+to\s+', term_raw, flags=re.IGNORECASE)
                dates = [_parse_date(p.strip()) for p in parts]
                dates = [d for d in dates if d]
                if len(dates) >= 2:
                    term_start_dt, term_end_dt = dates[0], dates[1]
                    meta["purchase_module_term"] = (
                        f"{term_start_dt.strftime('%d-%b-%Y')} to {term_end_dt.strftime('%d-%b-%Y')}"
                    )
                elif len(dates) == 1:
                    term_start_dt = dates[0]
                    meta["purchase_module_term"] = term_start_dt.strftime('%d-%b-%Y')
                else:
                    meta["purchase_module_term"] = term_raw or "Not Found"
            else:
                meta["purchase_module_term"] = term_raw or "Not Found"

    # ── Parse module_dated ────────────────────────────────────
    module_dated_str   = meta.get("module_dated", "")
    print(f"\n📅 Raw module_dated: '{module_dated_str}'")
    parsed_module_date = _parse_date(module_dated_str)

    # ── Validation: module_dated <= term_start_date ───────────
    print(f"\n🔎 Validating: module_dated={parsed_module_date}, term_start={term_start_dt}")

    # Rule: current date < term_start_dt → Valid
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if term_start_dt:
        if today < term_start_dt:
            meta["module_dated_validation"] = " Valid"
        else:
            meta["module_dated_validation"] = (
                f" Invalid (Today {today.strftime('%d-%b-%Y')} "
                f">= Term start {term_start_dt.strftime('%d-%b-%Y')})"
            )
    else:
        meta["module_dated_validation"] = f" Cannot parse term start from: '{term_raw}'"

    # ── Column order — validation last ────────────────────────
    ordered_cols = [
        "company_name",
        "module_dated",
        "purchase_module_term",
        "parties_in_entire_agreement",
        "module_dated_validation",
    ]

    df = pd.DataFrame([meta])
    df.index = pd.RangeIndex(start=1, stop=len(df) + 1, step=1)
    existing = [c for c in ordered_cols if c in df.columns]
    df = df[existing]

    print(" Metadata extracted")
    return df