"""
extractors/extract_po.py
DO NOT MODIFY THE PROMPT.
"""
import sys, os
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *  # fix broken torch DLL on Windows

import json, re
import pandas as pd
from utils.gpu_client import call_gpu


def get_doc_markdown(file_path: str) -> str:
    from utils.doc_cache import get_markdown
    return get_markdown(file_path)[:12000]


from src.prompts.po_prompt import build_po_prompt as build_prompt


_MONTH_MAP = {
    "jan":"Jan","feb":"Feb","mar":"Mar","apr":"Apr","may":"May","jun":"Jun",
    "jul":"Jul","aug":"Aug","sep":"Sep","oct":"Oct","nov":"Nov","dec":"Dec",
    "january":"Jan","february":"Feb","march":"Mar","april":"Apr","june":"Jun",
    "july":"Jul","august":"Aug","september":"Sep","october":"Oct",
    "november":"Nov","december":"Dec",
    "1":"Jan","2":"Feb","3":"Mar","4":"Apr","5":"May","6":"Jun",
    "7":"Jul","8":"Aug","9":"Sep","10":"Oct","11":"Nov","12":"Dec",
    "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun",
    "07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec",
}

# Separator: hyphen, dot, slash, or spaces around any of those
_SEP = r"\s*[\-./]\s*"

_DATE_PATTERNS = [
    # dd-Mon-yyyy  (any separator + optional spaces)
    # e.g. 12-Jan-2025 | 12 - Jan - 2025 | 12.Jan.2025 | 12/Jan/2025
    (r"(\d{1,2})" + _SEP + r"([A-Za-z]{3,9})" + _SEP + r"(\d{4})", "dmy_text"),
    # dd-mm-yyyy  (numeric month, any separator)
    # e.g. 12-01-2025 | 12.01.2025 | 12/01/2025 | 12 - 01 - 2025
    (r"(\d{1,2})" + _SEP + r"(\d{1,2})" + _SEP + r"(\d{4})", "dmy_num"),
    # yyyy-mm-dd
    (r"(\d{4})" + _SEP + r"(\d{1,2})" + _SEP + r"(\d{1,2})", "ymd"),
    # dd Month yyyy  (space separated)
    (r"(\d{1,2})\s+([A-Za-z]{3,9}),?\s+(\d{4})", "dmy_space"),
    # Month dd, yyyy
    (r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", "mdy_space"),
]


def validate_date(value):
    """
    Parse a date string in any common format and return DD-Mon-YYYY.
    Returns raw string if a year is present but format is unrecognised.
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
        except Exception:
            continue

    # Last resort — preserve raw value if it contains a 4-digit year
    return v if re.search(r"\d{4}", v) else None


# ── Exchange rates (fetched once, cached in memory) ───────────
_FX_CACHE: dict = {}

def _get_rate(from_currency: str) -> float:
    """
    Fetch live INR exchange rate for the given currency code.
    Falls back to hardcoded rates if network unavailable.
    """
    from_currency = from_currency.upper()
    if from_currency == "INR":
        return 1.0
    if from_currency in _FX_CACHE:
        return _FX_CACHE[from_currency]

    # Hardcoded fallback rates (approx as of early 2026)
    fallback = {
        "EUR": 90.5,
        "USD": 83.5,
        "GBP": 106.0,
        "CHF": 94.0,
        "SEK": 8.0,
        "NOK": 7.8,
        "DKK": 12.1,
        "PLN": 21.0,
        "CZK": 3.7,
        "HUF": 0.23,
        "RON": 18.5,
    }

    try:
        import urllib.request
        url = f"https://api.exchangerate-api.com/v4/latest/{from_currency}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            import json as _json
            data = _json.loads(resp.read())
            rate = float(data["rates"]["INR"])
            _FX_CACHE[from_currency] = rate
            print(f"   💱 Live rate: 1 {from_currency} = ₹{rate:.2f}")
            return rate
    except Exception:
        rate = fallback.get(from_currency, 1.0)
        _FX_CACHE[from_currency] = rate
        print(f"   💱 Fallback rate: 1 {from_currency} = ₹{rate:.2f}")
        return rate


def _detect_currency(value: str) -> str:
    """Detect currency code from symbol or code prefix in the value string."""
    v = value.strip()
    if "€" in v or v.upper().startswith("EUR"): return "EUR"
    if "$" in v or v.upper().startswith("USD"): return "USD"
    if "£" in v or v.upper().startswith("GBP"): return "GBP"
    if "₹" in v or v.upper().startswith("INR"): return "INR"
    if "CHF" in v.upper():                      return "CHF"
    if "SEK" in v.upper():                      return "SEK"
    if "NOK" in v.upper():                      return "NOK"
    if "DKK" in v.upper():                      return "DKK"
    if "PLN" in v.upper():                      return "PLN"
    return "INR"  # default — assume already INR


def _parse_amount(value: str) -> float:
    """
    Parse a numeric amount from a string, handling both:
      - European format: 1.234,56  → 1234.56  (dot=thousands, comma=decimal)
      - Indian/US format: 1,234.56 → 1234.56  (comma=thousands, dot=decimal)
      - Indian grouping:  1,23,456 → 123456
      - Plain:            50000    → 50000
    """
    # Remove currency symbols, letters, spaces
    cleaned = re.sub(r'[₹€$£\s]', '', value.strip())
    cleaned = re.sub(r'[A-Za-z]', '', cleaned).strip()

    if not cleaned:
        return 0.0

    dot_pos   = cleaned.rfind('.')
    comma_pos = cleaned.rfind(',')
    dot_count   = cleaned.count('.')
    comma_count = cleaned.count(',')

    if dot_pos != -1 and comma_pos != -1:
        if comma_pos > dot_pos:
            # European: dot=thousands, comma=decimal  e.g. 1.234,56
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            # US/Indian: comma=thousands, dot=decimal  e.g. 1,234.56
            cleaned = cleaned.replace(',', '')

    elif dot_pos != -1 and comma_pos == -1:
        # Only dots present
        after_last_dot = cleaned[dot_pos + 1:]
        if len(after_last_dot) == 3 or dot_count > 1:
            # European thousands notation: 1.000 or 1.234.567
            cleaned = cleaned.replace('.', '')
        # else decimal notation: 1.50 → leave as-is

    elif comma_pos != -1 and dot_pos == -1:
        # Only commas: Indian grouping 1,23,456 or US 1,234
        cleaned = cleaned.replace(',', '')

    # else: plain number like 50000 — leave as-is

    return float(cleaned)


def _format_inr(amount: float) -> str:
    """Format a float as Indian Rupee string (₹1,23,456)."""
    paise    = round(amount * 100)
    rupees   = paise // 100
    paisa_r  = paise % 100
    s = str(rupees)
    if len(s) <= 3:
        result = s
    else:
        last3     = s[-3:]
        remaining = s[:-3]
        groups    = []
        while len(remaining) > 2:
            groups.append(remaining[-2:])
            remaining = remaining[:-2]
        if remaining:
            groups.append(remaining)
        groups.reverse()
        result = ",".join(groups) + "," + last3
    if paisa_r:
        return f"₹{result}.{paisa_r:02d}"
    return f"₹{result}"


def convert_to_inr(value: str) -> str:
    """
    Detect currency, fetch live exchange rate, convert to INR.
    Handles European format (1.234,56) and Indian/US format (1,234.56).
    """
    if not isinstance(value, str) or not value.strip():
        return value
    try:
        currency = _detect_currency(value)
        amount   = _parse_amount(value)
        if amount == 0.0:
            return value
        rate     = _get_rate(currency)
        inr_amt  = amount * rate
        result   = _format_inr(inr_amt)
        if currency != "INR":
            print(f"   💱 Converted: {value.strip()} → {result} (rate: {rate})")
        return result
    except Exception as e:
        print(f"   ⚠️ Currency conversion failed for '{value}': {e}")
        return value


# ── PO validation ─────────────────────────────────────────────
# Rule: po_date >= sow_date AND po_amt matches sow_amt
def validate_po(df: pd.DataFrame, df9: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    t_delivery = pd.to_datetime(df["po_delivery_date"], format="%d-%b-%Y", errors="coerce")

    t_sow = pd.Series([pd.NaT] * len(df))
    sow_amts = [""] * len(df)

    if df9 is not None and not df9.empty and "sow_date" in df9.columns:
        sow_dates = pd.to_datetime(df9["sow_date"].reset_index(drop=True),
                                   format="%d-%b-%Y", errors="coerce")
        for i in range(min(len(df), len(df9))):
            t_sow.iloc[i] = sow_dates.iloc[i]
            if "sow_amt" in df9.columns:
                sow_amts[i] = str(df9["sow_amt"].reset_index(drop=True).iloc[i])

    def check(i):
        po_d  = t_delivery.iloc[i]
        sow_d = t_sow.iloc[i]
        po_a  = re.sub(r'[₹,\s]', '', str(df["po_amt"].iloc[i]))
        sow_a = re.sub(r'[₹,\s]', '', sow_amts[i])
        errs  = []
        if pd.isna(po_d):   errs.append("Missing PO Date")
        elif pd.isna(sow_d): errs.append("Missing SOW Date")
        elif bool(po_d < sow_d): errs.append("PO Date < SOW Date")
        if po_a and sow_a and po_a not in sow_a and sow_a not in po_a:
            errs.append("Amount Mismatch")
        return "Invalid" if errs else "Valid"

    df["po_delivery_date >= sow_date & sow_amount == po_amount"] = [check(i) for i in range(len(df))]
    return df


def extract_po(file_path: str, df9: pd.DataFrame = None) -> pd.DataFrame:
    full_content = get_doc_markdown(file_path)
    prompt = build_prompt(full_content)
    print("🧠 GPU: extracting PO data...")
    response = call_gpu(prompt, max_new_tokens=1024)

    data  = {}
    clean = re.sub(r'```json\s*|```', '', response).strip()
    s, e  = clean.find('{'), clean.rfind('}')
    if s != -1 and e != -1:
        data = json.loads(clean[s:e+1])

    if "purchase_order_items" in data and data["purchase_order_items"]:
        df = pd.DataFrame(data["purchase_order_items"])

        for col in ["Item_Description", "Delivery_Date", "Item_Total"]:
            if col not in df.columns:
                df[col] = None

        # Rename to simple internal names
        df = df.rename(columns={"Delivery_Date": "po_delivery_date", "Item_Total": "po_amt"})

        df["po_delivery_date"] = df["po_delivery_date"].apply(validate_date)
        df["po_amt"]  = df["po_amt"].apply(convert_to_inr)

        df.index = [f"Milestone {i+1}" for i in range(len(df))]
        df = validate_po(df, df9)

        print("✅ PO extracted")
        return df

    return pd.DataFrame()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "PO Template.docx"
    df = extract_po(path)
    print(df.to_string())