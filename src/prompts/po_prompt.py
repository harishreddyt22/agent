"""
src/prompts/po_prompt.py
Prompt template for Purchase Order extraction.
DO NOT MODIFY — prompt is frozen and tested.
"""


def build_po_prompt(full_content: str) -> str:
    return f"""
You are a HIGH-PRECISION purchase order document extraction engine.
Return ONLY raw JSON.

### EXTRACTION RULES:
1. Capture Dates and convert them to DD-MMM-YYYY format (e.g., 11-Feb-2026).
   Whatever format the date appears in the document (dd/mm/yyyy, dd.mm.yyyy,
   dd-mm-yyyy, dd - MMM - yyyy, etc.), always return it as DD-MMM-YYYY.
2. Ensure "Item_Total" captures the currency value exactly as shown in the document.
   Include the currency symbol if present (e.g. €, $, £, ₹).
3. Every row in the PO table must be its own object in the array.

JSON Schema:
{{
 "purchase_order_items":[
   {{
     "Item_Description":"string",
     "Delivery_Date":"string",
     "Item_Total":"string"
   }}
 ]
}}

SOURCE DOCUMENT:
{full_content}
"""
