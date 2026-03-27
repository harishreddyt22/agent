"""
src/prompts/sow_prompt.py
Prompt template for SOW Schedule 1 & Schedule 9 extraction.
DO NOT MODIFY — prompt is frozen and tested.
"""


def build_sow_prompt(full_content: str) -> str:
    return f"""
You are a HYPER-PRECISION document extraction engine. Your goal is 100% data capture.

### MANDATORY EXTRACTION PROTOCOL:
1. **CELL-LEVEL SCANNING**: You must scan every character between the Markdown pipes (`|`).
   If a date or text exists within those boundaries, you MUST capture it.
2. **NO EARLY EXIT**: Do not move to the next row until you have verified every column
   (4 for Schedule 1, 3 for Schedule 9).
3. **HIDDEN DATA RECOVERY**: Check if a value for a column has slipped into the adjacent
   cell or the row immediately above/below due to formatting. If a date (e.g., DD-MMM-YYYY)
   is visible in the row but logically belongs in the 'Due Date' field, assign it there.
4. **NO NULLS FOR VISIBLE DATA**: If you see a text string or date, it is NOT null.
   Even if it is formatted poorly, extract the raw characters.
5. **STRICT MAPPING**:
   - Schedule 1: "#", "Services_Deliverables", "Deliverable_Due_Date", "Review_Completion_Date".
   - Schedule 9: "Deliverable_or_Milestone", "Date", "Amount".
6. **DATE FORMAT**: All dates MUST be returned in DD-MMM-YYYY format (e.g., 12-Jan-2025).
   Convert any date found in any format (dd/mm/yyyy, dd.mm.yyyy, dd - MMM - yyyy, etc.)
   to DD-MMM-YYYY before returning.

SOURCE MARKDOWN:
{full_content}

### OUTPUT INSTRUCTIONS:
Return ONLY raw JSON. Ensure every row in the source has a corresponding object in the JSON.

JSON FORMAT:
{{
 "schedule_1_services_milestones": [
  {{
   "Services_Deliverables": "string",
   "Deliverable_Due_Date": "string",
   "Review_Completion_Date": "string"
  }}
 ],
 "schedule_9_fixed_fee_engagement": [
  {{
   "Deliverable_or_Milestone": "string",
   "Date": "string",
   "Amount": "string"
  }}
 ]
}}
"""
