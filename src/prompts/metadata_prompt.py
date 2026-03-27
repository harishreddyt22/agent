"""
src/prompts/metadata_prompt.py
FROZEN — DO NOT MODIFY THIS PROMPT.
Exact prompt from Colab that gives correct results.
"""


def build_metadata_prompt(full_content: str) -> str:
    return f"""
### SYSTEM ROLE
High-Precision Legal Parser. Your goal is verbatim extraction into JSON.
CRITICAL: Zero inference. If data is missing, return "" or [].

### EXTRACTION LOGIC
1. **HEADER (First Paragraph Only)**
   - **DELIMITER RULE:** Locate the very first line of the document.
   - `company_name`: Extract the text located ONLY between the keyword **"This"** and the keyword **"Statement"**. 
   - **SEQUENCE:** 1. Identify "This [Company Name] Statement".
     2. `module_dated`: Extract the date that appears immediately AFTER that "Statement" keyword in the same paragraph.
   - **STRICT RULE:** Do not include the words "This" or "Statement" in the `company_name` value.

2. **SECTION: "Purchase Module Term"**
   - `purchase_module_term`: Verbatim string containing duration, start, and end dates.

3. **SECTION: "8. ENTIRE AGREEMENT" (STRICT ISOLATION RULE)**
   - **MANDATORY:** You must extract exactly TWO (2) legal entities for the `parties_in_entire_agreement` list.
   - **FORBIDDEN SOURCE:** DO NOT use the company name found in the first paragraph. 
   - **REQUIRED SOURCE:** Only extract names found between the heading "8. ENTIRE AGREEMENT" and the heading "Schedule 9".
   - **SEARCH TARGET:** Look for the specific sentence within Section 8 that says "This Agreement is between..." or "The parties to this Agreement are...".
   - **TEXT MATCH:** Extract the full legal names (e.g., "Company A Ltd" and "Company B Inc") as they appear in the body text of Section 8.

### SOURCE TEXT
{full_content}

### OUTPUT FORMAT (STRICT JSON ONLY)
{{
  "document_metadata": {{
    "company_name": "",
    "module_dated": "",
    "purchase_module_term": "",
    "parties_in_entire_agreement": ["Company from Section 8", "Second Company from Section 8"]
  }}
}}
"""