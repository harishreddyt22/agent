"""
src/prompts/audit_prompt.py
Prompt template for audit report generation.
DO NOT MODIFY — prompt is frozen and tested.
"""


def build_audit_prompt(
    company: str,
    notes_text: str,
    issues_text: str,
    val_summary: str,
) -> str:
    return f"""
You are a senior procurement auditor. Write a clear, professional audit report
based on the following data.

COMPANY: {company}

EXTRACTION NOTES:
{notes_text}

VALIDATION ISSUES:
{issues_text}

VALIDATION TABLE SUMMARY:
{val_summary}

Write a structured audit report with these sections:
1. Executive Summary (2-3 sentences)
2. Document Analysis Findings (what was found in SOW and PO)
3. Validation Results (milestone by milestone — which passed, which failed and why)
4. Risk Assessment (what risks exist based on the issues found)
5. Recommendations (concrete steps to fix the issues)

Be specific, professional, and concise. Use plain English. Do not use JSON.
"""
