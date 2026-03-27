"""
src/handlers/error_handler.py
Centralised error handling for agent nodes and extractors.
"""
import sys, os
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import traceback
from utils.logger import get_logger

log = get_logger("handlers.error_handler")


def handle_extraction_error(stage: str, error: Exception, retries: int = 0) -> dict:
    """
    Standardised error response for any extraction node failure.
    Logs the full traceback and returns a state-compatible dict.
    """
    tb = traceback.format_exc()
    log.error(f"[{stage}] failed (attempt {retries + 1}): {error}")
    log.debug(f"[{stage}] traceback:\n{tb}")
    return {
        "error": f"{stage} failed: {str(error)}",
    }


def handle_gpu_error(url: str, error: Exception) -> str:
    log.error(f"GPU server unreachable at {url}: {error}")
    return (
        "❌ Colab GPU server is unreachable. "
        "Check your .env COLAB_GPU_URL and ensure Cell 4 is running in Colab."
    )
