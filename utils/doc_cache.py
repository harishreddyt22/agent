"""
utils/doc_cache.py
Docling document cache — parse each file only once.
Saves ~20-30s by avoiding double-parsing the SOW file.
"""
import threading
from utils.logger import get_logger

log   = get_logger("utils.doc_cache")
_LOCK = threading.Lock()
_CACHE: dict = {}   # file_path → markdown string


def get_markdown(file_path: str) -> str:
    """
    Return cached markdown if already parsed, else parse and cache.
    Thread-safe — safe for parallel extraction threads.
    """
    with _LOCK:
        if file_path in _CACHE:
            log.info(f"DocCache HIT  → {file_path[-40:]}")
            return _CACHE[file_path]

    # Parse outside lock so other threads aren't blocked
    log.info(f"DocCache MISS → parsing {file_path[-40:]}")
    from docling.document_converter import DocumentConverter
    converter = DocumentConverter()
    result    = converter.convert(file_path)
    md        = result.document.export_to_markdown()

    with _LOCK:
        _CACHE[file_path] = md
        log.info(f"DocCache SET  → {len(md)} chars cached")

    return md


def clear():
    with _LOCK:
        _CACHE.clear()
