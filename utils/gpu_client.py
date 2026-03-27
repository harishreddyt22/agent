"""
utils/gpu_client.py
Single HTTP client that calls the Colab GPU Flask server.
All extractors import from here — one place to change the URL.
"""

import sys, os
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.torch_fix import *   # fix broken torch DLL on Windows
from utils.logger import get_logger

import requests
from dotenv import load_dotenv
import time

load_dotenv()
COLAB_GPU_URL = os.getenv("COLAB_GPU_URL", "http://localhost:5000")
log = get_logger("utils.gpu_client")


def call_gpu(prompt: str, max_new_tokens: int = 2048, max_retries: int = 3) -> str:
    """POST prompt to Colab GPU server with retry logic, return raw text response."""
    url = f"{COLAB_GPU_URL.rstrip('/')}/generate"
    log.info(f"GPU call → {url}")
    
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                url,
                json={"prompt": prompt, "max_new_tokens": max_new_tokens},
                timeout=600,
                verify=True
            )
            resp.raise_for_status()
            result = resp.json()
            if "error" in result:
                raise RuntimeError(f"GPU server error: {result['error']}")
            return result["response"]
        except requests.exceptions.SSLError as e:
            log.warning(f"SSL error on attempt {attempt + 1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # exponential backoff: 1s, 2s, 4s
                log.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
        except requests.exceptions.RequestException as e:
            log.error(f"Request failed on attempt {attempt + 1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                log.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise


def check_gpu_health() -> bool:
    """Returns True if the Colab GPU server is reachable."""
    try:
        url = f"{COLAB_GPU_URL.rstrip('/')}/health"
        resp = requests.get(url, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False
