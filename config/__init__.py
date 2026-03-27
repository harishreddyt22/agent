"""
config/__init__.py
Loads settings.yaml and logging_config.yaml on import.
"""
import os, sys, yaml, logging, logging.config

_HERE   = os.path.abspath(os.path.dirname(__file__))
_ROOT   = os.path.abspath(os.path.join(_HERE, ".."))

# ── Load settings ─────────────────────────────────────────────
with open(os.path.join(_HERE, "settings.yaml"), "r") as f:
    SETTINGS = yaml.safe_load(f)

# ── Setup logging ─────────────────────────────────────────────
_log_cfg_path = os.path.join(_HERE, "logging_config.yaml")
_log_dir = os.path.join(_ROOT, "data", "logs")
os.makedirs(_log_dir, exist_ok=True)

with open(_log_cfg_path, "r") as f:
    _log_cfg = yaml.safe_load(f)

logging.config.dictConfig(_log_cfg)

# ── Convenience helpers ───────────────────────────────────────
def get(section: str, key: str, default=None):
    return SETTINGS.get(section, {}).get(key, default)
