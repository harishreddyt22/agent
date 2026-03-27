"""
utils/logger.py
Central logger factory. All modules get their logger from here.

Usage:
    from utils.logger import get_logger
    log = get_logger(__name__)
    log.info("Agent started")
"""
import logging

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
