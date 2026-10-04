# decision_log.py
# Lightweight append-only JSONL decision log (Section 7). No database -
# one readable JSON object per line, doubling as demo-visible evidence of
# "explainable AI decision history".

import json
import logging
import threading

from config import DECISION_LOG_PATH

logger = logging.getLogger("UrbanPulse-AI.decision_log")
_lock = threading.Lock()


def log_decision(entry: dict):
    try:
        with _lock:
            with open(DECISION_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"Failed to write decision log entry: {e}")
