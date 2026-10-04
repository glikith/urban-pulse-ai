# traffic_state.py
# Holds the live, in-memory edge-weight store that the routing code reads from.
# Populated by a background Kafka consumer thread subscribed to
# TOPIC_TRAFFIC (the same topic the traffic-simulator publishes to).
#
# Congestion is a SOFT weight only (never removes an edge). Roadblocks are a
# HARD removal handled separately in main.py's `blocked_edges` set.

import json
import logging
import threading
import time

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_TRAFFIC

logger = logging.getLogger("UrbanPulse-AI.traffic_state")

# edge key is f"{u}_{v}" (string, matches what the simulator publishes)
# value is a congestion score in [0, 1]. Missing edges are treated as 0 (free flow).
congestion_store: dict[str, float] = {}
_store_lock = threading.Lock()
_last_update_ts: float | None = None


def get_congestion(u, v) -> float:
    with _store_lock:
        return congestion_store.get(f"{u}_{v}", 0.0)


def congestion_multiplier(u, v) -> float:
    """Turns a 0-1 congestion score into a travel_time multiplier.
    0 congestion -> x1.0, 1.0 (gridlock) -> x3.5. Tuned to be noticeable
    without ever making a congested edge "worse" than a blocked one."""
    score = get_congestion(u, v)
    return 1.0 + score * 2.5


def snapshot_size() -> int:
    with _store_lock:
        return len(congestion_store)


def last_update_age_seconds():
    if _last_update_ts is None:
        return None
    return time.time() - _last_update_ts


def _apply_update(payload: dict):
    global _last_update_ts
    event_type = payload.get("event_type")

    if event_type == "CONGESTION_UPDATE":
        edges = payload.get("edges", {})
        with _store_lock:
            for edge_key, score in edges.items():
                try:
                    congestion_store[edge_key] = max(0.0, min(1.0, float(score)))
                except (TypeError, ValueError):
                    continue
        _last_update_ts = time.time()
        logger.debug(f"Applied congestion update for {len(edges)} edges")

    elif event_type == "GRID_RESET":
        with _store_lock:
            congestion_store.clear()
        _last_update_ts = time.time()

    # ROADBLOCK_INJECTED events are handled by main.py's own producer/consumer
    # loop for blocked_edges, not here - congestion and roadblocks stay separate.


def start_consumer_thread():
    """Starts a daemon thread consuming TOPIC_TRAFFIC. Degrades gracefully:
    if Kafka isn't reachable at startup, logs a warning and the engine keeps
    running with whatever congestion state it has (empty = free flow)."""

    def _run():
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable

        while True:
            try:
                consumer = KafkaConsumer(
                    TOPIC_TRAFFIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    group_id="urbanpulse-ai-engine",
                    auto_offset_reset="latest",
                )
                logger.info(f"Kafka consumer connected, subscribed to '{TOPIC_TRAFFIC}'")
                for message in consumer:
                    try:
                        _apply_update(message.value)
                    except Exception as e:
                        logger.warning(f"Failed to apply traffic update: {e}")
            except NoBrokersAvailable:
                logger.warning(
                    "Kafka broker unreachable for congestion consumer. "
                    "Running in standalone mode (no live congestion) - retrying in 10s."
                )
                time.sleep(10)
            except Exception as e:
                logger.warning(f"Kafka consumer error: {e}. Retrying in 10s.")
                time.sleep(10)

    t = threading.Thread(target=_run, daemon=True, name="kafka-congestion-consumer")
    t.start()
    return t
