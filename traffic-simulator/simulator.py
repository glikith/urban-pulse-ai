# traffic-simulator/simulator.py
# Standalone process (Section 3a). Loads the same West Hyderabad bbox graph,
# computes a congestion score per edge every cycle from three layered
# factors, and publishes the payload to Kafka topic "live-traffic-updates".
# The Python AI engine's own Kafka consumer (traffic_state.py) picks this up
# and uses it as a soft routing weight - this script does not talk to the
# AI engine directly.

import json
import logging
import math
import random
import sys
import time
import os

# Allow running this script directly (python simulator.py) with the shared
# config.py living in ../python-ai-engine
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "python-ai-engine"))

import osmnx as ox

from config import (
    BBOX, KAFKA_BOOTSTRAP_SERVERS, TOPIC_TRAFFIC,
    HOTSPOTS, DEMO_ROAD_OVERRIDES, MAJOR_HIGHWAY_TAGS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UrbanPulse-Simulator")

CYCLE_SECONDS = 12  # within the required 10-15s window

# Road-class baseline congestion ranges: (min, max) picked fresh each cycle
# before jitter. Major roads run busier than residential by default.
ROAD_CLASS_BASELINE = {
    "motorway": (0.35, 0.65), "motorway_link": (0.30, 0.55),
    "trunk": (0.30, 0.60), "trunk_link": (0.25, 0.50),
    "primary": (0.25, 0.55), "primary_link": (0.20, 0.45),
    "secondary": (0.20, 0.45), "secondary_link": (0.15, 0.40),
    "tertiary": (0.10, 0.30), "tertiary_link": (0.10, 0.25),
    "residential": (0.05, 0.20), "living_street": (0.05, 0.15),
    "unclassified": (0.05, 0.20), "service": (0.02, 0.10),
}
DEFAULT_BASELINE = (0.05, 0.20)


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def load_graph():
    """Loads the SAME truncated graph the AI engine routes on - no separate
    Overpass download, no separate bbox logic. This guarantees the simulator
    is publishing congestion for exactly the edges main.py knows about,
    nothing more. main.py must be run at least once first to produce this
    cache file."""
    truncated_cache = os.path.join(
        os.path.dirname(__file__), "..", "python-ai-engine", "hyderabad_graph_truncated.graphml"
    )
    if not os.path.exists(truncated_cache):
        raise FileNotFoundError(
            f"'{truncated_cache}' not found. Start the Python AI engine (main.py) "
            f"first so it builds the truncated graph cache, then start the simulator."
        )
    logger.info(f"Loading truncated road network from cache '{truncated_cache}'...")
    G = ox.load_graphml(truncated_cache)
    logger.info(f"Simulator graph loaded: {len(G.nodes)} nodes, {len(G.edges)} edges.")
    return G


def hotspot_boost(mid_lat, mid_lon) -> float:
    boost = 0.0
    for (h_lat, h_lon, radius_m, peak) in HOTSPOTS:
        d = haversine_m(mid_lat, mid_lon, h_lat, h_lon)
        if d < radius_m:
            decay = 1.0 - (d / radius_m)  # linear decay to 0 at radius edge
            boost = max(boost, peak * decay)
    return boost


def find_override_edges(G):
    """Match each hardcoded demo-override entry to its nearest real edge,
    once, at startup. Returns {(u, v): forced_score}."""
    overrides = {}
    for name, cfg in DEMO_ROAD_OVERRIDES.items():
        lat, lon = cfg["coords"]
        try:
            u, v, _ = ox.distance.nearest_edges(G, lon, lat)
            overrides[(u, v)] = cfg["score"]
            logger.info(f"Demo override '{name}' bound to edge ({u}, {v}) -> forced score {cfg['score']}")
        except Exception as e:
            logger.warning(f"Could not bind demo override '{name}': {e}")
    return overrides


def compute_cycle(G, override_edges, previous_scores: dict) -> dict:
    """previous_scores persists across calls (passed in from main()'s loop)
    so each edge's score moves gradually toward a freshly-sampled target
    each cycle (exponential smoothing) instead of being fully re-randomized
    every 12s. Fully independent re-randomization per edge per cycle is what
    made the map look like it was flickering/glitching - adjacent road
    segments could swing to completely different colors every update with
    no visual continuity. SMOOTHING_FACTOR close to 1 = slow, smooth drift;
    close to 0 = jumps straight to the new random value every cycle."""
    SMOOTHING_FACTOR = 0.6
    edges_payload = {}

    for u, v, data in G.edges(data=True):
        key = f"{u}_{v}"

        if (u, v) in override_edges:
            score = override_edges[(u, v)]
        else:
            highway = data.get("highway")
            if isinstance(highway, list):
                highway = highway[0]
            lo, hi = ROAD_CLASS_BASELINE.get(highway, DEFAULT_BASELINE)
            baseline = random.uniform(lo, hi)

            mid_lat = (G.nodes[u]["y"] + G.nodes[v]["y"]) / 2
            mid_lon = (G.nodes[u]["x"] + G.nodes[v]["x"]) / 2
            boost = hotspot_boost(mid_lat, mid_lon)

            target = max(0.0, min(1.0, baseline + boost))
            prev = previous_scores.get(key, target)
            score = SMOOTHING_FACTOR * prev + (1 - SMOOTHING_FACTOR) * target

        score = round(score, 3)
        edges_payload[key] = score
        previous_scores[key] = score

    return {"event_type": "CONGESTION_UPDATE", "edges": edges_payload, "timestamp": time.time()}


def main():
    G = load_graph()
    override_edges = find_override_edges(G)

    producer = None
    while producer is None:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info(f"Connected to Kafka Broker at {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.warning(f"Kafka broker unreachable ({e}). Retrying in 10s - simulator will not publish until connected.")
            time.sleep(10)

    logger.info(f"Starting simulation loop, publishing every {CYCLE_SECONDS}s to '{TOPIC_TRAFFIC}'.")
    previous_scores: dict = {}  # persists across cycles for smoothing
    while True:
        cycle_start = time.time()
        try:
            payload = compute_cycle(G, override_edges, previous_scores)
            producer.send(TOPIC_TRAFFIC, payload)
            # No flush() here either - let the producer batch/send on its own
            # schedule rather than blocking this loop on a broker round trip.
            logger.info(f"Published congestion for {len(payload['edges'])} edges.")
        except Exception as e:
            logger.warning(f"Cycle failed: {e}")

        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, CYCLE_SECONDS - elapsed))


if __name__ == "__main__":
    main()