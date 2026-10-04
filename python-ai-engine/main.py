import json
import logging
import math
import os
import threading
import time
import uuid
from itertools import islice
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from geopy.geocoders import Photon
import osmnx as ox
import networkx as nx

# Local OSM extract (clipped to West Hyderabad via osmconvert), loaded
# directly - no Overpass API calls, no network dependency at startup.
LOCAL_OSM_FILE = os.path.join(os.path.dirname(__file__), "data", "hyd_region.osm")

# EMERGENCY FALLBACK: set to True to skip OSM entirely and use a small
# hand-built mesh graph over DEMO_LOCATIONS instead. Zero file/network
# dependency, loads instantly. Use only if the real OSM graph is broken
# right before a demo - straight-line "roads" instead of real streets.
USE_SYNTHETIC_GRAPH = False

from config import (
    BBOX, KAFKA_BOOTSTRAP_SERVERS, TOPIC_TRAFFIC, TOPIC_ROUTE_DECISIONS,
    DEMO_LOCATIONS, DECISION_LOG_PATH,
)
import traffic_state
from decision_log import log_decision
import agents

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UrbanPulse-AI")

# Graph is loaded asynchronously in background so FastAPI server starts immediately
G = None
DG = None
_graph_ready = threading.Event()


def _load_graph_async():
    """Load the graph in a background thread so server starts immediately."""
    global G, DG
    try:
        if USE_SYNTHETIC_GRAPH:
            from synthetic_graph import build_synthetic_graph
            logger.info("USE_SYNTHETIC_GRAPH=True - building hand-built mesh graph, skipping OSM entirely.")
            G = build_synthetic_graph()
            logger.info(f"Synthetic graph built: {len(G.nodes)} nodes, {len(G.edges)} edges.")
            # Collapse to a plain DiGraph the same way the real OSM path does,
            # keeping the cheapest edge between any duplicate pair.
            DG = nx.DiGraph()
            for u, v, data in G.edges(data=True):
                if DG.has_edge(u, v):
                    if data["travel_time"] < DG[u][v]["travel_time"]:
                        DG[u][v].update(data)
                else:
                    DG.add_edge(u, v, **data)
            for n, data in G.nodes(data=True):
                DG.add_node(n, **data)
            _graph_ready.set()
            logger.info("Synthetic graph ready - routing is now available.")
            return

        # FULL_CACHE_FILE is the original, expensive-to-produce full-metro
        # graph - never overwritten, never deleted automatically, so a
        # truncation experiment can never force a redo of that multi-hour
        # XML parse. TRUNCATED_CACHE_FILE is the fast-loading, routable-size
        # subgraph actually used for routing from here on.
        FULL_CACHE_FILE = "hyderabad_graph.graphml"
        TRUNCATED_CACHE_FILE = "hyderabad_graph_truncated.graphml"

        if os.path.exists(TRUNCATED_CACHE_FILE):
            logger.info(f"Loading truncated road network from cache '{TRUNCATED_CACHE_FILE}'...")
            G = ox.load_graphml(TRUNCATED_CACHE_FILE)
            logger.info(f"Truncated road network loaded: {len(G.nodes)} nodes, {len(G.edges)} edges.")
        else:
            if os.path.exists(FULL_CACHE_FILE):
                logger.info(f"Loading full road network from cache '{FULL_CACHE_FILE}'...")
                G = ox.load_graphml(FULL_CACHE_FILE)
                logger.info(f"Full road network loaded: {len(G.nodes)} nodes, {len(G.edges)} edges.")
            else:
                if not os.path.exists(LOCAL_OSM_FILE):
                    raise FileNotFoundError(
                        f"Local OSM extract not found at '{LOCAL_OSM_FILE}'. "
                        f"Place hyderabad-clip.osm in python-ai-engine/data/."
                    )
                logger.info(f"Loading West Hyderabad road network from local file '{LOCAL_OSM_FILE}'...")
                G = ox.graph_from_xml(LOCAL_OSM_FILE, bidirectional=False, simplify=True)

                # OSMnx 2.x's add_edge_speeds can still leave some edges with null
                # speed_kph/length even with fallback= set, depending on tag
                # quirks in this extract. Compute both manually instead - every
                # edge gets a real value, no dependency on OSMnx's internal
                # imputation logic at all.
                DEFAULT_SPEED_KMPH = 30
                for u, v, k, data in G.edges(keys=True, data=True):
                    length_m = data.get("length")
                    if length_m is None or not isinstance(length_m, (int, float)):
                        uy, ux = G.nodes[u]["y"], G.nodes[u]["x"]
                        vy, vx = G.nodes[v]["y"], G.nodes[v]["x"]
                        # quick equirectangular approximation, fine at city scale
                        dy = (vy - uy) * 111320
                        dx = (vx - ux) * 111320 * math.cos(math.radians((uy + vy) / 2))
                        length_m = max(1.0, math.hypot(dx, dy))
                        data["length"] = length_m
                    data["speed_kph"] = DEFAULT_SPEED_KMPH
                    data["travel_time"] = length_m / (DEFAULT_SPEED_KMPH * 1000 / 3600)

                ox.save_graphml(G, FULL_CACHE_FILE)
                logger.info(f"Full road network loaded and cached: {len(G.nodes)} nodes, {len(G.edges)} edges.")

            # Truncate the full graph down to the routable West Hyderabad
            # corridor. truncate_by_edge=True keeps an edge if EITHER endpoint
            # is inside the bbox, avoiding the "edge missing node" style gaps
            # we hit earlier with hard clipping.
            logger.info(f"Truncating graph to bbox {BBOX} ...")
            G = ox.truncate.truncate_graph_bbox(G, BBOX, truncate_by_edge=True)
            logger.info(f"Truncated graph: {len(G.nodes)} nodes, {len(G.edges)} edges.")
            ox.save_graphml(G, TRUNCATED_CACHE_FILE)
            logger.info(f"Truncated graph cached to '{TRUNCATED_CACHE_FILE}'.")
        
        DG = ox.convert.to_digraph(G, weight="travel_time")
        logger.info(f"Routing DiGraph built: {len(DG.nodes)} nodes, {len(DG.edges)} edges.")
        _graph_ready.set()
        logger.info("Graph load complete - routing is now available.")
    except Exception as e:
        logger.error(f"Graph load failed: {e}")
        _graph_ready.set()  # Signal completion even on failure so server doesn't hang forever

# Start graph load in background thread immediately
_graph_thread = threading.Thread(target=_load_graph_async, daemon=True, name="graph-loader")
_graph_thread.start()

app = FastAPI(title="UrbanPulse AI Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Kafka producer (roadblock/reset/route-decision events). Same graceful
# degradation pattern as the original code: if Kafka isn't up, log and
# continue in standalone mode rather than crashing the whole engine.
# ---------------------------------------------------------------------------
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info(f"Connected to Kafka Broker at {KAFKA_BOOTSTRAP_SERVERS}")
except NoBrokersAvailable:
    logger.warning("Kafka broker connection failed. Running in standalone mode (events will not publish).")
    producer = None
except Exception as e:
    logger.warning(f"Kafka broker connection failed: {e}. Running in standalone mode.")
    producer = None


def kafka_send(topic: str, payload: dict):
    """Fire-and-forget: hands the message to the producer's internal buffer
    and returns immediately. No flush() here - flush() blocks the calling
    HTTP request until Kafka acks, which turns any broker-side hiccup
    (rebalance, slow coordinator) into multi-second request latency. The
    producer flushes its buffer on its own linger_ms interval in the
    background, or on process shutdown."""
    if producer:
        try:
            producer.send(topic, payload)
        except Exception as e:
            logger.warning(f"Kafka publish to '{topic}' failed: {e}")


# Start the congestion consumer thread (populates traffic_state.congestion_store)
traffic_state.start_consumer_thread()

geolocator = Photon(user_agent="urbanpulse_ai_live")


def geocode_location(query: str):
    """Converts a place name to (lat, lon). Checks the local demo dict first
    (fast, offline-safe), then falls back to live Photon geocoding with a
    short retry loop."""
    clean_query = query.lower().replace(", hyderabad", "").strip()
    if clean_query in DEMO_LOCATIONS:
        return DEMO_LOCATIONS[clean_query]

    full_query = f"{query}, Hyderabad, Telangana, India"
    for attempt in range(3):
        try:
            location = geolocator.geocode(full_query)
            if not location:
                location = geolocator.geocode(f"{query}, Hyderabad")
            if location:
                return location.latitude, location.longitude
        except Exception:
            time.sleep(1.5)

    raise HTTPException(status_code=404, detail=f"Could not locate '{query}'. Try a nearby landmark inside West Hyderabad.")


# Roadblocks are a HARD removal, keyed by (u, v) edge tuples
blocked_edges: set[tuple] = set()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class RoadblockClickRequest(BaseModel):
    latitude: float
    longitude: float
    incident_type: str = "roadblock"
    severity: str = "high"


class RouteRequest(BaseModel):
    start_location: str
    destination_location: str
    emergency_type: str = "Ambulance"
    k: int = 3


class ResetRequest(BaseModel):
    pass


# ---------------------------------------------------------------------------
# Congestion-aware weight function
# ---------------------------------------------------------------------------
def weighted_travel_time(u, v, data) -> float:
    # DG edges have a single dict (not the {key: data} form of a MultiDiGraph)
    base = data.get("travel_time", data.get("length", 1))
    return base * traffic_state.congestion_multiplier(u, v)


def build_active_graph() -> nx.DiGraph:
    """DG minus hard-blocked edges. Congestion is NOT applied here - it's
    applied at path-cost time via weighted_travel_time so k-shortest-paths
    can still explore routes that dip through moderately congested edges."""
    if not blocked_edges:
        return DG
    active = DG.copy()
    for (u, v) in blocked_edges:
        if active.has_edge(u, v):
            active.remove_edge(u, v)
        if active.has_edge(v, u):
            active.remove_edge(v, u)
    return active


def k_shortest_paths(graph: nx.DiGraph, source, target, k: int):
    """Yen's algorithm via networkx.shortest_simple_paths, weighted by
    congestion-aware travel time. Returns up to k node-path lists."""
    try:
        paths_gen = nx.shortest_simple_paths(graph, source, target, weight=weighted_travel_time)
        return list(islice(paths_gen, k))
    except nx.NetworkXNoPath:
        return []


def summarize_candidate(graph: nx.DiGraph, path_nodes: list, index: int) -> dict:
    base_time = 0.0
    weighted_time = 0.0
    congested_edges = 0
    for u, v in zip(path_nodes[:-1], path_nodes[1:]):
        data = graph.get_edge_data(u, v)
        t = data.get("travel_time", data.get("length", 1))
        base_time += t
        wt = t * traffic_state.congestion_multiplier(u, v)
        weighted_time += wt
        if traffic_state.get_congestion(u, v) >= 0.5:
            congested_edges += 1
    return {
        "index": index,
        "node_count": len(path_nodes),
        "edge_count": len(path_nodes) - 1,
        "base_travel_time": round(base_time, 1),
        "weighted_travel_time": round(weighted_time, 1),
        "congested_edge_count": congested_edges,
    }


def path_to_coordinates(graph, path_nodes: list) -> list:
    return [[graph.nodes[n]["y"], graph.nodes[n]["x"]] for n in path_nodes]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    if not _graph_ready.is_set() or G is None or DG is None:
        return {
            "status": "LOADING",
            "kafka_connected": producer is not None,
        }
    return {
        "status": "OK",
        "nodes": len(G.nodes),
        "edges": len(G.edges),
        "blocked_edges": len(blocked_edges),
        "congestion_edges_tracked": traffic_state.snapshot_size(),
        "kafka_connected": producer is not None,
    }


@app.get("/api/status")
def get_status():
    """Lightweight endpoint for the frontend loading banner - just the
    graph_ready flag, polled every few seconds."""
    return {"graph_ready": _graph_ready.is_set() and G is not None and DG is not None}


@app.post("/api/roadblock")
def report_roadblock(req: RoadblockClickRequest):
    """Click-to-block: frontend sends the raw lat/lon under the admin's click."""
    if not _graph_ready.is_set():
        raise HTTPException(status_code=503, detail="Graph is still loading. Try again in a moment.")
    if G is None:
        raise HTTPException(status_code=500, detail="Graph load failed.")
    
    if USE_SYNTHETIC_GRAPH:
        from synthetic_graph import nearest_edge
        u, v, _ = nearest_edge(G, req.longitude, req.latitude)
    else:
        u, v, _ = ox.distance.nearest_edges(G, req.longitude, req.latitude)
    blocked_edges.add((u, v))

    edge_data = G.get_edge_data(u, v)
    road_name = None
    if edge_data:
        first = list(edge_data.values())[0]
        road_name = first.get("name")
        if isinstance(road_name, list):
            road_name = road_name[0]

    mid_lat = (G.nodes[u]["y"] + G.nodes[v]["y"]) / 2
    mid_lon = (G.nodes[u]["x"] + G.nodes[v]["x"]) / 2

    event_payload = {
        "event_type": "ROADBLOCK_INJECTED",
        "u": u,
        "v": v,
        "road_name": road_name or "Unnamed road",
        "latitude": mid_lat,
        "longitude": mid_lon,
        "severity": req.severity,
        "type": req.incident_type,
    }
    kafka_send(TOPIC_TRAFFIC, event_payload)
    logger.info(f"Roadblock added on edge ({u}, {v}) [{road_name}]")
    return {"status": "SUCCESS", "data": event_payload}


@app.get("/api/roadblocks")
def get_active_roadblocks():
    """Returns currently blocked edges as map markers (midpoint of each edge)."""
    if G is None:
        return {"roadblocks": []}
    markers = []
    for (u, v) in blocked_edges:
        if not G.has_node(u) or not G.has_node(v):
            continue
        mid_lat = (G.nodes[u]["y"] + G.nodes[v]["y"]) / 2
        mid_lon = (G.nodes[u]["x"] + G.nodes[v]["x"]) / 2
        markers.append({"u": u, "v": v, "latitude": mid_lat, "longitude": mid_lon})
    return {"roadblocks": markers}


@app.post("/api/reset")
def reset_grid():
    """Clears all roadblocks. (Congestion state is reset separately by the
    simulator/engine's own GRID_RESET handling in traffic_state.py.)"""
    blocked_edges.clear()
    kafka_send(TOPIC_TRAFFIC, {"event_type": "GRID_RESET"})
    return {"status": "GRID_RESET_COMPLETE"}


@app.get("/api/congestion")
def get_congestion_snapshot():
    """Major-road congestion snapshot for map coloring (Section 6)."""
    if G is None:
        return {"edges": []}
    edges_out = []
    for u, v, data in G.edges(data=True):
        highway = data.get("highway")
        if isinstance(highway, list):
            highway = highway[0]
        from config import MAJOR_HIGHWAY_TAGS
        if highway not in MAJOR_HIGHWAY_TAGS:
            continue
        score = traffic_state.get_congestion(u, v)
        if score <= 0:
            continue
        edges_out.append({
            "u": u, "v": v,
            "u_lat": G.nodes[u]["y"], "u_lon": G.nodes[u]["x"],
            "v_lat": G.nodes[v]["y"], "v_lon": G.nodes[v]["x"],
            "score": round(score, 2),
        })
    return {"edges": edges_out}


@app.post("/api/route")
def calculate_route(req: RouteRequest):
    """Fast path: geocode -> k-shortest-paths on the congestion-weighted,
    roadblock-pruned graph -> 2-agent arbitration -> logged decision."""
    if not _graph_ready.is_set():
        raise HTTPException(status_code=503, detail="Graph is still loading from OSM. Try again in a moment.")
    if G is None or DG is None:
        raise HTTPException(status_code=500, detail="Graph load failed. Check server logs.")
    
    request_id = str(uuid.uuid4())

    start_lat, start_lon = geocode_location(req.start_location)
    dest_lat, dest_lon = geocode_location(req.destination_location)

    if USE_SYNTHETIC_GRAPH:
        from synthetic_graph import nearest_node
        start_node = nearest_node(G, start_lon, start_lat)
        dest_node = nearest_node(G, dest_lon, dest_lat)
    else:
        start_node = ox.distance.nearest_nodes(G, start_lon, start_lat)
        dest_node = ox.distance.nearest_nodes(G, dest_lon, dest_lat)

    active_graph = build_active_graph()
    detour_active = len(blocked_edges) > 0

    path_list = k_shortest_paths(active_graph, start_node, dest_node, max(1, req.k))
    if not path_list:
        path_list = k_shortest_paths(DG, start_node, dest_node, max(1, req.k))
        detour_active = False
        if not path_list:
            raise HTTPException(status_code=404, detail="No route could be found between these locations.")

    candidates = [summarize_candidate(active_graph if active_graph.has_node(start_node) else DG, p, i)
                  for i, p in enumerate(path_list)]
    congestion_considered = any(c["congested_edge_count"] > 0 for c in candidates)

    arbitration = agents.run_arbitration_pipeline(
        emergency_type=req.emergency_type,
        start=req.start_location,
        dest=req.destination_location,
        candidates=candidates,
        detour_active=detour_active,
        congestion_considered=congestion_considered,
    )

    selected_index = arbitration["selected_index"]
    selected_path = path_list[selected_index]
    coordinates = path_to_coordinates(G, selected_path)

    response = {
        "request_id": request_id,
        "start": {"name": req.start_location, "coords": [start_lat, start_lon]},
        "destination": {"name": req.destination_location, "coords": [dest_lat, dest_lon]},
        "coordinates": coordinates,
        "base_coordinates": path_to_coordinates(G, path_list[0]),
        "detour_active": detour_active,
        "selected_index": selected_index,
        "selection_reason": arbitration["selection_reason"],
        "xai_explanation": arbitration["xai_explanation"],
        "candidate_count": len(candidates),
        "engine": arbitration["engine"],
    }

    log_decision({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "request_id": request_id,
        "start_location": req.start_location,
        "destination_location": req.destination_location,
        "candidate_routes": candidates,
        "selected_route_index": selected_index,
        "selection_reason": arbitration["selection_reason"],
        "xai_explanation": arbitration["xai_explanation"],
        "active_roadblocks": len(blocked_edges),
        "congestion_factor_considered": congestion_considered,
    })

    kafka_send(TOPIC_ROUTE_DECISIONS, response)

    return response