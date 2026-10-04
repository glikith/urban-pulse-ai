# config.py
# Shared constants used by both the Python AI engine and the traffic simulator.

# --- West Hyderabad bounding box ---
# Coordinates as (north, south, east, west) for ox.graph_from_bbox
# Widened from the earlier tight box, which excluded KPHB/Kukatpally
# (north cutoff too low) - nearest_nodes was silently snapping those
# queries to the wrong part of the graph instead of erroring.
# Covers Kukatpally/KPHB/Pragathi Nagar down to Kokapet/Narsingi,
# Ameerpet/Punjagutta across to Nanakramguda. (Bachupally, at 17.535, is
# the one demo location still just outside this - drop it from demos
# or push north to 17.55 if you need it.)
# Corridor bbox for truncating the full Hyderabad graph down to a
# routable-size subgraph: KPHB (north), Gachibowli (south),
# BHEL Township (west). East boundary pulled back from Bharath Nagar to
# Ameerpet/Punjagutta - Bharath Nagar sits right against dense central
# Hyderabad/Secunderabad, which was dragging in a disproportionate amount
# of intersections (64K nodes) and making Yen's algorithm painfully slow.
north, south, east, west = 17.5000, 17.4200, 78.4600, 78.3000
BBOX = (north, south, east, west)

KAFKA_BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
TOPIC_TRAFFIC = "live-traffic-updates"
TOPIC_ROUTE_DECISIONS = "route-decisions"

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:3b-instruct"

# Demo locations inside the bbox. Fallback for geocoding so demos don't
# depend on network access. All coordinates are within the bbox above.
DEMO_LOCATIONS = {
    "kukatpally": (17.4849, 78.4079),
    "kphb": (17.4863, 78.3920),
    "pragathi nagar": (17.5143, 78.4011),
    "miyapur": (17.4966, 78.3540),
    "bachupally": (17.5350, 78.3660),
    "gachibowli": (17.4401, 78.3489),
    "gachibowli flyover": (17.4437, 78.3487),
    "hitec city": (17.4474, 78.3762),
    "cyber towers": (17.4448, 78.3775),
    "madhapur": (17.4483, 78.3915),
    "kondapur": (17.4614, 78.3671),
    "jubilee hills": (17.4325, 78.4071),
    "apollo jubilee hills": (17.4172, 78.4101),
    "road no 36 jubilee hills": (17.4229, 78.4078),
    "banjara hills": (17.4156, 78.4347),
    "ameerpet": (17.4374, 78.4483),
    "punjagutta": (17.4258, 78.4522),
    "kokapet": (17.3949, 78.3370),
    "narsingi": (17.3901, 78.3477),
    "nanakramguda": (17.4180, 78.3390),
    "financial district": (17.4142, 78.3406),
    "cable bridge": (17.4300, 78.3916),
    "ikea hyderabad": (17.4399, 78.3761),
    "botanical garden": (17.4568, 78.3823),
    "jntu hitec city connector": (17.4930, 78.3910),
    "jntu hyderabad": (17.4933, 78.3915),
}

# Hotspot centers for congestion boost model: (lat, lon, radius_m, peak_boost)
HOTSPOTS = [
    (17.4474, 78.3762, 1400, 0.55),  # HITEC City
    (17.4374, 78.4483, 1200, 0.50),  # Ameerpet
    (17.4401, 78.3489, 1100, 0.45),  # Gachibowli
    (17.4483, 78.3915, 900, 0.40),   # Madhapur
    (17.4849, 78.4079, 1000, 0.35),  # Kukatpally
]

# Demo-control roads: hardcoded congestion overrides for presenter control
# matched to nearest edge at simulator startup.
DEMO_ROAD_OVERRIDES = {
    "jntu_hitec_connector": {"coords": (17.4930, 78.3910), "score": 0.9},
    "gachibowli_flyover": {"coords": (17.4437, 78.3487), "score": 0.75},
}

# Road classes that count as "major" for map coloring
MAJOR_HIGHWAY_TAGS = {"motorway", "trunk", "primary", "secondary",
                       "motorway_link", "trunk_link", "primary_link", "secondary_link"}

DECISION_LOG_PATH = "route_decisions.log"