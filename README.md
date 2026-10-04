# UrbanPulse AI

A decentralized, agentic system for explainable, real-time emergency vehicle routing in West Hyderabad. UrbanPulse AI combines classical graph algorithms (Yen's k-shortest-paths) with a local, offline multi-agent LLM pipeline (CrewAI + Ollama/Qwen) to generate congestion- and roadblock-aware routes for ambulances, accompanied by natural-language explanations for dispatchers and drivers.

Built for a hackathon/academic project at KL University, Hyderabad.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the System](#running-the-system)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Team](#team)

---

## Overview

Urban traffic gridlock introduces life-threatening delays during emergency medical response. Existing solutions polarize into two extremes: deterministic graph algorithms with no contextual reasoning or transparency, or opaque black-box ML models unsuited for high-stakes, time-critical decisions.

UrbanPulse AI bridges this gap with a two-stage hybrid design:

1. **Candidate Generation** — Yen's k-shortest-paths algorithm generates a distinct set of viable candidate routes (k=3) between origin and destination, with edge weights reflecting live simulated traffic conditions and hard-excluded blocked roads.
2. **Multi-Agent Arbitration** — A sequential two-agent pipeline (built with CrewAI, running against a locally-hosted Qwen model via Ollama) evaluates the candidates:
   - **Arbiter Agent** — selects the optimal route given congestion and roadblock context.
   - **Explainer Agent** — translates the selection into a human-readable justification for dispatchers and drivers.

The entire system runs **locally**, with no paid cloud API dependency. A deterministic rule-based fallback ensures routing never hard-fails even if the LLM pipeline is unreachable.

### Key Features

- **Hybrid routing**: classical graph algorithms for correctness + LLM agents for contextual reasoning and explainability
- **Real-time congestion simulation**: synthetic traffic generator publishing live updates over Kafka every ~12 seconds, with road-class baselines, spatial hotspot decay, and temporal smoothing
- **Click-to-block roadblocks**: dispatcher UI snaps map clicks to the nearest real road segment and hard-excludes it from routing
- **Explainable AI (XAI)**: every routing decision includes a natural-language justification, logged in an append-only audit trail
- **Offline-first**: no external LLM API calls; runs entirely on local infrastructure
- **Admin + Driver dashboards**: dispatcher-facing map with roadblock injection and congestion overlays, and a read-only driver-facing view with route + XAI display

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      React Frontend (Vite)                   │
│         Admin Dashboard          │       Driver Dashboard     │
│    (roadblocks, congestion)      │   (route + XAI insight)    │
└───────────────────┬───────────────────────────┬───────────────┘
                     │  HTTPS / WebSocket (STOMP)
┌────────────────────▼───────────────────────────▼───────────────┐
│                  Spring Boot Monolith Backend                   │
│          REST API Gateway  +  WebSocket Relay                   │
└────────────────────┬───────────────────────────┬───────────────┘
                      │  REST (routing/roadblocks)   │ Kafka consume/produce
┌─────────────────────▼───────────┐   ┌─────────────▼─────────────┐
│   Python FastAPI AI Engine      │   │     Apache Kafka            │
│  OSMnx + NetworkX graph routing │◄──┤  live-traffic-updates topic │
│  CrewAI agents + Ollama (Qwen)  │   │  route-decisions topic      │
└──────────────────────────────────┘   └─────────────▲──────────────┘
                                                        │
                                        ┌───────────────┴──────────────┐
                                        │   Traffic Simulator (Python)  │
                                        │  Synthetic congestion engine  │
                                        └────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React (Vite), Leaflet, Tailwind CSS, SockJS + STOMP |
| Backend | Spring Boot, Spring Kafka, Spring WebSocket |
| AI Engine | FastAPI, OSMnx, NetworkX, CrewAI, Ollama (Qwen) |
| Messaging | Apache Kafka (KRaft mode) |
| Routing Data | OpenStreetMap extract (via BBBike), OSMnx graph construction |
| Geocoding | Geopy (Photon), with a local demo-location fallback dictionary |
| Persistence | Append-only JSONL decision log (no database) |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.11–3.12 | 3.13 can be flaky with OSMnx wheels |
| Node.js | 18+ | for the frontend |
| Java (JDK) | 17 | for Spring Boot |
| Apache Kafka | 3.x | KRaft mode (no Zookeeper needed) |
| Ollama | latest | for local Qwen inference |

Pull the model once, ahead of time:
```bash
ollama pull qwen2.5:3b-instruct
```

---

## Setup

### 1. Clone and set up each module

```bash
git clone <your-repo-url>
cd urban-pulse-ai
```

**python-ai-engine**
```bash
cd python-ai-engine
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

**traffic-simulator**
```bash
cd ../traffic-simulator
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**spring-backend**
```bash
cd ../spring-backend
# No install step — ./mvnw (or mvnw.cmd) pulls dependencies on first run
```

**frontend**
```bash
cd ../frontend
npm install
```

### 2. Get the OSM road network data

The AI engine routes on a local OpenStreetMap extract. You need to provide this once:

1. Get a Hyderabad-region OSM extract — recommended via [BBBike Extract](https://extract.bbbike.org/) (format: **OSM XML 7z (xz)**).
2. Extract the `.xz` archive (7-Zip) to get a plain `.osm` file.
3. Place it at `python-ai-engine/data/hyd_region.osm` (or update `LOCAL_OSM_FILE` in `config.py` / `main.py` to match your filename).

On first run, `main.py` parses this file, builds the routing graph, truncates it to the configured West Hyderabad service corridor, and caches the result as `.graphml` files — subsequent restarts load instantly from cache.

### 3. Configure Kafka (local, KRaft mode)

```bash
# Format storage (one-time)
kafka-storage.sh random-uuid   # note the generated UUID
kafka-storage.sh format -t <uuid> -c config/kraft/server.properties

# Start broker
kafka-server-start.sh config/kraft/server.properties
```

---

## Running the System

Start each service **in this order**, in separate terminals:

1. **Kafka broker** — wait for `started (kafka.server.KafkaServer)`
2. **Ollama** — ensure `qwen2.5:3b-instruct` is pulled and the service is running
3. **Python AI engine**
   ```bash
   cd python-ai-engine
   venv\Scripts\activate
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```
   Wait for `Graph load complete - routing is now available.` Confirm with:
   ```bash
   curl http://localhost:8000/api/status
   ```
4. **Traffic simulator** (only after step 3 is fully ready — it loads the same truncated graph cache)
   ```bash
   cd traffic-simulator
   venv\Scripts\activate
   python simulator.py
   ```
5. **Spring backend**
   ```bash
   cd spring-backend
   ./mvnw spring-boot:run
   ```
6. **Frontend**
   ```bash
   cd frontend
   npm run dev
   ```
   Open the printed localhost URL. Navigate to `/admin` for the dispatcher view, `/driver` for the emergency-vehicle view.

---

## Usage

### Admin Dashboard (`/admin`)
- View live, color-coded congestion overlays on the road network (green → red)
- Click any road segment to select it, confirm to inject a roadblock
- Reset the grid to clear all active roadblocks

### Driver Dashboard (`/driver`)
- Enter a start location and destination (see demo-location list in `config.py`, e.g. `kphb`, `gachibowli`, `hitec city`, `madhapur`)
- Request a priority route — a fast preview renders instantly, then the AI-arbitrated route replaces it
- Read the XAI explanation panel for the reasoning behind the selected route

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `NoBrokersAvailable` warnings | Kafka not up yet | Start Kafka first; other services retry automatically |
| Route requests return `503` | Graph still loading | Poll `/api/status` until `graph_ready: true` |
| `engine: "fallback"` in every response | Ollama unreachable or model name mismatch | Check `ollama list`, confirm `OLLAMA_MODEL` matches exactly |
| Empty congestion overlay | Simulator not running, or graph mismatch | Ensure simulator started *after* the AI engine produced its truncated graph cache |
| Slow routing / roadblock injection | Graph too large for current bbox | Reduce `BBOX` in `config.py`, delete the truncated `.graphml` cache, restart |

---

## Team

**UrbanPulse AI** — Department of CSE, KL University, Hyderabad
- Gummadi Likith
- Akshaya Manda
- Gangotri Samhitha Sharma
