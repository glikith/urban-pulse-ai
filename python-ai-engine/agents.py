# agents.py
# Two-agent sequential pipeline (no back-and-forth):
#   Agent 1 (Arbiter)  - picks the best of k candidate routes given congestion/roadblock context
#   Agent 2 (Explainer) - writes the XAI explanation for Agent 1's pick
#
# Built on CrewAI with a local Ollama (Qwen) model as the LLM. Both agents run
# with process=Process.sequential so Agent 2 only ever sees Agent 1's final
# output, not the raw candidate data - keeps latency and prompt size down.
#
# Falls back to a deterministic rule-based picker + templated explanation if
# CrewAI/Ollama can't be reached, so a demo never hard-fails on this step.

import json
import logging
import requests

from config import OLLAMA_URL, OLLAMA_MODEL

logger = logging.getLogger("UrbanPulse-AI.agents")

_crewai_available = True
try:
    from crewai import Agent, Task, Crew, Process, LLM
except Exception as e:
    logger.warning(f"crewai not available ({e}); agent pipeline will use rule-based fallback only.")
    _crewai_available = False


def _ollama_reachable() -> bool:
    try:
        requests.get(OLLAMA_URL.replace("/api/generate", "/api/tags"), timeout=2)
        return True
    except Exception:
        return False


def _fallback_pick(candidates: list[dict]) -> tuple[int, str]:
    """Rule-based: pick the candidate with the lowest weighted travel time
    that doesn't cross a blocked edge (candidates are pre-filtered upstream,
    so here it's just picking the min-cost one and saying why)."""
    best_idx = min(range(len(candidates)), key=lambda i: candidates[i]["weighted_travel_time"])
    reason = (
        f"Selected candidate {best_idx} as it has the lowest congestion-weighted "
        f"travel time ({candidates[best_idx]['weighted_travel_time']:.0f}s) among "
        f"{len(candidates)} viable candidates."
    )
    return best_idx, reason


def _fallback_explanation(emergency_type, start, dest, detour_active, selection_reason, congestion_considered) -> str:
    if detour_active:
        return (
            f"Route recalculated for the {emergency_type.lower()} due to an active roadblock on the direct "
            f"corridor to {dest}. {selection_reason} Priority clearance has been requested along this path."
        )
    if congestion_considered:
        return (
            f"Optimal corridor selected from {start} to {dest} after weighing current live congestion. "
            f"{selection_reason} No roadblocks are active on this path."
        )
    return (
        f"Direct optimal route calculated from {start} to {dest} with no active congestion or roadblocks "
        f"affecting the corridor. Dynamic priority clearance is in effect."
    )


def _build_llm():
    return LLM(model=f"ollama/{OLLAMA_MODEL}", base_url="http://localhost:11434")


def run_arbitration_pipeline(
    emergency_type: str,
    start: str,
    dest: str,
    candidates: list[dict],
    detour_active: bool,
    congestion_considered: bool,
) -> dict:
    """
    candidates: list of dicts, each a lightweight summary of one k-shortest-path
    candidate: {index, node_count, edge_count, base_travel_time, weighted_travel_time,
                congested_edge_count, crosses_hotspot}
    Returns: {"selected_index": int, "selection_reason": str, "xai_explanation": str, "engine": "crewai"|"fallback"}
    """
    if not candidates:
        raise ValueError("No candidates provided to arbitration pipeline")

    if len(candidates) == 1 or not _crewai_available or not _ollama_reachable():
        idx, reason = _fallback_pick(candidates)
        explanation = _fallback_explanation(emergency_type, start, dest, detour_active, reason, congestion_considered)
        return {"selected_index": idx, "selection_reason": reason, "xai_explanation": explanation, "engine": "fallback"}

    try:
        llm = _build_llm()

        candidates_json = json.dumps(candidates, indent=2)

        arbiter = Agent(
            role="Emergency Route Arbiter",
            goal="Pick the single best candidate route for an emergency vehicle from a short list, "
                 "prioritizing lowest arrival time while avoiding heavily congested edges.",
            backstory="You are a dispatch AI for Hyderabad emergency services. You never invent data; "
                      "you only reason over the candidate list you are given.",
            llm=llm,
            verbose=False,
        )
        explainer = Agent(
            role="XAI Dispatch Communicator",
            goal="Explain, in 2-3 plain sentences a driver can hear over radio, why a route was chosen.",
            backstory="You translate routing decisions into calm, confident dispatch language.",
            llm=llm,
            verbose=False,
        )

        pick_task = Task(
            description=(
                f"Emergency type: {emergency_type}. From: {start}. To: {dest}. "
                f"Detour active due to roadblock: {detour_active}. "
                f"Candidate routes (JSON):\n{candidates_json}\n\n"
                "Choose the single best candidate index. Respond with ONLY a JSON object: "
                '{"selected_index": <int>, "selection_reason": "<one sentence, plain English>"}'
            ),
            expected_output='A JSON object with selected_index and selection_reason.',
            agent=arbiter,
        )
        explain_task = Task(
            description=(
                f"Emergency type: {emergency_type}. From: {start}. To: {dest}. "
                f"Detour active: {detour_active}. Live congestion considered: {congestion_considered}. "
                "Using the previous task's selected route and reason, write a 2-3 sentence explanation "
                "for the driver confirming the route and why it was picked. Plain text only, no JSON."
            ),
            expected_output="2-3 sentence plain-text explanation.",
            agent=explainer,
            context=[pick_task],
        )

        crew = Crew(agents=[arbiter, explainer], tasks=[pick_task, explain_task], process=Process.sequential)
        crew.kickoff()

        pick_raw = str(pick_task.output).strip()
        try:
            start_brace = pick_raw.index("{")
            end_brace = pick_raw.rindex("}") + 1
            pick_data = json.loads(pick_raw[start_brace:end_brace])
            selected_index = int(pick_data["selected_index"])
            if not (0 <= selected_index < len(candidates)):
                raise ValueError("index out of range")
            selection_reason = str(pick_data.get("selection_reason", "")).strip()
        except Exception as parse_err:
            logger.warning(f"Could not parse arbiter output ({parse_err}); falling back to rule-based pick.")
            selected_index, selection_reason = _fallback_pick(candidates)

        explanation = str(explain_task.output).strip()
        if not explanation:
            explanation = _fallback_explanation(emergency_type, start, dest, detour_active, selection_reason, congestion_considered)

        return {
            "selected_index": selected_index,
            "selection_reason": selection_reason or "Selected by AI arbiter.",
            "xai_explanation": explanation,
            "engine": "crewai",
        }

    except Exception as e:
        logger.warning(f"CrewAI/Ollama pipeline failed ({e}); falling back to rule-based pick.")
        idx, reason = _fallback_pick(candidates)
        explanation = _fallback_explanation(emergency_type, start, dest, detour_active, reason, congestion_considered)
        return {"selected_index": idx, "selection_reason": reason, "xai_explanation": explanation, "engine": "fallback"}
