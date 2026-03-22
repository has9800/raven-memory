from __future__ import annotations

from langchain_core.tools import tool


@tool
def run_simulation(sim_name: str, duration_seconds: int = 60) -> dict:
    """Run a named simulation. Returns fake results after simulated delay."""
    return {
        "status": "completed",
        "sim": sim_name,
        "duration": duration_seconds,
        "result": f"drag_coefficient=0.23 for {sim_name}",
        "output_path": f"/workspace/results/{sim_name}/",
    }


@tool
def set_lights(state: str) -> dict:
    """Set lab lights on or off. state must be 'on' or 'off'."""
    return {"lights": state, "status": "ok"}


@tool
def read_file(path: str) -> dict:
    """Read a file from the workspace."""
    return {"path": path, "contents": f"[simulated contents of {path}]"}


@tool
def write_note(text: str) -> dict:
    """Write a note to the project log."""
    return {"noted": text, "status": "ok"}


TOOLS = [run_simulation, set_lights, read_file, write_note]
TOOL_MAP = {t.name: t for t in TOOLS}
