from __future__ import annotations

import os

from tcc.core.dag import TaskDAG
from tcc.core.reconciler import SessionReconciler
from tcc.core.store import TCCStore

from .adversarial import sample_noise_event, should_crash_session, should_inject_noise
from .locomo_loader import LoCoMoExample


def ingest_example(
    example: LoCoMoExample,
    db_path: str,
    mode: str = "standard",
    noise_ratio: float = 0.2,
    n_recent_inject: int = 10,
) -> tuple[TaskDAG, SessionReconciler, TCCStore]:
    """
    Convert a LoCoMo example into a Raven TCC chain.

    Args:
        example: LoCoMoExample to ingest
        db_path: path to SQLite database for this example
        mode: "standard" or "adversarial"
        noise_ratio: fraction of sessions to inject noise nodes (adversarial)
        n_recent_inject: how many recent nodes to inject at session start

    Returns:
        (dag, reconciler, store) ready for querying
    """
    store = TCCStore(db_path)
    dag = TaskDAG(store)
    reconciler = SessionReconciler()

    root_session = "persona_setup"
    if example.persona_a:
        dag.root(
            event=f"persona A: {example.persona_a}",
            actor="system",
            plan="background context",
            context={"type": "persona"},
            session_id=root_session,
        )
    if example.persona_b:
        dag.append(
            event=f"persona B: {example.persona_b}",
            actor="system",
            plan="background context",
            context={"type": "persona"},
            session_id=root_session,
        )

    sessions: dict[int, list] = {}
    for turn in example.turns:
        sessions.setdefault(turn.session_idx, []).append(turn)

    for sess_idx in sorted(sessions.keys()):
        session_turns = sessions[sess_idx]
        session_ctx = reconciler.start_session(dag, n_recent=n_recent_inject)
        session_id = session_ctx["session_id"]

        if mode == "adversarial" and should_inject_noise(noise_ratio):
            _inject_noise_nodes(dag, session_id)

        for turn in session_turns:
            dag.append(
                event=turn.text[:500],
                actor="user" if turn.speaker == "human" else "agent",
                plan=f"session {sess_idx} turn {turn.turn_idx}",
                context={
                    "session_idx": sess_idx,
                    "turn_idx": turn.turn_idx,
                    "locomo_timestamp": turn.timestamp,
                },
                session_id=session_id,
                status="confirmed",
            )

        crash = mode == "adversarial" and should_crash_session(crash_rate=0.3)
        if not crash:
            reconciler.end_session(dag, session_id)

    return dag, reconciler, store


def _inject_noise_nodes(dag: TaskDAG, session_id: str) -> None:
    """Inject irrelevant filler nodes to test semantic search quality."""
    dag.append(
        event=sample_noise_event(),
        actor="user",
        plan="routine activity",
        context={"type": "noise"},
        session_id=session_id,
        status="confirmed",
    )


def create_example_db(example_id: str, tmp_dir: str) -> str:
    """Return a unique db path for a given example."""
    return os.path.join(tmp_dir, f"locomo_{example_id}.db")
