from __future__ import annotations

from tcc.core.dag import TaskDAG
from tcc.core.reconciler import SessionReconciler


class TCCInterceptor:
    def __init__(self, dag: TaskDAG, session_id: str):
        self.dag = dag
        self.session_id = session_id
        self.reconciler = SessionReconciler()

    def record_tool_call(
        self,
        tool_name: str,
        params: dict,
        result: dict,
        plan: str = "",
    ) -> None:
        """Called automatically after every tool execution."""
        context = {
            "tool": tool_name,
            "result": result,
            "open_threads": [],
        }
        if tool_name == "write_note":
            context["notes"] = params.get("text", "")

        if "output_path" in result:
            context["relevant_paths"] = [result["output_path"]]

        self.reconciler.record_tool_call(
            self.dag,
            self.session_id,
            tool=tool_name,
            params=params,
            result=result,
            status="confirmed" if result.get("status") != "error" else "failed",
        )

    def record_user_event(self, event: str, context: dict | None = None) -> None:
        """Record a user action or observation."""
        self.reconciler.record_event(
            self.dag,
            self.session_id,
            event=event,
            actor="user",
            plan="",
            context=context or {},
        )

    def record_agent_decision(self, decision: str, reasoning: str = "") -> None:
        """Record an agent decision or note."""
        self.reconciler.record_event(
            self.dag,
            self.session_id,
            event=decision,
            actor="agent",
            plan=reasoning,
            context={"type": "decision"},
        )
