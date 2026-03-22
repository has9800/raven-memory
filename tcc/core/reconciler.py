from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Optional

from .dag import TaskDAG, _now
from .node import TCCNode


def _human_time(iso: str) -> str:
    try:
        then = datetime.fromisoformat(iso)
        now = datetime.now(timezone.utc)
        delta = now - then
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            return f"{seconds // 60} minutes ago"
        if seconds < 86400:
            return f"{seconds // 3600} hours ago"
        days = seconds // 86400
        if days == 1:
            return "yesterday"
        if days < 7:
            return f"{days} days ago"
        if days < 30:
            return f"{days // 7} weeks ago"
        if days < 365:
            return f"{days // 30} months ago"
        return f"{days // 365} years ago"
    except Exception:
        return iso


class SessionReconciler:

    def start_session(
        self,
        dag: TaskDAG,
        n_recent: int = 10,
        search_query: str | None = None,
        n_search: int = 5,
    ) -> dict:
        """
        Start a new session and return context for agent injection.

        Args:
            dag: The TaskDAG to use
            n_recent: Number of recent nodes to include
            search_query: Optional semantic search query to find relevant
                          historical nodes beyond the recent window
            n_search: Number of semantic search results to include
        """
        session_id = uuid.uuid4().hex[:12]
        tip = dag.tip()
        is_fresh = tip is None

        if is_fresh:
            summary = "No history yet. Starting fresh."
        else:
            summary = self._build_summary(
                dag,
                tip,
                n_recent=n_recent,
                search_query=search_query,
                n_search=n_search,
            )

        return {
            "session_id": session_id,
            "summary": summary,
            "tip": tip,
            "is_fresh": is_fresh,
        }

    def _build_summary(
        self,
        dag: TaskDAG,
        tip: TCCNode,
        n_recent: int = 10,
        search_query: str | None = None,
        n_search: int = 5,
    ) -> str:
        recent = dag.recent(n_recent)
        lines = []
        lines.append(f"Last active: {_human_time(tip.timestamp)}")
        lines.append("")
        lines.append("Recent events:")
        for node in recent[:7]:
            lines.append(f"  [{node.actor}] {node.event} ({_human_time(node.timestamp)})")
            if node.plan:
                lines.append(f"    plan: {node.plan}")

        ctx = tip.context
        if ctx.get("open_threads"):
            lines.append("")
            lines.append(f"Open threads: {', '.join(ctx['open_threads'])}")
        if ctx.get("relevant_paths"):
            lines.append(f"Relevant paths: {', '.join(ctx['relevant_paths'])}")
        if ctx.get("notes"):
            lines.append(f"Notes: {ctx['notes']}")

        semantic_nodes = []
        if search_query and dag._store.is_vec_enabled:
            semantic_nodes = dag._store.search(search_query, n=n_search)
            recent_hashes = {n.hash for n in recent}
            semantic_nodes = [n for n in semantic_nodes if n.hash not in recent_hashes]

        if semantic_nodes:
            lines.append("")
            lines.append("Relevant historical context:")
            for node in semantic_nodes:
                lines.append(f"  [{node.actor}] {node.event}")

        return "\n".join(lines)

    def end_session(self, dag: TaskDAG, session_id: str,
                    notes: str = "") -> Optional[TCCNode]:
        session_nodes = dag.since(session_id)
        if not session_nodes:
            return None
        return dag.append(
            event="session ended",
            actor="system",
            plan="end of session",
            context={"notes": notes, "session_id": session_id},
            session_id=session_id,
            status="confirmed",
        )

    def record_event(self, dag: TaskDAG, session_id: str, event: str,
                     actor: str, plan: str, context: dict,
                     tool_call=None, status: str = "confirmed") -> TCCNode:
        if dag.tip() is None:
            return dag.root(event, actor, plan, context, session_id, tool_call)
        return dag.append(event, actor, plan, context, session_id,
                          tool_call=tool_call, status=status)

    def record_note(self, dag: TaskDAG, session_id: str, text: str,
                    actor: str = "user") -> TCCNode:
        return self.record_event(
            dag, session_id, text, actor, plan="",
            context={"type": "note"},
        )

    def record_tool_call(self, dag: TaskDAG, session_id: str,
                         tool: str, params: dict, result: dict,
                         status: str = "confirmed") -> TCCNode:
        return self.record_event(
            dag, session_id,
            event=f"{tool} called",
            actor="tool",
            plan="",
            context={"tool": tool, "params": params, "result": result},
            tool_call={"tool": tool, "params": params, "result": result},
            status=status,
        )
