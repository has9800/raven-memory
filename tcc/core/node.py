from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class TCCNode:
    hash: str
    parent_hashes: tuple[str, ...]
    timestamp: str
    event: str
    actor: str
    status: str
    plan: str
    tool_call: Optional[dict]
    context: dict
    session_id: str
    branch_id: str
    metadata: dict = field(default_factory=dict)

    @staticmethod
    def compute_hash(
        parent_hashes: tuple[str, ...],
        event: str,
        timestamp: str,
        actor: str,
    ) -> str:
        content = json.dumps(
            {
                "parents": sorted(parent_hashes),
                "event": event,
                "timestamp": timestamp,
                "actor": actor,
            },
            sort_keys=True,
        )
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    @classmethod
    def create(
        cls,
        parent_hashes: tuple[str, ...],
        timestamp: str,
        event: str,
        actor: str,
        status: str,
        plan: str,
        tool_call: Optional[dict],
        context: dict,
        session_id: str,
        branch_id: str = "main",
        metadata: Optional[dict] = None,
    ) -> "TCCNode":
        h = cls.compute_hash(parent_hashes, event, timestamp, actor)
        return cls(
            hash=h,
            parent_hashes=parent_hashes,
            timestamp=timestamp,
            event=event,
            actor=actor,
            status=status,
            plan=plan,
            tool_call=tool_call,
            context=context,
            session_id=session_id,
            branch_id=branch_id,
            metadata=metadata or {},
        )

    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "parent_hashes": list(self.parent_hashes),
            "timestamp": self.timestamp,
            "event": self.event,
            "actor": self.actor,
            "status": self.status,
            "plan": self.plan,
            "tool_call": self.tool_call,
            "context": self.context,
            "session_id": self.session_id,
            "branch_id": self.branch_id,
            "metadata": self.metadata,
        }
