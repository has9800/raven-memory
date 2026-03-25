from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Optional

from .store import DAGError, NodeNotFoundError
from .node import TCCNode
from .store import TCCStore


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskDAG:
    def __init__(self, store: TCCStore):
        self._store = store
        self._store._on_status_update = self._on_status_update
        self._index: dict[str, TCCNode] = {}
        self._tip_hash: Optional[str] = None
        self._branches: dict[str, str] = {}  # branch_id -> tip_hash
        self._load()

    def _load(self):
        nodes = self._store.load_all()
        for n in nodes:
            self._index[n.hash] = n
        self._tip_hash = self._store.get_meta("tip_hash")
        self._branches = self._store.get_all_branches()

    def _save_node(self, node: TCCNode) -> None:
        self._store.save(node)
        self._index[node.hash] = node

    def _on_status_update(self, hash: str, status: str) -> None:
        if hash in self._index:
            old = self._index[hash]
            updated = TCCNode(
                hash=old.hash,
                parent_hashes=old.parent_hashes,
                timestamp=old.timestamp,
                event=old.event,
                actor=old.actor,
                status=status,
                plan=old.plan,
                tool_call=old.tool_call,
                context=old.context,
                session_id=old.session_id,
                branch_id=old.branch_id,
                metadata=old.metadata,
            )
            self._index[hash] = updated
        self._auto_merge_check()

    def root(self, event: str, actor: str, plan: str, context: dict,
             session_id: str, tool_call=None) -> TCCNode:
        if self._tip_hash is not None:
            raise DAGError("Root already exists")
        node = TCCNode.create(
            parent_hashes=(),
            timestamp=_now(),
            event=event,
            actor=actor,
            status="confirmed",
            plan=plan,
            tool_call=tool_call,
            context=context,
            session_id=session_id,
            branch_id="main",
        )
        self._save_node(node)
        self._tip_hash = node.hash
        self._store.set_meta("tip_hash", node.hash)
        self._store.set_meta("root_hash", node.hash)
        return node

    def append(self, event: str, actor: str, plan: str, context: dict,
               session_id: str, tool_call=None,
               parent_hash: Optional[str] = None,
               status: str = "confirmed") -> TCCNode:
        parent = parent_hash or self._tip_hash
        if parent and parent not in self._index:
            raise NodeNotFoundError(f"Parent {parent} not found")
        parents = (parent,) if parent else ()
        node = TCCNode.create(
            parent_hashes=parents,
            timestamp=_now(),
            event=event,
            actor=actor,
            status=status,
            plan=plan,
            tool_call=tool_call,
            context=context,
            session_id=session_id,
            branch_id="main",
        )
        self._save_node(node)
        self._tip_hash = node.hash
        self._store.set_meta("tip_hash", node.hash)
        return node

    def branch(self, from_hash: str, event: str, actor: str, plan: str,
               context: dict, session_id: str,
               tool_call=None) -> tuple[TCCNode, str]:
        if from_hash not in self._index:
            raise NodeNotFoundError(f"Node {from_hash} not found")
        branch_id = uuid.uuid4().hex[:8]
        while branch_id == "main" or branch_id in self._branches:
            branch_id = uuid.uuid4().hex[:8]
        node = TCCNode.create(
            parent_hashes=(from_hash,),
            timestamp=_now(),
            event=event,
            actor=actor,
            status="running",
            plan=plan,
            tool_call=tool_call,
            context=context,
            session_id=session_id,
            branch_id=branch_id,
        )
        self._save_node(node)
        self._branches[branch_id] = node.hash
        self._store.set_branch_tip(branch_id, node.hash)
        return node, branch_id

    def branch_from_tip(self, session_id: str) -> str:
        """
        Create a new branch forking from the current tip without
        writing a node. Returns the new branch_id.
        Use this when you want to start parallel work from the
        current tip without recording an artificial fork event.
        """
        if not self._tip_hash:
            raise DAGError("Cannot branch from empty DAG")
        branch_id = uuid.uuid4().hex[:8]
        while branch_id == "main" or branch_id in self._branches:
            branch_id = uuid.uuid4().hex[:8]
        self._branches[branch_id] = self._tip_hash
        self._store.set_branch_tip(branch_id, self._tip_hash)
        return branch_id

    def merge(self, branch_hashes: list[str],
              event: str = "merged parallel branches",
              plan: str = "explicit merge",
              session_id: str = "system") -> TCCNode:
        """
        Explicitly merge a list of branch tip hashes into the main chain.
        Creates a merge node with all provided hashes as parents.
        Returns the merge node.
        """
        # Validate all hashes exist
        for h in branch_hashes:
            if h not in self._index:
                raise NodeNotFoundError(f"Branch tip {h} not found")

        # Include current tip as a parent if not already in list
        all_parents = list(dict.fromkeys(
            ([self._tip_hash] if self._tip_hash else []) + branch_hashes
        ))

        # Build merged context from all branch tips
        merged_context: dict = {
            "open_threads": [],
            "relevant_paths": [],
            "notes": [],
            "merged_branches": branch_hashes,
        }
        for h in branch_hashes:
            node = self._index[h]
            c = node.context
            merged_context["open_threads"] += c.get("open_threads", [])
            merged_context["relevant_paths"] += c.get("relevant_paths", [])
            if c.get("notes"):
                merged_context["notes"].append(c["notes"])
        merged_context["open_threads"] = list(set(merged_context["open_threads"]))
        merged_context["relevant_paths"] = list(set(merged_context["relevant_paths"]))

        merge_node = TCCNode.create(
            parent_hashes=tuple(all_parents),
            timestamp=_now(),
            event=event,
            actor="system",
            status="confirmed",
            plan=plan,
            tool_call=None,
            context=merged_context,
            session_id=session_id,
            branch_id="main",
        )
        self._save_node(merge_node)
        self._tip_hash = merge_node.hash
        self._store.set_meta("tip_hash", merge_node.hash)

        # Clear any branches whose tips are now merged
        for bid in list(self._branches.keys()):
            if self._branches[bid] in branch_hashes:
                self._store.mark_branch_merged(bid)
                del self._branches[bid]

        return merge_node

    def update_status(self, hash: str, status: str) -> None:
        self._store.update_status(hash, status)

    def _auto_merge_check(self) -> Optional[TCCNode]:
        if not self._branches:
            return None

        branch_tips = {
            bid: self._index.get(h)
            for bid, h in self._branches.items()
        }

        # All branch tips must be confirmed or failed
        for tip_node in branch_tips.values():
            if tip_node is None:
                return None
            if tip_node.status not in ("confirmed", "failed"):
                return None

        # All branches ready — create merge node
        branch_tip_hashes = list(self._branches.values())

        merged_context: dict = {
            "open_threads": [],
            "relevant_paths": [],
            "notes": [],
            "merged_branches": branch_tip_hashes,
        }
        for tip_node in branch_tips.values():
            if tip_node:
                c = tip_node.context
                merged_context["open_threads"] += c.get("open_threads", [])
                merged_context["relevant_paths"] += c.get("relevant_paths", [])
                if c.get("notes"):
                    merged_context["notes"].append(c["notes"])
        merged_context["open_threads"] = list(set(merged_context["open_threads"]))
        merged_context["relevant_paths"] = list(set(merged_context["relevant_paths"]))

        branch_events = [self._index[h].event for h in branch_tip_hashes if h in self._index]
        merge_event = f"merged: {', '.join(branch_events)}"

        # Find session_id from one of the branch tips
        session_id = next(
            (self._index[h].session_id
             for h in branch_tip_hashes
             if h in self._index),
            "system"
        )

        # Include current main tip + all branch tips as parents
        all_parents = list(dict.fromkeys(
            ([self._tip_hash] if self._tip_hash else []) + branch_tip_hashes
        ))

        merge_node = TCCNode.create(
            parent_hashes=tuple(all_parents),
            timestamp=_now(),
            event=merge_event,
            actor="system",
            status="confirmed",
            plan="auto-merge of completed parallel branches",
            tool_call=None,
            context=merged_context,
            session_id=session_id,
            branch_id="main",
        )
        self._save_node(merge_node)
        self._tip_hash = merge_node.hash
        self._store.set_meta("tip_hash", merge_node.hash)

        # Mark all branches as merged
        for bid in list(self._branches.keys()):
            self._store.mark_branch_merged(bid)
        self._branches.clear()

        return merge_node

    def rollback(self, n: int = 1) -> TCCNode:
        """
        Roll the tip back n steps through parent_hashes[0].
        Clears any branches whose tips are now ahead of the
        rolled-back tip to prevent inconsistent state.
        """
        if not self._tip_hash:
            raise DAGError("Empty DAG, cannot rollback")
        current = self._index[self._tip_hash]
        for i in range(n):
            if not current.parent_hashes:
                raise DAGError(
                    f"Cannot rollback {n} hops, only {i} available"
                )
            current = self._index[current.parent_hashes[0]]
        self._tip_hash = current.hash
        self._store.set_meta("tip_hash", current.hash)

        # Clear branches whose tips are ahead of the new tip
        # (i.e. not ancestors of the new tip)
        stale = []
        for bid, tip_hash in self._branches.items():
            if not self.is_ancestor_of_tip(tip_hash):
                stale.append(bid)
        for bid in stale:
            self._store.mark_branch_merged(bid)
            del self._branches[bid]

        return current

    def tip(self) -> Optional[TCCNode]:
        if not self._tip_hash:
            return None
        return self._index.get(self._tip_hash)

    def get(self, hash: str) -> TCCNode:
        if hash not in self._index:
            raise NodeNotFoundError(f"Node {hash} not found")
        return self._index[hash]

    def recent(self, n: int = 10) -> list[TCCNode]:
        if not self._tip_hash:
            return []
        result = []
        seen = set()
        # Use a list as a queue, sorted by timestamp descending
        frontier = [self._index[self._tip_hash]] if self._tip_hash in self._index else []
        while frontier and len(result) < n:
            # Pick the most recent node from frontier
            frontier.sort(key=lambda node: node.timestamp, reverse=True)
            current = frontier.pop(0)
            if current.hash in seen:
                continue
            seen.add(current.hash)
            result.append(current)
            # Add all parents to frontier
            for ph in current.parent_hashes:
                if ph in self._index and ph not in seen:
                    frontier.append(self._index[ph])
        return result

    def since(self, session_id: str) -> list[TCCNode]:
        return [n for n in self._index.values() if n.session_id == session_id]

    def path(self, from_hash: str, to_hash: str) -> list[TCCNode]:
        if from_hash not in self._index or to_hash not in self._index:
            return []
        from collections import deque
        queue = deque([[from_hash]])
        visited = {from_hash}
        while queue:
            path = queue.popleft()
            node = self._index[path[-1]]
            if node.hash == to_hash:
                return [self._index[h] for h in path]
            for child in self._get_children(node.hash):
                if child not in visited:
                    visited.add(child)
                    queue.append(path + [child])
        return []

    def _get_children(self, hash: str) -> list[str]:
        return [
            n.hash for n in self._index.values()
            if hash in n.parent_hashes
        ]

    def is_ancestor_of_tip(self, hash: str) -> bool:
        if not self._tip_hash:
            return False
        current = self._index.get(self._tip_hash)
        while current:
            if current.hash == hash:
                return True
            if not current.parent_hashes:
                break
            current = self._index.get(current.parent_hashes[0])
        return False

    def speculate(self, events: list[dict], session_id: str) -> list[TCCNode]:
        nodes = []
        parent = self._tip_hash
        for e in events:
            node = TCCNode.create(
                parent_hashes=(parent,) if parent else (),
                timestamp=_now(),
                event=e["event"],
                actor=e.get("actor", "agent"),
                status="speculative",
                plan=e.get("plan", ""),
                tool_call=e.get("tool_call"),
                context=e.get("context", {}),
                session_id=session_id,
                branch_id="main",
            )
            self._save_node(node)
            parent = node.hash
            nodes.append(node)
        return nodes

    def confirm_speculative(self, hash: str) -> TCCNode:
        self._store.update_status(hash, "confirmed")
        return self._index[hash]

    def prune_speculative(self, from_hash: str) -> int:
        count = 0
        to_prune = [from_hash]
        while to_prune:
            h = to_prune.pop()
            node = self._index.get(h)
            if node and node.status == "speculative":
                self._store.update_status(h, "pruned")
                count += 1
                to_prune.extend(self._get_children(h))
        return count
