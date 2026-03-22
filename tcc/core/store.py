from __future__ import annotations
import json
import sqlite3
import threading
import warnings
from typing import Callable, Optional

from .node import TCCNode

VALID_STATUSES = {"confirmed", "failed", "speculative", "pruned", "running"}


class TCCError(Exception): pass
class NodeNotFoundError(TCCError): pass
class DuplicateNodeError(TCCError): pass
class DAGError(TCCError): pass
class InvalidStatusError(TCCError): pass


class TCCStore:
    def __init__(self, path: str = ":memory:", on_status_update: Optional[Callable] = None):
        self.path = path
        self._lock = threading.Lock()
        self._on_status_update = on_status_update
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._vec_enabled = self._load_sqlite_vec(self._conn)
        self._init_schema()

    def _load_sqlite_vec(self, conn: sqlite3.Connection) -> bool:
        """Load sqlite-vec extension if available."""
        if self.path == ":memory:":
            return False
        try:
            import sqlite_vec

            conn.enable_load_extension(True)
            sqlite_vec.load(conn)
            conn.enable_load_extension(False)
            return True
        except Exception as exc:
            warnings.warn(
                f"sqlite-vec not available — semantic search disabled: {exc}",
                UserWarning,
            )
            return False

    def _init_schema(self):
        with self._lock:
            cur = self._conn.cursor()
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS nodes (
                    hash            TEXT PRIMARY KEY,
                    parent_hashes   TEXT NOT NULL,
                    timestamp       TEXT NOT NULL,
                    event           TEXT NOT NULL,
                    actor           TEXT NOT NULL,
                    status          TEXT NOT NULL,
                    plan            TEXT NOT NULL,
                    tool_call       TEXT,
                    context         TEXT NOT NULL,
                    session_id      TEXT NOT NULL,
                    branch_id       TEXT NOT NULL DEFAULT 'main',
                    metadata        TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_session   ON nodes(session_id);
                CREATE INDEX IF NOT EXISTS idx_branch    ON nodes(branch_id);
                CREATE INDEX IF NOT EXISTS idx_status    ON nodes(status);
                CREATE INDEX IF NOT EXISTS idx_timestamp ON nodes(timestamp);
            """)
            if self._vec_enabled:
                cur.executescript("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS node_embeddings
                    USING vec0(
                        hash        TEXT PRIMARY KEY,
                        embedding   FLOAT[384]
                    );
                """)
            self._conn.commit()

    def save(self, node: TCCNode) -> None:
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        node.hash,
                        json.dumps(list(node.parent_hashes)),
                        node.timestamp,
                        node.event,
                        node.actor,
                        node.status,
                        node.plan,
                        json.dumps(node.tool_call) if node.tool_call else None,
                        json.dumps(node.context),
                        node.session_id,
                        node.branch_id,
                        json.dumps(node.metadata),
                    ),
                )
                if self._vec_enabled and self.path != ":memory:":
                    try:
                        from .embedder import embed_node

                        vec = embed_node(node)
                        self._conn.execute(
                            "INSERT OR REPLACE INTO node_embeddings(hash, embedding) VALUES (?, ?)",
                            (node.hash, json.dumps(vec)),
                        )
                    except Exception as exc:
                        warnings.warn(f"Failed to embed node {node.hash}: {exc}", UserWarning)
                self._conn.commit()
            except sqlite3.IntegrityError:
                raise DuplicateNodeError(f"Node {node.hash} already exists")

    def load(self, hash: str) -> TCCNode:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM nodes WHERE hash=?", (hash,)
            ).fetchone()
        if not row:
            raise NodeNotFoundError(f"Node {hash} not found")
        return self._row_to_node(row)

    def load_all(self) -> list[TCCNode]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM nodes ORDER BY timestamp ASC"
            ).fetchall()
        return [self._row_to_node(r) for r in rows]

    def update_status(self, hash: str, status: str) -> None:
        if status not in VALID_STATUSES:
            raise InvalidStatusError(f"Invalid status: {status}")
        with self._lock:
            affected = self._conn.execute(
                "UPDATE nodes SET status=? WHERE hash=?", (status, hash)
            ).rowcount
            self._conn.commit()
        if affected == 0:
            raise NodeNotFoundError(f"Node {hash} not found")
        if self._on_status_update:
            self._on_status_update(hash, status)

    def query_before(self, timestamp: str) -> list[TCCNode]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM nodes WHERE timestamp < ? ORDER BY timestamp ASC",
                (timestamp,),
            ).fetchall()
        return [self._row_to_node(r) for r in rows]

    def delete(self, hashes: list[str]) -> None:
        with self._lock:
            self._conn.executemany(
                "DELETE FROM nodes WHERE hash=?", [(h,) for h in hashes]
            )
            self._conn.commit()

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, value)
            )
            self._conn.commit()

    def get_branch_tip(self, branch_id: str) -> Optional[str]:
        return self.get_meta(f"branch_{branch_id}_tip")

    def set_branch_tip(self, branch_id: str, hash: str) -> None:
        self.set_meta(f"branch_{branch_id}_tip", hash)

    def get_all_branches(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM meta WHERE key LIKE 'branch_%_tip'"
            ).fetchall()
        result = {}
        for key, value in rows:
            branch_id = key[len("branch_"):-len("_tip")]
            if branch_id == "main":
                continue
            merged = self.get_meta(f"branch_{branch_id}_merged")
            if not merged:
                result[branch_id] = value
        return result

    def mark_branch_merged(self, branch_id: str) -> None:
        self.set_meta(f"branch_{branch_id}_merged", "true")

    def search(
        self,
        query: str,
        n: int = 5,
        session_id: str | None = None,
    ) -> list[TCCNode]:
        """
        Semantic search over node embeddings.
        Returns up to n nodes most similar to the query string.
        Falls back to empty list if sqlite-vec not available.

        Args:
            query: Natural language search query
            n: Number of results to return
            session_id: Optional filter to search within a specific session
        """
        if not self._vec_enabled:
            return []

        try:
            from .embedder import embed

            vec = embed(query)
            vec_json = json.dumps(vec)

            with self._lock:
                if session_id:
                    rows = self._conn.execute(
                        """
                        SELECT n.hash
                        FROM node_embeddings e
                        JOIN nodes n ON e.hash = n.hash
                        WHERE n.session_id = ?
                        ORDER BY vec_distance_cosine(e.embedding, ?) ASC
                        LIMIT ?
                        """,
                        (session_id, vec_json, n),
                    ).fetchall()
                else:
                    rows = self._conn.execute(
                        """
                        SELECT hash
                        FROM node_embeddings
                        ORDER BY vec_distance_cosine(embedding, ?) ASC
                        LIMIT ?
                        """,
                        (vec_json, n),
                    ).fetchall()

            return [self.load(row[0]) for row in rows]
        except Exception as exc:
            warnings.warn(f"Search failed: {exc}", UserWarning)
            return []

    @property
    def is_vec_enabled(self) -> bool:
        return self._vec_enabled

    def _row_to_node(self, row) -> TCCNode:
        return TCCNode(
            hash=row[0],
            parent_hashes=tuple(json.loads(row[1])),
            timestamp=row[2],
            event=row[3],
            actor=row[4],
            status=row[5],
            plan=row[6],
            tool_call=json.loads(row[7]) if row[7] else None,
            context=json.loads(row[8]),
            session_id=row[9],
            branch_id=row[10],
            metadata=json.loads(row[11]),
        )
