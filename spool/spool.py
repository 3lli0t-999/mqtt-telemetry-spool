"""SQLite backed store-and-forward queue for telemetry.

Design goals, learned from sites with a microwave link and a diesel generator:

* enqueue never blocks on the network;
* a row is only removed after the broker acknowledged it;
* retries back off per message, not globally, so one poisoned payload does
  not stall the whole queue;
* the file survives power loss (WAL + synchronous=FULL).
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT    NOT NULL,
    payload      TEXT    NOT NULL,
    qos          INTEGER NOT NULL DEFAULT 1,
    created_at   REAL    NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL    NOT NULL,
    delivered_at REAL
);
CREATE INDEX IF NOT EXISTS idx_queue_pending
    ON queue (delivered_at, next_attempt, id);
"""


@dataclass
class Message:
    id: int
    topic: str
    payload: str
    qos: int
    attempts: int

    def as_dict(self) -> dict:
        return {"id": self.id, "topic": self.topic, "payload": self.payload, "qos": self.qos}


class Spool:
    def __init__(self, path: str | Path, max_bytes: int = 256 * 1024 * 1024):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.max_bytes = max_bytes

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Spool":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    # -- writing -------------------------------------------------------------
    def enqueue(self, topic: str, payload: str | dict, qos: int = 1) -> int:
        if isinstance(payload, dict):
            payload = json.dumps(payload, separators=(",", ":"))
        now = time.time()
        cursor = self.conn.execute(
            "INSERT INTO queue (topic, payload, qos, created_at, next_attempt) "
            "VALUES (?, ?, ?, ?, ?)",
            (topic, payload, qos, now, now),
        )
        self.conn.commit()
        self._enforce_size()
        return int(cursor.lastrowid)

    def _enforce_size(self) -> None:
        try:
            size = Path(self.path).stat().st_size
        except OSError:
            return
        if size <= self.max_bytes:
            return
        # Drop the oldest already delivered rows first, then refuse new data
        # rather than silently deleting undelivered telemetry.
        self.conn.execute(
            "DELETE FROM queue WHERE id IN ("
            "  SELECT id FROM queue WHERE delivered_at IS NOT NULL ORDER BY id LIMIT 500"
            ")"
        )
        self.conn.commit()
        if Path(self.path).stat().st_size > self.max_bytes:
            raise RuntimeError(
                f"spool {self.path} is full ({size} bytes) and all rows are undelivered"
            )

    # -- reading -------------------------------------------------------------
    def claim(self, limit: int = 50, now: float | None = None) -> list[Message]:
        now = now or time.time()
        rows = self.conn.execute(
            "SELECT id, topic, payload, qos, attempts FROM queue "
            "WHERE delivered_at IS NULL AND next_attempt <= ? "
            "ORDER BY id LIMIT ?",
            (now, limit),
        ).fetchall()
        return [Message(r["id"], r["topic"], r["payload"], r["qos"], r["attempts"]) for r in rows]

    def delivered(self, message_id: int) -> None:
        self.conn.execute(
            "UPDATE queue SET delivered_at = ?, attempts = attempts + 1 WHERE id = ?",
            (time.time(), message_id),
        )
        self.conn.commit()

    def failed(self, message_id: int, retry_after: float = 30.0) -> None:
        self.conn.execute(
            "UPDATE queue SET attempts = attempts + 1, next_attempt = ? WHERE id = ?",
            (time.time() + retry_after, message_id),
        )
        self.conn.commit()

    def stats(self) -> dict:
        row = self.conn.execute(
            "SELECT "
            "  SUM(CASE WHEN delivered_at IS NULL THEN 1 ELSE 0 END) AS pending,"
            "  SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered,"
            "  MIN(CASE WHEN delivered_at IS NULL THEN created_at END) AS oldest,"
            "  MAX(attempts) AS max_attempts "
            "FROM queue"
        ).fetchone()
        pending = row["pending"] or 0
        oldest = row["oldest"]
        return {
            "pending": pending,
            "delivered": row["delivered"] or 0,
            "oldest_pending_s": round(time.time() - oldest, 1) if oldest else 0.0,
            "max_attempts": row["max_attempts"] or 0,
            "path": self.path,
        }

    def prune(self, older_than: float = 86400.0) -> int:
        cutoff = time.time() - older_than
        cursor = self.conn.execute(
            "DELETE FROM queue WHERE delivered_at IS NOT NULL AND delivered_at < ?", (cutoff,)
        )
        self.conn.commit()
        return cursor.rowcount

    def purge(self) -> int:
        cursor = self.conn.execute("DELETE FROM queue")
        self.conn.commit()
        return cursor.rowcount
