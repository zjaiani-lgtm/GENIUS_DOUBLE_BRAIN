# execution/db/db.py
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("/var/data/genius_bot.db")  # Persistent disk on Render


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    """
    Creates tables and bootstraps system_state row if missing.
    """
    conn = get_connection()
    cur = conn.cursor()

    # --- schema (minimal, safe) ---
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS system_state (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            startup_sync_ok INTEGER NOT NULL,
            kill_switch INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            size REAL NOT NULL,
            entry_price REAL NOT NULL,
            status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            pnl REAL
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )

    # --- bootstrap system_state ---
    cur.execute("SELECT COUNT(*) FROM system_state WHERE id = 1")
    exists = int(cur.fetchone()[0] or 0)

    if exists == 0:
        cur.execute(
            """
            INSERT INTO system_state (id, status, startup_sync_ok, kill_switch, updated_at)
            VALUES (1, 'PAUSED', 0, 1, ?)
            """,
            (datetime.now(timezone.utc).isoformat(),),
        )

    conn.commit()
    conn.close()
