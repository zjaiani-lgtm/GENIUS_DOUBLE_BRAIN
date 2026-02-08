# execution/db/db.py
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("/var/data/genius_bot.db")  # Persistent disk on Render


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
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

    CREATE TABLE IF NOT EXISTS oco_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        base_asset TEXT,
        tp_order_id TEXT,
        sl_order_id TEXT,
        tp_price REAL,
        sl_stop_price REAL,
        sl_limit_price REAL,
        amount REAL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    -- ✅ idempotency table (prevents executing same signal twice)
    CREATE TABLE IF NOT EXISTS executed_signals (
        signal_id TEXT PRIMARY KEY,
        executed_at TEXT NOT NULL,
        mode TEXT NOT NULL,
        symbol TEXT,
        verdict TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
    CREATE INDEX IF NOT EXISTS idx_oco_links_status ON oco_links(status);
    """)

    # ✅ ensure system_state row exists (id=1)
    now = _utc_now()
    cur.execute("SELECT COUNT(*) FROM system_state WHERE id=1")
    exists = int(cur.fetchone()[0] or 0)
    if exists == 0:
        cur.execute(
            "INSERT INTO system_state (id, status, startup_sync_ok, kill_switch, updated_at) VALUES (1, ?, ?, ?, ?)",
            ("RUNNING", 1, 0, now),
        )
    else:
        # touch updated_at so logs show fresh state if needed
        cur.execute("UPDATE system_state SET updated_at=? WHERE id=1", (now,))

    conn.commit()
    conn.close()
