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

    -- 🔽 ეს დაამატე
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
    """)

    conn.commit()
    conn.close()


