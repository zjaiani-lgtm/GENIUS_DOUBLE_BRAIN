# execution/db/db.py
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("/var/data/genius_bot.db")
SCHEMA_PATH = Path("execution/db/schema.sql")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,))
    return cur.fetchone() is not None


def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return col in [r[1] for r in cur.fetchall()]


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, col_def: str) -> None:
    if _table_exists(conn, table) and not _column_exists(conn, table, col):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def};")


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    # 1) apply schema.sql BUT (important) avoid executed_signals indexes here
    if SCHEMA_PATH.exists():
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

        # ✅ strip indexes that may reference columns not yet migrated
        filtered_lines = []
        for line in schema_sql.splitlines():
            l = line.strip().lower()
            if "create index" in l and "executed_signals" in l:
                # skip executed_signals indexes for now
                continue
            filtered_lines.append(line)
        conn.executescript("\n".join(filtered_lines))

    # 2) MIGRATIONS for older DB versions
    # system_state might be older without mode
    _add_column_if_missing(conn, "system_state", "mode", "TEXT")

    # executed_signals: upgrade old table to new columns
    if _table_exists(conn, "executed_signals"):
        _add_column_if_missing(conn, "executed_signals", "signal_hash", "TEXT")
        _add_column_if_missing(conn, "executed_signals", "action", "TEXT")
        _add_column_if_missing(conn, "executed_signals", "symbol", "TEXT")
        _add_column_if_missing(conn, "executed_signals", "executed_at", "TEXT")
    else:
        # create fresh compatible table
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS executed_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL UNIQUE,
            signal_hash TEXT,
            action TEXT,
            symbol TEXT,
            executed_at TEXT NOT NULL
        );
        """)

    # 3) create indexes AFTER migration (now columns exist)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_executed_signals_signal_id ON executed_signals(signal_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_executed_signals_signal_hash ON executed_signals(signal_hash);")

    # 4) ensure system_state row exists (id=1)
    now = _utc_now()
    cur.execute("SELECT COUNT(*) FROM system_state WHERE id=1")
    exists = int(cur.fetchone()[0] or 0)

    if exists == 0:
        # mode default: DEMO (შეცვალე თუ გინდა)
        cur.execute(
            """
            INSERT INTO system_state (id, mode, status, startup_sync_ok, kill_switch, updated_at)
            VALUES (1, ?, ?, ?, ?, ?)
            """,
            ("DEMO", "RUNNING", 1, 0, now),
        )
    else:
        cur.execute("UPDATE system_state SET updated_at=? WHERE id=1", (now,))

    conn.commit()
    conn.close()
