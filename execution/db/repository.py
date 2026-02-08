# execution/db/repository.py
from datetime import datetime, timezone
from execution.db.db import get_connection


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------- SYSTEM STATE ----------------

def get_system_state():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM system_state WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    return row


def update_system_state(status=None, startup_sync_ok=None, kill_switch=None):
    conn = get_connection()
    cur = conn.cursor()

    fields = []
    values = []

    if status is not None:
        fields.append("status = ?")
        values.append(str(status))

    if startup_sync_ok is not None:
        fields.append("startup_sync_ok = ?")
        values.append(int(startup_sync_ok))

    if kill_switch is not None:
        fields.append("kill_switch = ?")
        values.append(int(kill_switch))

    fields.append("updated_at = ?")
    values.append(_utc_now())

    sql = f"UPDATE system_state SET {', '.join(fields)} WHERE id = 1"
    cur.execute(sql, values)

    conn.commit()
    conn.close()


# ---------------- POSITIONS ----------------

def get_open_positions():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM positions WHERE status = 'OPEN'")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_latest_open_position(symbol: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl
        FROM positions
        WHERE status = 'OPEN' AND symbol = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(symbol),)
    )
    row = cur.fetchone()
    conn.close()
    return row


def open_position(symbol, side, size, entry_price):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO positions
        (symbol, side, size, entry_price, status, opened_at)
        VALUES (?, ?, ?, ?, 'OPEN', ?)
        """,
        (str(symbol), str(side), float(size), float(entry_price), _utc_now())
    )
    conn.commit()
    conn.close()


def close_position(position_id: int, close_price: float, pnl: float):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET status='CLOSED', closed_at=?, pnl=?
        WHERE id=?
        """,
        (_utc_now(), float(pnl), int(position_id))
    )
    conn.commit()
    conn.close()


# ---------------- AUDIT LOG ----------------

def log_event(event_type, message):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO audit_log (event_type, message, created_at)
        VALUES (?, ?, ?)
        """,
        (str(event_type), str(message), _utc_now())
    )
    conn.commit()
    conn.close()


# ---------------- OCO LINKS ----------------

def create_oco_link(
    signal_id: str,
    symbol: str,
    base_asset: str,
    tp_order_id: str,
    sl_order_id: str,
    tp_price: float,
    sl_stop_price: float,
    sl_limit_price: float,
    amount: float,
):
    conn = get_connection()
    cur = conn.cursor()
    now = _utc_now()
    cur.execute(
        """
        INSERT INTO oco_links
        (signal_id, symbol, base_asset, tp_order_id, sl_order_id, tp_price, sl_stop_price, sl_limit_price, amount, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
        """,
        (
            str(signal_id), str(symbol), str(base_asset),
            str(tp_order_id), str(sl_order_id),
            float(tp_price), float(sl_stop_price), float(sl_limit_price),
            float(amount),
            now, now
        )
    )
    conn.commit()
    conn.close()


def set_oco_status(link_id: int, status: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE oco_links
        SET status=?, updated_at=?
        WHERE id=?
        """,
        (str(status), _utc_now(), int(link_id))
    )
    conn.commit()
    conn.close()


def list_active_oco_links(limit: int = 50):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, signal_id, symbol, base_asset, tp_order_id, sl_order_id, tp_price, sl_stop_price, sl_limit_price, amount, status, created_at, updated_at
        FROM oco_links
        WHERE status='ACTIVE'
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),)
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def has_active_oco_for_symbol(symbol: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM oco_links
        WHERE status='ACTIVE' AND UPPER(symbol)=UPPER(?)
        LIMIT 1
        """,
        (str(symbol),)
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_open_positions_count() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM positions WHERE status = 'OPEN'")
    n = int(cur.fetchone()[0] or 0)
    conn.close()
    return n


# ---------------- EXECUTED SIGNALS (IDEMPOTENCY + AUDIT) ----------------

def signal_id_already_executed(signal_id: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM executed_signals WHERE signal_id = ? LIMIT 1",
        (str(signal_id),)
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_signal_id_executed(
    signal_id: str,
    signal_hash: str = None,
    action: str = None,
    symbol: str = None
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO executed_signals
        (signal_id, signal_hash, action, symbol, executed_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            str(signal_id),
            str(signal_hash) if signal_hash is not None else None,
            str(action) if action is not None else None,
            str(symbol) if symbol is not None else None,
            _utc_now()
        )
    )
    conn.commit()
    conn.close()
