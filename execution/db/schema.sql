-- execution/db/schema.sql

CREATE TABLE IF NOT EXISTS system_state (
    id INTEGER PRIMARY KEY,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    kill_switch INTEGER NOT NULL,
    startup_sync_ok INTEGER NOT NULL,
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

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    size REAL NOT NULL,
    price REAL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_state (
    id INTEGER PRIMARY KEY,
    daily_loss REAL NOT NULL,
    daily_profit REAL NOT NULL,
    max_daily_loss REAL NOT NULL,
    current_drawdown REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    updated_at TEXT NOT NULL
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

-- ✅ Double Brain idempotency + audit trail
CREATE TABLE IF NOT EXISTS executed_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id TEXT NOT NULL UNIQUE,
    signal_hash TEXT,
    action TEXT,
    symbol TEXT,
    executed_at TEXT NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS ix_audit_event_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS ix_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_oco_links_status ON oco_links(status);

CREATE INDEX IF NOT EXISTS idx_executed_signals_signal_id ON executed_signals(signal_id);
CREATE INDEX IF NOT EXISTS idx_executed_signals_signal_hash ON executed_signals(signal_hash);
