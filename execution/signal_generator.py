import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import openpyxl

from execution.signal_client import append_signal


EXCEL_PATH = Path(os.getenv("BRAIN_XLSX_PATH", "/var/data/brain.xlsx"))


def _read_cfg():
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)
    ws = wb["GENERATOR_CONFIG"]

    def get(addr, cast, default):
        v = ws[addr].value
        if v is None:
            return default
        try:
            return cast(v)
        except Exception:
            return default

    symbol = get("B1", str, "BTC/USDT").strip()
    tf = get("B2", str, "1m").strip()
    limit = get("B3", int, 50)
    ma_period = get("B4", int, 20)
    min_conf = get("B5", float, 0.70)
    usdt_size = get("B6", float, 5.0)
    tp_pct = get("B7", float, 0.03)
    sl_pct = get("B8", float, 0.015)

    enabled_raw = ws["B9"].value
    enabled = str(enabled_raw).strip().lower() in ("true", "1", "yes", "y")

    return dict(
        symbol=symbol, tf=tf, limit=limit, ma_period=ma_period,
        min_conf=min_conf, usdt_size=usdt_size, tp_pct=tp_pct, sl_pct=sl_pct,
        enabled=enabled
    )


def _sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def run_once(outbox_path: str) -> bool:
    """
    Returns True if a signal was written.
    Writes a signal in the JSON structure your signal_client expects.
    """
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"brain.xlsx not found at {EXCEL_PATH}")

    cfg = _read_cfg()
    if not cfg["enabled"]:
        return False

    ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "spot"}})

    ohlcv = ex.fetch_ohlcv(cfg["symbol"], timeframe=cfg["tf"], limit=cfg["limit"])
    closes = [c[4] for c in ohlcv]
    last = closes[-1]
    ma = _sma(closes, cfg["ma_period"])
    if ma is None:
        return False

    # მაგალითი rule (შემდეგ შენს Excel/DYZEN წესზე გადავიყვანთ):
    confidence = 0.75 if last > ma else 0.50
    if not (last > ma and confidence >= cfg["min_conf"]):
        return False

    # USDT → base amount
    position_size = cfg["usdt_size"] / last

    tp_price = last * (1 + cfg["tp_pct"])
    sl_stop = last * (1 - cfg["sl_pct"])
    sl_limit = sl_stop * 0.999  # პატარა slip buffer

    signal = {
        "signal_id": f"DYZEN-{uuid.uuid4().hex[:12]}",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "final_verdict": "TRADE",
        "certified_signal": True,
        "execution": {
            "symbol": cfg["symbol"],
            "direction": "LONG",
            "position_size": float(position_size),
            "entry": {"type": "MARKET"},
            "exits": {
                "tp": {"type": "LIMIT", "price": float(tp_price)},
                "sl": {"type": "STOP_LIMIT", "stop_price": float(sl_stop), "limit_price": float(sl_limit)}
            }
        }
    }

    append_signal(signal, outbox_path)
    return True
