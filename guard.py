# guard.py
import sys
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# ---------- CONFIG ----------
POLICY_PATH = Path("shared/policy.json")

# support both names (you currently have genius-state.json)
GENIUS_STATE_PATH_PRIMARY = Path("shared/genius_state.json")
GENIUS_STATE_PATH_FALLBACK = Path("shared/genius-state.json")

# IMPORTANT: in your structure, execution entry is execution/main.py
GENIUS_CMD = ["python", "execution/main.py"]
# ----------------------------


def stop(reason: str):
    print(f"[GUARD] STOP: {reason}")
    sys.exit(1)


def load_json(path: Path):
    if not path.exists():
        stop(f"{path} not found")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        stop(f"cannot read {path}: {e}")


def now_utc():
    return datetime.now(timezone.utc)


def _load_genius_state():
    if GENIUS_STATE_PATH_PRIMARY.exists():
        return load_json(GENIUS_STATE_PATH_PRIMARY)
    if GENIUS_STATE_PATH_FALLBACK.exists():
        return load_json(GENIUS_STATE_PATH_FALLBACK)
    stop(f"genius state not found: {GENIUS_STATE_PATH_PRIMARY} or {GENIUS_STATE_PATH_FALLBACK}")


def main():
    print("[GUARD] starting checks...")

    # 1) Load & validate policy
    policy = load_json(POLICY_PATH)

    required_fields = [
        "policy_version",
        "valid_until",
        "max_daily_drawdown",
        "max_open_positions",
        "allowed_strategies",
        "emergency_stop",
    ]
    for k in required_fields:
        if k not in policy:
            stop(f"policy missing field: {k}")

    # 2) Policy expiry
    try:
        valid_until = datetime.fromisoformat(policy["valid_until"].replace("Z", "+00:00"))
    except Exception:
        stop("invalid valid_until format")

    if now_utc() > valid_until:
        stop("policy expired")

    # 3) Emergency stop
    if policy.get("emergency_stop") is True:
        stop("emergency_stop is TRUE")

    # 4) Load GENIUS state (from shared/)
    state = _load_genius_state()

    daily_dd = float(state.get("daily_drawdown", 0.0))
    open_positions = int(state.get("open_positions", 0))

    # 5) Global limits
    if daily_dd >= float(policy["max_daily_drawdown"]):
        stop("daily drawdown limit exceeded")

    if open_positions > int(policy["max_open_positions"]):
        stop("open positions limit exceeded")

    # 6) All checks passed → run execution worker
    print("[GUARD] checks passed. starting GENIUS BOT MAN execution...")
    subprocess.run(GENIUS_CMD, check=True)


if __name__ == "__main__":
    main()

