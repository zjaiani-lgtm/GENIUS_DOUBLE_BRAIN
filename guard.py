# guard.py
import sys
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

POLICY_PATH = Path("shared/policy.json")

GENIUS_STATE_PATH_PRIMARY = Path("shared/genius_state.json")
GENIUS_STATE_PATH_FALLBACK = Path("shared/genius-state.json")

GENIUS_CMD = ["python", "execution/main.py"]


def stop(reason: str):
    print(f"[GUARD] STOP: {reason}")
    sys.exit(1)


def _read_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_policy() -> dict:
    if not POLICY_PATH.exists():
        stop("shared/policy.json not found")
    try:
        return _read_json(POLICY_PATH)
    except Exception as e:
        stop(f"cannot read shared/policy.json: {e}")


def now_utc():
    return datetime.now(timezone.utc)


def _pick_state_path() -> Path:
    # Prefer underscore file, fallback to hyphen if that exists and underscore doesn't
    if GENIUS_STATE_PATH_PRIMARY.exists():
        return GENIUS_STATE_PATH_PRIMARY
    if GENIUS_STATE_PATH_FALLBACK.exists():
        return GENIUS_STATE_PATH_FALLBACK
    return GENIUS_STATE_PATH_PRIMARY


def load_or_bootstrap_genius_state() -> dict:
    """
    If genius_state.json is missing/empty/invalid -> create BOOT state.
    This prevents deploy/start deadlocks.
    """
    path = _pick_state_path()

    boot = {
        "open_positions": 0,
        "daily_drawdown": 0.0,
        "worker_status": "BOOT",
        "mode": "DEMO",
        "updated_at_utc": datetime.utcnow().isoformat() + "Z",
    }

    # missing -> create
    if not path.exists():
        _write_json_atomic(path, boot)
        print(f"[GUARD] genius state missing -> created BOOT at {path}")
        return boot

    # empty file -> create
    try:
        if path.stat().st_size == 0:
            _write_json_atomic(path, boot)
            print(f"[GUARD] genius state empty -> replaced with BOOT at {path}")
            return boot
    except Exception:
        pass

    # invalid json -> replace
    try:
        return _read_json(path)
    except Exception as e:
        _write_json_atomic(path, boot)
        print(f"[GUARD] genius state invalid ({e}) -> replaced with BOOT at {path}")
        return boot


def main():
    print("[GUARD] starting checks...")

    policy = load_policy()

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

    # expiry
    try:
        valid_until = datetime.fromisoformat(policy["valid_until"].replace("Z", "+00:00"))
    except Exception:
        stop("invalid valid_until format")

    if now_utc() > valid_until:
        stop("policy expired")

    # emergency stop
    if policy.get("emergency_stop") is True:
        stop("emergency_stop is TRUE")

    # state
    state = load_or_bootstrap_genius_state()
    daily_dd = float(state.get("daily_drawdown", 0.0))
    open_positions = int(state.get("open_positions", 0))

    # limits
    if daily_dd >= float(policy["max_daily_drawdown"]):
        stop("daily drawdown limit exceeded")

    if open_positions > int(policy["max_open_positions"]):
        stop("open positions limit exceeded")

    print("[GUARD] checks passed. starting GENIUS BOT MAN execution...")
    subprocess.run(GENIUS_CMD, check=True)


if __name__ == "__main__":
    main()
