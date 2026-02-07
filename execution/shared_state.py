# execution/shared_state.py
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


PRIMARY_PATH = Path("shared/genius_state.json")
FALLBACK_PATH = Path("shared/genius-state.json")  # your current name (kept for compatibility)


def _pick_path() -> Path:
    # Prefer underscore version; if only hyphen exists, write there too
    if PRIMARY_PATH.exists() or not FALLBACK_PATH.exists():
        return PRIMARY_PATH
    return FALLBACK_PATH


def write_genius_state(state: Dict[str, Any]) -> None:
    """
    Writes shared genius state atomically (safe on crashes/restarts).
    Guard reads this file.
    """
    path = _pick_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        **state,
        "updated_at_utc": datetime.utcnow().isoformat() + "Z",
    }

    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
