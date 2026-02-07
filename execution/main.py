# execution/main.py
import os
import time
import logging
from typing import Optional, Dict, Any

from execution.db.db import init_db
from execution.db.repository import (
    get_system_state,
    update_system_state,
    log_event,
    get_open_positions_count,
)
from execution.execution_engine import ExecutionEngine
from execution.signal_client import pop_next_signal
from execution.kill_switch import is_kill_switch_active
from execution.shared_state import write_genius_state

logger = logging.getLogger("gbm")


def _bootstrap_state_if_needed() -> None:
    """
    IMPORTANT for dual-brain:
    - We do NOT 'self-heal' into RUNNING automatically anymore.
    - Guard + DB gates must control when system runs.
    This function only logs current DB state for visibility.
    """
    raw = get_system_state()
    if not isinstance(raw, (list, tuple)) or len(raw) < 5:
        logger.warning("BOOTSTRAP_STATE | system_state row missing or invalid -> skip")
        return

    status = str(raw[1] or "").upper()
    startup_sync_ok = int(raw[2] or 0)
    kill_switch_db = int(raw[3] or 0)

    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"

    logger.info(
        f"BOOTSTRAP_STATE | status={status} startup_sync_ok={startup_sync_ok} "
        f"kill_db={kill_switch_db} env_kill={env_kill}"
    )


def _safe_pop_next_signal(outbox_path: str) -> Optional[Dict[str, Any]]:
    try:
        return pop_next_signal(outbox_path)
    except Exception as e:
        logger.exception(f"OUTBOX_POP_FAIL | path={outbox_path} err={e}")
        try:
            log_event("OUTBOX_POP_FAIL", f"path={outbox_path} err={e}")
        except Exception:
            pass
        return None


def _write_shared_state(mode: str, worker_status: str, last_signal_id: str = None) -> None:
    """
    Guard reads this file. Keep it simple and always update.
    """
    try:
        state = {
            "mode": mode,
            "worker_status": worker_status,
            "open_positions": get_open_positions_count(),
            "daily_drawdown": 0.0,  # TODO later: compute from wallet/orders
        }
        if last_signal_id:
            state["last_signal_id"] = last_signal_id
        write_genius_state(state)
    except Exception as e:
        logger.warning(f"STATE_WRITE_WARN | err={e}")


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

    mode = os.getenv("MODE", "DEMO").upper()
    outbox_path = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")
    sleep_s = float(os.getenv("LOOP_SLEEP_SECONDS", "10"))

    init_db()
    _bootstrap_state_if_needed()

    engine = ExecutionEngine()

    # initial reconcile (best-effort)
    try:
        engine.reconcile_oco()
    except Exception as e:
        logger.warning(f"OCO_RECONCILE_START_WARN | err={e}")

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"OUTBOX_PATH={outbox_path}")
    logger.info(f"LOOP_SLEEP_SECONDS={sleep_s}")

    # Write initial state (so Guard sees we're alive)
    _write_shared_state(mode=mode, worker_status="RUNNING")

    while True:
        last_signal_id = None
        try:
            # 0) ABSOLUTE KILL SWITCH (before everything)
            if is_kill_switch_active():
                logger.warning("KILL_SWITCH_ACTIVE | worker will not pop/execute signals")
                try:
                    log_event("WORKER_KILL_SWITCH_ACTIVE", "blocked before loop actions")
                except Exception:
                    pass

                _write_shared_state(mode=mode, worker_status="KILL_SWITCH_ACTIVE")
                time.sleep(sleep_s)
                continue

            # 1) reconcile OCO
            try:
                engine.reconcile_oco()
            except Exception as e:
                logger.warning(f"OCO_RECONCILE_LOOP_WARN | err={e}")

            # 2) pop + execute (NO generator in dual-brain)
            sig = _safe_pop_next_signal(outbox_path)
            if sig:
                last_signal_id = str(sig.get("signal_id") or "")
                logger.info(f"Signal received | id={last_signal_id} | verdict={sig.get('final_verdict')}")
                engine.execute_signal(sig)
            else:
                logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")

        except Exception as e:
            logger.exception(f"WORKER_LOOP_ERROR | err={e}")
            try:
                log_event("WORKER_LOOP_ERROR", f"err={e}")
            except Exception:
                pass

        # 3) update shared state every loop (so Guard always has fresh info)
        _write_shared_state(mode=mode, worker_status="RUNNING", last_signal_id=last_signal_id)

        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
