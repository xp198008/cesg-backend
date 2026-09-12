"""多 worker 时只让一个进程跑后台调度，避免报警/OBD/AI 各跑三份。"""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_lock_fh = None


def should_run_schedulers() -> bool:
    flag = (os.getenv("CESG_RUN_SCHEDULERS") or "").strip().lower()
    if flag in {"0", "false", "no"}:
        return False
    if flag in {"1", "true", "yes"}:
        return True
    return _try_acquire_lock()


def _try_acquire_lock() -> bool:
    global _lock_fh
    if os.name == "nt":
        return True
    try:
        import fcntl
    except ImportError:
        return True

    raw = (os.getenv("CESG_SCHEDULER_LOCK") or "").strip()
    lock_path = Path(raw) if raw else Path("/tmp/cesg-scheduler.lock")
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        _lock_fh = open(lock_path, "a+", encoding="utf-8")
        fcntl.flock(_lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fh.seek(0)
        _lock_fh.truncate()
        _lock_fh.write(str(os.getpid()))
        _lock_fh.flush()
        logger.info("本进程持有调度锁 %s pid=%s", lock_path, os.getpid())
        return True
    except OSError as exc:
        logger.info("本进程仅处理 HTTP，不启动后台调度：%s", exc)
        if _lock_fh is not None:
            try:
                _lock_fh.close()
            except Exception:
                pass
            _lock_fh = None
        return False
