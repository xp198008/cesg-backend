"""AI 评估调度的跨 worker 开关。

start/stop/status 可能打到不同 HTTP 进程；真正的循环只在持调度锁的进程里跑。
用 .run 目录下的开关文件 + 心跳，让任意进程看到同一份运行状态。
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


def _run_dir() -> Path:
    raw = (os.getenv("CESG_SCHEDULER_LOCK") or "").strip()
    if raw:
        return Path(raw).parent
    return Path("/tmp")


def desired_path() -> Path:
    return _run_dir() / "ai_assess.desired"


def state_path() -> Path:
    return _run_dir() / "ai_assess.state"


def set_desired(on: bool) -> None:
    path = desired_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("1" if on else "0", encoding="utf-8")


def get_desired(default: bool = True) -> bool:
    path = desired_path()
    if not path.exists():
        return default
    try:
        return path.read_text(encoding="utf-8").strip() == "1"
    except OSError:
        return default


def desired_age_sec() -> float:
    path = desired_path()
    if not path.exists():
        return 1e9
    try:
        return max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return 1e9


def write_state(payload: dict[str, Any]) -> None:
    path = state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body["updated"] = time.time()
    body["pid"] = os.getpid()
    tmp = path.with_suffix(".state.tmp")
    tmp.write_text(json.dumps(body, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)


def read_state() -> dict[str, Any]:
    path = state_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def state_age_sec(state: dict[str, Any] | None = None) -> float:
    st = state if state is not None else read_state()
    try:
        return max(0.0, time.time() - float(st.get("updated") or 0))
    except (TypeError, ValueError):
        return 1e9


def resolve_running(*, local_running: bool | None, is_holder: bool) -> bool:
    """任意 worker 上的「运行中」口径。"""
    desired = get_desired(default=True)
    age = desired_age_sec()
    if not desired:
        return False
    if is_holder and local_running is not None:
        if local_running:
            return True
        return age < 12
    st = read_state()
    fresh = state_age_sec(st) < 25 and bool(st.get("running"))
    if fresh:
        return True
    return age < 12
