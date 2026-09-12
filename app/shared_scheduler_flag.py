"""跨 worker 的调度开关：start/stop/status 可能打到不同进程。"""
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


class SharedSchedulerFlag:
    def __init__(self, stem: str) -> None:
        self.stem = stem

    def desired_path(self) -> Path:
        return _run_dir() / f"{self.stem}.desired"

    def state_path(self) -> Path:
        return _run_dir() / f"{self.stem}.state"

    def set_desired(self, on: bool) -> None:
        path = self.desired_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("1" if on else "0", encoding="utf-8")

    def get_desired(self, default: bool = True) -> bool:
        path = self.desired_path()
        if not path.exists():
            return default
        try:
            return path.read_text(encoding="utf-8").strip() == "1"
        except OSError:
            return default

    def desired_age_sec(self) -> float:
        path = self.desired_path()
        if not path.exists():
            return 1e9
        try:
            return max(0.0, time.time() - path.stat().st_mtime)
        except OSError:
            return 1e9

    def write_state(self, payload: dict[str, Any]) -> None:
        path = self.state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        body = dict(payload)
        body["updated"] = time.time()
        body["pid"] = os.getpid()
        tmp = path.with_suffix(".state.tmp")
        tmp.write_text(json.dumps(body, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(path)

    def read_state(self) -> dict[str, Any]:
        path = self.state_path()
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def state_age_sec(self, state: dict[str, Any] | None = None) -> float:
        st = state if state is not None else self.read_state()
        try:
            return max(0.0, time.time() - float(st.get("updated") or 0))
        except (TypeError, ValueError):
            return 1e9

    def resolve_running(self, *, local_running: bool | None, is_holder: bool) -> bool:
        desired = self.get_desired(default=True)
        age = self.desired_age_sec()
        if not desired:
            return False
        if is_holder and local_running is not None:
            if local_running:
                return True
            return age < 12
        st = self.read_state()
        fresh = self.state_age_sec(st) < 25 and bool(st.get("running"))
        if fresh:
            return True
        return age < 12


obd_speed_flag = SharedSchedulerFlag("obd_speed")
