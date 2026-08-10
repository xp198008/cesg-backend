"""进程内短 TTL 缓存，供列表热点字典/组织树复用。"""
from __future__ import annotations

import time
from threading import Lock
from typing import Any, Callable, TypeVar

T = TypeVar("T")

_lock = Lock()
_store: dict[str, tuple[Any, float]] = {}


def ttl_get(key: str, ttl_seconds: float) -> Any | None:
    now = time.time()
    with _lock:
        hit = _store.get(key)
        if not hit:
            return None
        value, ts = hit
        if (now - ts) < ttl_seconds:
            return value
        _store.pop(key, None)
    return None


def ttl_set(key: str, value: Any, *, max_entries: int = 2048) -> None:
    with _lock:
        if len(_store) > max_entries:
            _store.clear()
        _store[key] = (value, time.time())


def ttl_get_or_set(key: str, ttl_seconds: float, factory: Callable[[], T]) -> T:
    cached = ttl_get(key, ttl_seconds)
    if cached is not None:
        return cached  # type: ignore[return-value]
    value = factory()
    ttl_set(key, value)
    return value


async def ttl_get_or_set_async(key: str, ttl_seconds: float, factory) -> Any:
    cached = ttl_get(key, ttl_seconds)
    if cached is not None:
        return cached
    value = await factory()
    ttl_set(key, value)
    return value


def ttl_clear(prefix: str | None = None) -> None:
    with _lock:
        if prefix is None:
            _store.clear()
            return
        for key in list(_store.keys()):
            if key.startswith(prefix):
                _store.pop(key, None)
