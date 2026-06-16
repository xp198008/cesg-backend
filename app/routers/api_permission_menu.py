"""角色授权功能树。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/api/permission-menu", tags=["permission-menu"])

_DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "permission_menu.json"


def _read_tree() -> list[dict[str, Any]]:
    if not _DATA_FILE.is_file():
        return []
    raw = json.loads(_DATA_FILE.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("tree"), list):
        return raw["tree"]
    return []


@router.get("/tree")
async def permission_menu_tree():
    return {"ok": True, "tree": _read_tree()}
