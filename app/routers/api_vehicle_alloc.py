"""基础数据管理：车辆分配规则（管控车辆、分配用户）。"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import DATABASE_URL, get_db
from app.models import (
    Fleet,
    OrgCompany,
    SysUser,
    Vehicle,
    VehicleAllocRule,
    VehicleAllocRuleUser,
    VehicleAllocRuleVehicle,
)
from app.org_scope import collect_org_company_subtree_ids


def _db_fingerprint() -> str:
    raw = (DATABASE_URL or "").replace("sqlite+aiosqlite:///", "").replace("sqlite:///", "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _no_cache(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate, private"
    response.headers["Pragma"] = "no-cache"


router = APIRouter(
    prefix="/api/vehicle-alloc",
    tags=["vehicle-alloc"],
    dependencies=[Depends(_no_cache)],
)


class RuleCreate(BaseModel):
    company_id: int = Field(..., ge=1)
    fleet_id: int | None = None
    name: str = Field(..., min_length=1, max_length=128)
    remark: str | None = Field(None, max_length=512)


class RuleUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=128)
    remark: str | None = Field(None, max_length=512)


class VehiclesSet(BaseModel):
    vehicle_ids: list[int] = Field(default_factory=list)


class UsersSet(BaseModel):
    user_ids: list[int] = Field(default_factory=list)


def _normalize_rule_name(name: str | None) -> str:
    raw = unicodedata.normalize("NFKC", name or "")
    raw = re.sub(r"[\s\u200b\u200c\u200d\ufeff]+", "", raw, flags=re.UNICODE)
    return raw.casefold()


async def _ensure_fleet_belongs_company(
    db: AsyncSession,
    company_id: int,
    fleet_id: int | None,
) -> None:
    if fleet_id is None:
        return
    fleet = await db.scalar(select(Fleet).where(Fleet.id == fleet_id).limit(1))
    if fleet is None or fleet.company_id != company_id:
        raise HTTPException(status_code=400, detail="车队不存在或不属于所选公司")


async def _rule_name_taken(
    db: AsyncSession,
    company_id: int,
    fleet_id: int | None,
    name: str,
    exclude_id: int | None = None,
) -> bool:
    wanted = _normalize_rule_name(name)
    if not wanted:
        return False
    stmt = select(VehicleAllocRule.id, VehicleAllocRule.name).where(
        VehicleAllocRule.company_id == company_id
    )
    if fleet_id is None:
        stmt = stmt.where(VehicleAllocRule.fleet_id.is_(None))
    else:
        stmt = stmt.where(VehicleAllocRule.fleet_id == fleet_id)
    if exclude_id is not None:
        stmt = stmt.where(VehicleAllocRule.id != exclude_id)
    rows = (await db.execute(stmt)).all()
    return any(_normalize_rule_name(row_name) == wanted for _, row_name in rows)


def _scope_label(fleet_id: int | None, fleet_name: str | None) -> str:
    return fleet_name or f"车队#{fleet_id}" if fleet_id is not None else "全公司"


@router.get("/fleets")
async def list_fleets(company_id: int = Query(..., ge=1), db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(select(Fleet).where(Fleet.company_id == company_id).order_by(Fleet.id))
    ).scalars().all()
    return {"ok": True, "items": [{"id": x.id, "name": x.name, "company_id": x.company_id} for x in rows]}


@router.get("/vehicles")
async def list_company_vehicles(
    company_id: int = Query(..., ge=1),
    fleet_id: int | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Vehicle).where(Vehicle.company_id == company_id)
    if fleet_id is not None:
        stmt = stmt.where(Vehicle.fleet_id == fleet_id)
    rows = (await db.execute(stmt.order_by(Vehicle.plate_no))).scalars().all()
    return {
        "ok": True,
        "items": [
            {"id": v.id, "plate_no": v.plate_no, "fleet_id": v.fleet_id, "company_id": v.company_id}
            for v in rows
        ],
    }


@router.get("/users")
async def list_company_users(company_id: int = Query(..., ge=1), db: AsyncSession = Depends(get_db)):
    org_ids = await collect_org_company_subtree_ids(db, company_id)
    rows = (
        await db.execute(select(SysUser).where(SysUser.org_id.in_(org_ids)).order_by(SysUser.org_id, SysUser.id))
    ).scalars().all()
    return {
        "ok": True,
        "items": [
            {"id": u.id, "username": u.username, "real_name": u.real_name or "", "org_id": u.org_id}
            for u in rows
        ],
    }


@router.get("/rules")
async def list_rules(
    company_id: int = Query(..., ge=1),
    fleet_id: int | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(VehicleAllocRule)
        .options(selectinload(VehicleAllocRule.fleet))
        .where(VehicleAllocRule.company_id == company_id)
    )
    if fleet_id is not None:
        stmt = stmt.where(VehicleAllocRule.fleet_id == fleet_id)
    rules = (await db.execute(stmt.order_by(VehicleAllocRule.id))).scalars().unique().all()

    vehicle_counts: dict[int, int] = {}
    user_counts: dict[int, int] = {}
    ids = [r.id for r in rules]
    if ids:
        for rid, count in (
            await db.execute(
                select(VehicleAllocRuleVehicle.rule_id, func.count())
                .where(VehicleAllocRuleVehicle.rule_id.in_(ids))
                .group_by(VehicleAllocRuleVehicle.rule_id)
            )
        ).all():
            vehicle_counts[int(rid)] = int(count)
        for rid, count in (
            await db.execute(
                select(VehicleAllocRuleUser.rule_id, func.count())
                .where(VehicleAllocRuleUser.rule_id.in_(ids))
                .group_by(VehicleAllocRuleUser.rule_id)
            )
        ).all():
            user_counts[int(rid)] = int(count)

    items: list[dict[str, Any]] = []
    for rule in rules:
        fleet_name = rule.fleet.name if rule.fleet else None
        items.append(
            {
                "id": rule.id,
                "name": rule.name,
                "company_id": rule.company_id,
                "fleet_id": rule.fleet_id,
                "fleet_name": fleet_name,
                "scope_label": _scope_label(rule.fleet_id, fleet_name),
                "remark": rule.remark or "",
                "vehicle_count": vehicle_counts.get(rule.id, 0),
                "user_count": user_counts.get(rule.id, 0),
            }
        )
    return {"ok": True, "list": items, "total": len(items), "db_fp": _db_fingerprint()}


@router.get("/rule-counts")
async def rule_counts(db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(
            select(VehicleAllocRule.company_id, func.count())
            .group_by(VehicleAllocRule.company_id)
        )
    ).all()
    return {
        "ok": True,
        "counts": {str(company_id): int(count or 0) for company_id, count in rows},
    }


@router.get("/rules/{rule_id}")
async def get_rule(rule_id: int, db: AsyncSession = Depends(get_db)):
    rule = await db.scalar(
        select(VehicleAllocRule)
        .options(
            selectinload(VehicleAllocRule.company),
            selectinload(VehicleAllocRule.fleet),
            selectinload(VehicleAllocRule.vehicles),
            selectinload(VehicleAllocRule.users),
        )
        .where(VehicleAllocRule.id == rule_id)
        .limit(1)
    )
    if rule is None:
        raise HTTPException(status_code=404, detail="规则不存在")
    fleet_name = rule.fleet.name if rule.fleet else None
    return {
        "ok": True,
        "data": {
            "id": rule.id,
            "name": rule.name,
            "company_id": rule.company_id,
            "company_name": rule.company.name if rule.company else "",
            "fleet_id": rule.fleet_id,
            "fleet_name": fleet_name,
            "scope_label": _scope_label(rule.fleet_id, fleet_name),
            "remark": rule.remark or "",
            "vehicles": [
                {"id": v.id, "plate_no": v.plate_no, "fleet_id": v.fleet_id}
                for v in (rule.vehicles or [])
            ],
            "users": [
                {"id": u.id, "username": u.username, "real_name": u.real_name or ""}
                for u in (rule.users or [])
            ],
        },
    }


@router.post("/rules")
async def create_rule(payload: RuleCreate, db: AsyncSession = Depends(get_db)):
    company = await db.scalar(select(OrgCompany).where(OrgCompany.id == payload.company_id).limit(1))
    if company is None:
        raise HTTPException(status_code=400, detail="公司不存在")
    await _ensure_fleet_belongs_company(db, payload.company_id, payload.fleet_id)
    name = payload.name.strip()
    if await _rule_name_taken(db, payload.company_id, payload.fleet_id, name):
        raise HTTPException(status_code=400, detail="该范围内已存在同名规则")
    rule = VehicleAllocRule(
        company_id=payload.company_id,
        fleet_id=payload.fleet_id,
        name=name,
        remark=(payload.remark or "").strip() or None,
    )
    db.add(rule)
    await db.flush()
    return {"ok": True, "message": "已创建", "data": {"id": rule.id}}


@router.post("/rules/{rule_id}/update")
async def update_rule(rule_id: int, payload: RuleUpdate, db: AsyncSession = Depends(get_db)):
    rule = await db.scalar(select(VehicleAllocRule).where(VehicleAllocRule.id == rule_id).limit(1))
    if rule is None:
        raise HTTPException(status_code=404, detail="规则不存在")
    if payload.name is not None:
        name = payload.name.strip()
        if await _rule_name_taken(db, rule.company_id, rule.fleet_id, name, exclude_id=rule_id):
            raise HTTPException(status_code=400, detail="该范围内已存在同名规则")
        rule.name = name
    if payload.remark is not None:
        rule.remark = payload.remark.strip() or None
    await db.flush()
    return {"ok": True, "message": "已保存"}


async def _delete_rule(db: AsyncSession, rule_id: int) -> dict:
    rule = await db.scalar(select(VehicleAllocRule).where(VehicleAllocRule.id == rule_id).limit(1))
    if rule is None:
        raise HTTPException(status_code=404, detail="规则不存在")
    await db.execute(delete(VehicleAllocRuleVehicle).where(VehicleAllocRuleVehicle.rule_id == rule_id))
    await db.execute(delete(VehicleAllocRuleUser).where(VehicleAllocRuleUser.rule_id == rule_id))
    await db.execute(delete(VehicleAllocRule).where(VehicleAllocRule.id == rule_id))
    await db.flush()
    return {"ok": True, "message": "已删除", "db_fp": _db_fingerprint(), "purged_same_name": 0}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int, db: AsyncSession = Depends(get_db)):
    return await _delete_rule(db, rule_id)


@router.post("/rules/{rule_id}/delete")
async def delete_rule_post(rule_id: int, db: AsyncSession = Depends(get_db)):
    return await _delete_rule(db, rule_id)


async def _get_rule_or_404(db: AsyncSession, rule_id: int) -> VehicleAllocRule:
    rule = await db.scalar(select(VehicleAllocRule).where(VehicleAllocRule.id == rule_id).limit(1))
    if rule is None:
        raise HTTPException(status_code=404, detail="规则不存在")
    return rule


async def _validate_vehicles(db: AsyncSession, rule: VehicleAllocRule, vehicle_ids: list[int]) -> None:
    ids = list(dict.fromkeys(vehicle_ids))
    if not ids:
        return
    rows = (await db.execute(select(Vehicle).where(Vehicle.id.in_(ids)))).scalars().all()
    if len(rows) != len(ids):
        raise HTTPException(status_code=400, detail="存在无效的车辆 ID")
    for vehicle in rows:
        if vehicle.company_id != rule.company_id:
            raise HTTPException(status_code=400, detail=f"车辆 {vehicle.plate_no} 不属于该规则所属公司")
        if rule.fleet_id is not None and vehicle.fleet_id != rule.fleet_id:
            raise HTTPException(status_code=400, detail=f"车队级规则下车辆须属于该车队：{vehicle.plate_no}")


async def _validate_users(db: AsyncSession, rule: VehicleAllocRule, user_ids: list[int]) -> None:
    ids = list(dict.fromkeys(user_ids))
    if not ids:
        return
    org_ids = await collect_org_company_subtree_ids(db, rule.company_id)
    rows = (await db.execute(select(SysUser).where(SysUser.id.in_(ids)))).scalars().all()
    if len(rows) != len(ids):
        raise HTTPException(status_code=400, detail="存在无效的用户 ID")
    for user in rows:
        if user.org_id not in org_ids:
            raise HTTPException(status_code=400, detail=f"用户 {user.username} 不属于该规则所属公司或下级组织")


@router.post("/rules/{rule_id}/vehicles")
async def set_rule_vehicles(rule_id: int, payload: VehiclesSet, db: AsyncSession = Depends(get_db)):
    rule = await _get_rule_or_404(db, rule_id)
    ids = list(dict.fromkeys(payload.vehicle_ids))
    await _validate_vehicles(db, rule, ids)
    await db.execute(delete(VehicleAllocRuleVehicle).where(VehicleAllocRuleVehicle.rule_id == rule_id))
    for vehicle_id in ids:
        db.add(VehicleAllocRuleVehicle(rule_id=rule_id, vehicle_id=vehicle_id))
    await db.flush()
    await db.commit()
    return {"ok": True, "message": "管控车辆已更新", "count": len(ids)}


@router.post("/rules/{rule_id}/users")
async def set_rule_users(rule_id: int, payload: UsersSet, db: AsyncSession = Depends(get_db)):
    rule = await _get_rule_or_404(db, rule_id)
    ids = list(dict.fromkeys(payload.user_ids))
    await _validate_users(db, rule, ids)
    await db.execute(delete(VehicleAllocRuleUser).where(VehicleAllocRuleUser.rule_id == rule_id))
    for user_id in ids:
        db.add(VehicleAllocRuleUser(rule_id=rule_id, user_id=user_id))
    await db.flush()
    await db.commit()
    return {"ok": True, "message": "分配用户已更新", "count": len(ids)}
