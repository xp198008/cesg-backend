"""车辆风险画像：对接外部周报 API，月报优先官方接口，否则由周报拼出。"""
from __future__ import annotations

import logging
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from fastapi import HTTPException

from app.alarm_type_gate import load_disabled_alarm_type_names
from app.config import settings
from app.models import Driver, OrgCompany, Vehicle, VehicleViolation
from app.org_scope import collect_org_company_subtree_ids, require_user_company_subtree_ids
from app.violation_filters import violation_list_visibility

logger = logging.getLogger(__name__)

COUNT_FIELDS = (
    "over_6h_count",
    "over_4h_count",
    "over_3h_count",
    "mental_alarm_count",
    "behavior_alarm_count",
    "route_alarm_count",
    "weather_alarm_count",
)


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _parse_ymd(value: str) -> date:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return datetime.strptime(text, "%Y-%m-%d").date()
    raise ValueError(f"日期格式错误: {value!r}，须为 yyyyMMdd 或 yyyy-MM-dd")


def _parse_month(value: str) -> tuple[int, int]:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 6 or not text.isdigit():
        raise ValueError(f"月份格式错误: {value!r}，须为 yyyyMM")
    year, month = int(text[:4]), int(text[4:6])
    if month < 1 or month > 12:
        raise ValueError(f"月份无效: {value!r}")
    return year, month


def week_end_dates_in_month(year: int, month: int) -> list[date]:
    """取该自然月内所有「周结束日」（与样例一致：以周三为周末）。

    样例周：20260709~20260715（周三结束）。用这些周末的周报 SUM 拼成月报。
    """
    first = date(year, month, 1)
    # Python: Monday=0 ... Wednesday=2
    delta = (2 - first.weekday()) % 7
    cursor = first + timedelta(days=delta)
    ends: list[date] = []
    while cursor.month == month:
        ends.append(cursor)
        cursor += timedelta(days=7)
    return ends


def _empty_counts() -> dict[str, int]:
    return {key: 0 for key in COUNT_FIELDS}


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _merge_counts(target: dict[str, int], source: dict[str, Any]) -> None:
    for key in COUNT_FIELDS:
        target[key] = target.get(key, 0) + _as_int(source.get(key))


def total_alarm_count(row: dict[str, Any]) -> int:
    return sum(_as_int(row.get(key)) for key in COUNT_FIELDS)


def risk_score_from_counts(row: dict[str, Any]) -> int:
    """简易综合风险分：报警越多分越高，封顶 100。"""
    weighted = (
        _as_int(row.get("over_6h_count")) * 8
        + _as_int(row.get("over_4h_count")) * 5
        + _as_int(row.get("over_3h_count")) * 3
        + _as_int(row.get("mental_alarm_count")) * 0.08
        + _as_int(row.get("behavior_alarm_count")) * 0.08
        + _as_int(row.get("route_alarm_count")) * 0.05
        + _as_int(row.get("weather_alarm_count")) * 0.04
    )
    return max(0, min(100, int(round(weighted))))


def risk_level_from_score(score: int) -> str:
    if score >= 70:
        return "高风险"
    if score >= 40:
        return "中风险"
    return "低风险"


def radar_values(row: dict[str, Any]) -> list[int]:
    """将计数归一到雷达图 0~100。页面雷达 6 维沿用现有 UI 轴名。"""
    behavior = min(100, int(_as_int(row.get("behavior_alarm_count")) * 100 / 200))
    fatigue = min(100, int(_as_int(row.get("mental_alarm_count")) * 100 / 200))
    overtime = min(
        100,
        int(
            (
                _as_int(row.get("over_3h_count"))
                + _as_int(row.get("over_4h_count")) * 2
                + _as_int(row.get("over_6h_count")) * 3
            )
            * 100
            / 20
        ),
    )
    route = min(100, int(_as_int(row.get("route_alarm_count")) * 100 / 200))
    weather = min(100, int(_as_int(row.get("weather_alarm_count")) * 100 / 100))
    alarm_event = min(100, int(total_alarm_count(row) * 100 / 800))
    return [behavior, overtime, weather, route, fatigue, alarm_event]


class RiskProfileClient:
    def __init__(self) -> None:
        self.base_url = (settings.risk_api_base_url or "").rstrip("/")
        self.timeout = httpx.Timeout(settings.risk_api_timeout, connect=min(8.0, settings.risk_api_timeout))

    async def fetch_weekly_page(
        self,
        weekly_end_date: str,
        *,
        car_id: int | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "weekly_end_date": weekly_end_date,
            "page": page,
            "page_size": page_size,
        }
        if car_id is not None:
            params["car_id"] = car_id
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            resp = await client.get(f"{self.base_url}/api/v1/risk/weekly", params=params)
            resp.raise_for_status()
            body = resp.json()
        if isinstance(body, dict) and body.get("code") not in (None, 0, "0"):
            raise RuntimeError(body.get("message") or "周风险查询失败")
        return body.get("data") if isinstance(body, dict) else body

    async def fetch_weekly_all(
        self,
        weekly_end_date: str,
        *,
        car_id: int | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        total_pages = 1
        while page <= total_pages:
            data = await self.fetch_weekly_page(
                weekly_end_date, car_id=car_id, page=page, page_size=100
            )
            if not isinstance(data, dict):
                break
            batch = data.get("items") or []
            items.extend(batch)
            total_pages = max(1, _as_int(data.get("total_pages") or 1))
            if not batch:
                break
            page += 1
            if page > 50:
                break
        return items

    async def fetch_monthly_official(
        self,
        report_month: str,
        *,
        car_id: int | None = None,
    ) -> list[dict[str, Any]] | None:
        """官方月报可用则直接用；404/不可用返回 None，由调用方周报拼接。"""
        params: dict[str, Any] = {"report_month": report_month, "page": 1, "page_size": 100}
        if car_id is not None:
            params["car_id"] = car_id
        try:
            async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
                resp = await client.get(f"{self.base_url}/api/v1/risk/monthly", params=params)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                body = resp.json()
        except httpx.HTTPError as exc:
            logger.info("官方月报不可用，将改用周报拼接: %s", exc)
            return None
        if isinstance(body, dict) and body.get("code") not in (None, 0, "0"):
            return None
        data = body.get("data") if isinstance(body, dict) else body
        if not isinstance(data, dict):
            return None
        items = list(data.get("items") or [])
        total_pages = max(1, _as_int(data.get("total_pages") or 1))
        page = 2
        while page <= total_pages and page <= 50:
            params["page"] = page
            async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
                resp = await client.get(f"{self.base_url}/api/v1/risk/monthly", params=params)
                resp.raise_for_status()
                body = resp.json()
            data = body.get("data") if isinstance(body, dict) else {}
            batch = (data or {}).get("items") or []
            if not batch:
                break
            items.extend(batch)
            page += 1
        return items

    async def fetch_monthly_stitched(
        self,
        report_month: str,
        *,
        car_id: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        year, month = _parse_month(report_month)
        week_ends = week_end_dates_in_month(year, month)
        if not week_ends:
            return [], []
        merged: dict[int, dict[str, int]] = defaultdict(_empty_counts)
        used_weeks: list[str] = []
        for end in week_ends:
            end_s = _ymd(end)
            rows = await self.fetch_weekly_all(end_s, car_id=car_id)
            if not rows:
                continue
            used_weeks.append(end_s)
            for row in rows:
                cid = _as_int(row.get("car_id"))
                if not cid:
                    continue
                _merge_counts(merged[cid], row)
        items = []
        for cid, counts in merged.items():
            item = {"car_id": cid, "report_month": f"{year:04d}{month:02d}", **counts}
            items.append(item)
        items.sort(key=lambda x: (-total_alarm_count(x), x["car_id"]))
        return items, used_weeks


def parse_plate_by_car_id(
    *,
    car_ids: list[int] | None = None,
    plates: list[str] | None = None,
    car_plates: str | None = None,
) -> dict[int, str]:
    """解析 car_id → 车牌。只作风险计数与本地车辆的对齐键，不含公司/司机。"""
    result: dict[int, str] = {}
    raw = (car_plates or "").strip()
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if not part or ":" not in part:
                continue
            left, right = part.split(":", 1)
            try:
                cid = int(left.strip())
            except ValueError:
                continue
            plate = right.strip()
            if cid and plate:
                result[cid] = plate
    ids = [int(x) for x in (car_ids or []) if x is not None]
    plate_list = [str(x).strip() for x in (plates or []) if str(x).strip()]
    if ids and plate_list and len(ids) == len(plate_list):
        for cid, plate in zip(ids, plate_list, strict=False):
            if cid and plate:
                result[cid] = plate
    return result


async def load_local_vehicles(db: AsyncSession) -> list[Vehicle]:
    """本地基础数据：车辆 + 公司 + 车队 + 司机 + 终端。"""
    stmt = select(Vehicle).options(
        selectinload(Vehicle.company),
        selectinload(Vehicle.fleet),
        selectinload(Vehicle.driver),
        selectinload(Vehicle.devices),
    )
    return list((await db.execute(stmt)).scalars().all())


def _vehicle_tid(vehicle: Vehicle | None) -> str:
    if not vehicle:
        return ""
    for dev in vehicle.devices or []:
        for key in (dev.device_no, dev.device_sn, dev.sim_no, getattr(dev, "actual_sim", None)):
            text = str(key or "").strip()
            if text:
                return text
    return ""


async def enrich_vehicle_dimensions(
    db: AsyncSession,
    items: list[dict[str, Any]],
    *,
    plate_by_car_id: dict[int, str] | None = None,
) -> list[dict[str, Any]]:
    """用本地 vehicle 挂公司/司机；car_id 仅通过车牌映射对齐风险行。"""
    vehicles = await load_local_vehicles(db)
    by_plate: dict[str, Vehicle] = {}
    by_tid: dict[str, Vehicle] = {}
    for v in vehicles:
        if v.plate_no:
            by_plate[str(v.plate_no).strip()] = v
        tid = _vehicle_tid(v)
        if tid:
            by_tid[tid] = v

    plate_map = dict(plate_by_car_id or {})
    enriched: list[dict[str, Any]] = []
    for raw in items:
        cid = _as_int(raw.get("car_id"))
        plate = plate_map.get(cid) or ""
        vehicle = by_plate.get(plate) if plate else None
        if vehicle is None and plate:
            vehicle = by_tid.get(plate)
        # 无外部车牌映射时，不猜公司/司机，避免脏数据
        score = risk_score_from_counts(raw)
        row = {
            **{k: _as_int(raw.get(k)) for k in COUNT_FIELDS},
            "car_id": cid,
            "weekly_start_date": raw.get("weekly_start_date"),
            "weekly_end_date": raw.get("weekly_end_date"),
            "report_month": raw.get("report_month"),
            "plate_no": (vehicle.plate_no if vehicle else plate) or "",
            "tid": _vehicle_tid(vehicle),
            "vehicle_id": vehicle.id if vehicle else None,
            "company_id": vehicle.company_id if vehicle else None,
            "company_name": (vehicle.company.name if vehicle and vehicle.company else None),
            "fleet_id": vehicle.fleet_id if vehicle else None,
            "fleet_name": (vehicle.fleet.name if vehicle and vehicle.fleet else None),
            "driver_id": vehicle.driver_id if vehicle else None,
            "driver_name": (
                (vehicle.driver_name or (vehicle.driver.name if vehicle.driver else None))
                if vehicle
                else None
            ),
            "vin": vehicle.vin if vehicle else None,
            "vehicle_type": vehicle.vehicle_type if vehicle else None,
            "total_alarm_count": total_alarm_count(raw),
            "risk_score": score,
            "risk_level": risk_level_from_score(score),
            "radar": radar_values(raw),
        }
        enriched.append(row)
    return enriched


async def load_org_company_maps(
    db: AsyncSession,
) -> tuple[dict[int, int | None], dict[int, str], dict[int, int], dict[int, int]]:
    """parent_map / name_map / gid→local / local→gid。"""
    rows = (
        await db.execute(select(OrgCompany.id, OrgCompany.parent_id, OrgCompany.name, OrgCompany.jt808_group_id))
    ).all()
    parent_map: dict[int, int | None] = {}
    name_map: dict[int, str] = {}
    gid_to_local: dict[int, int] = {}
    local_to_gid: dict[int, int] = {}
    for oid, parent_id, name, gid in rows:
        cid = int(oid)
        parent_map[cid] = int(parent_id) if parent_id is not None else None
        name_map[cid] = str(name or "").strip()
        if gid is not None:
            g = int(gid)
            gid_to_local[g] = cid
            local_to_gid[cid] = g
    return parent_map, name_map, gid_to_local, local_to_gid


async def resolve_allow_companies_via_jt808(
    selected_raw: list[int],
    *,
    gid_to_local: dict[int, int],
    local_to_gid: dict[int, int],
    known_local_ids: set[int],
) -> tuple[set[int], dict[int, set[int]], dict[int, str], list[dict[str, Any]]]:
    """
    按 JT808 分组树（与前端公司树一致）展开筛选，并返回直接下级分桶：
    child_gid -> 该下级及其全部子孙 gid；
    display_children：须展示的二级维度（即使数据为 0）。
    """
    from app.jt808_group import collect_group_subtree_gids, fetch_group_children

    allow_local: set[int] = set()
    child_buckets: dict[int, set[int]] = {}
    bucket_names: dict[int, str] = {}
    display_children: list[dict[str, Any]] = []
    seen_display: set[int] = set()

    for raw in selected_raw:
        try:
            n = int(raw)
        except (TypeError, ValueError):
            continue
        if n in gid_to_local:
            root_gid = n
        elif n in local_to_gid:
            root_gid = local_to_gid[n]
        elif n in known_local_ids:
            # 本地公司无 jt808_group_id，无法按平台树展开
            logger.warning("公司筛选跳过 JT808 展开：本地公司无 jt808_group_id id=%s", n)
            continue
        else:
            root_gid = n

        try:
            subtree_gids = await collect_group_subtree_gids(root_gid)
        except Exception as e:  # noqa: BLE001
            logger.warning("JT808 子树展开失败 root=%s: %s", root_gid, e)
            subtree_gids = {root_gid}

        for g in subtree_gids:
            if g in gid_to_local:
                allow_local.add(gid_to_local[g])

        children = await fetch_group_children(root_gid)
        covered: set[int] = set()
        if children:
            for node in children:
                raw_id = node.get("id")
                if raw_id is None:
                    continue
                try:
                    child_gid = int(raw_id)
                except (TypeError, ValueError):
                    continue
                name = str(node.get("name") or node.get("text") or "").strip()
                if name:
                    bucket_names[child_gid] = name
                if child_gid not in seen_display:
                    seen_display.add(child_gid)
                    display_children.append({"gid": child_gid, "name": name})
                try:
                    child_tree = await collect_group_subtree_gids(child_gid)
                except Exception:  # noqa: BLE001
                    child_tree = {child_gid}
                child_buckets[child_gid] = child_tree
                covered |= child_tree
                for g in child_tree:
                    if g in gid_to_local:
                        allow_local.add(gid_to_local[g])
            # 挂在选中节点自身、未落入任一直接下级的车辆，归入选中公司（不作为二级展示行）
            root_only = {root_gid} | (subtree_gids - covered)
            if root_only:
                child_buckets[root_gid] = root_only
        else:
            # 叶子：整树归到自身，并展示自身
            child_buckets[root_gid] = subtree_gids
            for g in subtree_gids:
                if g in gid_to_local:
                    allow_local.add(gid_to_local[g])
            if root_gid not in seen_display:
                seen_display.add(root_gid)
                display_children.append({"gid": root_gid, "name": ""})

    return allow_local, child_buckets, bucket_names, display_children


def rollup_items_by_jt808_child_buckets(
    items: list[dict[str, Any]],
    *,
    local_to_gid: dict[int, int],
    gid_to_local: dict[int, int],
    name_map: dict[int, str],
    child_buckets: dict[int, set[int]],
    bucket_names: dict[int, str],
) -> list[dict[str, Any]]:
    """按所选公司的 JT808 直接下级分桶（南区/西区/工作车等）。"""
    if not child_buckets:
        return items
    out: list[dict[str, Any]] = []
    for item in items:
        row = dict(item)
        local_cid = _as_int(row.get("company_id"))
        vehicle_gid = local_to_gid.get(local_cid) if local_cid is not None else None
        bucket_gid: int | None = None
        if vehicle_gid is not None:
            for child_gid, descendant_gids in child_buckets.items():
                if vehicle_gid in descendant_gids:
                    bucket_gid = child_gid
                    break
        if bucket_gid is not None:
            local_bucket = gid_to_local.get(bucket_gid)
            row["company_id"] = local_bucket if local_bucket is not None else bucket_gid
            row["company_name"] = (
                bucket_names.get(bucket_gid)
                or (name_map.get(local_bucket) if local_bucket is not None else None)
                or row.get("company_name")
                or "未匹配公司"
            )
        out.append(row)
    return out


def resolve_selected_to_local_org_ids(
    selected_ids: list[int],
    *,
    gid_to_local: dict[int, int],
    known_local_ids: set[int],
) -> set[int]:
    """前端组织树为 JT808 gid，优先按 jt808_group_id 映射到本地 org_company.id。"""
    out: set[int] = set()
    for raw in selected_ids:
        try:
            n = int(raw)
        except (TypeError, ValueError):
            continue
        # 优先 gid：避免 gid 与本地 id 数值碰巧相同却指不同公司
        if n in gid_to_local:
            out.add(gid_to_local[n])
        elif n in known_local_ids:
            out.add(n)
        else:
            out.add(n)
    return out


def build_children_map(parent_map: dict[int, int | None]) -> dict[int, list[int]]:
    children: dict[int, list[int]] = defaultdict(list)
    for cid, pid in parent_map.items():
        if pid is not None:
            children[int(pid)].append(int(cid))
    return children


def expand_org_ids_in_memory(
    root_ids: set[int],
    children_map: dict[int, list[int]],
) -> set[int]:
    """选中公司及其全部下级（内存 BFS，与 org_company.parent_id 一致）。"""
    out: set[int] = set()
    stack = [int(x) for x in root_ids]
    while stack:
        cur = stack.pop()
        if cur in out:
            continue
        out.add(cur)
        stack.extend(children_map.get(cur, []))
    return out


async def expand_company_filter_ids(db: AsyncSession, local_org_ids: set[int]) -> set[int]:
    """选中公司及其全部下级（车辆挂在三四级时也能命中）。"""
    allow: set[int] = set()
    for oid in local_org_ids:
        allow |= await collect_org_company_subtree_ids(db, int(oid))
    return allow


def resolve_level_two_org_id(
    org_id: int | None,
    parent_map: dict[int, int | None],
) -> int | None:
    """未选公司时：沿 parent_id 上溯到集团下的二级公司。"""
    if org_id is None:
        return None
    try:
        current: int | None = int(org_id)
    except (TypeError, ValueError):
        return None
    chain: list[int] = []
    visited: set[int] = set()
    while current and current not in visited:
        visited.add(current)
        chain.append(current)
        current = parent_map.get(current)
    if not chain:
        return None
    chain.reverse()  # [一级, 二级, 三级, ...]
    if len(chain) >= 2:
        return chain[1]
    return chain[0]


def resolve_bucket_under_selected(
    org_id: int | None,
    selected_id: int,
    *,
    parent_map: dict[int, int | None],
    direct_children: set[int],
) -> int | None:
    """
    车辆挂在选中公司子树内时，滚到「选中公司的直接下级」：
    如选本部车队 → 南区项目部 / 西区项目部 / 工作车 …
    挂在选中公司自身上的车辆归入选中公司。
    """
    if org_id is None:
        return None
    try:
        current: int | None = int(org_id)
    except (TypeError, ValueError):
        return None
    visited: set[int] = set()
    while current is not None and current not in visited:
        visited.add(current)
        if current == selected_id:
            return selected_id
        if current in direct_children:
            return current
        current = parent_map.get(current)
    return None


def rollup_items_for_company_dimension(
    items: list[dict[str, Any]],
    *,
    parent_map: dict[int, int | None],
    name_map: dict[int, str],
    children_map: dict[int, list[int]],
    selected_local_ids: set[int] | None,
) -> list[dict[str, Any]]:
    """
    企业风险画像聚合维度：
    - 选中公司：按「直接下级」分桶，其下所有车辆汇总到该下级；
    - 未选公司：滚到集团二级公司。
    """
    out: list[dict[str, Any]] = []
    selected = {int(x) for x in (selected_local_ids or set())}
    child_sets = {
        sid: set(children_map.get(sid, []))
        for sid in selected
    } if selected else {}

    for item in items:
        row = dict(item)
        bucket: int | None = None
        if selected:
            oid = _as_int(row.get("company_id"))
            for sid in selected:
                bucket = resolve_bucket_under_selected(
                    oid,
                    sid,
                    parent_map=parent_map,
                    direct_children=child_sets.get(sid, set()),
                )
                if bucket is not None:
                    break
            # 选中节点无下级时：整棵子树归到选中公司本身
            if bucket is None and oid is not None:
                for sid in selected:
                    if oid == sid or _org_is_under(oid, sid, parent_map):
                        bucket = sid
                        break
        else:
            bucket = resolve_level_two_org_id(row.get("company_id"), parent_map)

        if bucket is not None:
            row["company_id"] = bucket
            row["company_name"] = name_map.get(bucket) or row.get("company_name") or "未匹配公司"
        out.append(row)
    return out


def _org_is_under(
    org_id: int,
    ancestor_id: int,
    parent_map: dict[int, int | None],
) -> bool:
    current: int | None = int(org_id)
    visited: set[int] = set()
    while current is not None and current not in visited:
        if current == ancestor_id:
            return True
        visited.add(current)
        current = parent_map.get(current)
    return False


def _zero_company_row(company_id: Any, company_name: str) -> dict[str, Any]:
    counts = _empty_counts()
    score = risk_score_from_counts(counts)
    return {
        "company_id": company_id,
        "company_name": company_name or "未匹配公司",
        "vehicle_count": 0,
        "car_ids": [],
        "plates": [],
        **counts,
        "total_alarm_count": 0,
        "risk_score": score,
        "risk_level": risk_level_from_score(score),
        "radar": radar_values(counts),
    }


def ensure_company_placeholders(
    rows: list[dict[str, Any]],
    placeholders: list[tuple[Any, str]],
) -> list[dict[str, Any]]:
    """二级公司无论有无数据都保留名称行，缺数据补 0。"""
    if not placeholders:
        return rows
    by_id = {r.get("company_id"): r for r in rows if r.get("company_id") is not None}
    by_name = {
        str(r.get("company_name") or "").strip(): r
        for r in rows
        if str(r.get("company_name") or "").strip()
    }
    out = list(rows)
    for company_id, company_name in placeholders:
        name = str(company_name or "").strip()
        if company_id is not None and company_id in by_id:
            continue
        if name and name in by_name:
            continue
        row = _zero_company_row(company_id, name)
        out.append(row)
        if company_id is not None:
            by_id[company_id] = row
        if name:
            by_name[name] = row
    out.sort(key=lambda x: (-(x.get("total_alarm_count") or 0), str(x.get("company_name") or "")))
    return out


def aggregate_by_company(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[Any, dict[str, Any]] = {}
    for item in items:
        key = item.get("company_id") or f"unknown:{item.get('car_id')}"
        g = groups.get(key)
        if g is None:
            g = {
                "company_id": item.get("company_id"),
                "company_name": item.get("company_name") or "未匹配公司",
                "vehicle_count": 0,
                "car_ids": [],
                "plates": [],
                **_empty_counts(),
            }
            groups[key] = g
        g["vehicle_count"] += 1
        g["car_ids"].append(item.get("car_id"))
        if item.get("plate_no"):
            g["plates"].append(item["plate_no"])
        _merge_counts(g, item)
    result = []
    for g in groups.values():
        score = risk_score_from_counts(g)
        result.append(
            {
                **g,
                "total_alarm_count": total_alarm_count(g),
                "risk_score": score,
                "risk_level": risk_level_from_score(score),
                "radar": radar_values(g),
            }
        )
    result.sort(key=lambda x: (-x["total_alarm_count"], str(x.get("company_name") or "")))
    return result


def aggregate_by_driver(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[Any, dict[str, Any]] = {}
    for item in items:
        key = item.get("driver_id") or f"unbound:{item.get('car_id')}"
        g = groups.get(key)
        if g is None:
            g = {
                "driver_id": item.get("driver_id"),
                "driver_name": item.get("driver_name") or "未绑定司机",
                "company_id": item.get("company_id"),
                "company_name": item.get("company_name"),
                "vehicle_count": 0,
                "car_ids": [],
                "vehicle_ids": [],
                "plates": [],
                **_empty_counts(),
            }
            groups[key] = g
        g["vehicle_count"] += 1
        g["car_ids"].append(item.get("car_id"))
        if item.get("vehicle_id") is not None:
            try:
                g["vehicle_ids"].append(int(item["vehicle_id"]))
            except (TypeError, ValueError):
                pass
        if item.get("plate_no"):
            g["plates"].append(item["plate_no"])
        _merge_counts(g, item)
    result = []
    for g in groups.values():
        score = risk_score_from_counts(g)
        result.append(
            {
                **g,
                "vehicle_ids": sorted({int(v) for v in g.get("vehicle_ids") or []}),
                "plates": sorted({str(p).strip() for p in (g.get("plates") or []) if str(p).strip()}),
                "total_alarm_count": total_alarm_count(g),
                "risk_score": score,
                "risk_level": risk_level_from_score(score),
                "radar": radar_values(g),
            }
        )
    result.sort(key=lambda x: (-x["total_alarm_count"], str(x.get("driver_name") or "")))
    return result


def period_datetime_range(mode: str, period: str | None) -> tuple[datetime, datetime] | None:
    """画像周期 → 本地报警统计闭开区间 [start, end)。

    - weekly：周结束日（周三）往前 6 天共 7 日
    - monthly：自然月
    """
    text = str(period or "").strip()
    if not text:
        return None
    if mode == "monthly":
        year, month = _parse_month(text)
        start = datetime(year, month, 1)
        last_day = monthrange(year, month)[1]
        end = datetime(year, month, last_day) + timedelta(days=1)
        return start, end
    end_day = _parse_ymd(text)
    start_day = end_day - timedelta(days=6)
    start = datetime(start_day.year, start_day.month, start_day.day)
    end = datetime(end_day.year, end_day.month, end_day.day) + timedelta(days=1)
    return start, end


async def resolve_driver_vehicle_keys(
    db: AsyncSession,
    *,
    driver_id: int | None = None,
    driver_name: str | None = None,
    focus: dict[str, Any] | None = None,
) -> tuple[list[str], list[int]]:
    """解析当前司机关联车牌 / 本地车辆 id，供报警类型排名限定范围。"""
    plates: set[str] = set()
    vehicle_ids: set[int] = set()
    focus = focus or {}

    for plate in focus.get("plates") or []:
        text = str(plate or "").strip()
        if text:
            plates.add(text)
    for plate_no in (focus.get("plate_no"),):
        text = str(plate_no or "").strip()
        if text:
            plates.add(text)
    for vid in focus.get("vehicle_ids") or []:
        try:
            vehicle_ids.add(int(vid))
        except (TypeError, ValueError):
            pass
    focus_vid = focus.get("vehicle_id")
    if focus_vid is not None:
        try:
            vehicle_ids.add(int(focus_vid))
        except (TypeError, ValueError):
            pass

    did = driver_id if driver_id is not None else focus.get("driver_id")
    dname = (driver_name or focus.get("driver_name") or "").strip()
    if dname in ("未绑定司机", "—"):
        dname = ""

    if did is not None or dname:
        stmt = select(Vehicle.id, Vehicle.plate_no)
        if did is not None and _as_int(did) > 0:
            stmt = stmt.where(Vehicle.driver_id == int(did))
        elif dname:
            stmt = stmt.outerjoin(Driver, Vehicle.driver_id == Driver.id).where(
                or_(Vehicle.driver_name == dname, Driver.name == dname)
            )
        else:
            stmt = None
        if stmt is not None:
            for vid, plate in (await db.execute(stmt)).all():
                if vid is not None:
                    vehicle_ids.add(int(vid))
                text = str(plate or "").strip()
                if text:
                    plates.add(text)

    return sorted(plates), sorted(vehicle_ids)


async def query_driver_alarm_behavior_ranking(
    db: AsyncSession,
    *,
    plates: list[str],
    vehicle_ids: list[int],
    start_at: datetime,
    end_at: datetime,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """按当前司机报警记录的 violation_type_name 计数降序排名。"""
    if not plates and not vehicle_ids:
        return []

    scope_conds = []
    if plates:
        scope_conds.append(VehicleViolation.plate_no.in_(plates))
    if vehicle_ids:
        scope_conds.append(VehicleViolation.vehicle_id.in_(vehicle_ids))
    if not scope_conds:
        return []

    disabled = await load_disabled_alarm_type_names(db)
    stmt = (
        select(
            VehicleViolation.violation_type_name,
            func.count().label("cnt"),
        )
        .where(
            violation_list_visibility(disabled),
            VehicleViolation.violation_time >= start_at,
            VehicleViolation.violation_time < end_at,
            or_(*scope_conds),
            VehicleViolation.violation_type_name.isnot(None),
            VehicleViolation.violation_type_name != "",
        )
        .group_by(VehicleViolation.violation_type_name)
        .order_by(func.count().desc(), VehicleViolation.violation_type_name.asc())
        .limit(max(1, min(int(limit or 10), 50)))
    )
    rows = (await db.execute(stmt)).all()
    total = sum(int(cnt or 0) for _, cnt in rows)
    ranking: list[dict[str, Any]] = []
    for name, cnt in rows:
        count = int(cnt or 0)
        ranking.append(
            {
                "name": str(name or "").strip() or "—",
                "count": count,
                "ratio": round(count / total, 4) if total else 0.0,
                "score": count,
            }
        )
    return ranking


def build_profile_payload(
    *,
    dimension: str,
    mode: str,
    items: list[dict[str, Any]],
    meta: dict[str, Any],
    company_placeholders: list[tuple[Any, str]] | None = None,
) -> dict[str, Any]:
    if dimension == "company":
        rows = aggregate_by_company(items)
        rows = ensure_company_placeholders(rows, company_placeholders or [])
    elif dimension == "driver":
        rows = aggregate_by_driver(items)
    else:
        rows = sorted(items, key=lambda x: (-x.get("total_alarm_count", 0), x.get("car_id") or 0))

    focus = rows[0] if rows else None
    ranking = []
    # 司机维度「报警行为排名」由 query_risk_profile 按本地报警类型重算，此处不排司机名单
    if dimension != "driver":
        for row in rows[:10]:
            if dimension == "company":
                ranking.append(
                    {
                        "name": row.get("company_name") or "—",
                        "count": row.get("total_alarm_count") or 0,
                        "score": row.get("risk_score") or 0,
                    }
                )
            else:
                ranking.append(
                    {
                        "name": row.get("plate_no") or f"车{row.get('car_id')}",
                        "count": row.get("total_alarm_count") or 0,
                        "score": row.get("risk_score") or 0,
                    }
                )

    metrics = []
    if focus:
        if dimension == "company":
            metrics = [
                {"value": focus.get("vehicle_count") or 0, "label": "车辆数", "tone": "orange"},
                {"value": focus.get("total_alarm_count") or 0, "label": "报警次数", "tone": "red"},
                {"value": focus.get("mental_alarm_count") or 0, "label": "精神疲劳报警", "tone": "orange"},
                {"value": focus.get("behavior_alarm_count") or 0, "label": "驾驶行为报警", "tone": "orange"},
                {"value": focus.get("risk_score") or 0, "label": "综合风险分", "tone": "green"},
            ]
        elif dimension == "driver":
            metrics = [
                {"value": focus.get("vehicle_count") or 0, "label": "绑定车辆数", "tone": "orange"},
                {"value": focus.get("total_alarm_count") or 0, "label": "报警次数", "tone": "red"},
                {"value": focus.get("mental_alarm_count") or 0, "label": "精神疲劳报警", "tone": "orange"},
                {"value": focus.get("risk_score") or 0, "label": "安全风险分", "tone": "green"},
            ]
        else:
            metrics = [
                {"value": focus.get("total_alarm_count") or 0, "label": "报警数", "tone": "red"},
                {"value": focus.get("mental_alarm_count") or 0, "label": "精神疲劳报警", "tone": "orange"},
                {"value": focus.get("behavior_alarm_count") or 0, "label": "驾驶行为报警", "tone": "orange"},
                {"value": focus.get("risk_score") or 0, "label": "车辆安全风险分", "tone": "green"},
            ]

    profile_name = "—"
    if focus:
        if dimension == "company":
            profile_name = focus.get("company_name") or "—"
        elif dimension == "driver":
            profile_name = focus.get("driver_name") or "—"
        else:
            profile_name = focus.get("plate_no") or f"车{focus.get('car_id')}"

    vehicle_info_rows = []
    if focus and dimension == "vehicle":
        vehicle_info_rows = [
            ["车辆编号", str(focus.get("tid") or focus.get("car_id") or "—")],
            ["VIN", focus.get("vin") or "—"],
            ["所属组织", focus.get("company_name") or "—"],
            ["车辆类型", focus.get("vehicle_type") or "—"],
            ["绑定司机", focus.get("driver_name") or "—"],
            ["风险等级", focus.get("risk_level") or "—"],
            ["报警合计", str(focus.get("total_alarm_count") or 0)],
            ["综合风险分", str(focus.get("risk_score") or 0)],
        ]

    risk_overview = []
    risk_side = []
    if focus:
        risk_overview = [
            {"label": "综合风险分", "value": str(focus.get("risk_score") or 0)},
            {
                "label": "风险等级",
                "value": focus.get("risk_level") or "—",
                "tone": "red" if (focus.get("risk_score") or 0) >= 70 else "dark",
            },
            {"label": "报警合计", "value": str(focus.get("total_alarm_count") or 0), "tone": "dark"},
        ]
        if dimension == "vehicle":
            risk_side = [
                {"label": "精神疲劳报警", "value": str(focus.get("mental_alarm_count") or 0)},
                {"label": "驾驶行为报警", "value": str(focus.get("behavior_alarm_count") or 0)},
                {"label": "路线偏移报警", "value": str(focus.get("route_alarm_count") or 0)},
            ]
        elif dimension == "company":
            risk_side = [
                {"label": "车辆数", "value": str(focus.get("vehicle_count") or 0)},
                {"label": "精神疲劳报警", "value": str(focus.get("mental_alarm_count") or 0)},
                {"label": "驾驶行为报警", "value": str(focus.get("behavior_alarm_count") or 0)},
            ]
        else:
            risk_side = [
                {"label": "绑定车辆数", "value": str(focus.get("vehicle_count") or 0)},
                {"label": "精神疲劳报警", "value": str(focus.get("mental_alarm_count") or 0)},
                {"label": "驾驶行为报警", "value": str(focus.get("behavior_alarm_count") or 0)},
            ]

    bubbles = []
    if focus:
        mapping = [
            ("mental_alarm_count", "精神疲劳", "data"),
            ("behavior_alarm_count", "驾驶行为", "speed"),
            ("route_alarm_count", "路线偏移", "idle"),
            ("weather_alarm_count", "天气预警", "oil-high"),
            ("over_3h_count", "超3小时驾驶", "fuel-theft"),
            ("over_4h_count", "超4小时驾驶", "scr"),
            ("over_6h_count", "超6小时驾驶", "nox"),
        ]
        for key, label, class_name in mapping:
            if _as_int(focus.get(key)) > 0:
                bubbles.append({"label": f"{label}×{_as_int(focus.get(key))}", "className": class_name})

    trend = []
    if mode == "monthly" and meta.get("week_ends"):
        # 前端用周序列画趋势；明细在 weeks 里由接口另可扩展
        pass

    return {
        "ok": True,
        "mode": mode,
        "dimension": dimension,
        "source": meta.get("source"),
        "period": meta.get("period"),
        "week_ends": meta.get("week_ends") or [],
        "profile_name": profile_name,
        "metrics": metrics,
        "ranking": ranking,
        "risk_overview": risk_overview,
        "risk_side": risk_side,
        "radar": (focus or {}).get("radar") or [0, 0, 0, 0, 0, 0],
        "vehicle_info_rows": vehicle_info_rows,
        "risk_bubbles": bubbles,
        "focus": focus,
        "items": rows,
        "total": len(rows),
    }


async def query_risk_profile(
    db: AsyncSession,
    *,
    dimension: str = "vehicle",
    mode: str = "weekly",
    weekly_end_date: str | None = None,
    report_month: str | None = None,
    car_ids: list[int] | None = None,
    plates: list[str] | None = None,
    car_plates: str | None = None,
    company_id: int | None = None,
    company_ids: list[int] | None = None,
    driver_id: int | None = None,
    driver_name: str | None = None,
    x_org_id: str | None = None,
    x_user_id: str | None = None,
) -> dict[str, Any]:
    client = RiskProfileClient()
    mode = (mode or "weekly").strip().lower()
    if mode in ("周报", "week", "weekly"):
        mode = "weekly"
    elif mode in ("月报", "month", "monthly"):
        mode = "monthly"
    else:
        # 日报暂无接口：回退最近一周
        mode = "weekly"

    plate_by_car_id = parse_plate_by_car_id(
        car_ids=car_ids, plates=plates, car_plates=car_plates
    )
    filter_plates = {
        str(p).strip()
        for p in (plates or [])
        if str(p).strip()
    }
    for plate in plate_by_car_id.values():
        if plate:
            filter_plates.add(plate)

    # 用车牌向 808 反查真实风险 car_id（只作 ID 桥，公司/司机仍只读本地 vehicle）
    # 避免前端误传 CESG vehicle.id（渝DX7610 本地 80 ≠ 风险侧 105）
    if filter_plates:
        from app.vehicle_alloc_scope import _lookup_jt808_plate_car_id_map

        resolved = _lookup_jt808_plate_car_id_map(sorted(filter_plates))
        if resolved:
            plate_by_car_id = {cid: plate for plate, cid in resolved.items()}

    car_id_by_plate = {plate: cid for cid, plate in plate_by_car_id.items() if plate}

    filter_car_id: int | None = None
    if len(filter_plates) == 1:
        only_plate = next(iter(filter_plates))
        mapped = car_id_by_plate.get(only_plate)
        if mapped:
            filter_car_id = int(mapped)

    meta: dict[str, Any] = {"source": "weekly", "period": None, "week_ends": []}

    if mode == "monthly":
        if not report_month:
            today = date.today()
            report_month = f"{today.year:04d}{today.month:02d}"
        else:
            y, m = _parse_month(report_month)
            report_month = f"{y:04d}{m:02d}"
        meta["period"] = report_month
        official = await client.fetch_monthly_official(report_month, car_id=filter_car_id)
        if official is not None:
            raw_items = official
            meta["source"] = "monthly"
        else:
            raw_items, week_ends = await client.fetch_monthly_stitched(
                report_month, car_id=filter_car_id
            )
            meta["source"] = "weekly_stitched"
            meta["week_ends"] = week_ends
    else:
        if not weekly_end_date:
            weekly_end_date = default_weekly_end_date()
        else:
            # 任意日期对齐到不超过该日的最近周三（风险周报按周三周末）
            day = _parse_ymd(weekly_end_date)
            delta = (day.weekday() - 2) % 7
            weekly_end_date = _ymd(day - timedelta(days=delta))
        meta["period"] = weekly_end_date
        meta["source"] = "weekly"
        raw_items = await client.fetch_weekly_all(weekly_end_date, car_id=filter_car_id)
        # 若选中日对齐后的周无数据，向前再试两周
        if not raw_items and filter_car_id is None:
            day = _parse_ymd(weekly_end_date)
            for _ in range(2):
                day = day - timedelta(days=7)
                trial = _ymd(day)
                raw_items = await client.fetch_weekly_all(trial, car_id=filter_car_id)
                if raw_items:
                    weekly_end_date = trial
                    meta["period"] = trial
                    break

    # 无筛选时：按本周风险 car_id 反查车牌，再挂本地公司/司机
    if not plate_by_car_id and raw_items:
        from app.vehicle_alloc_scope import _lookup_jt808_car_id_plate_map

        plate_by_car_id = _lookup_jt808_car_id_plate_map(
            [_as_int(x.get("car_id")) for x in raw_items]
        )

    # 公司/司机/车队一律来自本地 vehicle；car_id→车牌仅作对齐键
    enriched = await enrich_vehicle_dimensions(
        db, raw_items, plate_by_car_id=plate_by_car_id
    )

    if filter_plates:
        enriched = [
            x
            for x in enriched
            if str(x.get("plate_no") or "").strip() in filter_plates
            or _as_int(x.get("car_id")) in {
                car_id_by_plate[p] for p in filter_plates if p in car_id_by_plate
            }
        ]
    elif car_ids:
        allow = {int(x) for x in car_ids}
        enriched = [x for x in enriched if _as_int(x.get("car_id")) in allow]

    parent_map, name_map, gid_to_local, local_to_gid = await load_org_company_maps(db)
    children_map = build_children_map(parent_map)
    local_selected: set[int] = set()
    jt808_child_buckets: dict[int, set[int]] = {}
    jt808_bucket_names: dict[int, str] = {}
    jt808_display_children: list[dict[str, Any]] = []
    company_placeholders: list[tuple[Any, str]] = []

    # 权限：仅本公司及下级；无显式公司筛选时企业画像默认查当前用户所属公司
    scope_root, scope_allow = await require_user_company_subtree_ids(
        db, x_org_id=x_org_id, x_user_id=x_user_id
    )
    meta["scope_org_id"] = scope_root
    enriched = [
        x for x in enriched if _as_int(x.get("company_id")) in scope_allow
    ]

    selected_raw: list[int] = []
    if company_ids:
        selected_raw = [int(x) for x in company_ids if x is not None]
    elif company_id is not None:
        selected_raw = [int(company_id)]
    elif dimension == "company":
        # 默认：当前用户所属公司（优先 JT808 gid，便于与组织树一致展开）
        selected_raw = [int(local_to_gid.get(scope_root, scope_root))]

    if selected_raw:
        local_selected = resolve_selected_to_local_org_ids(
            selected_raw,
            gid_to_local=gid_to_local,
            known_local_ids=set(parent_map.keys()),
        )
        if not local_selected.issubset(scope_allow):
            local_selected &= scope_allow
            if not local_selected:
                raise HTTPException(status_code=403, detail="无权查看所选公司风险画像")
            selected_raw = [
                int(local_to_gid.get(oid, oid)) for oid in sorted(local_selected)
            ]
        # 优先按 JT808 树展开（与页面公司树一致：本部车队→南区/西区/工作车）
        (
            allow_jt808,
            jt808_child_buckets,
            jt808_bucket_names,
            jt808_display_children,
        ) = await resolve_allow_companies_via_jt808(
            selected_raw,
            gid_to_local=gid_to_local,
            local_to_gid=local_to_gid,
            known_local_ids=set(parent_map.keys()),
        )
        allow_companies = set(allow_jt808)
        allow_companies |= expand_org_ids_in_memory(local_selected, children_map)
        if allow_companies <= local_selected:
            allow_companies |= await expand_company_filter_ids(db, local_selected)
        allow_companies &= scope_allow
        before = len(enriched)
        enriched = [
            x for x in enriched if _as_int(x.get("company_id")) in allow_companies
        ]
        if before and not enriched:
            logger.warning(
                "企业风险画像公司筛选后为空 selected_raw=%s local_selected=%s allow=%s jt808_buckets=%s",
                selected_raw,
                sorted(local_selected),
                sorted(allow_companies)[:40],
                {k: len(v) for k, v in jt808_child_buckets.items()},
            )

    if driver_id is not None and _as_int(driver_id) > 0:
        did = int(driver_id)
        enriched = [x for x in enriched if _as_int(x.get("driver_id")) == did]
    elif driver_name:
        keyword = driver_name.strip()
        enriched = [
            x
            for x in enriched
            if keyword in str(x.get("driver_name") or "")
        ]

    # 企业风险画像：选中公司按 JT808 直接下级分桶；失败则回退本地组织树；未选则滚到集团二级
    if dimension == "company":
        if enriched:
            if jt808_child_buckets:
                enriched = rollup_items_by_jt808_child_buckets(
                    enriched,
                    local_to_gid=local_to_gid,
                    gid_to_local=gid_to_local,
                    name_map=name_map,
                    child_buckets=jt808_child_buckets,
                    bucket_names=jt808_bucket_names,
                )
            else:
                enriched = rollup_items_for_company_dimension(
                    enriched,
                    parent_map=parent_map,
                    name_map=name_map,
                    children_map=children_map,
                    selected_local_ids=local_selected or None,
                )
        # 二级维度名称始终展示（无车辆/无报警也补 0）
        if jt808_display_children:
            for child in jt808_display_children:
                gid = int(child["gid"])
                local_id = gid_to_local.get(gid, gid)
                cname = (
                    str(child.get("name") or "").strip()
                    or jt808_bucket_names.get(gid)
                    or name_map.get(local_id)
                    or f"公司{gid}"
                )
                company_placeholders.append((local_id, cname))
        elif local_selected:
            for sid in sorted(local_selected):
                kids = children_map.get(sid, [])
                if kids:
                    for cid in kids:
                        company_placeholders.append(
                            (cid, name_map.get(cid) or f"公司{cid}")
                        )
                else:
                    company_placeholders.append(
                        (sid, name_map.get(sid) or f"公司{sid}")
                    )

    payload = build_profile_payload(
        dimension=dimension,
        mode=mode,
        items=enriched,
        meta=meta,
        company_placeholders=company_placeholders if dimension == "company" else None,
    )
    if dimension == "vehicle":
        payload = await attach_vehicle_obd_indicators(
            db, payload, plates=filter_plates or None
        )
    elif dimension == "driver":
        # 「报警行为排名」= 当前司机各报警类型条数降序（非司机间总报警排名）
        focus = payload.get("focus") if isinstance(payload.get("focus"), dict) else None
        dt_range = period_datetime_range(mode, meta.get("period"))
        if dt_range is None:
            payload["ranking"] = []
        else:
            start_at, end_at = dt_range
            driver_plates, driver_vehicle_ids = await resolve_driver_vehicle_keys(
                db,
                driver_id=driver_id,
                driver_name=driver_name,
                focus=focus,
            )
            payload["ranking"] = await query_driver_alarm_behavior_ranking(
                db,
                plates=driver_plates,
                vehicle_ids=driver_vehicle_ids,
                start_at=start_at,
                end_at=end_at,
            )
            payload["alarm_rank_period"] = {
                "start_at": start_at.isoformat(sep=" "),
                "end_at": (end_at - timedelta(seconds=1)).isoformat(sep=" "),
            }
    return payload


def default_weekly_end_date() -> str:
    today = date.today()
    delta = (today.weekday() - 2) % 7
    return _ymd(today - timedelta(days=delta))


# ---------------------------------------------------------------------------
# 车辆风险画像：核心指标 ← Redis OBD（车牌 → 终端 → {tid}_OBD）
# ---------------------------------------------------------------------------


def _fmt_obd_num(value: Any, *, digits: int | None = None) -> str:
    if value is None or value == "":
        return "—"
    try:
        num = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or "—"
    if digits is None:
        if abs(num - round(num)) < 1e-9:
            return str(int(round(num)))
        return f"{num:.1f}".rstrip("0").rstrip(".")
    return f"{num:.{digits}f}"


def _obd_metric(
    label: str,
    value: Any,
    unit: str,
    *,
    tone: str = "green",
    digits: int | None = None,
) -> dict[str, Any]:
    return {
        "label": label,
        "value": _fmt_obd_num(value, digits=digits),
        "unit": unit,
        "tone": tone,
    }


def _nox_efficiency(nox1: Any, nox2: Any) -> Any:
    try:
        a = float(nox1)
        b = float(nox2)
    except (TypeError, ValueError):
        return None
    if a <= 0:
        return None
    return max(0.0, min(100.0, (a - b) / a * 100.0))


def _coolant_tone(value: Any) -> str:
    try:
        temp = float(value)
    except (TypeError, ValueError):
        return "green"
    if temp >= 100:
        return "red"
    if temp >= 90:
        return "orange"
    return "green"


def build_core_indicators_from_obd(
    snapshot: dict[str, Any],
    *,
    obd_type: str = "yc",
) -> list[dict[str, Any]]:
    """把 OBD 快照映射为车辆风险画像「核心指标」四组卡片。"""
    from app.obd_speed_monitor import _dict_get_ci

    s = snapshot or {}
    get = lambda k: _dict_get_ci(s, k)  # noqa: E731

    if obd_type == "dc":
        return [
            {
                "title": "运行状态指标",
                "className": "running",
                "items": [
                    _obd_metric("当前车速", get("speed"), "km/h"),
                    _obd_metric("电机转速", get("cddjzs"), "rpm"),
                    _obd_metric("电机转矩", get("cddjzj"), "N·m"),
                    _obd_metric("SOC", get("soc"), "%"),
                    _obd_metric("总电压", get("zdy"), "V", digits=1),
                ],
            },
            {
                "title": "动力电池指标",
                "className": "engine",
                "items": [
                    _obd_metric("总电流", get("zdl"), "A", digits=1),
                    _obd_metric("电机温度", get("cddjwd"), "°C"),
                    _obd_metric("绝缘电阻", get("jydz"), "Ω"),
                    _obd_metric("加速踏板", get("jsdbbcxfd"), "%"),
                    _obd_metric("制动踏板", get("zddbcxfd"), "%"),
                ],
            },
            {
                "title": "车辆状态指标",
                "className": "after",
                "items": [
                    _obd_metric("车辆状态", get("clzt"), ""),
                    _obd_metric("充电状态", get("cdzt"), ""),
                    _obd_metric("档位", get("dw"), ""),
                    _obd_metric("方向盘转角", get("fxpdqzxjd"), "°", digits=1),
                ],
                "compact": [],
            },
            {
                "title": "油耗里程指标",
                "className": "mileage",
                "items": [
                    _obd_metric("累计里程", get("zlc"), "km", digits=1),
                    _obd_metric("小计里程", get("bclc"), "km", digits=1),
                    _obd_metric("日行驶里程", None, "km"),
                    _obd_metric("燃料消耗率", None, "L/h"),
                    _obd_metric("百公里油耗", None, "L/100km"),
                    _obd_metric("油箱液位", None, "%"),
                ],
            },
        ]

    nox_eff = _nox_efficiency(get("scrnox1"), get("scrnox2"))
    coolant = get("fdjncywd")
    dpf = get("dpfyc")
    scr_in = get("scrwd1")
    scr_out = get("scrwd2")
    return [
        {
            "title": "运行状态指标",
            "className": "running",
            "items": [
                _obd_metric("当前车速", get("speed"), "km/h"),
                _obd_metric("发动机转速", get("fdjzs"), "rpm"),
                _obd_metric("发动机扭矩", get("fdjjscnj"), "%"),
                _obd_metric("怠速时长", None, "min"),
                _obd_metric("运行时长", None, "h"),
            ],
        },
        {
            "title": "发动机健康指标",
            "className": "engine",
            "items": [
                _obd_metric("冷却液温度", coolant, "°C", tone=_coolant_tone(coolant)),
                _obd_metric("最高水温", None, "°C"),
                _obd_metric("高温次数", None, "次"),
                _obd_metric("高转速次数", None, "次"),
                _obd_metric("高负荷次数", None, "次"),
            ],
        },
        {
            "title": "排放后处理指标",
            "className": "after",
            "items": [
                _obd_metric("SCR上游NOx", get("scrnox1"), "ppm"),
                _obd_metric("SCR下游NOx", get("scrnox2"), "ppm"),
                _obd_metric("NOx转化效率", nox_eff, "%", digits=1),
                _obd_metric("反应剂余量", get("fyjyl"), "%"),
            ],
            "compact": [
                ["DPF压差", f"{_fmt_obd_num(dpf)}kPa" if dpf is not None else "—"],
                ["SCR入口温度", f"{_fmt_obd_num(scr_in)}°C" if scr_in is not None else "—"],
                ["SCR出口温度", f"{_fmt_obd_num(scr_out)}°C" if scr_out is not None else "—"],
            ],
        },
        {
            "title": "油耗里程指标",
            "className": "mileage",
            "items": [
                _obd_metric("燃料消耗率", get("fdjrlll"), "L/h", digits=2),
                _obd_metric("百公里油耗", None, "L/100km"),
                _obd_metric("油箱液位", get("yxyw"), "%"),
                _obd_metric("累计里程", get("zlc"), "km", digits=1),
                _obd_metric("小计里程", get("bclc"), "km", digits=1),
                _obd_metric("日行驶里程", None, "km"),
            ],
        },
    ]


def _device_key_candidates(raw: str | None) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    out: list[str] = [text]
    # 去前导零 / 补常见长度，兼容终端号写法差异
    stripped = text.lstrip("0") or "0"
    if stripped != text:
        out.append(stripped)
    if text.isdigit():
        for width in (11, 12, 20):
            padded = text.zfill(width)
            if padded not in out:
                out.append(padded)
    return out


async def _redis_get_obd_payload(device_candidates: list[str], plate: str) -> tuple[str | None, str | None]:
    """按终端号取 Redis `{tid}_OBD`；失败再尝试车牌作 key。"""
    from app.obd_speed_monitor import _new_redis

    redis = _new_redis()
    try:
        for tid in device_candidates:
            key = f"{tid}_OBD"
            try:
                payload = await redis.get(key)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Redis 读取 OBD 失败 key=%s: %s", key, exc)
                continue
            if payload:
                text = payload if isinstance(payload, str) else payload.decode("utf-8", "ignore")
                return text, tid
        # 少数环境直接以车牌作 key
        plate_key = f"{plate}_OBD"
        try:
            payload = await redis.get(plate_key)
        except Exception:  # noqa: BLE001
            payload = None
        if payload:
            text = payload if isinstance(payload, str) else payload.decode("utf-8", "ignore")
            return text, plate
        return None, None
    finally:
        try:
            await redis.aclose()
        except Exception:  # noqa: BLE001
            pass


async def load_vehicle_obd_indicators(
    db: AsyncSession,
    plate_no: str,
) -> dict[str, Any]:
    """
    按车牌从 Redis 取实时 OBD，并映射为核心指标。
    Redis Key：`{终端号}_OBD`；同时合并当日油/电能量快照补全字段。
    """
    from app.obd_speed_monitor import (
        _OBD_DC_SNAPSHOT_KEYS,
        _OBD_YC_SNAPSHOT_KEYS,
        _dict_get_ci,
        _infer_obd_type,
        _json_dict,
        _latest_energy_raw,
        _pick_snapshot_fields,
        build_obd_snapshot,
        parse_obd_payload,
    )

    plate = str(plate_no or "").strip()
    empty_groups = build_core_indicators_from_obd({}, obd_type="yc")
    result: dict[str, Any] = {
        "ok": False,
        "plate_no": plate,
        "device_no": None,
        "obd_type": "yc",
        "source": None,
        "ts": None,
        "snapshot": {},
        "groups": empty_groups,
        "message": "",
    }
    if not plate:
        result["message"] = "未提供车牌"
        return result

    vehicle = (
        await db.execute(
            select(Vehicle)
            .options(selectinload(Vehicle.devices))
            .where(Vehicle.plate_no == plate)
            .limit(1)
        )
    ).scalar_one_or_none()
    if vehicle is None:
        # 宽松再查一次（去空格）
        vehicles = await load_local_vehicles(db)
        for v in vehicles:
            if str(v.plate_no or "").strip() == plate:
                vehicle = v
                break
    if vehicle is None:
        result["message"] = f"本地未找到车辆 {plate}"
        return result

    candidates: list[str] = []
    for dev in vehicle.devices or []:
        for key in (dev.device_no, dev.device_sn, dev.sim_no, getattr(dev, "actual_sim", None)):
            for c in _device_key_candidates(key):
                if c not in candidates:
                    candidates.append(c)

    redis_raw, used_tid = await _redis_get_obd_payload(candidates, plate)
    energy_raw, energy_mapped = (None, None)
    if used_tid:
        energy_raw, energy_mapped = await _latest_energy_raw(db, used_tid)
    elif candidates:
        energy_raw, energy_mapped = await _latest_energy_raw(db, candidates[0])
        used_tid = used_tid or candidates[0]

    snapshot: dict[str, Any] = {}
    obd_type = "yc"
    source = None
    if redis_raw:
        reading = parse_obd_payload(used_tid or plate, redis_raw)
        if reading is not None:
            snapshot, obd_type = build_obd_snapshot(
                reading=reading,
                plate_no=plate,
                energy_raw=energy_raw,
                energy_type=energy_mapped,
            )
            # reading.raw 可能被截断，再用完整 Redis JSON 覆盖补全
            merged = {}
            merged.update(_json_dict(energy_raw))
            merged.update(_json_dict(redis_raw))
            merged.update(snapshot)
            keys = _OBD_DC_SNAPSHOT_KEYS if obd_type == "dc" else _OBD_YC_SNAPSHOT_KEYS
            snapshot = _pick_snapshot_fields(merged, keys)
            source = "redis"
        else:
            # Redis 有报文但缺时速字段：仍尽量解析 JSON + 能量快照
            merged = {}
            merged.update(_json_dict(energy_raw))
            merged.update(_json_dict(redis_raw))
            if plate:
                merged["carno"] = plate
            obd_type = _infer_obd_type(merged, energy_mapped)
            keys = _OBD_DC_SNAPSHOT_KEYS if obd_type == "dc" else _OBD_YC_SNAPSHOT_KEYS
            snapshot = _pick_snapshot_fields(merged, keys)
            source = "redis_raw"
    elif energy_raw:
        merged = _json_dict(energy_raw)
        if plate:
            merged["carno"] = plate
        obd_type = _infer_obd_type(merged, energy_mapped)
        keys = _OBD_DC_SNAPSHOT_KEYS if obd_type == "dc" else _OBD_YC_SNAPSHOT_KEYS
        snapshot = _pick_snapshot_fields(merged, keys)
        source = "energy_snapshot"
    else:
        result["message"] = "Redis 无该车 OBD 数据"
        result["device_no"] = used_tid or (candidates[0] if candidates else None)
        return result

    result.update(
        {
            "ok": True,
            "device_no": used_tid,
            "obd_type": obd_type,
            "source": source,
            "ts": _dict_get_ci(snapshot, "ts"),
            "snapshot": snapshot,
            "groups": build_core_indicators_from_obd(snapshot, obd_type=obd_type),
            "message": "",
        }
    )
    return result


async def attach_vehicle_obd_indicators(
    db: AsyncSession,
    payload: dict[str, Any],
    *,
    plates: set[str] | None = None,
) -> dict[str, Any]:
    """车辆风险画像响应中挂上核心指标（Redis OBD）。"""
    plate = ""
    if plates:
        plate = next(iter(plates))
    if not plate:
        focus = payload.get("focus") or {}
        plate = str(focus.get("plate_no") or "").strip()
    if not plate:
        payload["core_indicators"] = build_core_indicators_from_obd({}, obd_type="yc")
        payload["obd"] = {"ok": False, "message": "未选择车辆", "groups": payload["core_indicators"]}
        return payload
    obd = await load_vehicle_obd_indicators(db, plate)
    payload["obd"] = obd
    payload["core_indicators"] = obd.get("groups") or build_core_indicators_from_obd({}, obd_type="yc")
    return payload
