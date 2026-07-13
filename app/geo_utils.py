"""地理工具：WGS84→GCJ02 转换与规则几何命中判定。

规则 geometry_json 由前端高德地图绘制，坐标系为 GCJ02；
车辆坐标（JT808 平台 / OBD）为 WGS84，判定前须先转换。
坐标统一为 [lng, lat]。
"""
from __future__ import annotations

import math

_PI = 3.14159265358979324
_A = 6378245.0
_EE = 0.00669342162296594323
_EARTH_R = 6371008.8  # 平均地球半径（米）


def _out_of_china(lng: float, lat: float) -> bool:
    return lng < 72.004 or lng > 137.8347 or lat < 0.8293 or lat > 55.8271


def _transform_lat(x: float, y: float) -> float:
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * _PI) + 20.0 * math.sin(2.0 * x * _PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * _PI) + 40.0 * math.sin(y / 3.0 * _PI)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * _PI) + 320 * math.sin(y * _PI / 30.0)) * 2.0 / 3.0
    return ret


def _transform_lng(x: float, y: float) -> float:
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * _PI) + 20.0 * math.sin(2.0 * x * _PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * _PI) + 40.0 * math.sin(x / 3.0 * _PI)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * _PI) + 300.0 * math.sin(x / 30.0 * _PI)) * 2.0 / 3.0
    return ret


def wgs84_to_gcj02(lng: float, lat: float) -> tuple[float, float]:
    """与前端 maptools.js 的 wgs84ToGcj02 完全一致。"""
    if _out_of_china(lng, lat):
        return lng, lat
    d_lat = _transform_lat(lng - 105.0, lat - 35.0)
    d_lng = _transform_lng(lng - 105.0, lat - 35.0)
    rad_lat = lat / 180.0 * _PI
    magic = math.sin(rad_lat)
    magic = 1 - _EE * magic * magic
    sqrt_magic = math.sqrt(magic)
    d_lat = (d_lat * 180.0) / ((_A * (1 - _EE)) / (magic * sqrt_magic) * _PI)
    d_lng = (d_lng * 180.0) / (_A / sqrt_magic * math.cos(rad_lat) * _PI)
    return lng + d_lng, lat + d_lat


def bearing_deg(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    """两点方位角（度，正北为 0，顺时针）。"""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_lambda = math.radians(lng2 - lng1)
    y = math.sin(d_lambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lambda)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def offset_point_m(lng: float, lat: float, bearing: float, distance_m: float) -> tuple[float, float]:
    """沿方位角平移指定距离（米），返回新经纬度。"""
    brng = math.radians(bearing)
    lat1 = math.radians(lat)
    lng1 = math.radians(lng)
    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_m / _EARTH_R)
        + math.cos(lat1) * math.sin(distance_m / _EARTH_R) * math.cos(brng)
    )
    lng2 = lng1 + math.atan2(
        math.sin(brng) * math.sin(distance_m / _EARTH_R) * math.cos(lat1),
        math.cos(distance_m / _EARTH_R) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lng2), math.degrees(lat2)


def haversine_m(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    """两点球面距离（米）。"""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * _EARTH_R * math.asin(math.sqrt(a))


def _to_local_xy(lng: float, lat: float, ref_lat: float) -> tuple[float, float]:
    """经纬度→局部平面（米），小范围等距近似，够限速判定用。"""
    x = math.radians(lng) * _EARTH_R * math.cos(math.radians(ref_lat))
    y = math.radians(lat) * _EARTH_R
    return x, y


def point_in_polygon(lng: float, lat: float, path: list[list[float]]) -> bool:
    """射线法判断点是否在多边形内（顶点为 [lng, lat] 列表）。"""
    if not path or len(path) < 3:
        return False
    inside = False
    j = len(path) - 1
    for i in range(len(path)):
        xi, yi = float(path[i][0]), float(path[i][1])
        xj, yj = float(path[j][0]), float(path[j][1])
        if (yi > lat) != (yj > lat):
            x_cross = (xj - xi) * (lat - yi) / (yj - yi) + xi
            if lng < x_cross:
                inside = not inside
        j = i
    return inside


def point_to_polyline_distance_m(lng: float, lat: float, path: list[list[float]]) -> float:
    """点到折线的最小距离（米）。"""
    if not path:
        return float("inf")
    if len(path) == 1:
        return haversine_m(lng, lat, float(path[0][0]), float(path[0][1]))
    px, py = _to_local_xy(lng, lat, lat)
    best = float("inf")
    for i in range(len(path) - 1):
        x1, y1 = _to_local_xy(float(path[i][0]), float(path[i][1]), lat)
        x2, y2 = _to_local_xy(float(path[i + 1][0]), float(path[i + 1][1]), lat)
        dx, dy = x2 - x1, y2 - y1
        seg_len_sq = dx * dx + dy * dy
        if seg_len_sq <= 0:
            d = math.hypot(px - x1, py - y1)
        else:
            t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / seg_len_sq))
            d = math.hypot(px - (x1 + t * dx), py - (y1 + t * dy))
        if d < best:
            best = d
    return best


def geometry_hit(
    lng_gcj: float,
    lat_gcj: float,
    draw_shape_type: str,
    geometry_json: dict,
    polyline_buffer_m: float = 30.0,
) -> bool:
    """判断 GCJ02 坐标点是否命中规则几何。

    - circle / rectangle / polygon：点在图形内
    - polyline：点到折线距离 <= polyline_buffer_m（车在限速路段上）
    geometry_json 结构与前端 serializeOverlayGeometry 约定一致。
    """
    if not isinstance(geometry_json, dict):
        return False
    shape = (draw_shape_type or "").strip().lower()
    try:
        if shape == "circle":
            center = geometry_json.get("center")
            radius = float(geometry_json.get("radius_m") or 0)
            if not center or radius <= 0:
                return False
            return haversine_m(lng_gcj, lat_gcj, float(center[0]), float(center[1])) <= radius
        if shape == "rectangle":
            sw = geometry_json.get("southwest")
            ne = geometry_json.get("northeast")
            if not sw or not ne:
                return False
            return (
                float(sw[0]) <= lng_gcj <= float(ne[0])
                and float(sw[1]) <= lat_gcj <= float(ne[1])
            )
        if shape == "polygon":
            return point_in_polygon(lng_gcj, lat_gcj, geometry_json.get("path") or [])
        if shape == "polyline":
            path = geometry_json.get("path") or []
            return point_to_polyline_distance_m(lng_gcj, lat_gcj, path) <= polyline_buffer_m
    except (TypeError, ValueError, IndexError):
        return False
    return False
