"""OBD 时速违章监测管理接口 + 独立状态页。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from app.database import AsyncSessionLocal

from app.jt808_openapi_client import jt808_openapi_client
from app.jt808_obd_fuel_sync import (
    TABLE_NAME as OBD_FUEL_TABLE_NAME,
    force_full_sync_obd_fuel_daily,
    jt808_obd_fuel_sync_status,
)
from app.jt808_violation_sync import TABLE_NAME, jt808_violation_sync_status
from app.obd_speed_monitor import backfill_obd_speed_violation_limits, obd_speed_scheduler, ping_redis
from app.park_alarm_scheduler import park_alarm_scheduler

router = APIRouter(tags=["obd-speed-check"])


@router.get("/api/obd-speed-check/status")
async def obd_speed_check_status():
    sync_info: dict = {}
    fuel_sync_info: dict = {}
    async with AsyncSessionLocal() as db:
        try:
            sync_info = await jt808_violation_sync_status(db)
        except Exception as exc:  # noqa: BLE001
            sync_info = {
                "local_vehicle_violation_count": None,
                "jt808_mirror": {"table": TABLE_NAME, "error": str(exc), "mysql_ok": False},
            }
        try:
            fuel_sync_info = await jt808_obd_fuel_sync_status(db)
        except Exception as exc:  # noqa: BLE001
            fuel_sync_info = {
                "local_obd_fuel_daily_count": None,
                "jt808_mirror": {
                    "table": OBD_FUEL_TABLE_NAME,
                    "error": str(exc),
                    "mysql_ok": False,
                },
            }
    return {
        "ok": True,
        "scheduler": obd_speed_scheduler.status(),
        "jt808_openapi_configured": jt808_openapi_client.configured(),
        "violation_sync": sync_info,
        "obd_fuel_sync": fuel_sync_info,
        "park_alarm_scheduler": park_alarm_scheduler.status(),
    }


@router.get("/api/obd-speed-check/ping")
async def obd_speed_check_ping():
    """主动测试 Redis 连接并抓取 OBD 数据样例。"""
    return {"ok": True, "redis": await ping_redis()}


@router.post("/api/obd-speed-check/run-once")
async def obd_speed_check_run_once():
    try:
        result = await obd_speed_scheduler.run_once()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "result": result.__dict__}


@router.post("/api/obd-speed-check/backfill-limits")
async def obd_speed_check_backfill_limits():
    """重算已有 OBD 超速记录的限速值，并清零围栏规则遗留的自身限速。"""
    async with AsyncSessionLocal() as db:
        try:
            stats = await backfill_obd_speed_violation_limits(db)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, **stats}


@router.post("/api/obd-speed-check/sync-obd-fuel")
async def obd_speed_check_sync_obd_fuel():
    """立即把本地 obd_fuel_daily 全量同步到 808 cesg_obd_fuel_daily。"""
    async with AsyncSessionLocal() as db:
        try:
            result = await force_full_sync_obd_fuel_daily(db)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "同步失败")
    return {"ok": True, **result}


@router.post("/api/obd-speed-check/start")
async def obd_speed_check_start():
    """启动定时调度。"""
    obd_speed_scheduler.start()
    return {"ok": True, "scheduler": obd_speed_scheduler.status()}


@router.post("/api/obd-speed-check/stop")
async def obd_speed_check_stop():
    """停止定时调度（服务重启后会再次自动启动）。"""
    await obd_speed_scheduler.stop()
    return {"ok": True, "scheduler": obd_speed_scheduler.status()}


@router.post("/api/obd-speed-check/park-alarm/run-once")
async def park_alarm_run_once():
    """立即执行一轮停车超限扫描（1202/1105 stops → 808 cesg_park_alarm）。"""
    try:
        result = await park_alarm_scheduler.run_once()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "result": result, "scheduler": park_alarm_scheduler.status()}


@router.post("/api/obd-speed-check/park-alarm/reset-cursors")
async def park_alarm_reset_cursors():
    """清空停车扫描游标，下一轮按 lookback 窗口重扫。"""
    try:
        result = await park_alarm_scheduler.reset_cursors()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {**result, "scheduler": park_alarm_scheduler.status()}


@router.get("/api/obd-speed-check/park-alarm/diagnose")
async def park_alarm_diagnose(plate: str = "渝DX7610", hours: int = 48, time_stop: int = 3):
    """诊断：按车牌拉 1202/1105 停车点样例（不写库、不推进游标）。"""
    from datetime import timedelta

    from sqlalchemy import select

    from app.database import AsyncSessionLocal
    from app.jt808_openapi_client import jt808_openapi_client
    from app.models import Vehicle, VehicleDevice
    from app.park_alarm_scheduler import _duration_minutes, _extract_lng_lat, _fmt_hms, _parse_dt, _pick
    from app.timeutil import china_now_naive

    plate_q = (plate or "").strip()
    if not plate_q:
        raise HTTPException(status_code=400, detail="plate 必填")
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(Vehicle.id, Vehicle.plate_no, VehicleDevice.device_no)
                .outerjoin(VehicleDevice, VehicleDevice.vehicle_id == Vehicle.id)
                .where(Vehicle.plate_no.like(f"%{plate_q}%"))
                .limit(1)
            )
        ).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"未找到车辆 {plate_q}")
    vid, plate_no, device_no = int(row[0]), str(row[1] or ""), str(row[2] or "").strip()
    if not device_no:
        raise HTTPException(status_code=400, detail=f"{plate_no} 无设备号")
    now = china_now_naive()
    stime = _fmt_hms(now - timedelta(hours=max(1, int(hours))))
    etime = _fmt_hms(now)
    try:
        stops = await jt808_openapi_client.list_history_stops(
            device_id=device_no,
            stime=stime,
            etime=etime,
            time_stop_minutes=max(0, int(time_stop)),
        )
        err = None
    except Exception as exc:  # noqa: BLE001
        stops = []
        err = str(exc)
    samples = []
    for it in (stops or [])[:10]:
        start = _parse_dt(_pick(it, ("stime", "gpstime")))
        end = _parse_dt(_pick(it, ("etime",)))
        lng, lat = _extract_lng_lat(it)
        samples.append(
            {
                "stime": _pick(it, ("stime", "gpstime")),
                "etime": _pick(it, ("etime",)),
                "stopTime": _pick(it, ("stopTime",)),
                "duration_min": _duration_minutes(it, start, end),
                "lng": lng,
                "lat": lat,
                "keys": list(it.keys())[:20],
            }
        )
    return {
        "ok": err is None,
        "vehicle_id": vid,
        "plate_no": plate_no,
        "device_no": device_no,
        "stime": stime,
        "etime": etime,
        "time_stop_minutes": int(time_stop),
        "stops_count": len(stops or []),
        "error": err,
        "samples": samples,
    }


_STATUS_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>后台运维 · OBD / AI评估 / 报警类型 / 地图接口 / 短信平台</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "Microsoft YaHei", system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; padding: 24px; }
  .wrap { max-width: 1100px; margin: 0 auto; }
  h1 { font-size: 20px; margin-bottom: 4px; }
  .sub { color: #94a3b8; font-size: 13px; margin-bottom: 16px; }
  .tabs { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
  .tabs button { background: #334155; color: #cbd5e1; border: 1px solid #475569; border-radius: 8px; padding: 8px 16px; font-size: 14px; cursor: pointer; }
  .tabs button:hover { background: #3f5169; }
  .tabs button.active { background: #2563eb; border-color: #2563eb; color: #fff; }
  .panel { display: none; }
  .panel.active { display: block; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px 18px; margin-bottom: 14px; }
  .card h2 { font-size: 15px; margin-bottom: 12px; color: #cbd5e1; display: flex; align-items: center; gap: 8px; }
  .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; background: #64748b; }
  .dot.ok { background: #22c55e; box-shadow: 0 0 8px #22c55e88; }
  .dot.bad { background: #ef4444; box-shadow: 0 0 8px #ef444488; }
  .dot.warn { background: #eab308; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  td { padding: 6px 4px; border-bottom: 1px solid #33415555; vertical-align: top; }
  td:first-child { color: #94a3b8; width: 160px; white-space: nowrap; }
  .data-table th, .data-table td { padding: 8px 6px; border-bottom: 1px solid #33415566; text-align: left; color: #e2e8f0; }
  .data-table th { color: #94a3b8; font-weight: 600; white-space: nowrap; }
  .data-table td:first-child { width: auto; color: #e2e8f0; }
  .data-table .ops button { padding: 4px 10px; font-size: 12px; margin-right: 4px; }
  pre { background: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 10px; font-size: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-all; color: #a5f3fc; }
  .btns { display: flex; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; align-items: center; }
  button { background: #2563eb; color: #fff; border: 0; border-radius: 8px; padding: 9px 18px; font-size: 14px; cursor: pointer; }
  button:hover { background: #1d4ed8; }
  button:disabled { background: #475569; cursor: wait; }
  button.ghost { background: #334155; }
  button.danger { background: #dc2626; }
  button.danger:hover { background: #b91c1c; }
  button.success { background: #16a34a; }
  button.success:hover { background: #15803d; }
  .err { color: #fca5a5; }
  .okc { color: #86efac; }
  .muted { color: #64748b; font-size: 12px; margin-top: 6px; }
  .filters { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 12px; }
  .filters label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #94a3b8; }
  input, select, textarea {
    background: #0f172a; border: 1px solid #475569; color: #e2e8f0;
    border-radius: 6px; padding: 7px 10px; font-size: 13px; min-width: 140px;
  }
  textarea { min-width: 100%; min-height: 72px; resize: vertical; }
  .form-grid { display: grid; grid-template-columns: 140px 1fr; gap: 10px 12px; align-items: center; }
  .form-grid label { color: #94a3b8; font-size: 13px; }
  .form-grid .span { grid-column: 1 / -1; }
  .pager { display: flex; gap: 10px; align-items: center; margin-top: 12px; font-size: 13px; color: #94a3b8; }
  .modal-mask {
    position: fixed; inset: 0; background: #00000088; display: none;
    align-items: center; justify-content: center; z-index: 50; padding: 16px;
  }
  .modal-mask.show { display: flex; }
  .modal {
    background: #1e293b; border: 1px solid #475569; border-radius: 12px;
    padding: 18px; width: min(520px, 100%); max-height: 90vh; overflow: auto;
  }
  .modal h3 { margin-bottom: 14px; font-size: 16px; }
  .modal .form-grid { margin-bottom: 14px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>后台运维</h1>
  <div class="sub">OBD 监测 · AI 自动评估 · 报警类型 · 地图接口 · 短信平台</div>

  <div class="tabs">
    <button type="button" class="active" data-tab="status">监测状态</button>
    <button type="button" data-tab="ai">AI 评估</button>
    <button type="button" data-tab="alarms">报警类型</button>
    <button type="button" data-tab="map">地图接口管理</button>
    <button type="button" data-tab="sms">短信平台接口</button>
  </div>

  <div id="panel-status" class="panel active">
    <div class="sub" id="refreshed">加载中…</div>
    <div class="btns">
      <button id="btnToggle" class="success" disabled>加载中…</button>
      <button id="btnPing">测试 Redis 连接</button>
      <button id="btnRun" class="ghost">立即执行一轮检测</button>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotRedis"></span>Redis 连接</h2>
      <table id="tblRedis"><tr><td>状态</td><td>尚未测试，点击上方"测试 Redis 连接"</td></tr></table>
      <div id="sampleWrap" style="display:none">
        <div class="muted">OBD 数据样例（第一个 Key）：</div>
        <pre id="samplePayload"></pre>
      </div>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotSched"></span>定时调度器</h2>
      <table id="tblSched"></table>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotSync"></span>808 安全报警同步表</h2>
      <table id="tblSync"><tr><td colspan="2">加载中…</td></tr></table>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotFuelSync"></span>808 日油耗同步表</h2>
      <div class="btns" style="margin:0 0 10px">
        <button id="btnFuelSync" class="success">立即全量同步到 808</button>
      </div>
      <table id="tblFuelSync"><tr><td colspan="2">加载中…</td></tr></table>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotParkAlarm"></span>停车超限报警调度</h2>
      <p class="muted" style="margin-bottom:10px">与历史回放同源：OpenAPI 1202/1105 停车点 <code>stops</code> → 过滤规则车辆 → 时长/围栏命中后写入 808 <code>cesg_park_alarm</code>。</p>
      <div class="btns" style="margin:0 0 10px">
        <button id="btnParkAlarmRun" class="success">立即扫描一轮</button>
        <button id="btnParkAlarmReset" class="ghost">清空游标并重扫窗口</button>
      </div>
      <table id="tblParkAlarm"><tr><td colspan="2">加载中…</td></tr></table>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotRun"></span>最近一轮执行结果</h2>
      <table id="tblRun"><tr><td colspan="2">暂无</td></tr></table>
    </div>
  </div>


  <div id="panel-ai" class="panel">
    <div class="sub" id="aiRefreshed">加载中…</div>
    <div class="btns">
      <button id="btnAiToggle" class="success" disabled>加载中…</button>
      <button id="btnAiRun" class="ghost">立即评估一轮</button>
      <button id="btnAiRefresh" class="ghost">刷新统计</button>
    </div>
    <div class="card">
      <h2><span class="dot" id="dotAi"></span>自动 AI 评估调度</h2>
      <p class="muted" style="margin-bottom:10px">独立调度：待处理 + 非OBD超速 + 报警类型过滤可见 + 未AI评估 + 有图/视频证据 → 按处理弹窗同一规则询问模型并落库罚单建议。</p>
      <table id="tblAi"></table>
    </div>
    <div class="card">
      <h2>今日询问模型条数</h2>
      <table id="tblAiToday"><tr><td colspan="2">加载中…</td></tr></table>
    </div>
    <div class="card">
      <h2>最近已评估记录（最新 30 条）</h2>
      <div class="muted" style="margin-bottom:8px">这些已落库，打开处理界面会直接显示 AI 说明，不会再问模型。</div>
      <table class="data-table">
        <thead>
          <tr>
            <th>评估时间</th><th>ID</th><th>车牌</th><th>报警类型</th><th>状态</th><th>罚单建议</th><th>报警时间</th>
          </tr>
        </thead>
        <tbody id="aiRecentBody"><tr><td colspan="7">加载中…</td></tr></tbody>
      </table>
    </div>
    <div class="card">
      <h2>最近一轮结果</h2>
      <table id="tblAiRun"><tr><td colspan="2">暂无</td></tr></table>
    </div>
  </div>

  <div id="panel-alarms" class="panel">
    <div class="card">
      <h2>报警类型</h2>
      <div class="filters">
        <label>类型编码<input id="alCode" placeholder="模糊"></label>
        <label>类型名称<input id="alName" placeholder="模糊"></label>
        <label>状态
          <select id="alStatus">
            <option value="">全部</option>
            <option value="启用">启用</option>
            <option value="停用">停用</option>
          </select>
        </label>
        <label>安全级别
          <select id="alLevel">
            <option value="">全部</option>
            <option value="高">高</option>
            <option value="中">中</option>
            <option value="低">低</option>
          </select>
        </label>
      </div>
      <div class="btns">
        <button id="btnAlQuery">查询</button>
        <button id="btnAlAdd" class="success">新增</button>
        <button id="btnAlReset" class="danger">从 808 重置灌数</button>
        <span class="muted" id="alTotal"></span>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>编码</th><th>名称</th><th>间隔(分)</th><th>状态</th><th>级别</th><th>创建时间</th><th>操作</th>
          </tr>
        </thead>
        <tbody id="alBody"><tr><td colspan="7">加载中…</td></tr></tbody>
      </table>
      <div class="pager">
        <button id="btnAlPrev" class="ghost">上一页</button>
        <span id="alPageInfo">—</span>
        <button id="btnAlNext" class="ghost">下一页</button>
      </div>
    </div>
  </div>

  <div id="panel-map" class="panel">
    <div class="card">
      <h2>地图接口管理</h2>
      <p class="muted" style="margin-bottom:12px">维护高德 Web 端 Key（画地图）与 Web 服务 Key（逆地理/纠偏等），保存后写入本系统数据库。</p>
      <div class="form-grid" id="mapForm">
        <label>服务商</label><input id="mProvider" value="amap" readonly>
        <label>Web端 Key</label><input id="mApiKey" placeholder="高德 JS API Key">
        <label>安全密钥</label><input id="mSecret" type="password" placeholder="可选，JS 安全密钥">
        <label>Web服务 Key</label><input id="mWebKey" placeholder="逆地理、轨迹纠偏等">
        <label>默认缩放</label><input id="mZoom" type="number" min="1" max="20" placeholder="1-20">
        <label>默认经度</label><input id="mLng" placeholder="如 106.55156">
        <label>默认纬度</label><input id="mLat" placeholder="如 29.56301">
        <label>备注</label><textarea id="mRemark" placeholder="可选"></textarea>
      </div>
      <div class="btns" style="margin-top:14px;margin-bottom:0">
        <button id="btnMapSave">保存</button>
        <button id="btnMapReload" class="ghost">重新加载</button>
        <button id="btnMapSync" class="ghost">从 808 同步 Web服务 Key</button>
        <span class="muted" id="mapMsg"></span>
      </div>
    </div>
  </div>

  <div id="panel-sms" class="panel">
    <div class="card">
      <h2><span class="dot" id="dotSms"></span>短信平台接口管理（云 MAS）</h2>
      <p class="muted" style="margin-bottom:12px">
        维护中国移动云 MAS 必需项。未启用、字段不全或平台鉴权失败时，登录页「获取验证码」仅提示无法获取短信，不影响其它功能。
        HTTPS 路径一般为 <code>/sms/submit</code>；HTTP 联调示例可能为 <code>/sms/norsubmit</code>。
      </p>
      <div class="form-grid" id="smsForm">
        <label>启用</label>
        <select id="sEnabled"><option value="0">停用</option><option value="1">启用</option></select>
        <label>接口根地址</label><input id="sBaseUrl" placeholder="https://主机:端口 或 http://112.35.1.155:1992">
        <label>普通短信路径</label><input id="sSubmitPath" placeholder="/sms/submit">
        <label>模板短信路径</label><input id="sTplPath" placeholder="/sms/tmpsubmit">
        <label>企业名称 ecName</label><input id="sEcName" placeholder="云 MAS 企业名称">
        <label>接口账号 apId</label><input id="sApId" placeholder="接口用户名">
        <label>接口密码 secretKey</label><input id="sSecret" type="password" placeholder="接口密码">
        <label>签名编码 sign</label><input id="sSign" placeholder="签名下载中的签名编码">
        <label>扩展码 addSerial</label><input id="sAddSerial" placeholder="精确匹配填空；模糊匹配可填扩展码">
        <label>发送模式</label>
        <select id="sMode">
          <option value="normal">普通短信</option>
          <option value="template">模板短信</option>
        </select>
        <label>模板 ID</label><input id="sTplId" placeholder="模板模式必填">
        <label>内容模板</label><textarea id="sContentTpl" placeholder="普通短信正文，须含 {code}"></textarea>
        <label>验证码有效秒</label><input id="sTtl" type="number" min="60" max="3600" value="300">
        <label>备注</label><textarea id="sRemark" placeholder="可选"></textarea>
        <label>就绪状态</label><div id="sReadyText" class="muted">—</div>
      </div>
      <div class="btns" style="margin-top:14px;margin-bottom:0">
        <button id="btnSmsSave">保存</button>
        <button id="btnSmsReload" class="ghost">重新加载</button>
        <input id="sTestPhone" placeholder="试发手机号" style="min-width:160px">
        <button id="btnSmsTest" class="ghost">试发一条</button>
        <span class="muted" id="smsMsg"></span>
      </div>
    </div>
  </div>
</div>

<div class="modal-mask" id="alModal">
  <div class="modal">
    <h3 id="alModalTitle">新增报警类型</h3>
    <div class="form-grid">
      <label>类型编码</label><input id="fCode" readonly placeholder="自动生成">
      <label>类型名称</label><input id="fName" placeholder="必填">
      <label>最小间隔(分)</label><input id="fInterval" type="number" min="0" value="15">
      <label>状态</label>
      <select id="fStatus"><option value="启用">启用</option><option value="停用">停用</option></select>
      <label>安全级别</label>
      <select id="fLevel"><option value="高">高</option><option value="中" selected>中</option><option value="低">低</option></select>
    </div>
    <div class="btns" style="margin-bottom:0">
      <button id="btnAlSave">保存</button>
      <button id="btnAlCancel" class="ghost">取消</button>
    </div>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const row = (k, v, cls) => `<tr><td>${esc(k)}</td><td class="${cls||""}">${v}</td></tr>`;

let schedRunning = false;
let alPage = 1;
const alPageSize = 20;
let alEditingId = null;
let statusTimer = null;

document.querySelectorAll(".tabs button").forEach((btn) => {
  btn.onclick = () => {
    document.querySelectorAll(".tabs button").forEach((b) => b.classList.toggle("active", b === btn));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    const panel = $("panel-" + btn.dataset.tab);
    if (panel) panel.classList.add("active");
    if (btn.dataset.tab === "alarms") loadAlarms();
    if (btn.dataset.tab === "map") loadMapConfig();
    if (btn.dataset.tab === "sms") loadSmsConfig();
    if (btn.dataset.tab === "status") loadStatus();
    if (btn.dataset.tab === "ai") loadAiStatus();
  };
});

function renderToggleBtn() {
  const btn = $("btnToggle");
  btn.disabled = false;
  if (schedRunning) {
    btn.className = "danger";
    btn.textContent = "停止定时调度";
  } else {
    btn.className = "success";
    btn.textContent = "启动定时调度";
  }
}

function renderStatus(data) {
  const s = data.scheduler || {};
  schedRunning = !!s.running;
  renderToggleBtn();
  $("dotSched").className = "dot " + (s.running ? "ok" : "bad");
  $("tblSched").innerHTML =
    row("运行说明", "服务启动后默认自动运行，重启后也会自动恢复") +
    row("循环运行中", s.running ? '<span class="okc">运行中</span>' : '<span class="err">已停止</span>') +
    row("检测间隔", esc(s.interval_seconds) + " 秒") +
    row("Redis 目标", esc(s.redis)) +
    row("最低处理时速", esc(s.min_speed_kmh) + " km/h") +
    row("轨迹纠偏", '<span class="okc">已启用（高德 GraspRoad）</span>') +
    row("JT808 定位接口", data.jt808_openapi_configured ? '<span class="okc">已配置</span>' : '<span class="err">未配置（无法取车辆坐标）</span>') +
    row("最近执行时间", esc(s.last_run_at || "从未执行"));
  renderSync(data.violation_sync || {});
  renderFuelSync(data.obd_fuel_sync || {});
  renderParkAlarm(data.park_alarm_scheduler || {});
  renderRun(s.last_result, s.last_error);
}

function renderParkAlarm(ps) {
  const m = ps.jt808_mirror || {};
  const mysqlOk = !!m.mysql_ok;
  const exists = !!m.table_exists;
  let dot = "bad";
  if (ps.running && mysqlOk && exists) dot = "ok";
  else if (ps.running || mysqlOk) dot = "warn";
  $("dotParkAlarm").className = "dot " + dot;
  const lr = ps.last_result || {};
  let html =
    row("启用配置", ps.enabled ? '<span class="okc">已启用</span>' : '<span class="err">已关闭</span>') +
    row("循环运行中", ps.running ? '<span class="okc">运行中</span>' : '<span class="err">已停止</span>') +
    row("扫描间隔", esc(ps.interval_seconds) + " 秒") +
    row("无游标回看", esc(ps.lookback_hours) + " 小时") +
    row("JT808 OpenAPI", ps.jt808_openapi_configured ? '<span class="okc">已配置</span>' : '<span class="err">未配置</span>') +
    row("808 表名", esc(m.table || "cesg_park_alarm")) +
    row("808 MySQL", mysqlOk ? '<span class="okc">正常</span>' : '<span class="err">不可用</span>') +
    row("表是否存在", exists ? '<span class="okc">已存在</span>' : '<span class="err">尚未创建（首写时建表）</span>') +
    row("808 表记录数", `<b>${esc(m.row_count == null ? "—" : m.row_count)}</b> 条`) +
    row("最近执行时间", esc(ps.last_run_at || "从未执行")) +
    row("最近入库", `<b>${esc(lr.inserted ?? "—")}</b> 条 / 拉取 ${esc(lr.pulled ?? "—")} / 超限 ${esc(lr.over_limit ?? "—")} / 围栏命中 ${esc(lr.fence_hits ?? "—")}`) +
    row("过滤明细", `无围栏 ${esc(lr.skipped_no_fence ?? "—")} / 无坐标 ${esc(lr.skipped_no_coord ?? "—")} / 过短 ${esc(lr.skipped_short ?? "—")} / 游标跳过 ${esc(lr.skipped_cursor ?? "—")}`);
  if (ps.last_error || m.error) {
    html += row("说明", `<span class="err">${esc(ps.last_error || m.error)}</span>`);
  }
  $("tblParkAlarm").innerHTML = html;
}

function renderSync(vs) {
  const m = vs.jt808_mirror || {};
  const mysqlOk = !!m.mysql_ok;
  const exists = !!m.table_exists;
  let dot = "bad";
  if (mysqlOk && exists) dot = "ok";
  else if (mysqlOk) dot = "warn";
  $("dotSync").className = "dot " + dot;
  let html =
    row("本地安全报警表", `<b>${esc(vs.local_vehicle_violation_count ?? "—")}</b> 条（CESG vehicle_violation）`) +
    row("808 镜像表名", esc(m.table || "cesg_vehicle_violation")) +
    row("808 MySQL", esc(m.mysql_target || "—")) +
    row("MySQL 连接", mysqlOk ? '<span class="okc">正常</span>' : '<span class="err">不可用</span>') +
    row("表是否存在", exists ? '<span class="okc">已存在</span>' : '<span class="err">尚未创建</span>') +
    row("808 表记录数", `<b>${esc(m.row_count == null ? "—" : m.row_count)}</b> 条`);
  if (m.error) html += row("说明", `<span class="err">${esc(m.error)}</span>`);
  $("tblSync").innerHTML = html;
}

function renderFuelSync(fs) {
  const m = fs.jt808_mirror || {};
  const mysqlOk = !!m.mysql_ok;
  const exists = !!m.table_exists;
  let dot = "bad";
  if (mysqlOk && exists) dot = "ok";
  else if (mysqlOk) dot = "warn";
  $("dotFuelSync").className = "dot " + dot;
  let html =
    row("本地日油耗表", `<b>${esc(fs.local_obd_fuel_daily_count ?? "—")}</b> 条（CESG obd_fuel_daily）`) +
    row("808 镜像表名", esc(m.table || "cesg_obd_fuel_daily")) +
    row("808 MySQL", esc(m.mysql_target || "—")) +
    row("MySQL 连接", mysqlOk ? '<span class="okc">正常</span>' : '<span class="err">不可用</span>') +
    row("表是否存在", exists ? '<span class="okc">已存在</span>' : '<span class="err">尚未创建</span>') +
    row("808 表记录数", `<b>${esc(m.row_count == null ? "—" : m.row_count)}</b> 条`) +
    row("同步时机", "Redis 油车 OBD 写入 / 报表回填后即时 upsert");
  if (m.error) html += row("说明", `<span class="err">${esc(m.error)}</span>`);
  $("tblFuelSync").innerHTML = html;
}

function renderRun(r, err) {
  if (!r) {
    $("dotRun").className = "dot";
    $("tblRun").innerHTML = row("结果", err ? `<span class="err">${esc(err)}</span>` : "暂无");
    return;
  }
  const bad = r.error;
  $("dotRun").className = "dot " + (bad ? "bad" : "ok");
  let html =
    row("扫描 OBD Key 数", esc(r.scanned_keys)) +
    row("成功解析", esc(r.parsed)) +
    row("低速跳过(≤阈值)", esc(r.skipped_low_speed)) +
    row("数据过期跳过", esc(r.skipped_stale)) +
    row("未关联车辆", esc(r.skipped_no_vehicle)) +
    row("无坐标跳过", esc(r.skipped_no_position)) +
    row("无适用规则", esc(r.skipped_no_rule)) +
    row("完成规则判定", esc(r.checked)) +
    row("轨迹纠偏成功", esc(r.grasp_road_corrected ?? 0)) +
    row("轨迹纠偏回落", esc(r.grasp_road_fallback ?? 0)) +
    row("新增违章", `<b>${esc(r.violations_inserted)}</b> 条`);
  if (bad) html += row("错误", `<span class="err">${esc(bad)}</span>`);
  if (r.detail && r.detail.length) {
    html += row("违章明细", `<pre>${esc(JSON.stringify(r.detail, null, 2))}</pre>`);
  }
  $("tblRun").innerHTML = html;
}

function renderPing(p) {
  $("dotRedis").className = "dot " + (p.connected ? "ok" : "bad");
  let html =
    row("目标", esc(p.target)) +
    row("连接", p.connected ? `<span class="okc">成功（PING ${esc(p.ping_ms)} ms）</span>` : `<span class="err">失败</span>`);
  if (p.error) html += row("错误", `<span class="err">${esc(p.error)}</span>`);
  if (p.connected) {
    html += row("OBD Key 数量", esc(p.obd_key_count));
    if (p.sample_keys && p.sample_keys.length) html += row("Key 样例", esc(p.sample_keys.join("、")));
    if (p.sample_parsed) html += row("解析结果", `<pre>${esc(JSON.stringify(p.sample_parsed, null, 2))}</pre>`);
  }
  $("tblRedis").innerHTML = html;
  if (p.sample_payload) {
    $("sampleWrap").style.display = "";
    $("samplePayload").textContent = p.sample_payload;
  } else {
    $("sampleWrap").style.display = "none";
  }
}

async function loadStatus() {
  try {
    const res = await fetch("/api/obd-speed-check/status");
    renderStatus(await res.json());
    $("refreshed").textContent = "状态刷新于 " + new Date().toLocaleString() + "（每 15 秒自动刷新）";
  } catch (e) {
    $("refreshed").textContent = "状态接口请求失败：" + e;
  }
}

$("btnToggle").onclick = async () => {
  const btn = $("btnToggle");
  btn.disabled = true;
  btn.textContent = schedRunning ? "停止中…" : "启动中…";
  try {
    const res = await fetch(schedRunning ? "/api/obd-speed-check/stop" : "/api/obd-speed-check/start", { method: "POST" });
    const data = await res.json();
    schedRunning = !!(data.scheduler && data.scheduler.running);
  } catch (e) {
    alert("操作失败：" + e);
  }
  renderToggleBtn();
  loadStatus();
};

$("btnPing").onclick = async () => {
  const btn = $("btnPing");
  btn.disabled = true; btn.textContent = "连接中…";
  try {
    const res = await fetch("/api/obd-speed-check/ping");
    renderPing((await res.json()).redis || {});
  } catch (e) {
    renderPing({ connected: false, error: String(e), target: "-" });
  }
  btn.disabled = false; btn.textContent = "测试 Redis 连接";
};

$("btnRun").onclick = async () => {
  const btn = $("btnRun");
  btn.disabled = true; btn.textContent = "执行中…";
  try {
    const res = await fetch("/api/obd-speed-check/run-once", { method: "POST" });
    const data = await res.json();
    renderRun(data.result, data.result && data.result.error);
  } catch (e) {
    renderRun(null, String(e));
  }
  btn.disabled = false; btn.textContent = "立即执行一轮检测";
  loadStatus();
};

$("btnFuelSync").onclick = async () => {
  const btn = $("btnFuelSync");
  btn.disabled = true; btn.textContent = "同步中…";
  try {
    const data = await apiJson("/api/obd-speed-check/sync-obd-fuel", { method: "POST" });
    alert("同步完成：写入 " + (data.upserted ?? 0) + " 条，808 现有 " + (data.row_count ?? "—") + " 条");
  } catch (e) {
    alert("同步失败：" + e);
  }
  btn.disabled = false; btn.textContent = "立即全量同步到 808";
  loadStatus();
};

$("btnParkAlarmRun").onclick = async () => {
  const btn = $("btnParkAlarmRun");
  btn.disabled = true; btn.textContent = "扫描中…";
  try {
    const data = await apiJson("/api/obd-speed-check/park-alarm/run-once", { method: "POST" });
    const r = data.result || {};
    alert(
      "扫描完成：入库 " + (r.inserted ?? 0) +
      "，拉取 " + (r.pulled ?? 0) +
      "，超限 " + (r.over_limit ?? 0) +
      "，围栏命中 " + (r.fence_hits ?? 0) +
      "，无围栏 " + (r.skipped_no_fence ?? 0) +
      "，无坐标 " + (r.skipped_no_coord ?? 0)
    );
  } catch (e) {
    alert("扫描失败：" + e);
  }
  btn.disabled = false; btn.textContent = "立即扫描一轮";
  loadStatus();
};

$("btnParkAlarmReset").onclick = async () => {
  if (!confirm("清空停车扫描游标后，将按回看窗口重新拉取 1240。确定？")) return;
  const btn = $("btnParkAlarmReset");
  btn.disabled = true; btn.textContent = "处理中…";
  try {
    const reset = await apiJson("/api/obd-speed-check/park-alarm/reset-cursors", { method: "POST" });
    const data = await apiJson("/api/obd-speed-check/park-alarm/run-once", { method: "POST" });
    const r = data.result || {};
    alert(
      "已清空游标 " + (reset.deleted ?? 0) + " 条，重扫入库 " + (r.inserted ?? 0) +
      "，拉取 " + (r.pulled ?? 0) +
      "，围栏命中 " + (r.fence_hits ?? 0) +
      "，无围栏 " + (r.skipped_no_fence ?? 0)
    );
  } catch (e) {
    alert("重扫失败：" + e);
  }
  btn.disabled = false; btn.textContent = "清空游标并重扫窗口";
  loadStatus();
};

async function apiJson(url, opts) {
  const res = await fetch(url, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = data.detail;
    const msg = typeof detail === "string" ? detail : (detail && JSON.stringify(detail)) || res.statusText;
    throw new Error(msg || "请求失败");
  }
  return data;
}

async function loadAlarms() {
  const qs = new URLSearchParams({
    page: String(alPage),
    page_size: String(alPageSize),
  });
  const code = $("alCode").value.trim();
  const name = $("alName").value.trim();
  const status = $("alStatus").value;
  const level = $("alLevel").value;
  if (code) qs.set("type_code", code);
  if (name) qs.set("type_name", name);
  if (status) qs.set("status", status);
  if (level) qs.set("safety_level", level);
  try {
    const data = await apiJson("/api/alarm-type/list?" + qs.toString());
    const items = data.items || [];
    const total = data.total || 0;
    $("alTotal").textContent = "共 " + total + " 条";
    $("alPageInfo").textContent = "第 " + alPage + " 页 / 共 " + Math.max(1, Math.ceil(total / alPageSize)) + " 页";
    if (!items.length) {
      $("alBody").innerHTML = '<tr><td colspan="7">暂无数据</td></tr>';
      return;
    }
    $("alBody").innerHTML = items.map((it) => {
      const created = (it.created_at || "").replace("T", " ").slice(0, 19);
      return `<tr>
        <td>${esc(it.type_code)}</td>
        <td>${esc(it.type_name)}</td>
        <td>${esc(it.min_interval_minutes)}</td>
        <td>${esc(it.status)}</td>
        <td>${esc(it.safety_level)}</td>
        <td>${esc(created)}</td>
        <td class="ops">
          <button type="button" class="ghost" data-edit="${it.id}">编辑</button>
          <button type="button" class="danger" data-del="${it.id}">删除</button>
        </td>
      </tr>`;
    }).join("");
    $("alBody").querySelectorAll("[data-edit]").forEach((b) => {
      b.onclick = () => openAlarmModal(items.find((x) => String(x.id) === b.dataset.edit));
    });
    $("alBody").querySelectorAll("[data-del]").forEach((b) => {
      b.onclick = () => removeAlarm(b.dataset.del);
    });
  } catch (e) {
    $("alBody").innerHTML = `<tr><td colspan="7" class="err">${esc(e.message || e)}</td></tr>`;
  }
}

function openAlarmModal(row) {
  alEditingId = row ? row.id : null;
  $("alModalTitle").textContent = row ? "编辑报警类型" : "新增报警类型";
  $("fCode").value = row ? (row.type_code || "") : "";
  $("fName").value = row ? (row.type_name || "") : "";
  $("fInterval").value = row ? (row.min_interval_minutes ?? 15) : 15;
  $("fStatus").value = row ? (row.status || "启用") : "启用";
  $("fLevel").value = row ? (row.safety_level || "中") : "中";
  $("alModal").classList.add("show");
}

function closeAlarmModal() {
  $("alModal").classList.remove("show");
  alEditingId = null;
}

async function removeAlarm(id) {
  if (!confirm("确认删除该报警类型？")) return;
  try {
    await apiJson("/api/alarm-type/" + id, { method: "DELETE" });
    loadAlarms();
  } catch (e) {
    alert("删除失败：" + e.message);
  }
}

$("btnAlQuery").onclick = () => { alPage = 1; loadAlarms(); };
$("btnAlAdd").onclick = () => openAlarmModal(null);
$("btnAlCancel").onclick = closeAlarmModal;
$("btnAlPrev").onclick = () => { if (alPage > 1) { alPage -= 1; loadAlarms(); } };
$("btnAlNext").onclick = () => { alPage += 1; loadAlarms(); };
$("btnAlSave").onclick = async () => {
  const body = {
    type_name: $("fName").value.trim(),
    min_interval_minutes: Number($("fInterval").value || 0),
    status: $("fStatus").value,
    safety_level: $("fLevel").value,
  };
  if (!body.type_name) { alert("请填写类型名称"); return; }
  try {
    if (alEditingId) {
      await apiJson("/api/alarm-type/" + alEditingId, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    } else {
      await apiJson("/api/alarm-type", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    }
    closeAlarmModal();
    loadAlarms();
  } catch (e) {
    alert("保存失败：" + e.message);
  }
};
$("btnAlReset").onclick = async () => {
  if (!confirm("将清空全部报警类型并从 808 目录重新灌入，确认？")) return;
  try {
    const data = await apiJson("/api/alarm-type/reset-from-jt808", { method: "POST" });
    alert("已重置：清除 " + (data.cleared ?? 0) + "，写入 " + (data.inserted ?? 0));
    alPage = 1;
    loadAlarms();
  } catch (e) {
    alert("重置失败：" + e.message);
  }
};

function fillMapForm(d) {
  d = d || {};
  $("mProvider").value = d.provider || "amap";
  $("mApiKey").value = d.api_key || "";
  $("mSecret").value = d.secret_key || "";
  $("mWebKey").value = d.web_service_key || "";
  $("mZoom").value = d.default_zoom != null ? d.default_zoom : 12;
  $("mLng").value = d.default_center_lng != null ? d.default_center_lng : "106.55156";
  $("mLat").value = d.default_center_lat != null ? d.default_center_lat : "29.56301";
  $("mRemark").value = d.remark || "";
}

async function loadMapConfig() {
  $("mapMsg").textContent = "加载中…";
  try {
    const data = await apiJson("/api/map-api-config?provider=amap");
    fillMapForm(data.data);
    $("mapMsg").textContent = data.data ? ("已加载" + (data.data.updated_at ? " · 更新于 " + data.data.updated_at.replace("T", " ").slice(0, 19) : "")) : "尚无配置，可直接填写保存";
  } catch (e) {
    $("mapMsg").textContent = "加载失败：" + e.message;
  }
}

$("btnMapReload").onclick = () => loadMapConfig();
$("btnMapSave").onclick = async () => {
  const body = {
    provider: "amap",
    api_key: $("mApiKey").value.trim() || null,
    secret_key: $("mSecret").value.trim() || null,
    web_service_key: $("mWebKey").value.trim() || null,
    default_zoom: $("mZoom").value === "" ? null : Number($("mZoom").value),
    default_center_lng: $("mLng").value === "" ? null : Number($("mLng").value),
    default_center_lat: $("mLat").value === "" ? null : Number($("mLat").value),
    remark: $("mRemark").value.trim() || null,
  };
  try {
    await apiJson("/api/map-api-config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    $("mapMsg").textContent = "保存成功 " + new Date().toLocaleString();
    loadMapConfig();
  } catch (e) {
    $("mapMsg").textContent = "保存失败：" + e.message;
    alert("保存失败：" + e.message);
  }
};
$("btnMapSync").onclick = async () => {
  try {
    const data = await apiJson("/api/map-api-config/sync-web-service-key?provider=amap", { method: "POST" });
    alert(data.message || (data.ok ? "已同步" : "同步失败"));
    if (data.data) fillMapForm(data.data);
    else loadMapConfig();
  } catch (e) {
    alert("同步失败：" + e.message);
  }
};

function fillSmsForm(d) {
  d = d || {};
  $("sEnabled").value = d.enabled ? "1" : "0";
  $("sBaseUrl").value = d.base_url || "";
  $("sSubmitPath").value = d.submit_path || "/sms/submit";
  $("sTplPath").value = d.template_path || "/sms/tmpsubmit";
  $("sEcName").value = d.ec_name || "";
  $("sApId").value = d.ap_id || "";
  $("sSecret").value = d.secret_key || "";
  $("sSign").value = d.sign || "";
  $("sAddSerial").value = d.add_serial || "";
  $("sMode").value = d.send_mode === "template" ? "template" : "normal";
  $("sTplId").value = d.template_id || "";
  $("sContentTpl").value = d.content_template || "您的验证码为{code}，5分钟内有效。";
  $("sTtl").value = d.code_ttl_seconds != null ? d.code_ttl_seconds : 300;
  $("sRemark").value = d.remark || "";
  const ready = !!d.ready;
  $("dotSms").className = "dot " + (ready ? "ok" : "bad");
  $("sReadyText").innerHTML = ready
    ? '<span class="okc">就绪，可发送验证码</span>'
    : ('<span class="err">未就绪：' + esc(d.ready_reason || "请完善并启用配置") + "</span>");
}

async function loadSmsConfig() {
  $("smsMsg").textContent = "加载中…";
  try {
    const data = await apiJson("/api/sms-api-config?provider=mas");
    fillSmsForm(data.data || {});
    $("smsMsg").textContent = data.data
      ? ("已加载" + (data.data.updated_at ? " · 更新于 " + String(data.data.updated_at).replace("T", " ").slice(0, 19) : ""))
      : "尚无配置，填写后保存";
  } catch (e) {
    $("smsMsg").textContent = "加载失败：" + e.message;
  }
}

$("btnSmsReload").onclick = () => loadSmsConfig();
$("btnSmsSave").onclick = async () => {
  const body = {
    provider: "mas",
    enabled: $("sEnabled").value === "1",
    base_url: $("sBaseUrl").value.trim() || null,
    submit_path: $("sSubmitPath").value.trim() || "/sms/submit",
    template_path: $("sTplPath").value.trim() || "/sms/tmpsubmit",
    ec_name: $("sEcName").value.trim() || null,
    ap_id: $("sApId").value.trim() || null,
    secret_key: $("sSecret").value.trim() || null,
    sign: $("sSign").value.trim() || null,
    add_serial: $("sAddSerial").value.trim(),
    send_mode: $("sMode").value,
    template_id: $("sTplId").value.trim() || null,
    content_template: $("sContentTpl").value.trim() || null,
    code_ttl_seconds: $("sTtl").value === "" ? 300 : Number($("sTtl").value),
    remark: $("sRemark").value.trim() || null,
  };
  try {
    const data = await apiJson("/api/sms-api-config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    fillSmsForm(data.data || {});
    $("smsMsg").textContent = "保存成功 " + new Date().toLocaleString()
      + (data.ready ? " · 已就绪" : (" · 未就绪：" + (data.ready_reason || "")));
  } catch (e) {
    $("smsMsg").textContent = "保存失败：" + e.message;
    alert("保存失败：" + e.message);
  }
};
$("btnSmsTest").onclick = async () => {
  const phone = ($("sTestPhone").value || "").trim();
  if (!/^1\\d{10}$/.test(phone)) {
    alert("请输入正确的试发手机号");
    return;
  }
  try {
    const data = await apiJson("/api/sms-api-config/test-send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ phone }),
    });
    alert((data.ok ? "试发成功" : "无法获取短信") + (data.detail ? ("\\n" + data.detail) : ""));
  } catch (e) {
    alert("试发失败：" + e.message);
  }
};

let aiSchedRunning = false;

function renderAiToggleBtn() {
  const btn = $("btnAiToggle");
  btn.disabled = false;
  if (aiSchedRunning) {
    btn.className = "danger";
    btn.textContent = "停止 AI 评估调度";
  } else {
    btn.className = "success";
    btn.textContent = "启动 AI 评估调度";
  }
}

function renderAiStatus(s) {
  s = s || {};
  aiSchedRunning = !!s.running;
  renderAiToggleBtn();
  let dot = "bad";
  if (s.running && s.agent_worker_configured) dot = "ok";
  else if (s.running || s.agent_worker_configured) dot = "warn";
  $("dotAi").className = "dot " + dot;
  $("tblAi").innerHTML =
    row("运行中", s.running ? '<span class="okc">是</span>' : '<span class="err">否</span>') +
    row("配置启用", s.enabled ? '<span class="okc">是</span>' : '<span class="err">否</span>') +
    row("处理节奏", "一条完成后立刻下一条（无间隔；仅无候选时歇 10 秒）") +
    row("每轮条数", esc(s.batch_size ?? 1)) +
    row("取数顺序", esc(s.order_label || "优先最新")) +
    row("Agent Worker", s.agent_worker_configured ? '<span class="okc">已配置</span>' : '<span class="err">未配置</span>') +
    row("Worker 地址", esc(s.agent_worker_base_url || "—")) +
    row("调度用户", esc(s.user_id || "—")) +
    row("暂缓条数", esc(s.deferred_count ?? 0) + "（无证据/失败暂缓）") +
    row("待评估积压", `<b>${esc(s.pending_unassessed_estimate ?? "—")}</b> 条`) +
    row("最近执行", esc(s.last_run_at || "从未执行")) +
    row("规则说明", esc(s.rules || ""));
  $("tblAiToday").innerHTML =
    row("今日询问模型并落库", `<b style="font-size:22px;color:#86efac">${esc(s.today_assessed_db ?? 0)}</b> 条`) +
    row("统计口径", "violation_ai_assessment.created_at 属今日（东八区）的成功评估数");
  const recent = s.recent_assessed || [];
  if (!recent.length) {
    $("aiRecentBody").innerHTML = '<tr><td colspan="7">暂无已评估记录</td></tr>';
  } else {
    $("aiRecentBody").innerHTML = recent.map((it) => {
      const ticket = it.ticket_process_type
        ? (it.ticket_process_type === "罚款" ? ("罚款 " + (it.ticket_amount ?? "")) : it.ticket_process_type)
        : "—";
      return `<tr>
        <td>${esc(it.assessed_at || "—")}</td>
        <td>${esc(it.violation_id)}</td>
        <td>${esc(it.plate_no || "—")}</td>
        <td>${esc(it.alarm_type || "—")}</td>
        <td>${esc(it.status || "—")}</td>
        <td>${esc(ticket)}</td>
        <td>${esc(it.violation_time || "—")}</td>
      </tr>`;
    }).join("");
  }
  const r = s.last_result || null;
  const err = s.last_error;
  if (!r && !err) {
    $("tblAiRun").innerHTML = row("结果", "暂无");
    return;
  }
  let html = "";
  if (r) {
    html +=
      row("扫描候选", esc(r.scanned)) +
      row("成功评估落库", `<b>${esc(r.assessed)}</b>`) +
      row("命中缓存", esc(r.cached)) +
      row("跳过(无证据)", esc(r.skipped_no_evidence)) +
      row("跳过(其它)", esc(r.skipped_other)) +
      row("错误", esc(r.errors));
    if (r.detail && r.detail.length) {
      html += row("明细", `<pre>${esc(JSON.stringify(r.detail, null, 2))}</pre>`);
    }
  }
  if (err) html += row("错误", `<span class="err">${esc(err)}</span>`);
  $("tblAiRun").innerHTML = html;
}

async function loadAiStatus() {
  try {
    const res = await fetch("/api/violation-ai-assess/status");
    const data = await res.json();
    renderAiStatus(data.scheduler || {});
    $("aiRefreshed").textContent = "AI 状态刷新于 " + new Date().toLocaleString() + "（停留本页时每 15 秒刷新）";
  } catch (e) {
    $("aiRefreshed").textContent = "AI 状态接口失败：" + e;
  }
}

$("btnAiToggle").onclick = async () => {
  const btn = $("btnAiToggle");
  btn.disabled = true;
  btn.textContent = aiSchedRunning ? "停止中…" : "启动中…";
  try {
    await fetch(aiSchedRunning ? "/api/violation-ai-assess/stop" : "/api/violation-ai-assess/start", { method: "POST" });
  } catch (e) {
    alert("操作失败：" + e);
  }
  loadAiStatus();
};

$("btnAiRun").onclick = async () => {
  const btn = $("btnAiRun");
  btn.disabled = true; btn.textContent = "评估中…";
  try {
    const res = await fetch("/api/violation-ai-assess/run-once", { method: "POST" });
    const data = await res.json();
    if (!res.ok) alert((data && data.detail) || "执行失败");
    renderAiStatus(data.scheduler || {});
  } catch (e) {
    alert("执行失败：" + e);
  }
  btn.disabled = false; btn.textContent = "立即评估一轮";
  loadAiStatus();
};

$("btnAiRefresh").onclick = () => loadAiStatus();

loadStatus();
statusTimer = setInterval(() => {
  if ($("panel-status").classList.contains("active")) loadStatus();
  if ($("panel-ai").classList.contains("active")) loadAiStatus();
}, 15000);

const hash = (location.hash || "").replace("#", "");
if (hash === "alarms" || hash === "map" || hash === "ai" || hash === "sms") {
  const btn = document.querySelector('.tabs button[data-tab="' + hash + '"]');
  if (btn) btn.click();
}
</script>
</body>
</html>"""



@router.get("/obd-status", response_class=HTMLResponse, include_in_schema=False)
async def obd_status_page():
    """后台运维页：OBD / AI / 报警类型 / 地图 / 短信。访问 /obd-status（#ai / #alarms / #map / #sms）。"""
    return HTMLResponse(_STATUS_PAGE)
