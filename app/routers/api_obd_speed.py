"""OBD 时速违章监测管理接口 + 独立状态页。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from app.jt808_openapi_client import jt808_openapi_client
from app.obd_speed_monitor import obd_speed_scheduler, ping_redis

router = APIRouter(tags=["obd-speed-check"])


@router.get("/api/obd-speed-check/status")
async def obd_speed_check_status():
    return {
        "ok": True,
        "scheduler": obd_speed_scheduler.status(),
        "jt808_openapi_configured": jt808_openapi_client.configured(),
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


@router.post("/api/obd-speed-check/start")
async def obd_speed_check_start():
    """启动定时调度，并持久化到配置文件（重启后自动恢复运行）。"""
    obd_speed_scheduler.start(force=True, persist=True)
    return {"ok": True, "scheduler": obd_speed_scheduler.status()}


@router.post("/api/obd-speed-check/stop")
async def obd_speed_check_stop():
    """停止定时调度，并持久化到配置文件（重启后保持停止）。"""
    await obd_speed_scheduler.stop(persist=True)
    return {"ok": True, "scheduler": obd_speed_scheduler.status()}


_STATUS_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OBD 超速监测状态</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "Microsoft YaHei", system-ui, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; padding: 24px; }
  .wrap { max-width: 860px; margin: 0 auto; }
  h1 { font-size: 20px; margin-bottom: 4px; }
  .sub { color: #94a3b8; font-size: 13px; margin-bottom: 20px; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px 18px; margin-bottom: 14px; }
  .card h2 { font-size: 15px; margin-bottom: 12px; color: #cbd5e1; display: flex; align-items: center; gap: 8px; }
  .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; background: #64748b; }
  .dot.ok { background: #22c55e; box-shadow: 0 0 8px #22c55e88; }
  .dot.bad { background: #ef4444; box-shadow: 0 0 8px #ef444488; }
  .dot.warn { background: #eab308; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  td { padding: 6px 4px; border-bottom: 1px solid #33415555; vertical-align: top; }
  td:first-child { color: #94a3b8; width: 160px; white-space: nowrap; }
  pre { background: #0f172a; border: 1px solid #334155; border-radius: 6px; padding: 10px; font-size: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-all; color: #a5f3fc; }
  .btns { display: flex; gap: 10px; margin-bottom: 18px; flex-wrap: wrap; }
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
</style>
</head>
<body>
<div class="wrap">
  <h1>OBD 超速监测状态</h1>
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
    <h2><span class="dot" id="dotRun"></span>最近一轮执行结果</h2>
    <table id="tblRun"><tr><td colspan="2">暂无</td></tr></table>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const row = (k, v, cls) => `<tr><td>${esc(k)}</td><td class="${cls||""}">${v}</td></tr>`;

let schedRunning = false;

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
  $("dotSched").className = "dot " + (s.running ? "ok" : (s.enabled ? "warn" : "bad"));
  $("tblSched").innerHTML =
    row("重启后自动运行", (s.enabled ? '<span class="okc">是</span>' : '否') + `（来源：${esc(s.config_source || ".env")}，页面启停会自动保存）`) +
    row("循环运行中", s.running ? '<span class="okc">运行中</span>' : '<span class="err">已停止</span>') +
    row("检测间隔", esc(s.interval_seconds) + " 秒") +
    row("Redis 目标", esc(s.redis)) +
    row("最低处理时速", esc(s.min_speed_kmh) + " km/h") +
    row("JT808 定位接口", data.jt808_openapi_configured ? '<span class="okc">已配置</span>' : '<span class="err">未配置（无法取车辆坐标）</span>') +
    row("最近执行时间", esc(s.last_run_at || "从未执行"));
  renderRun(s.last_result, s.last_error);
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

loadStatus();
setInterval(loadStatus, 15000);
</script>
</body>
</html>"""


@router.get("/obd-status", response_class=HTMLResponse, include_in_schema=False)
async def obd_status_page():
    """独立状态页：部署后浏览器直接访问 /obd-status 即可查看连接与检测状态。"""
    return HTMLResponse(_STATUS_PAGE)
