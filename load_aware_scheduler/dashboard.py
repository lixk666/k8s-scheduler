import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Lock, Thread
from typing import Dict, List
from urllib.parse import urlparse

from load_aware_scheduler.cache import ClusterCache
from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.k8s_resources import pod_identity
from load_aware_scheduler.models import ClusterSnapshot, NodeInfo, NodeScore
from load_aware_scheduler.quantity import clamp

LOGGER = logging.getLogger(__name__)


class ScheduleHistory:
    def __init__(self, size: int):
        self._items = deque(maxlen=max(1, size))
        self._lock = Lock()

    def record(
        self,
        pod: object,
        scores: List[NodeScore],
        rejected: Dict[str, List[str]],
        selected_node: str,
        status: str,
        cfg: SchedulerConfig,
    ) -> None:
        identity = pod_identity(pod, cfg)
        item = {
            "timestamp": now_iso(),
            "namespace": pod.metadata.namespace,
            "pod": pod.metadata.name,
            "workload": identity.workload,
            "role": identity.role,
            "app_id": identity.app_id,
            "selected_node": selected_node,
            "status": status,
            "candidates": [node_score_json(score, identity.app_id) for score in scores],
            "rejected": [
                {"node": node, "reasons": reasons}
                for node, reasons in sorted(rejected.items())
            ],
        }
        with self._lock:
            self._items.appendleft(item)

    def items(self) -> List[dict]:
        with self._lock:
            return list(self._items)


class DashboardServer:
    def __init__(
        self,
        cfg: SchedulerConfig,
        cache: ClusterCache,
        history: ScheduleHistory,
        stop_event: Event,
    ):
        self.cfg = cfg
        self.cache = cache
        self.history = history
        self.stop_event = stop_event
        self._server = None
        self._thread = Thread(target=self._run, daemon=True)

    def start(self) -> None:
        if not self.cfg.dashboard_enabled:
            LOGGER.info("dashboard disabled")
            return
        self._thread.start()

    def _run(self) -> None:
        handler = self._make_handler()
        try:
            self._server = ReusableThreadingHTTPServer(
                (self.cfg.dashboard_host, self.cfg.dashboard_port),
                handler,
            )
            LOGGER.info(
                "dashboard listening on %s:%s",
                self.cfg.dashboard_host,
                self.cfg.dashboard_port,
            )
            while not self.stop_event.is_set():
                self._server.handle_request()
        except Exception:
            LOGGER.exception("dashboard server failed")
        finally:
            if self._server:
                self._server.server_close()

    def _make_handler(self):
        dashboard = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                path = urlparse(self.path).path
                if path == "/":
                    self._write(200, "text/html; charset=utf-8", DASHBOARD_HTML)
                    return
                if path == "/api/nodes":
                    self._write_json(200, dashboard.nodes_payload())
                    return
                if path == "/api/schedules":
                    self._write_json(200, dashboard.schedules_payload())
                    return
                if path == "/healthz":
                    self._write_json(200, {"ok": True})
                    return
                self._write_json(404, {"error": "not found"})

            def log_message(self, fmt: str, *args) -> None:
                LOGGER.debug("dashboard %s", fmt % args)

            def _write_json(self, status: int, payload: dict) -> None:
                self._write(
                    status,
                    "application/json; charset=utf-8",
                    json.dumps(payload, separators=(",", ":")),
                )

            def _write(self, status: int, content_type: str, body: str) -> None:
                data = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler

    def nodes_payload(self) -> dict:
        snapshot = self.cache.snapshot()
        return {
            "timestamp": now_iso(),
            "scheduler": self.cfg.scheduler_name,
            "nodes": nodes_json(snapshot),
        }

    def schedules_payload(self) -> dict:
        return {
            "timestamp": now_iso(),
            "scheduler": self.cfg.scheduler_name,
            "items": self.history.items(),
        }


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    timeout = 1


def nodes_json(snapshot: ClusterSnapshot) -> List[dict]:
    nodes = []
    for node in snapshot.nodes.values():
        nodes.append(
            {
                "name": node.name,
                "ready": node.ready,
                "unschedulable": node.unschedulable,
                "node_type": node.node_type,
                "allocatable_cpu_cores": round(node.allocatable_cpu_cores, 3),
                "allocatable_memory_bytes": node.allocatable_memory_bytes,
                "requested_cpu_cores": round(node.requested_cpu_cores, 3),
                "requested_memory_bytes": node.requested_memory_bytes,
                "actual_cpu_pct": round_or_none(node.actual_cpu_pct),
                "actual_memory_pct": round_or_none(node.actual_memory_pct),
                "effective_cpu_pct": round(node.effective_cpu_pct(), 2),
                "effective_memory_pct": round(node.effective_memory_pct(), 2),
                "request_cpu_pct": round(node.request_cpu_pct(), 2),
                "request_memory_pct": round(node.request_memory_pct(), 2),
                "pod_count": sum(node.app_counts.values()),
                "app_count": len(node.app_counts),
                "baseline_score": round(baseline_score(node), 2),
            }
        )
    nodes.sort(key=lambda item: item["baseline_score"], reverse=True)
    return nodes


def node_score_json(score: NodeScore, app_id: str) -> dict:
    node = score.node
    return {
        "node": node.name,
        "score": round(score.score, 2),
        "estimated_cpu_pct": round(score.estimated_cpu_pct, 2),
        "estimated_memory_pct": round(score.estimated_memory_pct, 2),
        "actual_cpu_pct": round_or_none(node.actual_cpu_pct),
        "actual_memory_pct": round_or_none(node.actual_memory_pct),
        "request_cpu_pct": round(node.request_cpu_pct(), 2),
        "request_memory_pct": round(node.request_memory_pct(), 2),
        "app_count": node.app_counts.get(app_id, 0),
        "reasons": score.reasons,
    }


def baseline_score(node: NodeInfo) -> float:
    if not node.ready or node.unschedulable:
        return 0.0
    cpu_idle = 100.0 - clamp(node.effective_cpu_pct(), 0.0, 100.0)
    memory_idle = 100.0 - clamp(node.effective_memory_pct(), 0.0, 100.0)
    request_headroom = (
        100.0
        - clamp((node.request_cpu_pct() + node.request_memory_pct()) / 2.0, 0.0, 100.0)
    )
    stability = min(cpu_idle, memory_idle)
    weighted = 0.35 * cpu_idle + 0.20 * memory_idle + 0.10 * request_headroom + 0.10 * stability
    return weighted / 0.75


def round_or_none(value: object) -> object:
    if value is None:
        return None
    return round(float(value), 2)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


DASHBOARD_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>节点资源看板</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f8fb;
      --surface: #ffffff;
      --ink: #152033;
      --muted: #65758b;
      --line: #dbe3ee;
      --green: #218a5a;
      --amber: #b06a00;
      --red: #bd3a3a;
      --blue: #2468b2;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 18px 24px;
      border-bottom: 1px solid var(--line);
      background: var(--surface);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    h1 {
      margin: 0;
      font-size: 20px;
      line-height: 1.2;
      font-weight: 720;
    }
    main {
      width: min(1440px, 100%);
      margin: 0 auto;
      padding: 18px 20px 28px;
    }
    .meta {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .tile {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      min-height: 82px;
    }
    .tile .label {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.3;
    }
    .tile .value {
      margin-top: 8px;
      font-size: 24px;
      line-height: 1.1;
      font-weight: 760;
    }
    section {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      margin-top: 16px;
    }
    .section-title {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
    }
    h2 {
      margin: 0;
      font-size: 15px;
      line-height: 1.25;
    }
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      min-width: 980px;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 10px 12px;
      text-align: left;
      vertical-align: middle;
      font-size: 13px;
      line-height: 1.35;
    }
    th {
      color: var(--muted);
      font-weight: 680;
      background: #fbfcfe;
    }
    tr:last-child td {
      border-bottom: 0;
    }
    .node {
      font-weight: 720;
      overflow-wrap: anywhere;
    }
    .sub {
      color: var(--muted);
      font-size: 12px;
      margin-top: 3px;
    }
    .bar {
      display: grid;
      grid-template-columns: minmax(90px, 1fr) 48px;
      align-items: center;
      gap: 8px;
    }
    .track {
      height: 9px;
      background: #e8eef5;
      border-radius: 999px;
      overflow: hidden;
    }
    .fill {
      height: 100%;
      background: var(--green);
      border-radius: 999px;
      width: 0%;
    }
    .fill.warn { background: var(--amber); }
    .fill.bad { background: var(--red); }
    .score {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 58px;
      height: 28px;
      border-radius: 999px;
      color: #fff;
      background: var(--green);
      font-weight: 760;
    }
    .score.warn { background: var(--amber); }
    .score.bad { background: var(--red); }
    .status {
      display: inline-flex;
      align-items: center;
      height: 24px;
      padding: 0 9px;
      border-radius: 999px;
      background: #e9f3ed;
      color: #17623e;
      font-weight: 680;
      font-size: 12px;
    }
    .status.off {
      background: #f7e9e9;
      color: #9d2828;
    }
    .schedule {
      display: grid;
      grid-template-columns: 260px 160px 1fr;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
    }
    .schedule:last-child {
      border-bottom: 0;
    }
    .candidate-row {
      display: grid;
      grid-template-columns: 24px minmax(130px, 1fr) 78px 120px 120px 72px;
      gap: 8px;
      align-items: center;
      margin-bottom: 7px;
      font-size: 12px;
    }
    .rank {
      color: var(--muted);
      font-variant-numeric: tabular-nums;
    }
    .empty {
      padding: 22px 16px;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 860px) {
      header {
        align-items: flex-start;
        flex-direction: column;
        gap: 6px;
      }
      main { padding: 14px 12px 22px; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .schedule { grid-template-columns: 1fr; }
      .candidate-row {
        grid-template-columns: 24px minmax(120px, 1fr) 66px 92px 92px 62px;
      }
    }
  </style>
</head>
<body>
  <header>
    <h1>节点资源看板</h1>
    <div class="meta" id="updated">--</div>
  </header>
  <main>
    <div class="summary" id="summary"></div>
    <section>
      <div class="section-title">
        <h2>节点资源</h2>
        <div class="meta" id="node-count">--</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th style="width: 220px;">节点</th>
              <th style="width: 90px;">基础分</th>
              <th style="width: 160px;">CPU</th>
              <th style="width: 160px;">内存</th>
              <th style="width: 160px;">CPU Request</th>
              <th style="width: 160px;">内存 Request</th>
              <th style="width: 100px;">Pods</th>
              <th style="width: 96px;">状态</th>
            </tr>
          </thead>
          <tbody id="nodes"></tbody>
        </table>
      </div>
    </section>
    <section>
      <div class="section-title">
        <h2>最近调度评分</h2>
        <div class="meta" id="schedule-count">--</div>
      </div>
      <div id="schedules"></div>
    </section>
  </main>
  <script>
    const fmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });
    const compact = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

    function cls(value, reverse = false) {
      if (value == null) return "";
      if (!reverse) {
        if (value >= 85) return "bad";
        if (value >= 70) return "warn";
        return "";
      }
      if (value < 40) return "bad";
      if (value < 65) return "warn";
      return "";
    }
    function pct(value) {
      return value == null ? "unknown" : `${fmt.format(value)}%`;
    }
    function gib(bytes) {
      return `${compact.format(bytes / 1024 / 1024 / 1024)} Gi`;
    }
    function bar(value) {
      const width = value == null ? 0 : Math.max(0, Math.min(100, value));
      return `<div class="bar"><div class="track"><div class="fill ${cls(value)}" style="width:${width}%"></div></div><span>${pct(value)}</span></div>`;
    }
    function score(value) {
      return `<span class="score ${cls(value, true)}">${fmt.format(value)}</span>`;
    }
    function status(node) {
      const ok = node.ready && !node.unschedulable;
      return `<span class="status ${ok ? "" : "off"}">${ok ? "Ready" : "Blocked"}</span>`;
    }
    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[ch]));
    }
    function tile(label, value) {
      return `<div class="tile"><div class="label">${label}</div><div class="value">${value}</div></div>`;
    }
    async function load() {
      const [nodesRes, schedulesRes] = await Promise.all([
        fetch("/api/nodes", { cache: "no-store" }),
        fetch("/api/schedules", { cache: "no-store" })
      ]);
      const nodesPayload = await nodesRes.json();
      const schedulesPayload = await schedulesRes.json();
      renderNodes(nodesPayload);
      renderSchedules(schedulesPayload);
    }
    function renderNodes(payload) {
      const nodes = payload.nodes || [];
      const ready = nodes.filter(n => n.ready && !n.unschedulable).length;
      const avgScore = nodes.length ? nodes.reduce((sum, n) => sum + n.baseline_score, 0) / nodes.length : 0;
      const avgCpu = avg(nodes.map(n => n.effective_cpu_pct));
      const avgMem = avg(nodes.map(n => n.effective_memory_pct));
      document.getElementById("updated").textContent = `更新时间 ${new Date(payload.timestamp).toLocaleString()}`;
      document.getElementById("node-count").textContent = `${nodes.length} 个节点`;
      document.getElementById("summary").innerHTML =
        tile("就绪节点", `${ready}/${nodes.length}`) +
        tile("平均基础分", fmt.format(avgScore)) +
        tile("平均 CPU", pct(avgCpu)) +
        tile("平均内存", pct(avgMem));
      document.getElementById("nodes").innerHTML = nodes.map(node => `
        <tr>
          <td><div class="node">${esc(node.name)}</div><div class="sub">${esc(node.node_type || "未标记 node type")}</div></td>
          <td>${score(node.baseline_score)}</td>
          <td>${bar(node.effective_cpu_pct)}</td>
          <td>${bar(node.effective_memory_pct)}</td>
          <td>${bar(node.request_cpu_pct)}<div class="sub">${compact.format(node.requested_cpu_cores)} / ${compact.format(node.allocatable_cpu_cores)} cores</div></td>
          <td>${bar(node.request_memory_pct)}<div class="sub">${gib(node.requested_memory_bytes)} / ${gib(node.allocatable_memory_bytes)}</div></td>
          <td><div>${node.pod_count}</div><div class="sub">${node.app_count} 个应用</div></td>
          <td>${status(node)}</td>
        </tr>
      `).join("");
    }
    function renderSchedules(payload) {
      const items = payload.items || [];
      document.getElementById("schedule-count").textContent = `${items.length} 条记录`;
      if (!items.length) {
        document.getElementById("schedules").innerHTML = `<div class="empty">暂无调度记录</div>`;
        return;
      }
      document.getElementById("schedules").innerHTML = items.map(item => `
        <div class="schedule">
          <div>
            <div class="node">${esc(item.namespace)}/${esc(item.pod)}</div>
            <div class="sub">${new Date(item.timestamp).toLocaleString()}</div>
          </div>
          <div>
            <div>${esc(item.selected_node)}</div>
            <div class="sub">${esc(item.status)} · ${esc(item.role)}</div>
          </div>
          <div>${item.candidates.slice(0, 8).map((c, idx) => `
            <div class="candidate-row">
              <span class="rank">#${idx + 1}</span>
              <span>${esc(c.node)}</span>
              <strong>${fmt.format(c.score)}</strong>
              <span>CPU ${pct(c.estimated_cpu_pct)}</span>
      <span>Mem ${pct(c.estimated_memory_pct)}</span>
      <span>App ${c.app_count}</span>
            </div>
          `).join("")}</div>
        </div>
      `).join("");
    }
    function avg(values) {
      const filtered = values.filter(v => v != null);
      return filtered.length ? filtered.reduce((sum, v) => sum + v, 0) / filtered.length : null;
    }
    load().catch(err => {
      document.getElementById("nodes").innerHTML = `<tr><td colspan="8">${esc(err.message)}</td></tr>`;
    });
    setInterval(() => load().catch(() => {}), 5000);
  </script>
</body>
</html>
"""
