"""故障演练控制台(FastAPI + SQLite)。

职责:
- 选择副本注入 5~30 秒故障(其 /health、/work 持续返回 503), 启动前要求两端都健康;
- 演练期间持续探测另一副本; 另一副本失效或期限到达时撤销注入;
- 撤销失败进入"待恢复"并持续重试; 只有受注入副本恢复健康, 轮次才标记"已结束";
- 重启后先把遗留故障恢复干净, 再接受新演练;
- 并发启动只允许一轮生效(asyncio 锁 + SQLite 唯一部分索引双保险);
- 所有状态迁移都按 (id, status) 守卫, 故障撤销携带轮次令牌,
  旧轮次迟到的探测或撤销无法更改新轮状态或解除新轮故障。
"""
import asyncio
import logging
import os
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

REPLICAS = {
    "replica1": os.environ.get("REPLICA1_URL", "http://127.0.0.1:8001"),
    "replica2": os.environ.get("REPLICA2_URL", "http://127.0.0.1:8002"),
}
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "drill.db"))
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "1.0"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "2.0"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("drill")

# ---------------------------------------------------------------- SQLite

_SCHEMA = """
CREATE TABLE IF NOT EXISTS drills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token TEXT NOT NULL,
  target TEXT NOT NULL,
  peer TEXT NOT NULL,
  duration INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',   -- active / recovering / pending_recovery / finished
  termination_reason TEXT,                 -- duration_reached / peer_failed / restart_interrupted
  conclusion TEXT,                         -- available / unavailable / interrupted
  started_at TEXT NOT NULL,
  deadline_at TEXT NOT NULL,
  terminated_at TEXT,
  ended_at TEXT,
  revoke_attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);
-- 数据库级保证: 任意时刻最多一轮未结束的演练
CREATE UNIQUE INDEX IF NOT EXISTS one_open_drill ON drills((1)) WHERE status != 'finished';
CREATE TABLE IF NOT EXISTS probe_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  drill_id INTEGER,
  replica TEXT NOT NULL,
  ok INTEGER NOT NULL,
  status_code INTEGER,
  latency_ms REAL,
  detail TEXT,
  ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS work_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  drill_id INTEGER,
  replica TEXT NOT NULL,
  ok INTEGER NOT NULL,
  status_code INTEGER,
  latency_ms REAL,
  detail TEXT,
  ts TEXT NOT NULL
);
"""

_db_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    global _conn
    parent = os.path.dirname(DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    with _db_lock:
        _conn.executescript(_SCHEMA)
        # 重启恢复: 上次进程退出时仍处于 active 的轮次, 其后台逻辑已随进程消失,
        # 一律转入恢复流程, 由监督循环撤销遗留故障后再接受新演练。
        cur = _conn.execute(
            "UPDATE drills SET status='recovering', termination_reason='restart_interrupted', "
            "conclusion='interrupted', terminated_at=? WHERE status='active'",
            (_now(),),
        )
        _conn.commit()
    if cur.rowcount:
        log.warning("startup recovery: %d interrupted drill(s) moved to recovering", cur.rowcount)


def get_open_drill() -> dict | None:
    with _db_lock:
        row = _conn.execute(
            "SELECT * FROM drills WHERE status != 'finished' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def terminate_drill(drill_id: int, reason: str, conclusion: str) -> bool:
    """active -> recovering。按 (id, status) 守卫, 迟到的终止不会覆盖他人。"""
    with _db_lock:
        cur = _conn.execute(
            "UPDATE drills SET status='recovering', termination_reason=?, conclusion=?, "
            "terminated_at=? WHERE id=? AND status='active'",
            (reason, conclusion, _now(), drill_id),
        )
        _conn.commit()
        return cur.rowcount == 1


def mark_revoke_ok(drill_id: int) -> None:
    with _db_lock:
        _conn.execute(
            "UPDATE drills SET status='recovering', revoke_attempts=revoke_attempts+1, "
            "last_error=NULL WHERE id=? AND status IN ('recovering','pending_recovery')",
            (drill_id,),
        )
        _conn.commit()


def mark_revoke_failed(drill_id: int, err: str) -> None:
    with _db_lock:
        _conn.execute(
            "UPDATE drills SET status='pending_recovery', revoke_attempts=revoke_attempts+1, "
            "last_error=? WHERE id=? AND status IN ('recovering','pending_recovery')",
            (err, drill_id),
        )
        _conn.commit()


def finish_drill(drill_id: int) -> None:
    with _db_lock:
        _conn.execute(
            "UPDATE drills SET status='finished', ended_at=? "
            "WHERE id=? AND status IN ('recovering','pending_recovery')",
            (_now(), drill_id),
        )
        _conn.commit()


def add_probe_event(drill_id: int, replica: str, res: dict) -> None:
    with _db_lock:
        _conn.execute(
            "INSERT INTO probe_events (drill_id, replica, ok, status_code, latency_ms, detail, ts) "
            "VALUES (?,?,?,?,?,?,?)",
            (drill_id, replica, 1 if res["ok"] else 0, res["status_code"],
             res["latency_ms"], res["detail"], _now()),
        )
        _conn.commit()


def add_work_event(drill_id: int | None, replica: str, res: dict) -> None:
    with _db_lock:
        _conn.execute(
            "INSERT INTO work_events (drill_id, replica, ok, status_code, latency_ms, detail, ts) "
            "VALUES (?,?,?,?,?,?,?)",
            (drill_id, replica, 1 if res["ok"] else 0, res["status_code"],
             res["latency_ms"], res["detail"], _now()),
        )
        _conn.commit()


# ---------------------------------------------------------------- 探测与监督

http: httpx.AsyncClient | None = None

live: dict[str, dict] = {
    name: {"ok": None, "status_code": None, "latency_ms": None, "detail": "尚未探测",
           "fault_injected": None, "manual_down": None, "url": url}
    for name, url in REPLICAS.items()
}


async def probe_replica(name: str) -> dict:
    t0 = time.monotonic()
    try:
        r = await http.get(f"{REPLICAS[name]}/health", timeout=HTTP_TIMEOUT)
        lat = round((time.monotonic() - t0) * 1000, 1)
        try:
            body = r.json()
        except Exception:
            body = {}
        return {
            "ok": r.status_code == 200,
            "status_code": r.status_code,
            "latency_ms": lat,
            "detail": "" if r.status_code == 200 else f"HTTP {r.status_code}",
            "fault_injected": bool(body.get("fault_injected")),
            "manual_down": bool(body.get("manual_down")),
        }
    except Exception as e:
        lat = round((time.monotonic() - t0) * 1000, 1)
        return {
            "ok": False,
            "status_code": None,
            "latency_ms": lat,
            "detail": f"{type(e).__name__}: {e}",
            "fault_injected": None,
            "manual_down": None,
        }


async def supervisor_loop() -> None:
    """唯一的后台循环: 探测、终止判定、恢复重试都按数据库当前状态执行,
    不存在随轮次游离的旧任务, 因此旧轮次不会有迟到的探测或撤销。"""
    while True:
        try:
            await supervise_once()
        except Exception:
            log.exception("supervisor tick failed")
        await asyncio.sleep(PROBE_INTERVAL)


async def supervise_once() -> None:
    names = list(REPLICAS)
    results = dict(zip(names, await asyncio.gather(*(probe_replica(n) for n in names))))
    for n, res in results.items():
        live[n] = {**res, "url": REPLICAS[n]}

    drill = get_open_drill()
    if not drill:
        return

    if drill["status"] == "active":
        peer_res = results[drill["peer"]]
        add_probe_event(drill["id"], drill["peer"], peer_res)
        if not peer_res["ok"]:
            log.warning("drill #%s: peer %s failed -> terminate", drill["id"], drill["peer"])
            terminate_drill(drill["id"], "peer_failed", "unavailable")
        elif datetime.now(timezone.utc) >= datetime.fromisoformat(drill["deadline_at"]):
            log.info("drill #%s: duration reached -> terminate", drill["id"])
            terminate_drill(drill["id"], "duration_reached", "available")
    elif drill["status"] in ("recovering", "pending_recovery"):
        await recover_drill(drill)


async def recover_drill(drill: dict) -> None:
    """撤销注入并确认受注入副本恢复健康; 撤销失败标记待恢复, 下一轮循环继续重试。"""
    url = REPLICAS[drill["target"]]
    try:
        r = await http.post(f"{url}/admin/fault/revoke",
                            json={"token": drill["token"]}, timeout=HTTP_TIMEOUT)
        # 409 = 令牌不匹配(如副本进程重启后内存状态丢失), 以健康检查结果为准
        if r.status_code in (200, 409):
            mark_revoke_ok(drill["id"])
        else:
            mark_revoke_failed(drill["id"], f"revoke HTTP {r.status_code}")
            return
    except Exception as e:
        mark_revoke_failed(drill["id"], f"revoke {type(e).__name__}: {e}")
        return

    res = await probe_replica(drill["target"])
    add_probe_event(drill["id"], drill["target"], res)
    if res["ok"]:
        finish_drill(drill["id"])
        log.info("drill #%s: target %s healthy again -> finished", drill["id"], drill["target"])


# ---------------------------------------------------------------- FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    init_db()
    http = httpx.AsyncClient()
    task = asyncio.create_task(supervisor_loop())
    yield
    task.cancel()
    await http.aclose()


app = FastAPI(title="fault-drill-console", lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


class StartReq(BaseModel):
    target: str
    duration: int = Field(ge=5, le=30)


_start_lock = asyncio.Lock()


@app.post("/api/drills", status_code=201)
async def start_drill(req: StartReq):
    if req.target not in REPLICAS:
        raise HTTPException(400, f"未知副本: {req.target}")
    async with _start_lock:
        if get_open_drill():
            raise HTTPException(409, "已有未结束的演练轮次, 请等待其结束")
        # 启动前两端都须健康
        checks = await asyncio.gather(*(probe_replica(n) for n in REPLICAS))
        for name, res in zip(REPLICAS, checks):
            if not res["ok"]:
                raise HTTPException(409, f"副本 {name} 当前不健康, 不满足启动条件(两端都须健康)")
        token = uuid.uuid4().hex
        try:
            r = await http.post(f"{REPLICAS[req.target]}/admin/fault/inject",
                                json={"token": token}, timeout=HTTP_TIMEOUT)
        except Exception as e:
            raise HTTPException(502, f"故障注入失败: {type(e).__name__}: {e}")
        if r.status_code != 200:
            raise HTTPException(502, f"故障注入失败: HTTP {r.status_code}")
        now = datetime.now(timezone.utc)
        peer = next(n for n in REPLICAS if n != req.target)
        try:
            with _db_lock:
                _conn.execute(
                    "INSERT INTO drills (token, target, peer, duration, status, started_at, deadline_at) "
                    "VALUES (?,?,?,?,'active',?,?)",
                    (token, req.target, peer, req.duration, now.isoformat(),
                     (now + timedelta(seconds=req.duration)).isoformat()),
                )
                _conn.commit()
        except sqlite3.IntegrityError:
            # 极端并发下数据库兜底: 本轮不生效, 立即撤销刚注入的故障
            try:
                await http.post(f"{REPLICAS[req.target]}/admin/fault/revoke",
                                json={"token": token}, timeout=HTTP_TIMEOUT)
            except Exception:
                pass
            raise HTTPException(409, "已有未结束的演练轮次, 请等待其结束")
        log.info("drill started: target=%s peer=%s duration=%ss", req.target, peer, req.duration)
        return {"ok": True, "drill": get_open_drill()}


class OverrideReq(BaseModel):
    down: bool


@app.post("/api/replicas/{name}/override")
async def set_replica_override(name: str, req: OverrideReq):
    """切换副本健康状态。演练进行中只允许切换"另一副本"; 无演练时可用于恢复。"""
    if name not in REPLICAS:
        raise HTTPException(400, f"未知副本: {name}")
    drill = get_open_drill()
    if drill and not (drill["status"] == "active" and name == drill["peer"]):
        raise HTTPException(409, "演练期间只允许切换另一副本的健康状态")
    try:
        r = await http.post(f"{REPLICAS[name]}/admin/override",
                            json={"down": req.down}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        raise HTTPException(502, f"切换失败: {type(e).__name__}: {e}")
    if r.status_code != 200:
        raise HTTPException(502, f"切换失败: HTTP {r.status_code}")
    return {"replica": name, "manual_down": req.down}


class WorkReq(BaseModel):
    replica: str


@app.post("/api/work")
async def send_work(req: WorkReq):
    """向任一副本发送真实 /work 请求并记录结果。"""
    if req.replica not in REPLICAS:
        raise HTTPException(400, f"未知副本: {req.replica}")
    drill = get_open_drill()
    drill_id = drill["id"] if drill else None
    t0 = time.monotonic()
    try:
        r = await http.post(f"{REPLICAS[req.replica]}/work", timeout=HTTP_TIMEOUT)
        lat = round((time.monotonic() - t0) * 1000, 1)
        try:
            body = r.json()
        except Exception:
            body = None
        res = {"ok": r.status_code == 200, "status_code": r.status_code,
               "latency_ms": lat, "detail": r.text[:200], "body": body}
    except Exception as e:
        lat = round((time.monotonic() - t0) * 1000, 1)
        res = {"ok": False, "status_code": None, "latency_ms": lat,
               "detail": f"{type(e).__name__}: {e}", "body": None}
    add_work_event(drill_id, req.replica, res)
    return {"replica": req.replica, **res}


@app.get("/api/state")
async def api_state():
    drill = get_open_drill()
    with _db_lock:
        last_finished = _conn.execute(
            "SELECT * FROM drills WHERE status='finished' ORDER BY id DESC LIMIT 1").fetchone()
        history = [dict(r) for r in _conn.execute(
            "SELECT * FROM drills ORDER BY id DESC LIMIT 10").fetchall()]
        display_id = drill["id"] if drill else (last_finished["id"] if last_finished else None)
        probes = []
        if display_id is not None:
            rows = _conn.execute(
                "SELECT * FROM probe_events WHERE drill_id=? ORDER BY id DESC LIMIT 60",
                (display_id,)).fetchall()
            probes = [dict(r) for r in reversed(rows)]
        works = [dict(r) for r in _conn.execute(
            "SELECT * FROM work_events ORDER BY id DESC LIMIT 20").fetchall()]
    return {
        "now": _now(),
        "replicas": live,
        "drill": drill,
        "last_finished": dict(last_finished) if last_finished else None,
        "probes": probes,
        "works": works,
        "history": history,
    }


@app.get("/api/drills")
async def list_drills():
    with _db_lock:
        rows = _conn.execute("SELECT * FROM drills ORDER BY id DESC LIMIT 50").fetchall()
    return {"drills": [dict(r) for r in rows]}
