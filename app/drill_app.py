"""故障演练 + 双副本接单耗时策略发布控制台(FastAPI + SQLite)。

职责:
- 选择副本注入 5~30 秒故障(其 /health、/work 持续返回 503), 启动前要求两端都健康;
- 演练期间持续探测另一副本; 另一副本失效或期限到达时撤销注入;
- 撤销失败进入"待恢复"并持续重试; 只有受注入副本恢复健康, 轮次才标记"已结束";
- 重启后先把遗留故障恢复干净, 再接受新演练;
- 并发启动只允许一轮生效(asyncio 锁 + SQLite 唯一部分索引双保险);
- 所有状态迁移都按 (id, status) 守卫, 故障撤销携带轮次令牌,
  旧轮次迟到的探测或撤销无法更改新轮状态或解除新轮故障。

接单耗时策略发布(两阶段提交):
- 网页提交 {expected_version(所见策略版), max_ms(50~500)}, 控制台生成全局 publish_id
  与新版本号 expected_version+1, 先对两副本 prepare, 全部成功后才逐台 commit;
- 任一副本不可达、写入失败或版本冲突 -> 发布失败, 已 prepare 的副本一律 abort,
  两端继续按旧规则服务;
- commit 阶段带有限重试并在收尾时回读两副本实际版本核对; 无法确认两端同版则判失败,
  绝不把部分生效当成功; 迟到的确认在副本端按 (publish_id, version) 拒绝或幂等忽略;
- 控制台重启只把在途发布标记 interrupted 并尽力 abort 预案, 绝不自动提交;
- 已受理 /work 请求在副本入口快照入站时规则, 按入站规则完成。
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
COMMIT_ATTEMPTS = int(os.environ.get("COMMIT_ATTEMPTS", "3"))
LIMIT_MIN_MS = 50
LIMIT_MAX_MS = 500

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
  ts TEXT NOT NULL,
  duration_ms INTEGER,
  policy_version INTEGER
);
-- 接单耗时策略发布记录。非终态(preparing/committing)的在途发布重启后一律转 interrupted。
CREATE TABLE IF NOT EXISTS policy_publishes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  publish_id TEXT NOT NULL UNIQUE,
  expected_version INTEGER NOT NULL,
  version INTEGER NOT NULL,
  max_ms INTEGER NOT NULL,
  status TEXT NOT NULL,                 -- preparing / committing / committed / aborted / failed / interrupted
  detail TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS policy_publishes_status ON policy_publishes(status);
"""

# 旧库增量列(已存在则忽略)
_MIGRATIONS = (
    ("work_events", "duration_ms", "INTEGER"),
    ("work_events", "policy_version", "INTEGER"),
)

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
        # PRAGMA table_info 列: cid, name, type, ...
        existing_cols = {r[1] for r in _conn.execute("PRAGMA table_info(work_events)").fetchall()}
        for table, column, decl in _MIGRATIONS:
            if column not in existing_cols:
                _conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        # 重启恢复: 上次进程退出时仍处于 active 的轮次, 其后台逻辑已随进程消失,
        # 一律转入恢复流程, 由监督循环撤销遗留故障后再接受新演练。
        cur = _conn.execute(
            "UPDATE drills SET status='recovering', termination_reason='restart_interrupted', "
            "conclusion='interrupted', terminated_at=? WHERE status='active'",
            (_now(),),
        )
        # 在途策略发布同理: 进程已死, 不可能再代为 commit, 标记 interrupted,
        # 启动后尽力 abort 副本端遗留预案, 绝不自动提交。
        cur2 = _conn.execute(
            "UPDATE policy_publishes SET status='interrupted', "
            "detail=COALESCE(detail,'') || ' 控制台重启, 在途发布中断(未自动提交)', updated_at=? "
            "WHERE status IN ('preparing','committing')",
            (_now(),),
        )
        _conn.commit()
    if cur.rowcount:
        log.warning("startup recovery: %d interrupted drill(s) moved to recovering", cur.rowcount)
    if cur2.rowcount:
        log.warning("startup recovery: %d in-flight policy publish(es) interrupted", cur2.rowcount)


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
            "INSERT INTO work_events (drill_id, replica, ok, status_code, latency_ms, detail, ts, "
            "duration_ms, policy_version) VALUES (?,?,?,?,?,?,?,?,?)",
            (drill_id, replica, 1 if res["ok"] else 0, res["status_code"],
             res["latency_ms"], res["detail"], _now(),
             res.get("duration_ms"), res.get("policy_version")),
        )
        _conn.commit()


# ------------------------------------------------------- 策略发布持久化


def create_publish(publish_id: str, expected_version: int, version: int, max_ms: int) -> None:
    with _db_lock:
        _conn.execute(
            "INSERT INTO policy_publishes (publish_id, expected_version, version, max_ms, "
            "status, created_at, updated_at) VALUES (?,?,?,?,'preparing',?,?)",
            (publish_id, expected_version, version, max_ms, _now(), _now()),
        )
        _conn.commit()


def update_publish(publish_id: str, status: str, detail: str | None = None) -> None:
    with _db_lock:
        if detail is None:
            _conn.execute(
                "UPDATE policy_publishes SET status=?, updated_at=? WHERE publish_id=?",
                (status, _now(), publish_id),
            )
        else:
            _conn.execute(
                "UPDATE policy_publishes SET status=?, detail=?, updated_at=? WHERE publish_id=?",
                (status, detail, _now(), publish_id),
            )
        _conn.commit()


def list_publishes(limit: int = 20) -> list[dict]:
    with _db_lock:
        rows = _conn.execute(
            "SELECT * FROM policy_publishes ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def list_interrupted_publish_ids() -> list[str]:
    with _db_lock:
        rows = _conn.execute(
            "SELECT publish_id FROM policy_publishes WHERE status='interrupted'"
        ).fetchall()
    return [r["publish_id"] for r in rows]


# ---------------------------------------------------------------- 探测与监督

http: httpx.AsyncClient | None = None

live: dict[str, dict] = {
    name: {"ok": None, "status_code": None, "latency_ms": None, "detail": "尚未探测",
           "fault_injected": None, "manual_down": None,
           "policy_version": None, "policy_max_ms": None, "url": url}
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
            "policy_version": body.get("policy_version"),
            "policy_max_ms": body.get("policy_max_ms"),
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
            "policy_version": None,
            "policy_max_ms": None,
        }


async def fetch_replica_state(name: str) -> dict:
    """读副本 /state(权威已提交版 + 在途预案), 失败抛异常。"""
    r = await http.get(f"{REPLICAS[name]}/state", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


async def abort_pending_everywhere(publish_id: str) -> None:
    """尽力撤销两副本上的预案; 已提交或不存在时副本端为幂等 no-op, 错误只记录不抛。"""
    async def _one(name: str) -> None:
        try:
            await http.post(f"{REPLICAS[name]}/admin/policy/abort",
                            json={"publish_id": publish_id}, timeout=HTTP_TIMEOUT)
        except Exception as e:
            log.warning("abort %s on %s failed: %s", publish_id, name, e)

    await asyncio.gather(*(_one(n) for n in REPLICAS))


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
    # 清理上次进程遗留的在途发布预案(不自动提交, 仅 abort)
    for pid in list_interrupted_publish_ids():
        await abort_pending_everywhere(pid)
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
    duration_ms: int | None = Field(default=None, ge=1, le=5000)


@app.post("/api/work")
async def send_work(req: WorkReq):
    """向任一副本发送真实 /work 请求并记录结果。

    不带 duration_ms 时副本按 50ms 默认耗时处理; 带耗时(毫秒)时若超过该副本
    当前已提交策略上限, 副本返回 429。
    """
    if req.replica not in REPLICAS:
        raise HTTPException(400, f"未知副本: {req.replica}")
    drill = get_open_drill()
    drill_id = drill["id"] if drill else None
    payload = {} if req.duration_ms is None else {"duration_ms": req.duration_ms}
    # 给模拟耗时留足时间, 避免长耗时试单被控制台的默认 HTTP 超时误杀
    timeout = max(HTTP_TIMEOUT, (req.duration_ms or 50) / 1000.0 + 2.0)
    t0 = time.monotonic()
    try:
        r = await http.post(f"{REPLICAS[req.replica]}/work", json=payload, timeout=timeout)
        lat = round((time.monotonic() - t0) * 1000, 1)
        try:
            body = r.json()
        except Exception:
            body = None
        res = {
            "ok": r.status_code == 200,
            "status_code": r.status_code,
            "latency_ms": lat,
            "detail": r.text[:200],
            "body": body,
            "duration_ms": (body or {}).get("duration_ms", req.duration_ms),
            "policy_version": (body or {}).get("policy_version"),
        }
    except Exception as e:
        lat = round((time.monotonic() - t0) * 1000, 1)
        res = {"ok": False, "status_code": None, "latency_ms": lat,
               "detail": f"{type(e).__name__}: {e}", "body": None,
               "duration_ms": req.duration_ms, "policy_version": None}
    add_work_event(drill_id, req.replica, res)
    return {"replica": req.replica, **res}


# ------------------------------------------------- 接单耗时策略两阶段发布


class PolicyPublishReq(BaseModel):
    expected_version: int = Field(ge=1)
    max_ms: int = Field(ge=LIMIT_MIN_MS, le=LIMIT_MAX_MS)


_policy_lock = asyncio.Lock()


async def _prepare_one(name: str, payload: dict) -> tuple[bool, str]:
    """对单副本 prepare; 遇到在途预案占位先清理一次再重试。返回 (ok, 错误描述)。"""
    url = f"{REPLICAS[name]}/admin/policy/prepare"
    for attempt in (1, 2):
        try:
            r = await http.post(url, json=payload, timeout=HTTP_TIMEOUT)
        except Exception as e:
            return False, f"{name} 不可达: {type(e).__name__}: {e}"
        if r.status_code == 200:
            return True, ""
        if r.status_code == 409:
            try:
                err = r.json().get("error")
            except Exception:
                err = "version_conflict"
            if err == "publish_in_progress" and attempt == 1:
                # 上次失败发布的遗留预案: 清理后重试一次(不清理他人正在用的同 publish_id,
                # 同 id 的情况副本端走幂等分支而不是该错误)
                blocking = r.json().get("blocking_publish_id")
                if blocking and blocking != payload["publish_id"]:
                    try:
                        await http.post(f"{REPLICAS[name]}/admin/policy/abort",
                                        json={"publish_id": blocking}, timeout=HTTP_TIMEOUT)
                    except Exception:
                        pass
                    continue
            return False, f"{name} 拒绝预案(409 {err}), 旧规则继续服务"
        return False, f"{name} prepare 失败: HTTP {r.status_code} {r.text[:120]}"
    return False, f"{name} prepare 重试后仍失败"


async def _commit_one(name: str, publish_id: str, version: int) -> tuple[bool, str]:
    """对单副本 commit, 有限重试; 409 中仅"同版已提交"视为幂等成功。"""
    url = f"{REPLICAS[name]}/admin/policy/commit"
    last = "未知错误"
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        try:
            r = await http.post(url, json={"publish_id": publish_id, "version": version},
                                timeout=HTTP_TIMEOUT)
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        else:
            if r.status_code == 200:
                return True, ""
            if r.status_code == 409:
                try:
                    body = r.json()
                except Exception:
                    body = {}
                # 迟到重发的确认且版本一致 -> 幂等成功; 其余 409 是实质冲突, 不重试
                if body.get("error") is None:
                    return True, ""
                return False, f"commit 409 {body.get('error')} (当前版 {body.get('committed_version')})"
            last = f"HTTP {r.status_code} {r.text[:100]}"
        if attempt < COMMIT_ATTEMPTS:
            await asyncio.sleep(0.2 * attempt)
    return False, last


@app.post("/api/policy/publish")
async def publish_policy(req: PolicyPublishReq):
    """两阶段发布接单耗时上限。全成才报成功; 任何环节失败都保证旧规则继续服务。"""
    async with _policy_lock:
        # 0. 回读两端实际已提交版, 校验"所见策略版"
        try:
            states = await asyncio.gather(*(fetch_replica_state(n) for n in REPLICAS))
        except Exception as e:
            raise HTTPException(502, f"读取副本状态失败, 已取消发布: {type(e).__name__}: {e}")
        actual = {name: st["committed_policy"]["version"] for name, st in zip(REPLICAS, states)}
        if any(v != req.expected_version for v in actual.values()):
            raise HTTPException(
                409,
                f"并发版冲突: 所见版 v{req.expected_version} 与两端实际版 {actual} 不一致, "
                "请刷新后按最新版重试",
            )
        if len(set(actual.values())) != 1:
            raise HTTPException(409, f"两端已提交版不一致 {actual}, 请先排查收敛后再发布")

        publish_id = uuid.uuid4().hex
        new_version = req.expected_version + 1
        create_publish(publish_id, req.expected_version, new_version, req.max_ms)
        payload = {"publish_id": publish_id, "expected_version": req.expected_version,
                   "version": new_version, "max_ms": req.max_ms}
        log.info("policy publish %s: v%s -> v%s, max_ms=%s",
                 publish_id, req.expected_version, new_version, req.max_ms)

        # 1. prepare 阶段: 全部成功才允许进入 commit
        prepared: list[str] = []
        for name in REPLICAS:
            ok, err = await _prepare_one(name, payload)
            if not ok:
                await abort_pending_everywhere(publish_id)
                update_publish(publish_id, "aborted", f"prepare 阶段失败: {err}")
                raise HTTPException(409 if "409" in err else 502,
                                    f"发布失败, 旧规则继续服务: {err}")
            prepared.append(name)
        update_publish(publish_id, "committing", "两副本预案均已确认, 等待提交")

        # 2. commit 阶段: 逐台有限重试
        commit_err: dict[str, str] = {}
        for name in REPLICAS:
            ok, err = await _commit_one(name, publish_id, new_version)
            if not ok:
                commit_err[name] = err

        # 3. 回读两端实际版本核对: 只有两端都确实是新版才算成功发布
        verified: dict[str, dict | None] = {}
        for name in REPLICAS:
            try:
                verified[name] = (await fetch_replica_state(name))["committed_policy"]
            except Exception as e:
                verified[name] = None
                commit_err.setdefault(name, f"回读状态失败: {type(e).__name__}: {e}")

        aligned = all(
            v is not None and v["version"] == new_version and v["max_ms"] == req.max_ms
            for v in verified.values()
        )
        if aligned and not commit_err:
            update_publish(publish_id, "committed",
                           f"两端已提交 v{new_version}, 上限 {req.max_ms}ms")
            log.info("policy publish %s committed on both replicas", publish_id)
            return {"ok": True, "publish_id": publish_id, "version": new_version,
                    "max_ms": req.max_ms, "replicas": verified}

        # 部分生效/无法确认: 撤销仍挂起的预案(已提交者 abort 为 no-op), 判失败
        await abort_pending_everywhere(publish_id)
        detail = "; ".join(f"{n}: {e}" for n, e in commit_err.items()) or "两端版本核对未通过"
        update_publish(publish_id, "failed",
                       f"提交阶段未完成双端确认: {detail}; 回读 {verified}")
        log.error("policy publish %s failed: %s; verified=%s", publish_id, detail, verified)
        raise HTTPException(
            502,
            f"发布未能在两端同时生效, 未当作成功: {detail}。"
            f"回读到的实际版: {verified}; 请按实际版收敛后重试",
        )


@app.get("/api/policy/publishes")
async def get_publishes():
    return {"publishes": list_publishes()}


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
        "publishes": list_publishes(),
    }


@app.get("/api/drills")
async def list_drills():
    with _db_lock:
        rows = _conn.execute("SELECT * FROM drills ORDER BY id DESC LIMIT 50").fetchall()
    return {"drills": [dict(r) for r in rows]}
