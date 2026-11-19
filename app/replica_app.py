"""双副本服务中的单个副本。

接口:
- GET  /health               健康检查(故障注入或手动宕机时返回 503), 同时回报当前已提交策略版
- POST /work                 接单接口; 可选 {"duration_ms": N} 指定模拟耗时,
                             不带参仍按 50ms 处理; 耗时超过当前策略上限返回 429;
                             故障注入或手动宕机时返回 503
- POST /admin/fault/inject   {"token": "..."} 注入故障, /health 与 /work 开始返回 503
- POST /admin/fault/revoke   {"token": "..."} 撤销故障; 令牌不匹配返回 409,
                             保证旧轮次迟到的撤销无法解除新轮次的故障
- POST /admin/override       {"down": true|false} 手动切换健康状态
- POST /admin/reset          运维逃生口: 清除全部故障状态(不影响已发布的接单策略)
- 接单耗时策略两阶段发布(由演练控制台驱动):
  POST /admin/policy/prepare {"publish_id","expected_version","version","max_ms"}
  POST /admin/policy/commit  {"publish_id","version"}
  POST /admin/policy/abort   {"publish_id"}
- GET  /state                查看内部状态(含已提交策略与待提交预案)

一致性要点:
- 只有 commit 会改写"已提交策略"并持久化; prepare 只在内存中登记预案,
  进程重启后预案消失, 未完成的发布不可能因重启而生效;
- commit/abort 按 publish_id 与 version 双重核对, 迟到或张冠李戴的确认返回 409,
  已提交版只会被对应预案的首次 commit 推进, 迟到确认不得覆盖新版;
- /work 在入口处快照当前已提交策略并据此裁决, 处理期间发布的新规则不影响已受理请求。
"""
import asyncio
import json
import os
import tempfile
import time
import uuid
from dataclasses import dataclass

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

REPLICA_NAME = os.environ.get("REPLICA_NAME", "replica")
POLICY_FILE = os.environ.get(
    "POLICY_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", f"policy-{REPLICA_NAME}.json"),
)
DEFAULT_WORK_MS = 50          # 无参 /work 的模拟处理耗时
DEFAULT_LIMIT_MS = 500        # 初始策略版(v1)的接单耗时上限
MIN_LIMIT_MS = 50
MAX_LIMIT_MS = 500
MAX_DURATION_MS = 5000

app = FastAPI(title=f"replica-{REPLICA_NAME}")

_lock = asyncio.Lock()
_fault_token: str | None = None
_manual_down = False


@dataclass(frozen=True)
class Policy:
    version: int
    max_ms: int


@dataclass(frozen=True)
class PendingPolicy:
    publish_id: str
    expected_version: int
    version: int
    max_ms: int
    created_at: float


# 已提交(生效中)策略。启动时从磁盘恢复, 始终只有 commit 能改写它。
_committed: Policy = Policy(version=1, max_ms=DEFAULT_LIMIT_MS)
# publish_id -> 待提交预案, 仅存内存: 进程重启即清空, 未完成发布不会因重启生效。
_pending: dict[str, PendingPolicy] = {}


# ---------------------------------------------------------------- 持久化


def _load_committed() -> Policy:
    try:
        with open(POLICY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        version = int(data["version"])
        max_ms = int(data["max_ms"])
        if version >= 1 and MIN_LIMIT_MS <= max_ms <= MAX_LIMIT_MS:
            return Policy(version=version, max_ms=max_ms)
    except FileNotFoundError:
        pass
    except Exception:
        # 文件损坏时回退默认策略, 不让副本无法启动
        pass
    return Policy(version=1, max_ms=DEFAULT_LIMIT_MS)


def _persist_committed(policy: Policy) -> None:
    """同目录临时文件 + 原子 rename, 避免半写文件。"""
    directory = os.path.dirname(POLICY_FILE) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".policy-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"version": policy.version, "max_ms": policy.max_ms,
                       "replica": REPLICA_NAME, "updated_at": time.time()}, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, POLICY_FILE)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


_committed = _load_committed()


# ---------------------------------------------------------------- 基础状态


def _unavailable() -> bool:
    return _fault_token is not None or _manual_down


def _health_body() -> dict:
    return {
        "name": REPLICA_NAME,
        "status": "unavailable" if _unavailable() else "ok",
        "fault_injected": _fault_token is not None,
        "manual_down": _manual_down,
        "policy_version": _committed.version,
        "policy_max_ms": _committed.max_ms,
        "ts": time.time(),
    }


@app.get("/health")
async def health():
    body = _health_body()
    if _unavailable():
        return JSONResponse(body, status_code=503)
    return body


# ---------------------------------------------------------------- /work


class WorkReq(BaseModel):
    duration_ms: int | None = Field(default=None, ge=1, le=MAX_DURATION_MS)


@app.post("/work")
async def work(req: WorkReq | None = None):
    # 故障优先: 演练注入的 503 语义保持不变
    if _unavailable():
        return JSONResponse(
            {"error": "replica unavailable", "replica": REPLICA_NAME,
             "policy_version": _committed.version},
            status_code=503,
        )
    # 入口处快照入站时的已提交规则, 处理期间的发布不影响本请求
    policy = _committed
    duration_ms = req.duration_ms if req and req.duration_ms is not None else DEFAULT_WORK_MS
    if duration_ms > policy.max_ms:
        return JSONResponse(
            {
                "error": "duration_exceeds_limit",
                "replica": REPLICA_NAME,
                "requested_ms": duration_ms,
                "limit_ms": policy.max_ms,
                "policy_version": policy.version,
                "ts": time.time(),
            },
            status_code=429,
        )
    await asyncio.sleep(duration_ms / 1000.0)  # 模拟接单处理耗时
    return {
        "order_id": uuid.uuid4().hex[:12],
        "replica": REPLICA_NAME,
        "accepted_at": time.time(),
        "duration_ms": duration_ms,
        "limit_ms": policy.max_ms,
        "policy_version": policy.version,
    }


# ---------------------------------------------------------------- 故障管理


class TokenReq(BaseModel):
    token: str


@app.post("/admin/fault/inject")
async def fault_inject(req: TokenReq):
    global _fault_token
    async with _lock:
        _fault_token = req.token
    return {"injected": True, "replica": REPLICA_NAME}


@app.post("/admin/fault/revoke")
async def fault_revoke(req: TokenReq):
    global _fault_token
    async with _lock:
        if _fault_token is None:
            return {"revoked": True, "was_active": False, "replica": REPLICA_NAME}
        if _fault_token != req.token:
            return JSONResponse(
                {"error": "token_mismatch", "replica": REPLICA_NAME},
                status_code=409,
            )
        _fault_token = None
    return {"revoked": True, "was_active": True, "replica": REPLICA_NAME}


class OverrideReq(BaseModel):
    down: bool


@app.post("/admin/override")
async def set_override(req: OverrideReq):
    global _manual_down
    async with _lock:
        _manual_down = req.down
    return {"manual_down": _manual_down, "replica": REPLICA_NAME}


@app.post("/admin/reset")
async def reset():
    """运维逃生口: 只清故障状态; 已发布的接单策略不受影响。"""
    global _fault_token, _manual_down
    async with _lock:
        _fault_token = None
        _manual_down = False
    return {"reset": True, "replica": REPLICA_NAME,
            "policy_version": _committed.version, "policy_max_ms": _committed.max_ms}


# ---------------------------------------------------------------- 策略两阶段发布


class PrepareReq(BaseModel):
    publish_id: str
    expected_version: int = Field(ge=1)
    version: int = Field(ge=2)
    max_ms: int = Field(ge=MIN_LIMIT_MS, le=MAX_LIMIT_MS)


class CommitReq(BaseModel):
    publish_id: str
    version: int = Field(ge=2)


class PublishIdReq(BaseModel):
    publish_id: str


@app.post("/admin/policy/prepare")
async def policy_prepare(req: PrepareReq):
    async with _lock:
        # 同一发布的重试: 内容一致则幂等返回, 内容打架则冲突
        existing = _pending.get(req.publish_id)
        if existing is not None:
            if (existing.version == req.version and existing.max_ms == req.max_ms
                    and existing.expected_version == req.expected_version):
                return {"prepared": True, "idempotent": True, "publish_id": req.publish_id,
                        "version": req.version, "max_ms": req.max_ms,
                        "committed_version": _committed.version, "replica": REPLICA_NAME}
            return JSONResponse(
                {"error": "publish_id_conflict", "replica": REPLICA_NAME,
                 "committed_version": _committed.version}, status_code=409)
        # 版本乐观锁: 所见版与本端已提交版不符(并发版冲突) -> 拒绝, 旧规则继续服务
        if _committed.version != req.expected_version:
            return JSONResponse(
                {"error": "version_conflict", "replica": REPLICA_NAME,
                 "expected_version": req.expected_version,
                 "committed_version": _committed.version}, status_code=409)
        if req.version != req.expected_version + 1:
            return JSONResponse(
                {"error": "bad_version_sequence", "replica": REPLICA_NAME,
                 "committed_version": _committed.version}, status_code=422)
        # 已有别的发布占着预案位: 同一时刻只允许一个在途发布
        if _pending:
            other = next(iter(_pending.values()))
            return JSONResponse(
                {"error": "publish_in_progress", "replica": REPLICA_NAME,
                 "blocking_publish_id": other.publish_id,
                 "committed_version": _committed.version}, status_code=409)
        _pending[req.publish_id] = PendingPolicy(
            publish_id=req.publish_id, expected_version=req.expected_version,
            version=req.version, max_ms=req.max_ms, created_at=time.time())
    return {"prepared": True, "publish_id": req.publish_id,
            "version": req.version, "max_ms": req.max_ms,
            "committed_version": req.expected_version, "replica": REPLICA_NAME}


@app.post("/admin/policy/commit")
async def policy_commit(req: CommitReq):
    global _committed
    async with _lock:
        pending = _pending.get(req.publish_id)
        if pending is None:
            # 幂等: 该发布已提交过(迟到重发的确认), 且版本对得上 -> 成功但不改写
            if _committed.version == req.version:
                return {"committed": True, "idempotent": True, "replica": REPLICA_NAME,
                        "version": _committed.version, "max_ms": _committed.max_ms}
            # 预案不存在(重启丢失/从未 prepare/已 abort) -> 拒绝, 不得凭空改已提交版
            return JSONResponse(
                {"error": "no_such_pending", "replica": REPLICA_NAME,
                 "committed_version": _committed.version}, status_code=409)
        if pending.version != req.version:
            return JSONResponse(
                {"error": "version_mismatch", "replica": REPLICA_NAME,
                 "committed_version": _committed.version}, status_code=409)
        # 唯一的生效路径: 预案 -> 已提交, 并立即持久化
        new_policy = Policy(version=pending.version, max_ms=pending.max_ms)
        _persist_committed(new_policy)
        _committed = new_policy
        del _pending[req.publish_id]
    return {"committed": True, "replica": REPLICA_NAME,
            "version": new_policy.version, "max_ms": new_policy.max_ms}


@app.post("/admin/policy/abort")
async def policy_abort(req: PublishIdReq):
    """撤销预案。已提交的策略永不回滚; 对未知/已提交 publish_id 为幂等 no-op。"""
    async with _lock:
        pending = _pending.pop(req.publish_id, None)
    return {"aborted": pending is not None, "publish_id": req.publish_id,
            "committed_version": _committed.version, "replica": REPLICA_NAME}


@app.get("/state")
async def state():
    return {
        "name": REPLICA_NAME,
        "fault_injected": _fault_token is not None,
        "manual_down": _manual_down,
        "committed_policy": {"version": _committed.version, "max_ms": _committed.max_ms},
        "pending": [
            {"publish_id": p.publish_id, "expected_version": p.expected_version,
             "version": p.version, "max_ms": p.max_ms}
            for p in _pending.values()
        ],
    }
