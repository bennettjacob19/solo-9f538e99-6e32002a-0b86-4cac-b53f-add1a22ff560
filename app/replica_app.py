"""双副本服务中的单个副本。

接口:
- GET  /health               健康检查(故障注入或手动宕机时返回 503)
- POST /work                 接单接口(故障注入或手动宕机时返回 503)
- POST /admin/fault/inject   {"token": "..."} 注入故障, /health 与 /work 开始返回 503
- POST /admin/fault/revoke   {"token": "..."} 撤销故障; 令牌不匹配返回 409,
                             保证旧轮次迟到的撤销无法解除新轮次的故障
- POST /admin/override       {"down": true|false} 手动切换健康状态(演练网页用于模拟另一副本失效)
- POST /admin/reset          运维逃生口: 清除全部故障状态
- GET  /state                查看内部状态
"""
import asyncio
import os
import time
import uuid

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

REPLICA_NAME = os.environ.get("REPLICA_NAME", "replica")

app = FastAPI(title=f"replica-{REPLICA_NAME}")

_lock = asyncio.Lock()
_fault_token: str | None = None
_manual_down = False


def _unavailable() -> bool:
    return _fault_token is not None or _manual_down


def _health_body() -> dict:
    return {
        "name": REPLICA_NAME,
        "status": "unavailable" if _unavailable() else "ok",
        "fault_injected": _fault_token is not None,
        "manual_down": _manual_down,
        "ts": time.time(),
    }


@app.get("/health")
async def health():
    body = _health_body()
    if _unavailable():
        return JSONResponse(body, status_code=503)
    return body


@app.post("/work")
async def work():
    if _unavailable():
        return JSONResponse(
            {"error": "replica unavailable", "replica": REPLICA_NAME},
            status_code=503,
        )
    await asyncio.sleep(0.05)  # 模拟接单处理耗时
    return {
        "order_id": uuid.uuid4().hex[:12],
        "replica": REPLICA_NAME,
        "accepted_at": time.time(),
    }


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
    global _fault_token, _manual_down
    async with _lock:
        _fault_token = None
        _manual_down = False
    return {"reset": True, "replica": REPLICA_NAME}


@app.get("/state")
async def state():
    return {
        "name": REPLICA_NAME,
        "fault_injected": _fault_token is not None,
        "manual_down": _manual_down,
    }
