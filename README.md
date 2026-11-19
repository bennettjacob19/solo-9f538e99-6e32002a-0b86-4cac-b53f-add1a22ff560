# 双副本故障演练平台

值班员用于验证"双副本服务在单副本故障时仍可接单"的故障演练系统。

- **`app/replica_app.py`** — 副本服务(`GET /health`、`POST /work`,以及受令牌保护的管理接口)
- **`app/drill_app.py`** — 演练控制台(FastAPI + SQLite),提供演练网页与 API
- **`app/static/index.html`** — 演练网页(单页,每秒轮询刷新)

## 启动

### Docker Compose(推荐)

```bash
docker compose up --build
```

| 服务 | 地址 |
|---|---|
| 演练网页 | http://localhost:8000 |
| replica1 | http://localhost:8001 |
| replica2 | http://localhost:8002 |

SQLite 数据库持久化在 `drill-data` 卷中,重启控制台后可恢复遗留故障。

### 本地运行

```bash
pip install -r requirements.txt
cd app
REPLICA_NAME=replica1 uvicorn replica_app:app --port 8001 &
REPLICA_NAME=replica2 uvicorn replica_app:app --port 8002 &
uvicorn drill_app:app --port 8000 &
```

## 演练流程

1. 网页选择目标副本与故障时长(5~30 秒),点击"开始演练"。
   启动前控制台会检查**两端都须健康**,任一不健康则拒绝(409)。
2. 控制台向目标副本注入故障:其 `/health`、`/work` 持续返回 503。
3. 演练期间控制台每秒持续探测**另一副本**并记录;网页可:
   - 切换另一副本的健康状态(模拟另一副本也失效);
   - 向任一副本发送真实 `/work` 请求并查看结果;
   - 实时查看本轮可用性结论与终止原因。
4. 终止条件(先到先触发):
   - **期限到达** → 结论"可用"(单副本故障期间服务保持可用);
   - **另一副本失效** → 立即终止,结论"不可用"。
5. 终止后撤销注入。**只有受注入副本恢复健康,本轮才显示"已结束"**;
   撤销失败显示"待恢复"并持续重试,期间不接受新演练。
6. 控制台重启后,先把遗留故障恢复干净(轮次标记"中断"),再接受新演练。

## 轮次状态机

```
active ──期限到达/另一副本失效──▶ recovering ──撤销成功且副本恢复健康──▶ finished(已结束)
                                  │  ▲
                                  └──撤销失败──▶ pending_recovery(待恢复,持续重试)
```

终止原因:`duration_reached`(期限到达)/ `peer_failed`(另一副本失效)/ `restart_interrupted`(服务重启)。
可用性结论:`available` / `unavailable` / `interrupted`。

## 并发与迟到保护

- **并发启动只允许一轮生效**:`asyncio` 锁串行化启动流程,SQLite 唯一部分索引
  (`WHERE status != 'finished'`)在数据库层兜底,败者收到 409 并立即撤销已注入的故障。
- **旧轮迟到的探测/撤销不能影响新轮**:
  - 探测、终止、恢复全部由唯一的监督循环按数据库当前状态执行,不存在随轮次游离的旧任务;
  - 状态迁移 SQL 均按 `(id, status)` 守卫(`UPDATE ... WHERE id=? AND status='active'`);
  - 每轮故障携带独立令牌,副本拒绝令牌不匹配的撤销(409),旧轮迟到的撤销无法解除新轮故障。

## API 一览

### 演练控制台(8000)

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 演练网页 |
| GET | `/api/state` | 副本实时状态、当前/最近轮次、探测与请求记录、历史 |
| POST | `/api/drills` | 开始演练 `{"target":"replica1","duration":10}`(duration 5~30) |
| GET | `/api/drills` | 历史轮次 |
| POST | `/api/replicas/{name}/override` | 切换副本健康状态 `{"down":true|false}`(演练中仅限另一副本) |
| POST | `/api/work` | 发送真实 /work 请求 `{"replica":"replica1"}` |

### 副本(8001/8002)

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查(故障时 503) |
| POST | `/work` | 接单(故障时 503) |
| POST | `/admin/fault/inject` | 注入故障 `{"token":"..."}` |
| POST | `/admin/fault/revoke` | 撤销故障,令牌不匹配返回 409 |
| POST | `/admin/override` | 手动宕机/恢复 |
| POST | `/admin/reset` | 运维逃生口:清除全部故障状态 |
| GET | `/state` | 内部状态 |

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `REPLICA_NAME` | `replica` | 副本名称(副本服务) |
| `REPLICA1_URL` / `REPLICA2_URL` | `http://127.0.0.1:8001/8002` | 副本地址(控制台) |
| `DB_PATH` | `app/drill.db` | SQLite 路径(控制台) |
| `PROBE_INTERVAL` | `1.0` | 探测/监督周期(秒) |
| `HTTP_TIMEOUT` | `2.0` | 调用副本的超时(秒) |
