# 双副本故障演练平台

值班员用于验证"双副本服务在单副本故障时仍可接单"的故障演练系统,
并支持向双副本**两阶段发布"接单耗时上限"策略**。

- **`app/replica_app.py`** — 副本服务(`GET /health`、`POST /work`,故障管理与策略两阶段管理接口)
- **`app/drill_app.py`** — 演练控制台(FastAPI + SQLite),提供演练网页、策略发布协调与 API
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

SQLite 数据库持久化在 `drill-data` 卷,两个副本的已提交策略分别持久化在
`replica1-data`、`replica2-data` 卷;重启控制台/副本后状态可恢复。

### 本地运行

```bash
pip install -r requirements.txt
cd app
REPLICA_NAME=replica1 uvicorn replica_app:app --port 8001 &
REPLICA_NAME=replica2 uvicorn replica_app:app --port 8002 &
uvicorn drill_app:app --port 8000 &
```

## 接单耗时策略发布(两阶段提交)

网页填写**新上限(50~500 毫秒)**与**所见策略版**,向两副本发布新的接单耗时规则;
也可在网页向任一副本发送指定耗时的 `/work` 试单,查看接单结果与裁决所用策略版。

### 规则语义

- 副本初始策略为 **v1 / 500ms**。`POST /work` 不带参数时仍按 **50ms** 模拟耗时处理;
  带 `{"duration_ms": N}` 时,`N` 大于当前已提交上限返回 **429**(等于上限仍受理)。
- 响应体带 `policy_version`、`limit_ms`,可核对每个请求实际按哪一版规则裁决。
- 请求在入口处**快照入站时的已提交策略**,处理期间发生的发布不影响已受理请求——
  已受理请求按入站时的规则完成。

### 两阶段提交流程

```
控制台                         replica1 / replica2
  │  回读 /state 校验"所见版"与两端实际版一致
  │── prepare(publish_id, expected_version, version=expected+1, max_ms) ──▶ 登记内存预案
  │        任一台 409(版本冲突/在途占位)或不可达 ──▶ 全部 abort, 发布失败, 旧规则继续
  │◀──────────────────────── 两台均 200 ────────────────────────────────
  │── commit(publish_id, version) ──▶ 预案转已提交并 fsync 持久化(有限重试)
  │        收尾回读两台 /state 核对版本/上限完全一致
  └── 全部一致才报 committed; 否则 abort 残留预案并判 failed, 绝不把部分生效当成功
```

- **两端确认后新规则才对新请求生效**:只有 `commit` 会改写副本的已提交策略。
- **任一环节失败旧规则继续服务**:副本不可达、写入失败、并发版冲突均中止发布并回滚预案。
- **迟到确认不得覆盖新版**:副本按 `(publish_id, version)` 核对 commit;
  无预案、版本不符返回 409;同发布同版的迟到重发为幂等 no-op,不会再推进版本。
- **成功发布后两端不会出现不同已提交版**:控制台收尾回读双端实际版本,
  不一致即判失败并在记录中明示,网页同时高亮版本分叉。
- **重启不使未完成发布生效**:预案只存副本内存,副本重启即消失;
  控制台重启把在途发布标记 `interrupted` 并尽力 abort,**绝不自动提交**;
  已提交策略落盘,副本重启后仍然生效。
- 版本号严格递增(`expected_version + 1`),乐观锁防止并发发布互相覆盖。

### 发布记录状态

`preparing`(预案下发中)/ `committing`(两台预案齐备,提交中)/
`committed`(两端已生效)/ `aborted`(prepare 阶段回滚)/
`failed`(commit/核对未完成双端确认)/ `interrupted`(控制台重启中断,未自动提交)。

## 故障演练流程(原有能力保持可用)

1. 网页选择目标副本与故障时长(5~30 秒),点击"开始演练"。
   启动前控制台会检查**两端都须健康**,任一不健康则拒绝(409)。
2. 控制台向目标副本注入故障:其 `/health`、`/work` 持续返回 503。
3. 演练期间控制台每秒持续探测**另一副本**并记录;网页可:
   - 切换另一副本的健康状态(模拟另一副本也失效);
   - 向任一副本发送真实 `/work` 请求(可带指定耗时)并查看结果;
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
- **策略发布串行化**:控制台 `asyncio` 锁 + 副本端单一预案位 + 版本乐观锁三重防护;
  上次失败发布的遗留预案由控制台在新发布 prepare 时自动清理后重试一次。

## API 一览

### 演练控制台(8000)

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 演练网页 |
| GET | `/api/state` | 副本实时状态(含策略版)、当前/最近轮次、探测与请求记录、历史、发布记录 |
| POST | `/api/drills` | 开始演练 `{"target":"replica1","duration":10}`(duration 5~30) |
| GET | `/api/drills` | 历史轮次 |
| POST | `/api/replicas/{name}/override` | 切换副本健康状态 `{"down":true\|false}`(演练中仅限另一副本) |
| POST | `/api/work` | 真实试单 `{"replica":"replica1","duration_ms":120}`,`duration_ms` 可省(默认 50) |
| POST | `/api/policy/publish` | 两阶段发布 `{"expected_version":1,"max_ms":120}`(max_ms 50~500) |
| GET | `/api/policy/publishes` | 策略发布历史 |

### 副本(8001/8002)

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查(故障时 503),回报 `policy_version`/`policy_max_ms` |
| POST | `/work` | 接单;可选 `{"duration_ms":N}`,超当前策略上限 429,故障时 503 |
| POST | `/admin/fault/inject` | 注入故障 `{"token":"..."}` |
| POST | `/admin/fault/revoke` | 撤销故障,令牌不匹配返回 409 |
| POST | `/admin/override` | 手动宕机/恢复 |
| POST | `/admin/reset` | 运维逃生口:清除故障状态(不影响已发布策略) |
| POST | `/admin/policy/prepare` | 两阶段-预案 `{"publish_id","expected_version","version","max_ms"}` |
| POST | `/admin/policy/commit` | 两阶段-提交 `{"publish_id","version"}` |
| POST | `/admin/policy/abort` | 两阶段-撤销预案 `{"publish_id"}`(已提交发布为幂等 no-op) |
| GET | `/state` | 内部状态(已提交策略 + 在途预案) |

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `REPLICA_NAME` | `replica` | 副本名称(副本服务) |
| `POLICY_FILE` | `app/data/policy-<名称>.json` | 已提交策略持久化文件(副本服务,原子 rename 写入) |
| `REPLICA1_URL` / `REPLICA2_URL` | `http://127.0.0.1:8001/8002` | 副本地址(控制台) |
| `DB_PATH` | `app/drill.db` | SQLite 路径(控制台) |
| `PROBE_INTERVAL` | `1.0` | 探测/监督周期(秒) |
| `HTTP_TIMEOUT` | `2.0` | 调用副本的超时(秒,长耗时试单自动放宽) |
| `COMMIT_ATTEMPTS` | `3` | 单副本 commit 的最大尝试次数 |

## 测试

```bash
python3 -I tests/integration_test.py
```

脚本自行拉起三个 uvicorn 进程并覆盖:默认策略与无参 `/work`、429 裁决、
两阶段发布成功/版本冲突/不可达/写入失败回滚、迟到确认防护、副本与控制台重启语义、
在途请求按入站规则完成,以及原故障演练全流程回归。
