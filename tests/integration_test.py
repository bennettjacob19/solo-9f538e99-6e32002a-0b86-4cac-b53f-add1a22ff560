"""端到端验证: 双副本接单耗时策略发布 + 原故障演练回归。

自行拉起 2 个 replica_app + 1 个 drill_app(uvicorn 子进程), 用标准库 http.client
驱动全部场景。运行: python3 -I tests/integration_test.py
"""
import http.client
import json
import os
import shutil
import signal
import subprocess
import sys
import time

# -I 模式下手动引入用户站点依赖
sys.path.insert(0, "/home/node/.local/lib/python3.11/site-packages")

APP_DIR = "/tmp/drilltest/app"
RUN_DIR = "/tmp/drillrun"
PORTS = {"replica1": 8101, "replica2": 8102, "drill": 8100}
PY = sys.executable

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def req(port, method, path, body=None, timeout=30):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    data = json.dumps(body) if body is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    try:
        conn.request(method, path, body=data, headers=headers)
        r = conn.getresponse()
        raw = r.read().decode()
        try:
            parsed = json.loads(raw) if raw else None
        except Exception:
            parsed = raw
        return r.status, parsed
    finally:
        conn.close()


def wait_up(port, path="/health", expect=(200, 503), tries=60):
    for _ in range(tries):
        try:
            st, _ = req(port, "GET", path, timeout=3)
            if st in expect:
                return True
        except OSError:
            pass
        time.sleep(0.5)
    return False


procs = []


def start(name, env_extra, module, port):
    env = dict(os.environ)
    env.update(env_extra)
    log = open(f"{RUN_DIR}/{name}.log", "w")
    p = subprocess.Popen(
        [PY, "-m", "uvicorn", f"{module}:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=APP_DIR, env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    procs.append((name, p))
    assert wait_up(port if module == "replica_app" else port,
                   "/health" if module == "replica_app" else "/api/state",
                   expect=(200, 503) if module == "replica_app" else (200,)), f"{name} 启动超时"
    return p


def stop(name):
    for i, (n, p) in enumerate(procs):
        if n == name:
            p.send_signal(signal.SIGTERM)
            p.wait(timeout=10)
            procs.pop(i)
            return


def main():
    shutil.rmtree(RUN_DIR, ignore_errors=True)
    os.makedirs(f"{RUN_DIR}/r1", exist_ok=True)
    os.makedirs(f"{RUN_DIR}/r2", exist_ok=True)

    print("== 启动三服务 ==")
    start("replica1", {"REPLICA_NAME": "replica1",
                       "POLICY_FILE": f"{RUN_DIR}/r1/policy.json"}, "replica_app", PORTS["replica1"])
    start("replica2", {"REPLICA_NAME": "replica2",
                       "POLICY_FILE": f"{RUN_DIR}/r2/policy.json"}, "replica_app", PORTS["replica2"])
    start("drill", {"REPLICA1_URL": f"http://127.0.0.1:{PORTS['replica1']}",
                    "REPLICA2_URL": f"http://127.0.0.1:{PORTS['replica2']}",
                    "DB_PATH": f"{RUN_DIR}/drill.db",
                    "PROBE_INTERVAL": "0.3"}, "drill_app", PORTS["drill"])
    time.sleep(1.0)

    r1, r2, dc = PORTS["replica1"], PORTS["replica2"], PORTS["drill"]

    print("== 1. 初始默认策略 v1 / 上限 500ms ==")
    for port in (r1, r2):
        st, b = req(port, "GET", "/state")
        check(f"{port} 初始已提交版 v1", st == 200 and b["committed_policy"] == {"version": 1, "max_ms": 500}, b)
        check(f"{port} 初始无待提交预案", b["pending"] == [], b)
        st, b = req(port, "GET", "/health")
        check(f"{port} health 回报策略版", st == 200 and b["policy_version"] == 1 and b["policy_max_ms"] == 500, b)

    print("== 2. 无参 /work 按 50ms 处理 ==")
    t0 = time.monotonic()
    st, b = req(r1, "POST", "/work")
    elapsed = (time.monotonic() - t0) * 1000
    check("无参 /work 200", st == 200 and b["duration_ms"] == 50 and b["policy_version"] == 1, b)
    check("无参 /work 实际耗时≈50ms", 45 <= elapsed <= 300, f"{elapsed:.0f}ms")

    print("== 3. 试单 429 与入参校验 ==")
    st, b = req(r1, "POST", "/work", {"duration_ms": 500})
    check("等于上限 500ms 受理", st == 200 and b["duration_ms"] == 500, b)
    st, b = req(r1, "POST", "/work", {"duration_ms": 501})
    check("超过上限 501ms 返回 429", st == 429 and b["error"] == "duration_exceeds_limit"
          and b["limit_ms"] == 500 and b["policy_version"] == 1, b)
    st, b = req(dc, "POST", "/api/work", {"replica": "replica2", "duration_ms": 700})
    # 控制台 /api/work 以 200 包装试单结果, 副本状态码在 body.status_code(沿用演练既有设计)
    check("控制台试单透传 429", st == 200 and b["status_code"] == 429 and b["body"]["limit_ms"] == 500, b)
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 1, "max_ms": 49})
    check("上限 49 被控制台拒绝", st == 422, b)
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 1, "max_ms": 501})
    check("上限 501 被控制台拒绝", st == 422, b)
    st, b = req(r1, "POST", "/work", {"duration_ms": 99999})
    check("副本侧耗时超界 422", st == 422, b)

    print("== 4. 正常两阶段发布 v1 -> v2(120ms) ==")
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 1, "max_ms": 120})
    check("发布成功", st == 200 and b["ok"] and b["version"] == 2 and b["max_ms"] == 120, b)
    for port in (r1, r2):
        st, b = req(port, "GET", "/state")
        check(f"{port} 已提交 v2/120 且无遗留预案",
              b["committed_policy"] == {"version": 2, "max_ms": 120} and b["pending"] == [], b)
    st, b = req(r2, "POST", "/work", {"duration_ms": 120})
    check("新规则: 120ms 受理", st == 200 and b["policy_version"] == 2, b)
    st, b = req(r2, "POST", "/work", {"duration_ms": 121})
    check("新规则: 121ms 返回 429", st == 429 and b["limit_ms"] == 120 and b["policy_version"] == 2, b)
    st, b = req(dc, "GET", "/api/policy/publishes")
    check("发布记录 committed", b["publishes"][0]["status"] == "committed", b["publishes"][0])

    print("== 5. 所见版过期 -> 并发版冲突, 旧规则继续 ==")
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 1, "max_ms": 200})
    check("过期 expected_version 被拒 409", st == 409 and "并发版冲突" in b["detail"], b)
    for port in (r1, r2):
        st, b = req(port, "GET", "/state")
        check(f"{port} 仍停留在 v2", b["committed_policy"]["version"] == 2 and b["pending"] == [], b)

    print("== 6. 副本端迟到/张冠李戴确认防护 ==")
    st, b = req(r1, "POST", "/admin/policy/commit", {"publish_id": "never-prepared", "version": 3})
    check("无预案且版本不同 -> 拒绝 409(不得凭空改写)", st == 409 and b["error"] == "no_such_pending", b)
    st, b = req(r1, "POST", "/admin/policy/commit", {"publish_id": "never-prepared", "version": 2})
    check("无预案但版本恰等于当前版 -> 幂等 no-op 且不改写(提交重试安全性所需)",
          st == 200 and b.get("idempotent") is True and b["version"] == 2, b)
    st, b = req(r1, "GET", "/state")
    check("版本仍为 v2 未被凭空推进", b["committed_policy"]["version"] == 2, b)
    # prepare 一个 v3 预案, 用错误 version commit
    st, b = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-x1", "expected_version": 2, "version": 3, "max_ms": 90})
    check("预案 prepare 成功", st == 200, b)
    st, b = req(r1, "POST", "/admin/policy/commit", {"publish_id": "p-x1", "version": 9})
    check("version 不匹配 commit 拒绝 409", st == 409 and b["error"] == "version_mismatch", b)
    st, b = req(r1, "POST", "/admin/policy/commit", {"publish_id": "p-x1", "version": 3})
    check("正确 commit 生效 v3/90", st == 200 and b["version"] == 3 and b["max_ms"] == 90, b)
    st, b = req(r1, "POST", "/admin/policy/commit", {"publish_id": "p-x1", "version": 3})
    check("迟到重发同版 commit 幂等, 不产生新版", st == 200 and b.get("idempotent") is True, b)
    st, b = req(r1, "GET", "/state")
    check("版本仍是 v3 未被覆盖", b["committed_policy"] == {"version": 3, "max_ms": 90}, b)
    # 把 r1 手动拉回与 r2 一致前, 先验证 abort 对已提交 publish 是 no-op
    st, b = req(r1, "POST", "/admin/policy/abort", {"publish_id": "p-x1"})
    check("abort 已提交发布为 no-op, 规则不变",
          st == 200 and b["aborted"] is False and b["committed_version"] == 3, b)
    # 收敛 r1 回 v2 不可能(版本只增), 改为把 r2 也升到 v3: 直接在 r2 prepare+commit
    st, b = req(r2, "POST", "/admin/policy/prepare",
                {"publish_id": "p-x2", "expected_version": 2, "version": 3, "max_ms": 90})
    st2, b2 = req(r2, "POST", "/admin/policy/commit", {"publish_id": "p-x2", "version": 3})
    check("手动收敛 r2 到 v3/90", st == 200 and st2 == 200 and b2["version"] == 3, b2)

    print("== 7. prepare 冲突与在途占位 ==")
    st, b = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-a", "expected_version": 3, "version": 4, "max_ms": 80})
    check("p-a prepare 成功", st == 200, b)
    st, b = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-b", "expected_version": 3, "version": 4, "max_ms": 70})
    check("并发第二个预案被拒 409 publish_in_progress", st == 409 and b["error"] == "publish_in_progress", b)
    st, b = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-a", "expected_version": 3, "version": 4, "max_ms": 81})
    check("同 publish_id 内容打架 409", st == 409 and b["error"] == "publish_id_conflict", b)
    st, b = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-a", "expected_version": 3, "version": 4, "max_ms": 80})
    check("同 publish_id 幂等重放", st == 200 and b.get("idempotent") is True, b)
    req(r1, "POST", "/admin/policy/abort", {"publish_id": "p-a"})
    st, b = req(r1, "GET", "/state")
    check("abort 后预案清空, 已提交版不变", b["pending"] == [] and b["committed_policy"]["version"] == 3, b)

    print("== 8. 端到端发布失败路径(旧规则继续, 预案回滚) ==")
    # 8a. 预读阶段副本不可达 -> 502, 不产生发布记录
    stop("replica2")
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 3, "max_ms": 150})
    check("预读阶段副本不可达 -> 502 取消", st == 502 and "读取副本状态失败" in b["detail"], b)
    st, b = req(r1, "GET", "/state")
    check("r1 旧规则继续(v3/90)且无遗留预案",
          b["committed_policy"] == {"version": 3, "max_ms": 90} and b["pending"] == [], b)

    # 8b. 预读通过但 prepare 阶段失败: 用假服务器顶替 r2(GET 正常, POST 一律连接失败)
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import threading as _th

    class FakeHandler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            payload = json.dumps({"name": "replica2",
                                  "committed_policy": {"version": 3, "max_ms": 90},
                                  "pending": []}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            # 模拟 prepare 写入失败: 直接断连
            self.close_connection = True
            try:
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
            except Exception:
                pass

    fake = HTTPServer(("127.0.0.1", r2), FakeHandler)
    fake_th = _th.Thread(target=fake.serve_forever, daemon=True)
    fake_th.start()
    time.sleep(0.3)
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 3, "max_ms": 150})
    check("prepare 阶段写入失败 -> 发布失败, 旧规则继续",
          st in (409, 502) and "旧规则继续" in b["detail"], b)
    st, b = req(r1, "GET", "/state")
    check("r1 预案已回滚, 旧规则 v3/90 继续",
          b["committed_policy"] == {"version": 3, "max_ms": 90} and b["pending"] == [], b)
    st, b = req(dc, "GET", "/api/policy/publishes")
    check("失败发布落库为 aborted", b["publishes"][0]["status"] == "aborted", b["publishes"][0])
    fake.shutdown()
    fake.server_close()
    time.sleep(0.3)

    # 8c. 预读通过但版本冲突: r2 已是 v4 时仍按所见 v3 发布 -> 409
    start("replica2", {"REPLICA_NAME": "replica2",
                       "POLICY_FILE": f"{RUN_DIR}/r2/policy.json"}, "replica_app", r2)
    req(r2, "POST", "/admin/policy/prepare",
        {"publish_id": "p-r2v4", "expected_version": 3, "version": 4, "max_ms": 95})
    req(r2, "POST", "/admin/policy/commit", {"publish_id": "p-r2v4", "version": 4})
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 3, "max_ms": 150})
    check("并发版冲突(两端实际版不一致) -> 409", st == 409 and "并发版冲突" in b["detail"], b)
    st, b = req(r1, "GET", "/state")
    check("冲突后 r1 仍 v3 且无预案", b["committed_policy"]["version"] == 3 and b["pending"] == [], b)
    # 把 r1 追平到 v4/95 以便后续场景两端同版
    req(r1, "POST", "/admin/policy/prepare",
        {"publish_id": "p-r1v4", "expected_version": 3, "version": 4, "max_ms": 95})
    req(r1, "POST", "/admin/policy/commit", {"publish_id": "p-r1v4", "version": 4})

    print("== 9. 副本重启: 已提交规则持久化, 预案不复活 ==")
    # 先在 r1 留一个未 commit 的预案
    st, b = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-ghost", "expected_version": 4, "version": 5, "max_ms": 66})
    check("重启前预案存在", st == 200, b)
    stop("replica1")
    start("replica1", {"REPLICA_NAME": "replica1",
                       "POLICY_FILE": f"{RUN_DIR}/r1/policy.json"}, "replica_app", r1)
    st, b = req(r1, "GET", "/state")
    check("r1 重启后已提交规则仍为 v4/95(持久化)",
          b["committed_policy"] == {"version": 4, "max_ms": 95}, b)
    check("r1 重启后待提交预案消失(未完成发布不生效)", b["pending"] == [], b)
    st, b = req(r1, "POST", "/admin/policy/commit", {"publish_id": "p-ghost", "version": 5})
    check("预案消失后迟到 commit 被拒", st == 409 and b["error"] == "no_such_pending", b)
    st, b = req(r2, "GET", "/state")
    check("r2 仍为 v4/95", b["committed_policy"] == {"version": 4, "max_ms": 95}, b)

    print("== 10. 重启后继续正常发布 v4 -> v5(200ms), 两端同版 ==")
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 4, "max_ms": 200})
    check("重启后发布成功 v5", st == 200 and b["version"] == 5, b)
    for port in (r1, r2):
        st, b = req(port, "GET", "/state")
        check(f"{port} 同版 v5/200", b["committed_policy"] == {"version": 5, "max_ms": 200}, b)
    st, b = req(r1, "POST", "/work", {"duration_ms": 201})
    check("v5 下 201ms 被 429", st == 429 and b["policy_version"] == 5, b)

    print("== 11. 已受理请求按入站时规则完成(发布不影响在途请求) ==")
    # 发一个 180ms 的请求(在 v5/200 下合法), 处理期间发布更严的 v6/100
    import threading
    outcome = {}

    def slow_call():
        outcome["status"], outcome["body"] = req(r1, "POST", "/work", {"duration_ms": 180}, timeout=10)

    t = threading.Thread(target=slow_call)
    t.start()
    time.sleep(0.05)
    st, b = req(dc, "POST", "/api/policy/publish", {"expected_version": 5, "max_ms": 100})
    check("在途请求期间发布 v6/100 成功", st == 200 and b["version"] == 6, b)
    t.join(timeout=10)
    check("在途请求按入站规则 v5 完成 200",
          outcome.get("status") == 200 and outcome["body"]["policy_version"] == 5, outcome)
    st, b = req(r1, "POST", "/work", {"duration_ms": 150})
    check("新请求按 v6 裁决: 150ms -> 429", st == 429 and b["policy_version"] == 6 and b["limit_ms"] == 100, b)

    print("== 12. 控制台重启: 在途发布不自动提交, 记录 interrupted ==")
    # 直接在 r1/r2 挂预案模拟在途发布, 然后重启控制台
    st, _ = req(r1, "POST", "/admin/policy/prepare",
                {"publish_id": "p-flight1", "expected_version": 6, "version": 7, "max_ms": 77})
    st2, _ = req(r2, "POST", "/admin/policy/prepare",
                 {"publish_id": "p-flight2", "expected_version": 6, "version": 7, "max_ms": 77})
    check("两台挂起预案", st == 200 and st2 == 200)
    stop("drill")
    start("drill", {"REPLICA1_URL": f"http://127.0.0.1:{r1}",
                    "REPLICA2_URL": f"http://127.0.0.1:{r2}",
                    "DB_PATH": f"{RUN_DIR}/drill.db",
                    "PROBE_INTERVAL": "0.3"}, "drill_app", dc)
    time.sleep(1.2)
    # 重启后副本上的预案未被代提交(版本仍 v6)
    for port in (r1, r2):
        st, b = req(port, "GET", "/state")
        check(f"{port} 重启控制台后规则仍 v6(预案未被代提交)",
              b["committed_policy"]["version"] == 6, b)
    # 手工验证 interrupted 记录路径: 用 sqlite 插一条 committing 再重启
    import sqlite3
    conn = sqlite3.connect(f"{RUN_DIR}/drill.db")
    conn.execute("INSERT INTO policy_publishes (publish_id, expected_version, version, max_ms, "
                 "status, created_at, updated_at) VALUES ('pid-sim',6,7,88,'committing',?,?)",
                 ("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"))
    conn.commit()
    conn.close()
    stop("drill")
    start("drill", {"REPLICA1_URL": f"http://127.0.0.1:{r1}",
                    "REPLICA2_URL": f"http://127.0.0.1:{r2}",
                    "DB_PATH": f"{RUN_DIR}/drill.db",
                    "PROBE_INTERVAL": "0.3"}, "drill_app", dc)
    time.sleep(1.0)
    st, b = req(dc, "GET", "/api/policy/publishes")
    sim = next(p for p in b["publishes"] if p["publish_id"] == "pid-sim")
    check("在途发布重启后标记 interrupted 且不自动提交", sim["status"] == "interrupted", sim)
    for port in (r1, r2):
        st, b = req(port, "GET", "/state")
        check(f"{port} 仍为 v6", b["committed_policy"]["version"] == 6, b)

    print("== 13. 原故障演练回归 ==")
    # 两端先恢复健康
    for port in (r1, r2):
        req(port, "POST", "/admin/reset", {})
    time.sleep(0.8)
    st, b = req(dc, "POST", "/api/drills", {"target": "replica1", "duration": 5})
    check("演练启动 201", st == 201 and b["ok"], b)
    st, b = req(r1, "GET", "/health")
    check("目标副本故障 503", st == 503 and b["fault_injected"] is True, b)
    st, b = req(r1, "POST", "/work")
    check("故障副本 /work 503", st == 503, b)
    st, b = req(r2, "GET", "/health")
    check("另一副本健康", st == 200, b)
    # 演练期间向另一副本试单仍受策略约束
    st, b = req(dc, "POST", "/api/work", {"replica": "replica2", "duration_ms": 100})
    check("演练中另一副本可接单(100ms<=100 上限)", st == 200 and b["status_code"] == 200, b)
    st, b = req(dc, "POST", "/api/work", {"replica": "replica2", "duration_ms": 101})
    check("演练中策略 429 仍然生效", st == 200 and b["status_code"] == 429 and b["body"]["policy_version"] == 6, b)
    # 等演练自然结束并恢复
    deadline = time.time() + 15
    finished = False
    while time.time() < deadline:
        st, b = req(dc, "GET", "/api/state")
        d = b["drill"]
        if not d:
            finished = True
            break
        time.sleep(0.5)
    check("演练在期限后结束并恢复", finished, b.get("drill"))
    st, b = req(r1, "GET", "/health")
    check("撤销后目标副本恢复 200", st == 200 and b["fault_injected"] is False, b)
    st, b = req(dc, "GET", "/api/drills")
    check("轮次结论 available/finished",
          b["drills"][0]["status"] == "finished" and b["drills"][0]["conclusion"] == "available",
          b["drills"][0])

    print("== 14. /admin/reset 不影响已发布策略 ==")
    before = req(r1, "GET", "/state")[1]["committed_policy"]
    req(r1, "POST", "/admin/reset", {})
    after = req(r1, "GET", "/state")[1]
    check("reset 后策略保留", after["committed_policy"] == before and after["fault_injected"] is False, after)

    print("== 15. 网页与状态接口 ==")
    st, b = req(dc, "GET", "/")
    check("首页 200", st == 200 and isinstance(b, str) and "接单耗时策略发布" in b, "")
    st, b = req(dc, "GET", "/api/state")
    check("state 含副本策略版与发布历史",
          all(v["policy_version"] == 6 for v in b["replicas"].values()) and len(b["publishes"]) >= 3, b)

    print(f"\n==== 结果: {PASS} 通过, {FAIL} 失败 ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    code = 1
    try:
        code = main()
    finally:
        for name, p in procs:
            try:
                p.send_signal(signal.SIGTERM)
                p.wait(timeout=10)
            except Exception:
                p.kill()
    sys.exit(code)
