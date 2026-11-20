"""開始／停止量測、畫面停格、運作狀態暖機、關掉網頁後自動結束 的測試。"""
import asyncio
import time

from fastapi.testclient import TestClient

from app import Engine, create_app
from storage import Storage
from test_phase4 import CFG, WIFI, _runner


def _counting_runner(counter: dict):
    async def run(cmd):
        if cmd[0] == "ping":
            counter["ping"] = counter.get("ping", 0) + 1
            return 0, "64 bytes from x: icmp_seq=0 ttl=57 time=12.0 ms"
        if cmd[0] == "system_profiler":
            counter["sp"] = counter.get("sp", 0) + 1
            return 0, WIFI
        return await _runner(cmd)
    return run


def test_opens_stopped_and_shows_the_last_data_frozen(tmp_path):
    """打開時不量測；畫面停在「上次停止前」的資料，即使那是一小時前。"""
    st = Storage(str(tmp_path / "f.db"))
    old = int(time.time()) - 3600
    st.add_rows([(old + i, "A", 10.0 + i % 3) for i in range(120)])
    n = {}
    app = create_app({**CFG}, st, _counting_runner(n), autostart=False, start_measuring=False)
    with TestClient(app) as c:
        eng = app.state.engine
        m = c.get("/api/measure").json()
        assert m["measuring"] is False and m["stopped_at"] == old + 119
        asyncio.run(eng.tick())
        assert n.get("ping", 0) == 0                                   # 停止量測：完全沒有 ping
        assert eng.latest["measuring"] is False and eng.latest["results"]["A"] == 10.0 + 119 % 3   # 大數字＝最後一筆
        s = c.get("/api/summary?range=5m").json()["targets"]["A"]
        assert s["count"] == 120                                       # 一小時前的資料仍顯示（停格）
        assert len(c.get("/api/series_all?range=5m").json()["series"]["A"]) > 0
        assert c.get("/api/export.csv?target=all&range=5m").text.count("\n") == 121
        assert c.get("/api/health").json()["engine"]["state"] == "stopped"
        assert c.get("/api/advice").status_code == 200


def test_start_stop_cycle_and_guards(tmp_path):
    n = {}
    app = create_app({**CFG}, Storage(str(tmp_path / "c.db")), _counting_runner(n), autostart=False,
                     start_measuring=False)
    with TestClient(app) as c:
        eng = app.state.engine
        assert c.post("/api/measure", json={"action": "start"}, headers={"Origin": "https://evil.example"}).status_code == 403
        assert c.post("/api/measure", json={"action": "jump"}).status_code == 400
        assert eng.measuring is False
        r = c.post("/api/test/start", json={"minutes": 5})
        assert r.status_code == 409 and "開始量測" in r.json()["detail"]   # 停止量測時不能開始定時測試

        r = c.post("/api/measure", json={"action": "start"}).json()
        assert r["measuring"] is True and r["changed"] is True and r["stopped_at"] is None
        assert c.post("/api/measure", json={"action": "start"}).json()["changed"] is False   # 重複按沒關係
        asyncio.run(eng.tick())
        assert n["ping"] >= 2                                          # A＋路由器
        assert c.get("/api/health").json()["engine"]["state"] == "warming"   # 剛開始量測先不判斷

        # 定時測試進行中：停止量測要另外確認，確認後測試一起結束並保存報告
        assert c.post("/api/test/start", json={"minutes": 5, "label": "停止測試"}).status_code == 200
        r = c.post("/api/measure", json={"action": "stop"})
        assert r.status_code == 409 and "定時測試進行中" in r.json()["detail"] and eng.measuring
        r = c.post("/api/measure", json={"action": "stop", "finish_test": True}).json()
        assert r["measuring"] is False and r["test_finished"] is True and r["report_id"]
        assert c.get("/api/test").json()["state"] == "done"
        assert [x["label"] for x in c.get("/api/reports").json()] == ["停止測試"]
        before = n["ping"]
        asyncio.run(eng.tick())
        asyncio.run(eng.tick())
        assert n["ping"] == before                                     # 停止後不再 ping
        texts = [e["text"] for e in c.get("/api/events").json()]
        assert "▶ 開始量測" in texts and "⏸ 停止量測" in texts
        assert all(e.get("kind") == "control" for e in eng.events if "量測" in e["text"])


def test_no_alerts_while_stopped_and_resume_is_quiet(tmp_path):
    cfg = {**CFG, "targets": [{"name": "OK", "host": "1.1.1.1"}, {"name": "DEADY", "host": "9.9.9.9"}],
           "gateway": {"enabled": False}, "alerts": {"macos_notification": False, "enabled": True}}

    async def run(cmd):
        if cmd[0] == "ping" and cmd[-1] == "9.9.9.9":
            return 2, ""
        if cmd[0] == "ping":
            return 0, "64 bytes from x: icmp_seq=0 ttl=57 time=10.0 ms"
        return await _runner(cmd)

    async def go():
        eng = Engine(cfg, Storage(str(tmp_path / "q.db")), run)
        for _ in range(31):
            await eng.tick()
        assert eng.dead == {"DEADY"}
        eng.stop_measuring()
        calls = []
        eng.alerts.evaluate = lambda *a: calls.append(a) or []
        for _ in range(3):
            await eng.tick()
        assert calls == []                                             # 停止量測時完全不評估警示
        n_events = len(eng.events)
        eng.start_measuring()
        assert eng.dead == set() and all(len(w) == 0 for w in eng.windows.values())
        assert [e["text"] for e in list(eng.events)[n_events:]] == ["▶ 開始量測"]   # 沒有假的「恢復回應」
    asyncio.run(go())


def test_clear_while_stopped(tmp_path):
    st = Storage(str(tmp_path / "x.db"))
    st.add_rows([(int(time.time()) - 50 + i, "A", 10.0) for i in range(30)])
    app = create_app({**CFG}, st, _runner, autostart=False, start_measuring=False)
    with TestClient(app) as c:
        assert c.get("/api/measure").json()["stopped_at"] is not None
        assert c.get("/api/latest").json()["results"] == {"A": 10.0}
        assert c.post("/api/clear", json={"what": ["samples"], "confirm": True}).status_code == 200
        m = c.get("/api/measure").json()
        assert m["measuring"] is False and m["stopped_at"] is None      # 沒有「停止前的資料」了
        assert c.get("/api/latest").json()["results"] == {}             # 卡片的大數字也清掉
        assert c.get("/api/summary?range=5m").json()["targets"]["A"]["count"] == 0


def test_wifi_scan_only_once_while_stopped(tmp_path):
    n = {}

    async def go():
        eng = Engine({**CFG, "wifi_refresh_sec": 300}, Storage(str(tmp_path / "w.db")), _counting_runner(n),
                     measuring=False)
        await eng.refresh_env(1000.0)
        await asyncio.sleep(0.05)
        assert n.get("sp") == 1                                       # 剛打開讀一次，面板才有內容
        await eng.refresh_env(1000.0 + 400)
        await asyncio.sleep(0.05)
        assert n.get("sp") == 1                                       # 停止量測時不再自動掃描
        eng.start_measuring()
        await eng.refresh_env(1000.0 + 800)
        await asyncio.sleep(0.05)
        assert n.get("sp") == 2                                       # 開始量測後恢復
    asyncio.run(go())


def test_health_states_and_warmup_reset(tmp_path):
    n = {}
    cfg = {**CFG, "interval_sec": 0.1, "gateway": {"enabled": False}}

    async def go():
        eng = Engine(cfg, Storage(str(tmp_path / "h.db")), _counting_runner(n))
        assert eng.engine_health()["state"] == "warming"
        eng._warm_until = time.monotonic() + 0.4
        task = asyncio.create_task(eng.run_forever())
        await asyncio.sleep(1.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return eng
    eng = asyncio.run(go())
    h = eng.engine_health()
    assert eng._warm_until == 0 and h["state"] == "ok" and h["ok"]
    assert 0 < h["pings"] < n["ping"]                                 # 暖機期間的紀錄已經丟掉
    eng.metrics["overhead"].extend([400.0] * 50)
    assert eng.engine_health()["state"] == "busy" and not eng.engine_health()["ok"]
    eng.stop_measuring()
    assert eng.engine_health()["state"] == "stopped"


def test_auto_quit_after_page_closed(tmp_path):
    notes = []
    app = create_app({**CFG}, Storage(str(tmp_path / "a.db")), _runner, autostart=False, auto_quit=True,
                     notifier=lambda t, m: notes.append(m))
    quits = []
    app.state.quit = lambda: quits.append(1)
    with TestClient(app) as c:
        eng = app.state.engine
        eng.started_mono, eng.last_seen = 0.0, 0.0
        assert eng.check_auto_quit(30.0) is False                     # 剛打開（等瀏覽器）
        eng.subscribers.add(object())
        assert eng.check_auto_quit(1000.0) is False                   # 網頁還開著
        eng.subscribers.clear()
        eng.last_seen = 980.0
        assert eng.check_auto_quit(1000.0) is False                   # 關掉還不到 1 分鐘
        eng.last_seen = 0.0
        eng.test = {"state": "running"}
        assert eng.check_auto_quit(1000.0) is False                   # 定時測試進行中：等它結束
        eng.test = {"state": "idle"}
        eng.curl_busy = True
        assert eng.check_auto_quit(1000.0) is False
        eng.curl_busy = False
        assert quits == []
        assert eng.check_auto_quit(1000.0) is True and quits == [1]
        assert "量測也跟著停止" in notes[-1]                           # 量測中被關掉：通知你
        assert eng.check_auto_quit(2000.0) is False and quits == [1]  # 只關一次

        eng.last_seen = 0.0
        c.get("/api/health")
        assert eng.last_seen == 0.0                                   # 只確認程式活著（例如啟動器）不算網頁
        c.get("/api/measure")
        assert eng.last_seen > 0                                      # 網頁來要資料就會更新

    # 本來就沒在量測：安靜結束；定時測試剛完成：告訴你報告已保存
    for measuring, test, expect in ((False, {"state": "idle"}, None),
                                    (False, {"state": "done", "finished": time.time()}, "報告已保存")):
        notes.clear()
        app2 = create_app({**CFG}, Storage(str(tmp_path / f"b{measuring}{len(test)}.db")), _runner, autostart=False,
                          auto_quit=True, start_measuring=measuring, notifier=lambda t, m: notes.append(m))
        app2.state.quit = lambda: None
        e2 = app2.state.engine
        e2.started_mono = e2.last_seen = 0.0
        e2.test = test
        assert e2.check_auto_quit(1000.0) is True
        if expect is None:
            assert notes == []
        else:
            assert expect in notes[-1]

    app3 = create_app({**CFG}, Storage(str(tmp_path / "n.db")), _runner, autostart=False)    # 預設（測試用）不自動結束
    e3 = app3.state.engine
    e3.started_mono = e3.last_seen = 0.0
    assert e3.check_auto_quit(10_000.0) is False


def test_quit_saves_a_running_timed_test(tmp_path):
    app = create_app({**CFG}, Storage(str(tmp_path / "t.db")), _runner, autostart=False)
    quits = []
    app.state.quit = lambda: quits.append(1)
    with TestClient(app) as c:
        asyncio.run(app.state.engine.tick())
        assert c.post("/api/test/start", json={"minutes": 30, "label": "結束前"}).status_code == 200
        r = c.post("/api/quit", json={"confirm": True}).json()
        assert r["ok"] and r["test_finished"] is True
        assert [x["label"] for x in c.get("/api/reports").json()] == ["結束前"]
        time.sleep(0.8)
        assert quits == [1]
