"""app.py —— 儀表板的「大腦」：負責量測、整理資料、提供網頁需要的資料。

白話流程：
  1. 量測：每個目標有自己的小迴圈，每隔幾秒 ping 一次（各目標錯開時間，不會一起擠在同一瞬間）。
  2. 整理：每秒把結果寫進資料庫，同時檢查要不要發警示、更新網頁。
  3. 提供資料：網頁透過 /api/... 這些網址來拿數字（例如 /api/summary 是各目標的統計）。
  4. 即時推送：/api/stream 讓網頁每秒自動收到最新結果，不用一直重新整理。
  5. 使用者按下的動作：開始／停止量測、儲存設定、開始定時測試、下載 CSV、清除紀錄、結束程式。
     「清除」「結束」這類不能復原的動作，只接受儀表板自己的網頁送來、而且確認過的請求（見 _guard）。

只開放給這台電腦（127.0.0.1），同一個 Wi-Fi 的其他裝置連不到。

兩個「狀態」要分清楚：
  • 程式開著／關著：從「應用程式」裡的 NetSonar 打開；按「結束 NetSonar」或關掉網頁約 1 分鐘後結束。
  • 量測中／停止量測：程式開著時才有意義。打開時預設「停止量測」，要在網頁按「開始量測」。
    停止量測時不 ping、不發警示、不自動掃描 Wi-Fi，但網頁照常可以看資料、匯出、清除紀錄、改設定；
    畫面會停在「停止前」的資料（見 ref_time），不會慢慢變成空白。

其他檔案的分工：probe.py 去 ping、stats.py 算統計、storage.py 存資料、alerts.py 發警示、
diagnose.py／jitter.py／advice.py 把數字翻成白話建議、netenv.py 讀網路環境、export.py 把紀錄轉成 CSV。
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse

import gateway
import netenv
import traceroute as trace_mod
import advice as advice_mod
import dns_info
import export as export_mod
import geo
import jitter as jitter_mod
import settings as settings_mod
import speedtest
import probe
import stats
from alerts import AlertManager
from diagnose import diagnose
from storage import Storage

BASE = Path(__file__).resolve().parent
RANGES = {"5m": 300, "1h": 3600, "24h": 86400, "7d": 7 * 86400}


def settings_path() -> Path:
    return Path(os.environ.get("NETPROBE_SETTINGS", BASE / "settings.json"))


def load_base_config() -> dict:
    # NETPROBE_CONFIG：測試或進階使用者可指定另一份設定檔
    path = Path(os.environ.get("NETPROBE_CONFIG", BASE / "config.json"))
    return json.loads(path.read_text(encoding="utf-8"))


def load_config() -> dict:
    """預設值（config.json）＋ 使用者在網頁上改的設定（settings.json）。"""
    return settings_mod.merge(load_base_config(), settings_mod.load_overrides(settings_path()))


class Engine:
    """監測引擎：持有設定、資料庫，以及「最新一筆」結果。"""

    WARMUP_SEC = 60      # 剛開始量測的這段時間，程式本身比較慢（啟動、讀 Wi-Fi 資訊），先不判斷「電腦偏忙」

    def __init__(self, cfg: dict, storage: Storage, runner=None, notifier=None, measuring: bool = True):
        self.cfg, self.storage = cfg, storage
        self.base_cfg = dict(cfg)         # 「恢復預設」用
        self._notifier = notifier
        self.runner = runner or probe._real_runner
        self.latest: dict = {"ts": 0, "results": {}}
        self.subscribers: set = set()      # 每個 SSE 連線一個 queue
        self._last_maint = 0
        # 路由器（預設閘道）：每分鐘重新偵測一次，換 Wi-Fi 時會自動跟著變
        gw = cfg.get("gateway", {})
        self.gateway_enabled = gw.get("enabled", False)
        self.router_name = gw.get("name", "路由器") if self.gateway_enabled else None
        self.gateway_ip = None
        self._last_gw_check = -10**9
        self.dns_names: dict = {}          # {DNS 伺服器 IP: {name, source}}
        self.windows: dict = {}            # 每個目標最近 60 筆結果（給告警用，免查資料庫）
        self.alerts = AlertManager(cfg.get("alerts"), self.router_name, notifier, self._per_target(cfg))
        self.loop = None                   # 伺服器的 event loop（關閉時要用）
        # 網路環境（連線類型、VPN、Wi-Fi…）與「環境變更事件」
        self.env: dict = {}
        self.wifi: dict | None = None
        self.public: dict | None = None    # 對外 IP/ISP（預設遮蔽顯示，由網頁決定）
        self.events: deque = deque(maxlen=200)
        self._fp = None
        self._last_env_check = -10**9
        self._last_wifi_check = -10**9
        self.wifi_refresh = cfg.get("wifi_refresh_sec", 300)
        self._wifi_busy = False
        self.public_lookup = cfg.get("public_ip_lookup", True)
        self.test: dict = {"state": "idle"}     # 定時測試狀態
        self.curl_results: deque = deque(maxlen=20)
        self.curl_busy = False
        # 每個目標各自獨立量測（見 _target_loop）；結果先放進緩衝區，每秒統一寫入資料庫
        self._buf: list = []
        self._tasks: dict = {}
        self.metrics = {"overhead": deque(maxlen=300), "lag": deque(maxlen=120), "hung": 0, "pings": 0}
        self.dead: set = set()             # 長期完全不回應 ping 的目標（多半是該位址擋 ICMP）
        self.trace_lines = None            # 測試時可注入假的 traceroute 輸出
        self.trace_jobs: dict = {}         # job_id → 路徑分析進度
        self.geo_lookup = geo.lookup_ip    # 查位址的大約位置與公司；測試時可換成假的
        self.geo_cache: dict = {}          # 查過的位址記起來，同一站不重複查
        # 量測中／停止量測（停止時畫面停在 stopped_at 之前的資料；沒有任何資料時是 None）
        self.measuring = measuring
        self.stopped_at = None if measuring else storage.last_sample_ts()
        if not measuring:                  # 卡片的大數字先顯示停止前每個目標的最後一筆
            self.latest = {"ts": self.stopped_at or 0, "results": storage.last_values(), "measuring": False}
        self._warm_until = time.monotonic() + self.WARMUP_SEC if measuring else 0.0
        # 關掉網頁後自動結束（正式執行時由 create_app 打開；測試預設關閉）
        self.auto_quit = False
        self.quit_after = 60               # 沒有任何網頁開著超過這麼多秒 → 自動結束
        self.quit_grace = 90               # 剛打開的這段時間不判斷（等瀏覽器打開網頁）
        self.started_mono = time.monotonic()
        self.last_seen = time.monotonic()  # 最後一次有網頁來要資料的時間（見 create_app 的 _Seen）
        self.on_quit = None                # 要自動結束時呼叫的函式（create_app 設定）
        self._quitting = False

    @property
    def targets(self) -> list:
        base = list(self.cfg["targets"])
        if self.gateway_enabled and self.gateway_ip:
            base.append({"name": self.router_name, "host": self.gateway_ip})
        return base

    def apply_config(self, cfg: dict) -> None:
        """設定改了就立即生效（不必重啟）：目標、間隔、路由器、警示、對外 IP 查詢。"""
        self.cfg = cfg
        gw = cfg.get("gateway", {})
        self.gateway_enabled = gw.get("enabled", False)
        self.router_name = gw.get("name", "路由器") if self.gateway_enabled else None
        if not self.gateway_enabled:
            self.gateway_ip = None
        self._last_gw_check = -10**9
        self.public_lookup = cfg.get("public_ip_lookup", True)
        self.wifi_refresh = cfg.get("wifi_refresh_sec", 300)
        history = self.alerts.history
        self.alerts = AlertManager(cfg.get("alerts"), self.router_name, self._notifier, self._per_target(cfg))
        self.alerts.history = history
        keep = {t["name"] for t in self.targets} | {t["name"] for t in cfg["targets"]}
        for n in list(self.windows):
            if n not in keep:
                del self.windows[n]

    def stop_streams(self) -> None:
        """通知所有 SSE 連線「該結束了」。

        💡 為什麼需要？瀏覽器開著儀表板 = 一條永遠不會自己結束的串流連線；
           uvicorn 收到 Ctrl+C 會等所有連線關閉才退出 → 看起來像「按了沒反應」。
           所以關閉前先主動把串流收掉（放入 None 當作結束訊號）。
        """
        def _push():
            for q in list(self.subscribers):
                q.put_nowait(None)
        if self.loop is not None:
            self.loop.call_soon_threadsafe(_push)   # 🔰 從別的執行緒/信號處理器安全地排進 event loop

    async def refresh_gateway(self, now: float) -> None:
        if not self.gateway_enabled or now - self._last_gw_check < 60:
            return
        self._last_gw_check = now
        self.gateway_ip = await gateway.get_gateway(self.runner)

    async def refresh_env(self, now: float) -> None:
        """每 15 秒更新一次網路環境；Wi-Fi 資訊較慢（約 7 秒），丟到背景每 60 秒更新。"""
        if now - self._last_env_check >= 15:
            self._last_env_check = now
            try:
                self.env = await netenv.collect_env(self.runner)
            except Exception as e:
                print("env error:", e)
                return
            if self.env.get("gateway") and self.gateway_enabled:
                self.gateway_ip = self.env["gateway"]      # 與路由器目標保持一致
            self._check_change()
        # 💡 Wi-Fi 詳細資訊來自 system_profiler，它會讓 Wi-Fi 網卡「掃描周圍網路」（約 7 秒），
        #    掃描時網卡暫時離開目前頻道 → 全部目標一起出現延遲尖峰。所以預設每 5 分鐘才更新一次，
        #    可在設定頁調整或關閉（0＝只在手動按「更新 Wi-Fi 資訊」時才執行）。
        #    停止量測時不自動掃描（剛打開程式時讀一次，讓「網路環境」面板有內容）。
        every = self.wifi_refresh
        first = self._last_wifi_check < 0
        if every and (self.measuring or first) and now - self._last_wifi_check >= every and not self._wifi_busy:
            self.request_wifi_refresh(now)

    def request_wifi_refresh(self, now: float | None = None) -> bool:
        if self._wifi_busy:
            return False
        self._last_wifi_check = now if now is not None else time.monotonic()
        self._wifi_busy = True
        self.events.append({"ts": int(time.time()), "minor": True,
                            "text": "Wi-Fi 詳細資訊更新（macOS 會掃描周圍網路，可能造成短暫延遲跳動）"})
        coro = self._refresh_wifi()
        try:
            asyncio.get_running_loop()
            asyncio.create_task(coro)
        except RuntimeError:                              # 從網頁請求的執行緒呼叫：交給主事件迴圈
            if self.loop is None:
                coro.close()
                self._wifi_busy = False
                return False
            asyncio.run_coroutine_threadsafe(coro, self.loop)
        return True

    async def _refresh_wifi(self) -> None:
        try:
            self.wifi = await asyncio.wait_for(netenv.collect_wifi(self.runner), 15)
        except Exception:
            self.wifi = None
        finally:
            self._wifi_busy = False
        self._check_change()

    def _check_change(self) -> None:
        if not self.env:
            return
        fp = netenv.fingerprint(self.env, self.wifi)
        for text in netenv.describe_changes(self._fp, fp):
            self.events.append({"ts": int(time.time()), "text": text})
        if self._fp is not None and (self._fp["gateway"], self._fp["vpn"]) != (fp["gateway"], fp["vpn"]):
            self.public = None                             # 網路換了，舊的對外 IP 作廢
        self._fp = fp
        self._lookup_dns_missing()
        if self.public is None and self.public_lookup and self.env.get("interface"):
            self.public = {}                               # 佔位，避免重複查詢
            if self.loop:
                asyncio.create_task(self._lookup_public())

    def _lookup_dns_missing(self) -> None:
        """DNS 伺服器是誰提供的：內建名單立刻有，認不出來的在背景查（ipinfo.io，需「查詢對外 IP」開啟）。"""
        for ip in (self.env or {}).get("dns", []):
            if ip in self.dns_names:
                continue
            c = dns_info.classify(ip)
            if c:
                self.dns_names[ip] = c
            else:
                self.dns_names[ip] = {"name": "查詢中…", "source": "pending"}
                if self.loop:
                    asyncio.create_task(self._dns_lookup(ip))

    async def _dns_lookup(self, ip: str) -> None:
        self.dns_names[ip] = await asyncio.to_thread(dns_info.lookup, ip, self.public_lookup)

    async def _lookup_public(self) -> None:
        self.public = await asyncio.to_thread(netenv.lookup_public) or {}

    # ------------------------------------------------------------------ 量測
    # 💡 設計：每個目標有「自己的」量測迴圈，起跑時間錯開（平均分散在一個間隔內），
    #    好處：①不會每秒同時送出一整批封包 ②某個目標一直逾時（例如擋 ICMP）不會拖慢其他目標。
    #    量測結果先放進緩衝區 _buf，由 _aggregate 每秒統一寫入資料庫、更新警示與網頁。
    def _record(self, name: str, rtt, wall_ms: float, timeout: float) -> None:
        if not self.measuring:
            return                                  # 剛按下停止時還在路上的那一次，不算
        self._buf.append((int(time.time()), name, rtt))
        m = self.metrics
        m["pings"] += 1
        if rtt is not None:
            m["overhead"].append(max(0.0, wall_ms - rtt))     # 啟動 ping 程式、讀取輸出的額外耗時
        elif wall_ms > timeout * 1000 + 900:
            m["hung"] += 1      # ping 沒有在逾時後自己結束、是被 -t 強制收掉的（目標不回應時偶爾發生）

    async def _target_loop(self, name: str, offset: float) -> None:
        await asyncio.sleep(offset)
        nxt = time.monotonic()
        while True:
            t = next((x for x in self.targets if x["name"] == name), None)
            if t is None:
                return                                         # 目標被刪除了
            timeout = self.cfg.get("ping_timeout_sec", 1)
            w0 = time.monotonic()
            rtt = await probe.ping_once(t["host"], timeout, self.runner)
            self._record(name, rtt, (time.monotonic() - w0) * 1000, timeout)
            nxt += self.cfg.get("interval_sec", 1.0)
            now = time.monotonic()
            if nxt < now:
                nxt = now                                      # 落後了不要補跑（避免連續狂送）
            await asyncio.sleep(nxt - now)

    def _cancel_tasks(self) -> None:
        """停掉所有目標的量測迴圈（停止量測、或程式結束時）。"""
        for t in self._tasks.values():
            t.cancel()
        self._tasks.clear()

    def _reconcile(self) -> None:
        """依目前的目標清單啟動／停止各目標的量測迴圈（設定改了、路由器剛偵測到時會用到）。"""
        names = [t["name"] for t in self.targets]
        interval = self.cfg.get("interval_sec", 1.0)
        for n in list(self._tasks):
            if n not in names or self._tasks[n].done():
                self._tasks.pop(n).cancel()
        for i, n in enumerate(names):
            if n not in self._tasks:
                self._tasks[n] = asyncio.create_task(self._target_loop(n, interval * i / max(1, len(names))))

    async def _aggregate(self) -> dict:
        """把緩衝區的結果寫入資料庫、更新統計視窗、評估警示、推送給網頁。"""
        rows, self._buf = self._buf, []
        names = [t["name"] for t in self.targets]
        results = {n: v for n, v in self.latest.get("results", {}).items() if n in names}
        if rows:
            await asyncio.to_thread(self.storage.add_rows, rows)     # 不在事件迴圈裡等資料庫鎖
        for ts, n, v in rows:
            self.windows.setdefault(n, deque(maxlen=60)).append(v)
            results[n] = v
        ts = int(time.time())
        if not self.measuring:
            # 停止量測：資料不會再變，不判斷「無回應」、不評估警示（否則冷卻時間一過，舊資料又會再通知一次）
            self.latest = {"ts": ts, "results": results, "alerts": [], "measuring": False}
            return await self._after_aggregate(ts)
        self._update_unreachable()
        # 警示對象：排除「已標記無回應」以及「整個視窗（≥5 筆）從沒回應過、而其他目標正常」的目標，
        # 避免剛啟動或剛新增一個擋 ICMP 的位址時，在判定成「無回應」之前先吵一輪通知
        ext = [n for n in self.windows if n != self.router_name]
        others_alive = lambda me: any(any(v is not None for v in list(self.windows[n])[-10:]) for n in ext if n != me)
        quiet = {n for n, w in self.windows.items() if n != self.router_name and len(w) >= 5
                 and all(v is None for v in w) and others_alive(n)}
        new_alerts = self.alerts.evaluate(
            {n: list(w) for n, w in self.windows.items() if n not in self.dead and n not in quiet}, ts)
        self.latest = {"ts": ts, "results": results, "alerts": new_alerts, "measuring": True}
        t = self.test
        if t["state"] == "running" and t["ends_at"] and ts >= t["ends_at"]:
            self.finish_test(ts)
        return await self._after_aggregate(ts)

    async def _after_aggregate(self, ts: int) -> dict:
        """推送給網頁＋每分鐘的資料整理（量測中、停止量測都要做）。"""
        for q in list(self.subscribers):
            if q.qsize() < 5:              # 瀏覽器太慢就丟棄舊訊息，避免記憶體堆積
                q.put_nowait(self.latest)
        if ts - self._last_maint >= 60:    # 每分鐘：彙總 → 再清理
            self._last_maint = ts
            ret = self.cfg.get("retention", {})
            await asyncio.to_thread(self.storage.rollup, ts)
            await asyncio.to_thread(self.storage.purge, ts, ret.get("raw_hours", 24),
                                    ret.get("minute_days", 90))
        return self.latest

    def _update_unreachable(self) -> None:
        """偵測「長期完全不回應」的目標：最近 30 筆全逾時，而其他外部目標最近仍有回應。
        這種目標多半是該位址（或你的 VPN）擋掉 ICMP，不是網路故障 → 暫停對它的警示與判讀，卡片標示「無回應」。
        只要它回應一次就恢復。若所有目標都逾時（真的斷線），不會被當成「不回應的目標」。"""
        ext = [t["name"] for t in self.targets if t["name"] != self.router_name]
        recent = {n: list(self.windows.get(n, [])) for n in ext}
        alive = any(any(v is not None for v in w[-10:]) for w in recent.values())
        new = set()
        for n, w in recent.items():
            if alive and len(w) >= 30 and all(v is None for v in w[-30:]):
                new.add(n)
            elif n in self.dead and w and w[-1] is None:
                new.add(n)                                    # 已標記的，要等到回應一次才解除
        now = int(time.time())
        for n in new - self.dead:
            self.events.append({"ts": now, "text": f"{n} 一直沒有回應（其他目標正常），已暫停對它的警示與判讀"})
        for n in self.dead - new:
            self.events.append({"ts": now, "text": f"{n} 恢復回應"})
        self.dead = new

    async def tick(self) -> dict:
        """（測試用）同步做一輪：所有目標同時 ping 一次，再彙總。正式執行改用各目標獨立迴圈。"""
        timeout = self.cfg.get("ping_timeout_sec", 1)
        await self.refresh_gateway(time.monotonic())
        await self.refresh_env(time.monotonic())
        if not self.measuring:
            return await self._aggregate()
        targets = self.targets
        rtts = await asyncio.gather(*[probe.ping_once(t["host"], timeout, self.runner) for t in targets])
        ts = int(time.time())
        self._buf.extend((ts, t["name"], r) for t, r in zip(targets, rtts))
        return await self._aggregate()

    async def run_forever(self) -> None:
        try:
            while True:
                started = time.monotonic()
                try:
                    # 網路環境、路由器位址是本機讀取（不會送出量測封包），停止量測時也照常更新
                    await self.refresh_gateway(started)
                    await self.refresh_env(started)
                    if self.measuring:
                        self._reconcile()
                    else:
                        self._cancel_tasks()
                    await self._aggregate()
                    self.check_auto_quit(time.monotonic())
                except Exception as e:     # 單輪失敗不要讓整個監測停掉
                    print("tick error:", e)
                interval = self.cfg.get("interval_sec", 1.0)
                await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))
                # 事件迴圈延遲：本來該睡 interval，實際多睡了多久。數字大＝本程式被拖慢
                self.metrics["lag"].append(max(0.0, (time.monotonic() - started - interval) * 1000))
                if self._warm_until and time.monotonic() >= self._warm_until:
                    self._warm_until = 0.0
                    self._reset_metrics()           # 暖機結束：丟掉剛啟動時偏慢的紀錄，之後才開始判斷
        finally:
            self._cancel_tasks()

    def _reset_metrics(self) -> None:
        self.metrics = {"overhead": deque(maxlen=300), "lag": deque(maxlen=120), "hung": 0, "pings": 0}

    def engine_health(self) -> dict:
        """運作狀態：用來分辨「是網路不穩」還是「本程式／本機太忙」。
        state：stopped（停止量測）、warming（剛開始量測，先不判斷）、busy（電腦偏忙）、ok（正常）。"""
        def pct(vals, p):
            v = sorted(vals)
            return round(v[min(len(v) - 1, int(len(v) * p))], 1) if v else None
        m = self.metrics
        oh, lag = list(m["overhead"]), list(m["lag"])
        # 本機忙＝額外耗時或排程延遲偏高；「被強制收掉」只在占比很高（>10%）時才算，
        # 因為目標不回應（例如 VPN 擋 ICMP）時偶爾也會發生，不代表電腦忙
        busy = bool((pct(oh, .95) or 0) > 150 or (pct(lag, .95) or 0) > 300
                    or m["hung"] > max(5, 0.1 * m["pings"]))
        state = ("stopped" if not self.measuring else
                 "warming" if time.monotonic() < self._warm_until else "busy" if busy else "ok")
        return {"ok": state != "busy", "state": state, "pings": m["pings"], "hung_pings": m["hung"],
                "overhead_ms": {"p50": pct(oh, .5), "p95": pct(oh, .95), "max": round(max(oh), 1) if oh else None},
                "loop_lag_ms": {"p50": pct(lag, .5), "p95": pct(lag, .95), "max": round(max(lag), 1) if lag else None},
                "targets": len(self.targets), "interval_sec": self.cfg.get("interval_sec", 1.0)}

    # ---- 定時測試：記下起訖時間，結束時用這段期間的原始資料產生報告 ----
    @staticmethod
    def _per_target(cfg: dict) -> dict:
        return {t["name"]: {k: t[k] for k in ("p95_ms", "mute") if k in t}
                for t in cfg.get("targets", []) if "p95_ms" in t or t.get("mute")}

    def context(self) -> dict:
        """目前的網路條件快照（存進報告，之後比較「有線 vs Wi-Fi」「5 GHz vs 2.4 GHz」時用）。"""
        e, w = self.env or {}, self.wifi or {}
        return {"interface": e.get("interface"), "interface_type": e.get("interface_type"),
                "vpn": bool((e.get("vpn") or {}).get("active")), "gateway": e.get("gateway"),
                "band": w.get("band"), "channel": w.get("channel"), "width": w.get("width"),
                "signal_dbm": w.get("signal_dbm"), "tx_rate_mbps": w.get("tx_rate_mbps")}

    @staticmethod
    def auto_label(c: dict) -> str:
        parts = [c.get("interface_type") or c.get("interface") or "未知連線"]
        if c.get("band"):
            parts.append(f"{c['band']} 頻道 {c.get('channel')}")
        if c.get("signal_dbm") is not None:
            parts.append(f"{c['signal_dbm']} dBm")
        parts.append("VPN 開" if c.get("vpn") else "無 VPN")
        return "｜".join(parts)

    def jitter(self, since: int, until: int | None = None) -> dict:
        """分析某段期間的延遲尖峰（路由器優先；沒開路由器就用第一個外部目標）。"""
        until = until or 10 ** 12
        series = lambda n: {ts: r for ts, r in self.storage.raw_rows(n, since) if ts <= until}
        ext = [t["name"] for t in self.targets if t["name"] != self.router_name and t["name"] not in self.dead]
        if self.router_name and self.gateway_ip and any(t["name"] == self.router_name for t in self.targets):
            return jitter_mod.analyze(series(self.router_name), {n: series(n) for n in ext}, True)
        if not ext:
            return {"ok": False, "reason": "沒有可分析的目標"}
        return jitter_mod.analyze(series(ext[0]), {n: series(n) for n in ext[1:]}, False)

    def start_test(self, minutes: int | None, label: str = "") -> dict:
        now = int(time.time())
        ctx = self.context()
        self.test = {"state": "running", "started": now, "minutes": minutes,
                     "label": label or self.auto_label(ctx), "context": ctx,
                     "ends_at": now + minutes * 60 if minutes else None,
                     "vpn": bool(self.env.get("vpn", {}).get("active")), "report": None}
        return self.test

    def finish_test(self, now: int | None = None) -> dict:
        t, now = self.test, now or int(time.time())
        if t["state"] != "running":
            return t
        t.update(state="done", finished=now)
        t["report"] = self.report(t["started"], now)
        t["report"]["jitter"] = self.jitter(t["started"], now)
        t["report"]["context"] = t.get("context") or self.context()
        t["report"]["label"] = t["label"]
        try:
            t["report_id"] = self.storage.save_report(t["started"], now, t["label"], t["report"])
        except Exception as e:
            print("save report error:", e)
        return t

    def report(self, since: int, until: int) -> dict:
        targets = {t["name"]: stats.summarize([r for ts, r in self.storage.raw_rows(t["name"], since) if ts <= until])
                   for t in self.targets}
        d = diagnose({n: s for n, s in targets.items() if n not in self.dead}, self.router_name, self.cfg.get("diagnosis"))
        return {"since": since, "until": until, "seconds": until - since, "targets": targets,
                "diagnosis": d, "alerts": [a for a in self.alerts.history if since <= a["ts"] <= until],
                "events": [e for e in self.events if since <= e["ts"] <= until],
                "curl": [c for c in self.curl_results if since <= c.get("ts", 0) <= until]}

    async def run_curl_test(self, mb: int = 25) -> dict:
        cc = self.cfg.get("curl_test", {})
        self.curl_busy = True
        try:
            res = await speedtest.run_curl(self.runner, mb * 1_000_000,
                                           cc.get("url", speedtest.DEFAULT_URL))
        finally:
            self.curl_busy = False
        res["ts"] = int(time.time())
        res["vpn"] = bool(self.env.get("vpn", {}).get("active"))
        self.curl_results.appendleft(res)
        return res

    def diagnosis(self, range_key: str = "5m") -> dict:
        s = {n: v for n, v in self.summary(range_key).items() if n not in self.dead}
        return diagnose(s, self.router_name, self.cfg.get("diagnosis"))

    CLEARABLE = ("samples", "alerts", "reports", "curl", "trace")

    def clear(self, what: list) -> dict:
        """清除紀錄。what 可包含：samples（量測資料）、alerts（警示與事件）、reports（測試報告）、
        curl（下載測速結果）、trace（路徑分析結果）。回傳各項清掉幾筆。量測本身不會停，清完會從零開始累積。"""
        done = {}
        if "samples" in what:
            done["samples"] = self.storage.clear_samples()
            self._buf.clear()
            for w in self.windows.values():        # 警示用的「最近 60 筆」也清掉，避免舊資料繼續觸發警示
                w.clear()
            self.dead = set()
            self.latest = {**self.latest, "results": {}}     # 卡片的大數字也一起清掉
            if not self.measuring:
                self.stopped_at = None             # 已經沒有「停止前的資料」可以顯示了
        if "alerts" in what:
            done["alerts"] = len(self.alerts.history) + len(self.events)
            self.alerts.history.clear()
            self.events.clear()
        if "reports" in what:
            done["reports"] = self.storage.clear_reports()
            if self.test.get("state") == "done":   # 畫面上「上次測試」的報告也一起收掉
                self.test = {"state": "idle"}
        if "curl" in what:
            done["curl"] = len(self.curl_results)
            self.curl_results.clear()
        if "trace" in what:
            keep = {k: v for k, v in self.trace_jobs.items() if v.get("running")}
            done["trace"] = len(self.trace_jobs) - len(keep)
            self.trace_jobs = keep
        return done

    # ---- 開始／停止量測、畫面的「現在」、關掉網頁後自動結束 ----
    def ref_time(self) -> int:
        """畫面上的「現在」：量測中＝真正的現在；停止量測時＝停止前最後一筆資料的時間。
        這樣停止量測後，卡片、建議、圖表會停在停止前的樣子，不會隨時間慢慢變成空白或「沒有回應」。"""
        if not self.measuring and self.stopped_at:
            return int(self.stopped_at)
        return int(time.time())

    def measure_state(self) -> dict:
        return {"measuring": self.measuring, "stopped_at": None if self.measuring else self.stopped_at,
                "test_running": self.test.get("state") == "running", "auto_quit": self.auto_quit,
                "quit_after_sec": self.quit_after}

    def start_measuring(self) -> dict:
        """開始量測。警示從新的資料重新判斷（停止前的舊資料不會觸發通知），第一分鐘先不判斷「電腦偏忙」。"""
        if self.measuring:
            return {"changed": False}
        self.measuring, self.stopped_at = True, None
        for w in self.windows.values():
            w.clear()
        self.dead = set()                           # 安靜重設，不產生「恢復回應」之類的事件
        self._reset_metrics()
        self._warm_until = time.monotonic() + self.WARMUP_SEC
        self._last_env_check = self._last_gw_check = -10**9     # 馬上重新讀一次網路環境
        self.events.append({"ts": int(time.time()), "kind": "control", "text": "▶ 開始量測"})
        return {"changed": True}

    def stop_measuring(self) -> dict:
        """停止量測：不再 ping、不發警示、不自動掃描 Wi-Fi；程式仍開著，網頁照常可用。
        定時測試進行中時，測試會一併結束，報告保存到目前為止（呼叫前請先跟使用者確認）。"""
        if not self.measuring:
            return {"changed": False}
        out = {"changed": True}
        if self.test.get("state") == "running":
            t = self.finish_test()
            out.update(test_finished=True, report_id=t.get("report_id"))
        self.measuring = False
        self._cancel_tasks()
        self._warm_until = 0.0
        now = int(time.time())
        self.stopped_at = now
        self.events.append({"ts": now, "kind": "control", "text": "⏸ 停止量測"})
        return out

    def check_auto_quit(self, now: float) -> bool:
        """關掉網頁後自動結束：沒有任何網頁開著（即時連線數＝0，而且超過 quit_after 秒沒有任何請求），
        就呼叫 on_quit 把程式關掉。剛打開的 quit_grace 秒內不判斷（等瀏覽器打開網頁）；
        定時測試、下載測速、路徑分析進行中也先不關，等它們結束。now 用 time.monotonic()（電腦睡眠時不會前進）。"""
        if not self.auto_quit or self._quitting or self.on_quit is None:
            return False
        if now - self.started_mono < self.quit_grace or self.subscribers or now - self.last_seen < self.quit_after:
            return False
        if (self.test.get("state") == "running" or self.curl_busy
                or any(j.get("running") for j in self.trace_jobs.values())):
            return False
        self._quitting = True
        t = self.test
        if t.get("state") == "done" and t.get("finished", 0) >= time.time() - 120:
            msg = "定時測試已完成，報告已保存。網頁沒有開著，NetSonar 已自動結束。"
        elif self.measuring:
            msg = "網頁關閉超過 1 分鐘，NetSonar 已自動結束，量測也跟著停止。要繼續量測，請重新打開 NetSonar。"
        else:
            msg = None                                  # 本來就沒在量測：安靜結束
        print("網頁已關閉，NetSonar 自動結束", flush=True)
        if msg and self.alerts.notifier:
            try:
                self.alerts.notifier("NetSonar", msg)
            except Exception:
                pass
        self.on_quit()
        return True

    def summary(self, range_key: str) -> dict:
        since = self.ref_time() - RANGES[range_key]
        return {t["name"]: {**stats.summarize(self.storage.recent_rtts(t["name"], since)),
                            "unreachable": t["name"] in self.dead}
                for t in self.targets}


async def _locate_hop(engine: Engine, hop: dict) -> None:
    """替路徑上的一站補上「大約位置＋公司名稱」。內部位址直接標示；公開位址才上網查（且要設定允許）。"""
    ip = hop.get("ip")
    if not ip:
        return
    kind = geo.local_kind(ip)
    if kind:
        hop["geo"] = geo.describe_local(kind)
        return
    if not engine.public_lookup:
        return
    if ip not in engine.geo_cache:
        try:
            engine.geo_cache[ip] = await asyncio.to_thread(engine.geo_lookup, ip)
        except Exception:
            return                                   # 查不到就算了，不影響路徑分析本身
        if len(engine.geo_cache) > 500:
            engine.geo_cache.pop(next(iter(engine.geo_cache)))
    hop["geo"] = engine.geo_cache[ip]


async def _run_trace(engine: Engine, jid: str, host: str) -> None:
    """背景執行 traceroute，邊跑邊把每一站放進 job（網頁輪詢即可看到進度）；每一站同時去查大約位置。"""
    job = engine.trace_jobs[jid]
    lines = engine.trace_lines or trace_mod.iter_lines
    lookups = []
    try:
        async for line in lines(trace_mod.build_cmd(host)):
            hop = trace_mod.parse_hop_line(line)
            if hop:
                job["hops"].append(hop)
                lookups.append(asyncio.create_task(_locate_hop(engine, hop)))
    except Exception as e:
        job["error"] = str(e)
    if lookups:
        await asyncio.wait(lookups, timeout=8)       # 最多再等 8 秒，位置查不完也照樣給結果
    job["geo_enabled"] = engine.public_lookup
    job["analysis"] = trace_mod.analyze_hops(job["hops"], bool(engine.env.get("vpn", {}).get("active")))
    job["running"] = False


SERVICE_LABEL = "com.netsonar.dashboard"


def _quit_process() -> None:
    """真的把自己關掉（「結束 NetSonar」按鈕，或關掉網頁後自動結束）。
    從 NetSonar App 打開的：請 macOS 把這個背景工作卸下；之後要再用，重新點 NetSonar App（登入時不會自動啟動）。
    終端機啟動：等同按 Ctrl+C。"""
    if os.environ.get("NETSONAR_SERVICE") == "1":
        try:
            subprocess.Popen(["launchctl", "bootout", f"gui/{os.getuid()}/{SERVICE_LABEL}"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            return
        except OSError:
            pass
    os.kill(os.getpid(), signal.SIGINT)


class _Seen:
    """記下「最後一次有網頁來要資料」的時間，給「關掉網頁後自動結束」判斷用。
    只是確認程式活著的 /api/health 不算（例如 NetSonar App 啟動時的檢查），免得它讓程式一直不結束。
    （寫成最簡單的 ASGI 中介層，不包裝回應內容，不會影響即時串流。）"""

    def __init__(self, app, engine):
        self.app, self.engine = app, engine

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") != "/api/health":
            self.engine.last_seen = time.monotonic()
        await self.app(scope, receive, send)


def create_app(cfg: dict | None = None, storage: Storage | None = None,
               runner=None, autostart: bool = True, notifier=None, settings_file: Path | None = None,
               start_measuring: bool = True, auto_quit: bool = False) -> FastAPI:
    """建立儀表板。start_measuring：一打開就量測嗎（正式執行時是 False，要在網頁按「開始量測」）；
    auto_quit：關掉網頁約 1 分鐘後自動結束嗎（正式執行時是 True）。兩者的預設值是給自動化測試用的。"""
    from_disk = cfg is None
    cfg = cfg or load_config()
    sfile = Path(settings_file) if settings_file else settings_path()
    storage = storage or Storage(os.environ.get("NETPROBE_DB", str(BASE / "netprobe.db")))
    engine = Engine(cfg, storage, runner, notifier, measuring=start_measuring)
    engine.auto_quit = auto_quit
    if from_disk:
        engine.base_cfg = load_base_config()      # 「恢復預設」要回到 config.json，而不是目前（含自訂）的設定

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine.loop = asyncio.get_running_loop()
        task = asyncio.create_task(engine.run_forever()) if autostart else None
        yield
        if task:
            task.cancel()
        storage.close()

    app = FastAPI(title="NetSonar", lifespan=lifespan)
    app.state.engine = engine
    app.state.quit = _quit_process            # 測試時可換成假的，避免真的把測試程式關掉
    engine.on_quit = lambda: app.state.quit()
    app.add_middleware(_Seen, engine=engine)

    @app.get("/api/about")
    def about():
        """這個程式目前怎麼執行、資料放在哪、關掉網頁後會不會自動結束（給「說明」頁顯示）。"""
        service = os.environ.get("NETSONAR_SERVICE") == "1"
        return {"mode": "service" if service else "terminal", "auto_quit": engine.auto_quit,
                "quit_after_sec": engine.quit_after,
                "db_path": str(Path(storage.path).resolve()) if storage.path != ":memory:" else "（記憶體，不保存）",
                "settings_path": str(sfile.resolve()),
                "log_path": str(Path.home() / "Library/Logs/NetSonar.log") if service else None,
                "port": engine.cfg.get("port", 8765)}

    def _same_origin(request: Request) -> None:
        """🔒 只接受從儀表板自己的網頁送來的請求：別的網站就算叫你的瀏覽器連過來，來源（Origin）不符也會被拒絕。"""
        origin = request.headers.get("origin")
        if origin and origin.split("//")[-1].split(":")[0] not in ("127.0.0.1", "localhost"):
            raise HTTPException(403, "只接受來自本機儀表板的請求")

    def _guard(body: dict, request: Request) -> None:
        """給「結束」「清除」這類不能復原的動作用：來源要是儀表板自己的網頁，而且要明確確認過。"""
        _same_origin(request)
        if body.get("confirm") is not True:
            raise HTTPException(400, "需要 confirm: true")

    @app.get("/api/measure")
    def measure_get():
        """目前是「量測中」還是「停止量測」（停止時附上畫面停在哪個時間點）。"""
        return engine.measure_state()

    @app.post("/api/measure")
    async def measure_set(body: dict, request: Request):
        """開始或停止量測。body：{"action": "start" 或 "stop"}。
        定時測試進行中要停止時，要再加上 "finish_test": true（測試會一併結束，報告保存到目前為止）。"""
        _same_origin(request)
        action = body.get("action")
        if action == "start":
            r = engine.start_measuring()
        elif action == "stop":
            if engine.test.get("state") == "running" and body.get("finish_test") is not True:
                raise HTTPException(409, "定時測試進行中。停止量測會一併結束測試（報告保存到目前為止）。")
            r = engine.stop_measuring()
        else:
            raise HTTPException(400, 'action 必須是 "start" 或 "stop"')
        return {**engine.measure_state(), **r}

    def _csv(text_or_iter, name: str):
        """回傳一個「下載檔案」的回應；檔名附上日期時間，方便分辨是哪一次匯出的。"""
        fn = f"netsonar-{name}-{time.strftime('%Y%m%d-%H%M%S')}.csv"
        headers = {"Content-Disposition": f"attachment; filename={fn}"}
        if isinstance(text_or_iter, str):
            return Response(text_or_iter, media_type="text/csv; charset=utf-8", headers=headers)
        return StreamingResponse(text_or_iter, media_type="text/csv; charset=utf-8", headers=headers)

    @app.post("/api/clear")
    def clear(body: dict, request: Request):
        """清除紀錄。body：{"what": ["samples", ...] 或 "all", "confirm": true}。清除後無法復原。"""
        _guard(body, request)
        what = body.get("what")
        what = list(Engine.CLEARABLE) if what == "all" else what
        if not isinstance(what, list) or not what or any(w not in Engine.CLEARABLE for w in what):
            raise HTTPException(400, f"what 必須是 all，或 {list(Engine.CLEARABLE)} 的清單")
        if engine.test.get("state") == "running" and ("samples" in what or "reports" in what):
            raise HTTPException(409, "定時測試進行中。請先到「定時測試」頁結束測試，再清除紀錄。")
        return {"ok": True, "cleared": engine.clear(what)}

    @app.post("/api/quit")
    async def quit_app(body: dict, request: Request):
        """結束 NetSonar（程式完全關閉，網頁也就無法使用；要再用請重新打開 NetSonar App）。
        進行中的定時測試會先結束，報告保存到目前為止，不會白白丟掉。"""
        _guard(body, request)
        finished = engine.test.get("state") == "running"
        if finished:
            engine.finish_test()
        def bye():
            engine.stop_streams()      # 先收掉網頁的即時連線，伺服器才不會一直等它們、遲遲關不掉
            app.state.quit()
        threading.Timer(0.5, bye).start()                  # 稍等一下再關，讓這個回應先送回網頁
        return {"ok": True, "test_finished": finished}

    @app.get("/api/health")
    def health():
        return {"ok": True, "targets": len(engine.targets), "engine": engine.engine_health()}

    @app.get("/api/targets")
    def targets():
        return engine.targets

    @app.get("/api/latest")
    def latest():
        return engine.latest

    @app.get("/api/summary")
    def summary(range: str = "5m"):
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        return {"range": range, "targets": engine.summary(range)}

    @app.get("/api/diagnosis")
    def diagnosis(range: str = "5m"):
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        return {"range": range, "router": engine.router_name,
                "gateway_ip": engine.gateway_ip, **engine.diagnosis(range)}

    @app.get("/api/advice")
    def advice(range: str = "5m"):
        """整體網路建議：把判讀、Wi-Fi、路由器、外部連線、抖動、DNS、VPN 整理成分段的白話結果。"""
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        try:
            jit = engine.jitter(engine.ref_time() - RANGES[range])
        except Exception:
            jit = None
        return advice_mod.build(engine.summary(range), engine.diagnosis(range), engine.env or {}, engine.wifi,
                                jit, engine.dns_names, engine.router_name, engine.dead)

    @app.get("/api/alerts")
    def alerts():
        return list(engine.alerts.history)

    @app.get("/api/history")
    def history(target: str, range: str = "24h"):
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        if target not in [t["name"] for t in engine.targets]:
            raise HTTPException(404, "找不到這個目標")
        since = int(time.time()) - RANGES[range]
        return {"target": target, "range": range,
                "points": engine.storage.minute_series(target, since)}

    def _settings_view():
        return {"settings": {k: engine.cfg.get(k) for k in settings_mod.EDITABLE if k in engine.cfg},
                "defaults": {k: engine.base_cfg.get(k) for k in settings_mod.EDITABLE if k in engine.base_cfg},
                "customized": sfile.exists(), "curl_default_url": speedtest.DEFAULT_URL}

    @app.get("/api/settings/export")
    def settings_export():
        """把目前的設定存成一個檔案（之後可以在「設定」頁匯入，或搬到另一台電腦用）。"""
        doc = {"app": "NetSonar", "type": "settings", "exported": time.strftime("%Y-%m-%d %H:%M:%S"),
               "settings": _settings_view()["settings"]}
        fn = f"netsonar-settings-{time.strftime('%Y%m%d-%H%M%S')}.json"
        return Response(json.dumps(doc, ensure_ascii=False, indent=2), media_type="application/json",
                        headers={"Content-Disposition": f"attachment; filename={fn}"})

    @app.get("/api/settings")
    def settings_get():
        return _settings_view()

    @app.put("/api/settings")
    def settings_put(body: dict):
        try:
            clean = settings_mod.validate(body)
        except settings_mod.SettingsError as e:
            raise HTTPException(400, str(e))
        settings_mod.save_overrides(sfile, clean)
        engine.apply_config({**engine.base_cfg, **clean})
        return _settings_view()

    @app.post("/api/settings/reset")
    def settings_reset():
        settings_mod.reset(sfile)
        engine.apply_config(dict(engine.base_cfg))
        return _settings_view()

    @app.get("/api/env")
    def env():
        """網路環境。SSID／對外 IP 預設由網頁遮蔽，這裡回傳原值（只綁本機）。"""
        return {"env": engine.env, "wifi": engine.wifi, "public": engine.public or None, "dns_names": engine.dns_names}

    @app.post("/api/wifi/refresh")
    def wifi_refresh():
        """手動更新 Wi-Fi 詳細資訊（會觸發 macOS 掃描，可能讓延遲短暫跳動）。"""
        return {"started": engine.request_wifi_refresh()}

    @app.get("/api/events")
    def events():
        return list(engine.events)

    @app.get("/api/series")
    def series(target: str, range: str = "1h"):
        """圖表資料：≤1h 用原始資料（分桶），24h/7d 用每分鐘彙總（loss/avg/p95）。"""
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        if target not in [t["name"] for t in engine.targets]:
            raise HTTPException(404, "找不到這個目標")
        since = engine.ref_time() - RANGES[range]          # 停止量測時，停在停止前的資料
        if range in ("5m", "1h"):
            bucket = 1 if range == "5m" else 10
            return {"target": target, "range": range, "bucket_sec": bucket,
                    "points": engine.storage.raw_series(target, since, bucket)}
        return {"target": target, "range": range, "bucket_sec": 60 if range == "24h" else 300,
                "points": engine.storage.minute_series(target, since)}

    @app.get("/api/series_all")
    def series_all(range: str = "1h"):
        """一次取回所有目標的圖表資料（網頁每幾秒更新一次，合併成一個請求比一個目標一個請求輕）。"""
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        since = engine.ref_time() - RANGES[range]
        out = {}
        for t in engine.targets:
            n = t["name"]
            out[n] = (engine.storage.raw_series(n, since, 1 if range == "5m" else 10) if range in ("5m", "1h")
                      else engine.storage.minute_series(n, since))
        return {"range": range, "series": out}

    @app.get("/api/export.csv")
    def export_csv(target: str = "all", range: str = "24h", since: int | None = None, until: int | None = None):
        """下載量測紀錄。target＝某個目標的名稱，或 all（全部目標）。每秒的原始資料最多保留 24 小時。"""
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        names = [t["name"] for t in engine.targets]
        if target != "all" and target not in names:
            raise HTTPException(404, "找不到這個目標")
        if since is None:
            since = engine.ref_time() - RANGES[range]
        end = until or 10**12
        chosen = names if target == "all" else [target]
        series = ((n, [(t, r) for t, r in engine.storage.raw_rows(n, since) if t <= end]) for n in chosen)
        return _csv(export_mod.samples_stream(series), "measurements")

    @app.get("/api/alerts.csv")
    def alerts_csv():
        return _csv(export_mod.alerts_table(list(engine.alerts.history), list(engine.events)), "alerts")

    @app.get("/api/reports.csv")
    def reports_csv():
        return _csv(export_mod.reports_table([_report_row(r) for r in engine.storage.list_reports()]), "reports")

    @app.get("/api/curl.csv")
    def curl_csv():
        return _csv(export_mod.curl_table(list(engine.curl_results)), "speedtest")

    @app.get("/api/traceroute.csv")
    def traceroute_csv(id: str):
        if id not in engine.trace_jobs:
            raise HTTPException(404, "找不到這個分析（可能已被清除，請重新分析一次）")
        return _csv(export_mod.trace_table(engine.trace_jobs[id]), "traceroute")

    @app.get("/api/test")
    def test_get():
        t = dict(engine.test)
        t["now"] = int(time.time())
        return t

    @app.post("/api/test/start")
    def test_start(body: dict):
        m = body.get("minutes")                 # None = 持續監測（手動停止）
        if m is not None:
            if not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= 720:
                raise HTTPException(400, "minutes 必須是 1–720 的整數")
        if engine.test["state"] == "running":
            raise HTTPException(409, "已有測試正在進行")
        if not engine.measuring:
            raise HTTPException(409, "量測已停止。請先按「開始量測」，再開始定時測試。")
        label = str(body.get("label", "")).strip()[:40]
        if "<" in label or ">" in label:
            raise HTTPException(400, "標籤不可包含 < >")
        return engine.start_test(m, label)

    @app.get("/api/jitter")
    def jitter(range: str = "5m"):
        if range not in RANGES:
            raise HTTPException(400, f"range 必須是 {list(RANGES)} 其中之一")
        return {"range": range, **engine.jitter(int(time.time()) - RANGES[range])}

    def _report_row(r):
        d = r["data"]
        tg = d.get("targets", {})
        rn = engine.router_name or "路由器"
        ext = [v for k, v in tg.items() if k != rn and v.get("avg") is not None and not v.get("unreachable")]
        med = lambda key: (sorted(v[key] for v in ext if v.get(key) is not None) or [None])[len([1 for v in ext if v.get(key) is not None]) // 2]
        jt = d.get("jitter") or {}
        return {"id": r["id"], "started": r["started"], "finished": r["finished"], "label": r["label"],
                "context": d.get("context", {}), "router": tg.get(rn),
                "external": {k: med(k) for k in ("avg", "p95", "jitter", "loss_pct")},
                "spikes": {"per_min": jt.get("per_min"), "period_s": jt.get("period_s")}}

    @app.get("/api/reports")
    def reports_list():
        return [_report_row(r) for r in engine.storage.list_reports()]

    @app.get("/api/reports/{rid}")
    def reports_get(rid: int):
        r = engine.storage.get_report(rid)
        if not r:
            raise HTTPException(404, "找不到這份報告")
        return r

    @app.patch("/api/reports/{rid}")
    def reports_rename(rid: int, body: dict):
        label = str(body.get("label", "")).strip()[:40]
        if not label or "<" in label or ">" in label:
            raise HTTPException(400, "標籤不可為空，也不可包含 < >")
        if not engine.storage.rename_report(rid, label):
            raise HTTPException(404, "找不到這份報告")
        return {"ok": True}

    @app.delete("/api/reports/{rid}")
    def reports_delete(rid: int):
        if not engine.storage.delete_report(rid):
            raise HTTPException(404, "找不到這份報告")
        return {"ok": True}

    @app.post("/api/test/stop")
    def test_stop():
        if engine.test["state"] != "running":
            raise HTTPException(409, "目前沒有進行中的測試")
        return engine.finish_test()

    @app.post("/api/curl")
    async def curl_start(body: dict | None = None):
        mb = (body or {}).get("mb", 25)
        if mb not in speedtest.SIZES_MB:
            raise HTTPException(400, f"mb 必須是 {list(speedtest.SIZES_MB)} 其中之一")
        if engine.curl_busy:
            raise HTTPException(409, "下載測試進行中")
        res = await engine.run_curl_test(mb)
        res["mb"] = mb
        return res

    @app.get("/api/curl")
    def curl_list():
        return {"busy": engine.curl_busy, "results": list(engine.curl_results)}

    @app.post("/api/traceroute")
    async def traceroute_start(body: dict):
        host = str(body.get("target", "")).strip()
        if not trace_mod.valid_target(host):
            raise HTTPException(400, "目標格式不正確（只接受網址或 IP）")
        if any(j["running"] for j in engine.trace_jobs.values()):
            raise HTTPException(409, "已有路徑分析正在進行")
        jid = str(int(time.time() * 1000))
        engine.trace_jobs[jid] = {"id": jid, "target": host, "running": True, "hops": [], "analysis": None}
        if len(engine.trace_jobs) > 5:
            engine.trace_jobs.pop(next(iter(engine.trace_jobs)))
        asyncio.create_task(_run_trace(engine, jid, host))
        return {"id": jid}

    @app.get("/api/traceroute/{jid}")
    def traceroute_get(jid: str):
        if jid not in engine.trace_jobs:
            raise HTTPException(404, "找不到這個分析")
        return engine.trace_jobs[jid]

    @app.get("/api/stream")
    async def stream():
        """SSE：保持連線，每秒收到一筆最新結果。格式：'data: {json}\\n\\n'。"""
        q: asyncio.Queue = asyncio.Queue()
        engine.subscribers.add(q)

        async def gen():
            try:
                yield f"data: {json.dumps(engine.latest)}\n\n"
                while True:
                    item = await q.get()
                    if item is None:       # 伺服器要關閉了
                        return
                    yield f"data: {json.dumps(item)}\n\n"
            finally:
                engine.subscribers.discard(q)
                engine.last_seen = time.monotonic()     # 從網頁關掉的這一刻開始算「關掉多久了」

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache"})

    @app.get("/world.json")
    def world():
        """路徑分析地圖用的簡化世界輪廓（內建檔案，不需要連到任何地圖網站）。"""
        return FileResponse(BASE / "web" / "world.json", media_type="application/json",
                            headers={"Cache-Control": "max-age=86400"})

    @app.get("/icon.svg")
    def icon():
        return FileResponse(BASE / "web" / "icon.svg", media_type="image/svg+xml")

    @app.get("/")
    def index():
        return FileResponse(BASE / "web" / "index.html")

    return app


# uvicorn 啟動時會找這個名稱：`uvicorn app:app`（serve.py 也是用它）。
# 正式執行：打開時「不量測」（要在網頁按「開始量測」），關掉網頁約 1 分鐘後自動結束。
# 進階：環境變數 NETSONAR_START_MEASURING=1 → 一打開就量測；NETSONAR_AUTO_QUIT=0 → 關掉網頁也不自動結束。
app = (create_app(start_measuring=os.environ.get("NETSONAR_START_MEASURING") == "1",
                  auto_quit=os.environ.get("NETSONAR_AUTO_QUIT", "1") != "0")
       if os.environ.get("NETPROBE_NO_AUTOAPP") != "1" else None)
