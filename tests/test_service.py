"""安裝／解除安裝腳本（scripts/netsonar_service.sh）的測試。

用「測試模式」執行：NETSONAR_HOME 指到暫存資料夾、NETSONAR_DRYRUN=1 不呼叫 macOS 的 launchctl。
真正的 launchd 行為（點圖示打開、登入時不會自動啟動）只能在 Mac 上確認。
"""
import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "netsonar_service.sh"
LABEL = "com.netsonar.dashboard"

pytestmark = pytest.mark.skipif(not shutil.which("shasum") or not shutil.which("bash"), reason="需要 bash 與 shasum")


def _prepare_home(tmp_path: Path) -> Path:
    """先放一個「假的」虛擬環境，安裝時就不必真的下載套件（測試不需要網路）。"""
    home = tmp_path / "home"
    venv = home / "Library/Application Support/NetSonar/app/.venv"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin/python").symlink_to(sys.executable)
    req_hash = subprocess.run(["shasum", str(ROOT / "dashboard/requirements.txt")],
                              capture_output=True, text=True, check=True).stdout.split()[0]
    (venv / ".req_hash").write_text(req_hash + "\n")
    return home


def _run(home: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "NETSONAR_HOME": str(home), "NETSONAR_DRYRUN": "1", "PYTHON": sys.executable}
    return subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=120)


def test_install_does_not_autostart_at_login_and_removes_old_setting(tmp_path):
    home = _prepare_home(tmp_path)
    old = home / f"Library/LaunchAgents/{LABEL}.plist"          # 舊版：登入時自動啟動
    old.parent.mkdir(parents=True)
    old.write_text("old")

    r = _run(home, "install")
    assert r.returncode == 0, r.stdout + r.stderr
    assert not old.exists() and "舊版" in r.stdout                # 舊的自動啟動設定被移除
    assert not any((home / "Library/LaunchAgents").iterdir())     # LaunchAgents 裡沒有任何 NetSonar 設定

    base = home / "Library/Application Support/NetSonar"
    plist = plistlib.loads((base / f"{LABEL}.plist").read_bytes())
    assert plist["Label"] == LABEL and plist["RunAtLoad"] is True  # 點圖示（載入）時才啟動
    assert "KeepAlive" not in plist                                # 結束就是結束，不會被系統自動重開
    assert plist["EnvironmentVariables"]["NETSONAR_SERVICE"] == "1"
    assert plist["ProgramArguments"][1].endswith("/app/serve.py") and (base / "app/serve.py").exists()

    launcher = home / "Applications/NetSonar.app/Contents/MacOS/NetSonar"
    text = launcher.read_text(encoding="utf-8")
    assert 'PLIST="$HOME/Library/Application Support/NetSonar/$LABEL.plist"' in text and "LaunchAgents" not in text
    assert 'LABEL="com.netsonar.dashboard"' in text
    assert 'URL="http://127.0.0.1:8765"' in text and os.access(launcher, os.X_OK)
    assert subprocess.run(["bash", "-n", str(launcher)]).returncode == 0
    info = plistlib.loads((home / "Applications/NetSonar.app/Contents/Info.plist").read_bytes())
    assert info["CFBundleExecutable"] == "NetSonar" and (home / "Applications/NetSonar.app/Contents/Resources/NetSonar.icns").exists()

    st = _run(home, "status")
    assert "已安裝" in st.stdout


def test_uninstall_keeps_data_unless_purge(tmp_path):
    home = _prepare_home(tmp_path)
    assert _run(home, "install").returncode == 0
    base = home / "Library/Application Support/NetSonar"
    (base / "data/netprobe.db").write_text("x")

    r = _run(home, "uninstall")
    assert r.returncode == 0
    assert not (base / f"{LABEL}.plist").exists() and not (base / "app").exists()
    assert not (home / "Applications/NetSonar.app").exists()
    assert (base / "data/netprobe.db").exists()                   # 沒加 --purge：資料保留

    assert _run(home, "install").returncode == 0
    assert _run(home, "uninstall", "--purge").returncode == 0
    assert not base.exists()
