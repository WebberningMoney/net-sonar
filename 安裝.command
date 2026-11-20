#!/usr/bin/env bash
# 雙擊這個檔案即可安裝 NetSonar。裝好後點「應用程式」裡的 NetSonar 就能使用，不需要終端機。
# （登入電腦時不會自動啟動；更新程式後再雙擊一次，就會換成新版。）
cd "$(dirname "$0")"
echo "═══ NetSonar 安裝 ═══"
bash scripts/netsonar_service.sh install
echo
read -n 1 -s -r -p "按任意鍵關閉這個視窗…"
