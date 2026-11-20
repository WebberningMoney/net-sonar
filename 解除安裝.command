#!/usr/bin/env bash
# 雙擊這個檔案即可移除 NetSonar（程式、「應用程式」裡的圖示；可選擇是否一併刪除歷史資料與設定）。
cd "$(dirname "$0")"
echo "═══ NetSonar 解除安裝 ═══"
read -r -p "是否同時刪除歷史資料與設定？(y/N) " a
if [ "$a" = "y" ] || [ "$a" = "Y" ]; then bash scripts/netsonar_service.sh uninstall --purge; else bash scripts/netsonar_service.sh uninstall; fi
echo
read -n 1 -s -r -p "按任意鍵關閉這個視窗…"
