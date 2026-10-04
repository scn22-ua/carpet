@echo off
title Estacion de Riego WS-01
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ===================================================
echo     Iniciando Estacion WS-01 (Monitor + Engine)
echo     Panel Web Dedicado WS-01: http://localhost:3001
echo ===================================================
start "WM_WS_M (WS-01)" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9101 127.0.0.1:9000 WS-01"
timeout /t 2 >nul
start "WM_WS_E (WS-01)" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001"
