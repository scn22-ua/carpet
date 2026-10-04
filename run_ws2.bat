@echo off
title Estacion de Riego WS-02
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ===================================================
echo     Iniciando Estacion WS-02 (Monitor + Engine)
echo     Panel Web Dedicado WS-02: http://localhost:3002
echo ===================================================
start "WM_WS_M (WS-02)" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9102 127.0.0.1:9000 WS-02"
timeout /t 2 >nul
start "WM_WS_E (WS-02)" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9102 --ws-id WS-02 --web-port 3002"
