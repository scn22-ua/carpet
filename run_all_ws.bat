@echo off
title Lanzar TODAS las Estaciones WS-01 a WS-15
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ===================================================
echo   Lanzando 15 Estaciones de Riego (WS-01..WS-15)
echo   Monitores en puertos 9101-9115
echo   Paneles Web en puertos 3001-3015
echo ===================================================

:: WS-01 (Monitor: 9101, Web: 3001)
start "WM_WS_M WS-01" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9101 127.0.0.1:9000 WS-01"
timeout /t 1 >nul
start "WM_WS_E WS-01" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001"
timeout /t 1 >nul

:: WS-02 (Monitor: 9102, Web: 3002)
start "WM_WS_M WS-02" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9102 127.0.0.1:9000 WS-02"
timeout /t 1 >nul
start "WM_WS_E WS-02" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9102 --ws-id WS-02 --web-port 3002"
timeout /t 1 >nul

:: WS-03 (Monitor: 9103, Web: 3003)
start "WM_WS_M WS-03" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9103 127.0.0.1:9000 WS-03"
timeout /t 1 >nul
start "WM_WS_E WS-03" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9103 --ws-id WS-03 --web-port 3003"
timeout /t 1 >nul

:: WS-04 (Monitor: 9104, Web: 3004)
start "WM_WS_M WS-04" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9104 127.0.0.1:9000 WS-04"
timeout /t 1 >nul
start "WM_WS_E WS-04" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9104 --ws-id WS-04 --web-port 3004"
timeout /t 1 >nul

:: WS-05 (Monitor: 9105, Web: 3005)
start "WM_WS_M WS-05" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9105 127.0.0.1:9000 WS-05"
timeout /t 1 >nul
start "WM_WS_E WS-05" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9105 --ws-id WS-05 --web-port 3005"
timeout /t 1 >nul

:: WS-06 (Monitor: 9106, Web: 3006)
start "WM_WS_M WS-06" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9106 127.0.0.1:9000 WS-06"
timeout /t 1 >nul
start "WM_WS_E WS-06" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9106 --ws-id WS-06 --web-port 3006"
timeout /t 1 >nul

:: WS-07 (Monitor: 9107, Web: 3007)
start "WM_WS_M WS-07" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9107 127.0.0.1:9000 WS-07"
timeout /t 1 >nul
start "WM_WS_E WS-07" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9107 --ws-id WS-07 --web-port 3007"
timeout /t 1 >nul

:: WS-08 (Monitor: 9108, Web: 3008)
start "WM_WS_M WS-08" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9108 127.0.0.1:9000 WS-08"
timeout /t 1 >nul
start "WM_WS_E WS-08" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9108 --ws-id WS-08 --web-port 3008"
timeout /t 1 >nul

:: WS-09 (Monitor: 9109, Web: 3009)
start "WM_WS_M WS-09" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9109 127.0.0.1:9000 WS-09"
timeout /t 1 >nul
start "WM_WS_E WS-09" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9109 --ws-id WS-09 --web-port 3009"
timeout /t 1 >nul

:: WS-10 (Monitor: 9110, Web: 3010)
start "WM_WS_M WS-10" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9110 127.0.0.1:9000 WS-10"
timeout /t 1 >nul
start "WM_WS_E WS-10" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9110 --ws-id WS-10 --web-port 3010"
timeout /t 1 >nul

:: WS-11 (Monitor: 9111, Web: 3011)
start "WM_WS_M WS-11" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9111 127.0.0.1:9000 WS-11"
timeout /t 1 >nul
start "WM_WS_E WS-11" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9111 --ws-id WS-11 --web-port 3011"
timeout /t 1 >nul

:: WS-12 (Monitor: 9112, Web: 3012)
start "WM_WS_M WS-12" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9112 127.0.0.1:9000 WS-12"
timeout /t 1 >nul
start "WM_WS_E WS-12" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9112 --ws-id WS-12 --web-port 3012"
timeout /t 1 >nul

:: WS-13 (Monitor: 9113, Web: 3013)
start "WM_WS_M WS-13" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9113 127.0.0.1:9000 WS-13"
timeout /t 1 >nul
start "WM_WS_E WS-13" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9113 --ws-id WS-13 --web-port 3013"
timeout /t 1 >nul

:: WS-14 (Monitor: 9114, Web: 3014)
start "WM_WS_M WS-14" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9114 127.0.0.1:9000 WS-14"
timeout /t 1 >nul
start "WM_WS_E WS-14" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9114 --ws-id WS-14 --web-port 3014"
timeout /t 1 >nul

:: WS-15 (Monitor: 9115, Web: 3015)
start "WM_WS_M WS-15" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_M.py 9115 127.0.0.1:9000 WS-15"
timeout /t 1 >nul
start "WM_WS_E WS-15" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9115 --ws-id WS-15 --web-port 3015"

echo.
echo Todas las estaciones lanzadas. Pulsa cualquier tecla para salir.
pause >nul
