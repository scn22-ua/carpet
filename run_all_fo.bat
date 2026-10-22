@echo off
title Lanzar Operarios FO-01 a FO-07
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ===================================================
echo   Lanzando 7 Operarios de Campo (FO-01..FO-07)
echo   Autenticación: vía Kafka
echo   Kafka: caboose.proxy.rlwy.net:37367
echo ===================================================

start "Operario FO-01" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01"
timeout /t 1 >nul
start "Operario FO-02" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-02"
timeout /t 1 >nul
start "Operario FO-03" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-03"
timeout /t 1 >nul
start "Operario FO-04" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-04"
timeout /t 1 >nul
start "Operario FO-05" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-05"
timeout /t 1 >nul
start "Operario FO-06" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-06"
timeout /t 1 >nul
start "Operario FO-07" cmd /k "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && python WM_FO.py caboose.proxy.rlwy.net:37367 FO-07"

echo.
echo Todos los operarios lanzados. Pulsa cualquier tecla para salir.
pause >nul
