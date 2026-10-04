@echo off
title Operario de Campo FO-01 (Interactivo)
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ===================================================
echo     Iniciando Operario FO-01 (Modo Interactivo)
echo     Kafka: caboose.proxy.rlwy.net:37367 / 9092
echo ===================================================
python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01
pause
