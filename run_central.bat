@echo off
title WM_Central - Servidor Central
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ===================================================
echo     Iniciando WM_Central (Sockets 9000, Web 3000)
echo     Kafka: caboose.proxy.rlwy.net:37367 / 9092
echo ===================================================
python WM_Central.py 9000 caboose.proxy.rlwy.net:37367 --web-port 3000
pause
