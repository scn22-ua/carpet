# -*- coding: utf-8 -*-
"""
Script de Pruebas de Integración y Sockets
Verifica el ciclo de vida de conexión, autenticación, aviso de fuga y desconexión.
"""

import sys
import time
import socket
import threading
import database.db as db
import common.protocol as protocol
from WM_Central import CentralSystem

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

def run_test():
    print("Iniciando prueba de integración de Sockets y Central...")
    db.init_db()
    db.reset_stations_to_offline()

    test_port = 9911
    # Crear Central sin kafka activo para el test rápido de sockets
    central = CentralSystem(test_port, "dummy:9092", 3001)

    t_srv = threading.Thread(target=central.start_socket_server, daemon=True)
    t_srv.start()
    time.sleep(0.5)

    # 1. Comprobar que en la BD arranca como OFFLINE (Punto 1 del PDF)
    st = db.get_station("WS-01")
    assert st["status"] == "OFFLINE", f"Esperado OFFLINE, obtenido {st['status']}"
    print("✅ Requisito 1 verificado: Estaciones inician como OFFLINE.")

    # 2. Conectar WS_M y enviar AUTH
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('127.0.0.1', test_port))

    protocol.send_frame(client, "AUTH#WS-01#Parque_Test", wait_ack=True)
    resp = protocol.recv_frame(client, send_ack=True)
    print("Respuesta de Central a AUTH:", resp)
    assert resp == "AUTH_OK#WS-01"
    time.sleep(0.2)

    st = db.get_station("WS-01")
    assert st["status"] == "AVAILABLE", f"Esperado AVAILABLE tras autenticar, obtenido {st['status']}"
    print("✅ Requisito 2/5 verificado: Estación autenticada y marcada como AVAILABLE.")

    # 3. Enviar FAULT (simulación de fuga)
    protocol.send_frame(client, "FAULT#WS-01#LEAK", wait_ack=True)
    time.sleep(0.2)
    st = db.get_station("WS-01")
    assert st["status"] == "LEAK", f"Esperado LEAK, obtenido {st['status']}"
    print("✅ Requisito 9 verificado: Notificación de fuga recibida y estado actualizado a LEAK.")

    # 4. Enviar RESOLVED (recuperación de avería)
    protocol.send_frame(client, "RESOLVED#WS-01", wait_ack=True)
    time.sleep(0.2)
    st = db.get_station("WS-01")
    assert st["status"] == "AVAILABLE", f"Esperado AVAILABLE, obtenido {st['status']}"
    print("✅ Requisito 9 (recuperación) verificado: Avería resuelta y estado restaurado a AVAILABLE.")

    # 5. Desconectar y comprobar paso a OFFLINE
    client.close()
    time.sleep(0.3)
    st = db.get_station("WS-01")
    assert st["status"] == "OFFLINE", f"Esperado OFFLINE al desconectar, obtenido {st['status']}"
    print("✅ Desconexión verificada: Estación pasa a OFFLINE automáticamente.")

    central.running = False
    print("\n🎉 TODAS LAS PRUEBAS DE SOCKETS Y BASE DE DATOS HAN PASADO CON ÉXITO.\n")

if __name__ == '__main__':
    run_test()
