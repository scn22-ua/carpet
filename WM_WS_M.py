# -*- coding: utf-8 -*-
"""
WM_WS_M - Monitor de la Estación de Riego (Watering Station Monitor)
Asignatura: Sistemas Distribuidos (SD 26/27) - Water Management Network

Parámetros requeridos por el PDF:
    python WM_WS_M.py <socket_port_engine> <central_ip:port> <ws_id>

Ejemplo:
    python WM_WS_M.py 9101 127.0.0.1:9000 WS-01
"""

import sys
import time
import socket
import select
import threading
import logging

# Configuración de codificación UTF-8 segura en Windows
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

import common.protocol as protocol

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [WM_WS_M] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("WM_WS_M")

class WateringStationMonitor:
    def __init__(self, engine_port: int, central_addr: tuple, ws_id: str):
        self.engine_port = engine_port
        self.central_host, self.central_port = central_addr
        self.ws_id = ws_id
        
        self.running = True
        self.central_sock = None
        self.central_lock = threading.Lock()
        
        self.engine_sock = None
        self.is_fault = False  # True si hay fuga/avería detectada

    def connect_and_auth_central(self) -> bool:
        """
        Se conecta a WM_Central y envía la trama de autenticación.
        """
        while self.running:
            try:
                logger.info(f"Conectando a WM_Central en {self.central_host}:{self.central_port}...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.central_host, self.central_port))
                
                # Autenticación con protocolo <STX><DATA><ETX><LRC>
                auth_payload = f"AUTH{protocol.DELIMITER}{self.ws_id}{protocol.DELIMITER}Estacion_{self.ws_id}"
                protocol.send_frame(s, auth_payload, wait_ack=True)
                
                # Recibir confirmación de Central
                resp = protocol.recv_frame(s, send_ack=True, timeout=10.0)
                if resp.startswith("AUTH_OK"):
                    logger.info(f"✅ Autenticado con éxito en WM_Central como {self.ws_id}")
                    with self.central_lock:
                        self.central_sock = s
                    return True
                else:
                    logger.error(f"Respuesta inesperada de Central: {resp}")
                    s.close()
            except Exception as e:
                logger.warning(f"No se pudo conectar a Central: {e}. Reintentando en 3s...")
                time.sleep(3)
        return False

    def send_to_central(self, msg: str):
        with self.central_lock:
            if self.central_sock:
                try:
                    protocol.send_frame(self.central_sock, msg, wait_ack=True, timeout=5.0)
                except Exception as e:
                    logger.error(f"Error enviando mensaje a Central: {e}")
                    self.central_sock = None

    def start_engine_server(self):
        """
        Abre servidor de sockets local esperando a WM_WS_E.
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(('0.0.0.0', self.engine_port))
        server_sock.listen(2)
        logger.info(f"Esperando conexión de WM_WS_E en puerto local {self.engine_port}...")

        while self.running:
            try:
                sock, addr = server_sock.accept()
                logger.info(f"✅ Conectado Engine WM_WS_E desde {addr}")
                self.engine_sock = sock
                self.monitor_engine_loop()
            except Exception as e:
                if self.running:
                    logger.error(f"Error en servidor para Engine: {e}")
                time.sleep(1)

    def monitor_engine_loop(self):
        """
        Supervisión continua cada 1 segundo (Requisito pág. 11):
        Envía PING al Engine. Si recibe KO o no responde -> notifica fuga a Central.
        Una desconexión de socket (Engine reiniciándose) NO es fuga: simplemente
        se cierra el bucle y start_engine_server espera al Engine de nuevo.
        """
        sock = self.engine_sock
        consecutive_timeouts = 0
        MAX_TIMEOUTS = 3  # fallos seguidos necesarios para declarar fuga

        while self.running and sock:
            try:
                # 1. Enviar comprobación de estado de salud
                protocol.send_frame(sock, "PING", wait_ack=False, timeout=2.0)

                # 2. Esperar respuesta de salud (PONG / OK o KO)
                r, _, _ = select.select([sock], [], [], 2.0)
                if not r:
                    # Timeout en respuesta
                    consecutive_timeouts += 1
                    logger.warning(f"Sin respuesta del Engine ({consecutive_timeouts}/{MAX_TIMEOUTS})...")
                    if consecutive_timeouts >= MAX_TIMEOUTS:
                        self.handle_fault("TIMEOUT_NO_RESP")
                else:
                    reply = protocol.recv_frame(sock, send_ack=False, timeout=2.0)
                    if not reply:
                        # Engine cerró la conexión limpiamente -> no es fuga, salir del bucle
                        logger.info("Engine desconectado. Esperando reconexión...")
                        break

                    if reply == "KO" or "KO" in reply:
                        self.handle_fault("KO_LEAK_SIMULATED")
                    else:
                        # Salud correcta (PONG / OK)
                        consecutive_timeouts = 0
                        if self.is_fault:
                            logger.info("🟢 Salud del Engine restablecida (OK). Informando a Central...")
                            self.send_to_central(f"RESOLVED{protocol.DELIMITER}{self.ws_id}")
                            self.is_fault = False

                time.sleep(1.0)
            except OSError as e:
                # Error de red/socket (ej. WinError 10054: conexión interrumpida por el host remoto)
                # El Engine se ha reconectado o reiniciado -> NO es fuga, simplemente salir del bucle
                logger.info(f"Engine desconectado ({e}). Esperando reconexión...")
                break
            except Exception as e:
                logger.warning(f"Error inesperado en ciclo de salud con Engine: {e}")
                break

        if self.engine_sock:
            try:
                self.engine_sock.close()
            except Exception:
                pass
            self.engine_sock = None

    def handle_fault(self, reason: str):
        if not self.is_fault:
            self.is_fault = True
            logger.error(f"🚨 ALERTA CRÍTICA: Avería/Fuga detectada en {self.ws_id} ({reason})")
            print("\n" + "!" * 65)
            print(f"   [ALERTA MONITOR {self.ws_id}] FUGA / AVERIA DETECTADA ({reason})")
            print("   Enviando reporte de avería inmediato a CENTRAL...")
            print("!" * 65 + "\n")
            self.send_to_central(f"FAULT{protocol.DELIMITER}{self.ws_id}{protocol.DELIMITER}LEAK")

    def run(self):
        # 1. Conectar y autenticar ante Central
        if not self.connect_and_auth_central():
            return

        # 2. Hilo de vigilancia de conexión con Central (reconexión si cae)
        def central_keepalive():
            while self.running:
                time.sleep(5)
                if not self.central_sock:
                    self.connect_and_auth_central()
        
        t_ka = threading.Thread(target=central_keepalive, daemon=True)
        t_ka.start()

        # 3. Arrancar servidor de sockets para el Engine
        self.start_engine_server()

def main():
    if len(sys.argv) < 4:
        print("Uso obligatorio: python WM_WS_M.py <socket_port_engine> <central_ip:port> <ws_id>")
        print("Ejemplo: python WM_WS_M.py 9101 127.0.0.1:9000 WS-01")
        sys.exit(1)

    engine_port = int(sys.argv[1])
    central_arg = sys.argv[2]
    ws_id = sys.argv[3]

    if ":" not in central_arg:
        print("Error: central_ip:port debe tener formato host:puerto (ej: 127.0.0.1:9000)")
        sys.exit(1)

    c_host, c_port = central_arg.split(":")
    c_port = int(c_port)

    print("=" * 65)
    print(f"   🛡️ WATERING STATION MONITOR - {ws_id} (SD 26/27) 🛡️")
    print(f"   * Puerto para WM_WS_E  : {engine_port}")
    print(f"   * Conexión a Central   : {c_host}:{c_port}")
    print(f"   * ID de Estación       : {ws_id}")
    print("=" * 65)

    monitor = WateringStationMonitor(engine_port, (c_host, c_port), ws_id)
    try:
        monitor.run()
    except KeyboardInterrupt:
        print("\nMonitor detenido por el usuario.")

if __name__ == '__main__':
    main()
