# -*- coding: utf-8 -*-
"""
WM_Central - Módulo Central de Control y Monitorización
Asignatura: Sistemas Distribuidos (SD 26/27) - Water Management Network

Parámetros requeridos por el PDF:
    python WM_Central.py <socket_port> <kafka_broker> [--web-port <port>]

Ejemplo de ejecución:
    python WM_Central.py 9000 caboose.proxy.rlwy.net:37367 --web-port 3000
    python WM_Central.py 9000 9092 --web-port 3000
"""

import sys
import os
import time
import socket
import select
import threading
import datetime
import logging
from flask import Flask, render_template, jsonify, request

# Configuración de codificación UTF-8 segura en Windows
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Importaciones locales
import common.protocol as protocol
import common.kafka_config as kconfig
import database.db as db

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [WM_Central] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("WM_Central")

# Inicialización de Flask para la interfaz web
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), 'web', 'templates'),
    static_folder=os.path.join(os.path.dirname(__file__), 'web', 'static')
)
# Desactivar logs ruidosos de werkzeug
logging.getLogger('werkzeug').setLevel(logging.WARNING)

class CentralSystem:
    def __init__(self, socket_port: int, kafka_broker: str, web_port: int = 3000):
        self.socket_port = socket_port
        self.kafka_broker = kafka_broker
        self.web_port = web_port
        self.running = True

        # Sockets conectados: { ws_id: client_socket }
        self.connected_ws_sockets = {}
        self.socket_lock = threading.Lock()

        # Productor y Consumidor Kafka
        self.producer = None
        self.consumer_thread = None

        # Historial reciente en memoria para panel web
        self.recent_events = []
        self.events_lock = threading.Lock()

        # Inicializar y reiniciar estados en BD (Punto 1: OFFLINE hasta que conecten)
        logger.info("Inicializando Base de Datos SQLite...")
        db.init_db()
        db.reset_stations_to_offline()
        self.add_event("SYSTEM", "CENTRAL", "AUTH", "Sistema Central iniciado. Estaciones en modo OFFLINE.")

    def add_event(self, ws_id: str, operator_id: str, status: str, details: str):
        with self.events_lock:
            evt = {
                "ws_id": ws_id,
                "operator_id": operator_id,
                "status": status,
                "details": details,
                "start_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self.recent_events.insert(0, evt)
            if len(self.recent_events) > 30:
                self.recent_events.pop()

    # =========================================================================
    # 1. SERVIDOR DE SOCKETS (Comunicación con WM_WS_M)
    # =========================================================================
    def start_socket_server(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(('0.0.0.0', self.socket_port))
        server_sock.listen(10)
        logger.info(f"✅ Servidor de Sockets escuchando en el puerto {self.socket_port}")

        while self.running:
            try:
                client_sock, client_addr = server_sock.accept()
                t = threading.Thread(target=self.handle_ws_monitor_connection, args=(client_sock, client_addr), daemon=True)
                t.start()
            except Exception as e:
                if self.running:
                    logger.error(f"Error aceptando conexión socket: {e}")
                break

    def handle_ws_monitor_connection(self, sock: socket.socket, addr):
        logger.info(f"Nueva conexión socket entrante desde {addr}")
        ws_id = None
        try:
            # 1. Handshake ENQ/ACK recomendado en protocolo
            # Recibir trama de autenticación o registro
            msg = protocol.recv_frame(sock, send_ack=True, timeout=15.0)
            if not msg:
                sock.close()
                return

            parts = msg.split(protocol.DELIMITER)
            cmd = parts[0]

            if cmd in ("AUTH", "REGISTER"):
                # Formato: AUTH#ws_id#location
                ws_id = parts[1]
                location = parts[2] if len(parts) > 2 else "Parque Urbano"
                logger.info(f"📡 Solicitud de autenticación de estación: {ws_id} ({location})")

                # Registrar o validar en BD
                db.register_station_if_new(ws_id, location)
                db.update_station_status(ws_id, "AVAILABLE")

                # Responder confirmación
                protocol.send_frame(sock, f"AUTH_OK{protocol.DELIMITER}{ws_id}", wait_ack=True)
                
                with self.socket_lock:
                    self.connected_ws_sockets[ws_id] = sock

                self.add_event(ws_id, "SYSTEM", "AUTH", f"Estación {ws_id} conectada y autenticada (DISPONIBLE)")
                logger.info(f"✅ Estación {ws_id} autenticada con éxito. Estado: DISPONIBLE")

            # 2. Bucle de recepción de eventos del Monitor (Fugas, recuperación, etc.)
            while self.running:
                try:
                    frame = protocol.recv_frame(sock, send_ack=True, timeout=30.0)
                    if not frame:
                        break
                    
                    data = frame.split(protocol.DELIMITER)
                    op = data[0]

                    if op == "FAULT":
                        # Formato: FAULT#ws_id#LEAK
                        fault_type = data[2] if len(data) > 2 else "LEAK"
                        logger.warning(f"🚨 ALERTA: Fuga o fallo detectado en {ws_id} ({fault_type})")
                        db.update_station_status(ws_id, "LEAK")
                        
                        # Si estaba regando, ordenar parada de emergencia en Kafka
                        self.send_kafka_command(ws_id, "STOP_WATERING", reason="LEAK_DETECTED")
                        self.add_event(ws_id, "SYSTEM", "LEAK", f"Fuga detectada en {ws_id}. Riego detenido.")

                    elif op == "RESOLVED":
                        logger.info(f"✅ Avería resuelta en estación {ws_id}. Vuelve a DISPONIBLE.")
                        db.update_station_status(ws_id, "AVAILABLE")
                        self.add_event(ws_id, "SYSTEM", "AUTH", f"Avería solucionada en {ws_id}. Estado: DISPONIBLE")

                    elif op == "PING":
                        # Keep-alive
                        protocol.send_frame(sock, "PONG", wait_ack=False)

                except socket.timeout:
                    # Timeout normal en select/recv, continuar
                    continue
                except (ConnectionResetError, BrokenPipeError):
                    break

        except Exception as e:
            logger.error(f"Error en comunicación socket con {addr} ({ws_id}): {e}")
        finally:
            if ws_id:
                logger.warning(f"❌ Estación {ws_id} desconectada del servidor de sockets.")
                with self.socket_lock:
                    if ws_id in self.connected_ws_sockets:
                        del self.connected_ws_sockets[ws_id]
                db.update_station_status(ws_id, "OFFLINE")
                self.add_event(ws_id, "SYSTEM", "OFFLINE", f"Estación {ws_id} desconectada (DESCONECTADA)")
            sock.close()

    # =========================================================================
    # 2. KAFKA: Streaming de eventos y peticiones
    # =========================================================================
    def start_kafka(self):
        logger.info(f"Conectando a Kafka broker en {self.kafka_broker}...")
        try:
            self.producer = kconfig.create_producer(self.kafka_broker)
            logger.info("✅ Productor Kafka inicializado con éxito")
        except Exception as e:
            logger.error(f"❌ Error al crear productor Kafka: {e}")
            return

        # Arrancar hilo consumidor de peticiones y telemetría
        self.consumer_thread = threading.Thread(target=self.kafka_consumer_loop, daemon=True)
        self.consumer_thread.start()

    def send_kafka_command(self, ws_id: str, command: str, duration: int = 0, operator_id: str = "CENTRAL", reason: str = ""):
        """
        Envía un comando a través del topic wm-commands hacia el WM_WS_E.
        """
        if not self.producer:
            logger.error("Productor Kafka no disponible")
            return
        payload = {
            "ws_id": ws_id,
            "command": command,
            "duration": duration,
            "operator_id": operator_id,
            "reason": reason,
            "timestamp": datetime.datetime.now().isoformat()
        }
        try:
            self.producer.send(kconfig.TOPIC_COMMANDS, payload)
            self.producer.flush()
            logger.info(f"📤 Orden Kafka enviada [{command}] para {ws_id} (Operario: {operator_id})")
        except Exception as e:
            logger.error(f"Error enviando orden Kafka a {ws_id}: {e}")

    def notify_operator(self, operator_id: str, ws_id: str, status: str, message: str, data: dict = None):
        """
        Envía respuesta/notificación a través del topic wm-responses hacia el WM_FO.
        """
        if not self.producer:
            return
        payload = {
            "operator_id": operator_id,
            "ws_id": ws_id,
            "status": status,
            "message": message,
            "data": data or {},
            "timestamp": datetime.datetime.now().isoformat()
        }
        try:
            self.producer.send(kconfig.TOPIC_RESPONSES, payload)
            self.producer.flush()
            logger.info(f"📤 Notificación enviada a Operario {operator_id} en '{kconfig.TOPIC_RESPONSES}': status={status} - {message}")
        except Exception as e:
            logger.error(f"Error enviando notificación a operario {operator_id}: {e}")

    def kafka_consumer_loop(self):
        """
        Consume peticiones de operarios (wm-requests) y telemetría de WS (wm-telemetry).
        """
        logger.info("Iniciando bucle consumidor Kafka...")
        consumer = None
        while self.running:
            try:
                consumer = kconfig.create_consumer(
                    topic=kconfig.TOPIC_REQUESTS,
                    bootstrap_servers=self.kafka_broker,
                    group_id="wm-central-requests-group"
                )
                telemetry_consumer = kconfig.create_consumer(
                    topic=kconfig.TOPIC_TELEMETRY,
                    bootstrap_servers=self.kafka_broker,
                    group_id="wm-central-telemetry-group"
                )
                break
            except Exception as e:
                logger.warning(f"Reintentando conexión con Kafka consumer: {e}")
                time.sleep(3)

        # Hilo secundario para telemetría
        t_telem = threading.Thread(target=self._process_telemetry, args=(telemetry_consumer,), daemon=True)
        t_telem.start()

        # Procesar solicitudes de operarios
        while self.running:
            try:
                msg_batch = consumer.poll(timeout_ms=500)
                for tp, messages in msg_batch.items():
                    for record in messages:
                        self.process_operator_request(record.value)
            except Exception as e:
                logger.error(f"Error en bucle de peticiones Kafka: {e}")
                time.sleep(1)

    def _process_telemetry(self, consumer):
        while self.running:
            try:
                msg_batch = consumer.poll(timeout_ms=500)
                for tp, messages in msg_batch.items():
                    for record in messages:
                        self.process_ws_telemetry(record.value)
            except Exception as e:
                logger.error(f"Error en procesado de telemetría: {e}")
                time.sleep(1)

    def process_operator_request(self, req: dict):
        """
        Requisito 4 del PDF: Comprobaciones para validar que la estación esté disponible.
        Notifica al operario de cada paso.
        """
        operator_id = req.get("operator_id")
        ws_id = req.get("ws_id")
        duration = int(req.get("duration", 15))
        req_id = req.get("request_id")
        extra_base = {"request_id": req_id}

        logger.info(f"📥 Solicitud de riego recibida de Operario {operator_id} para {ws_id} (Duración: {duration}s)")

        # 1. Validar operario
        op_info = db.get_operator(operator_id)
        if not op_info and operator_id != "CENTRAL":
            self.notify_operator(operator_id, ws_id, "REJECTED", f"Operario {operator_id} no está registrado en el sistema.", extra_base)
            logger.warning(f"Petición rechazada: Operario {operator_id} no registrado.")
            return

        # 2. Paso a paso de comprobaciones (Punto 4 de la especificación)
        self.notify_operator(operator_id, ws_id, "PROGRESS", f"Paso 1/3: Verificando disponibilidad de estación {ws_id}...", extra_base)

        st = db.get_station(ws_id)
        if not st:
            self.notify_operator(operator_id, ws_id, "REJECTED", f"La estación {ws_id} no existe en la red.", extra_base)
            return

        # Comprobar estado
        if st["status"] == "OFFLINE":
            self.notify_operator(operator_id, ws_id, "REJECTED", f"Denegado: Estación {ws_id} DESCONECTADA.", extra_base)
            return
        elif st["status"] == "LEAK":
            self.notify_operator(operator_id, ws_id, "REJECTED", f"Denegado: Estación {ws_id} presenta una FUGA activa.", extra_base)
            return
        elif st["status"] == "OUT_OF_SERVICE":
            self.notify_operator(operator_id, ws_id, "REJECTED", f"Denegado: Estación {ws_id} está FUERA DE SERVICIO por orden de CENTRAL.", extra_base)
            return
        elif st["status"] == "WATERING":
            self.notify_operator(operator_id, ws_id, "REJECTED", f"Denegado: Estación {ws_id} ya se encuentra REGANDO.", extra_base)
            return

        # 3. Notificar paso de autorización
        self.notify_operator(operator_id, ws_id, "PROGRESS", f"Paso 2/3: Solicitando autorización a la estación {ws_id}...", extra_base)
        time.sleep(0.4)

        # 4. Autorizar y enviar orden de riego a WS_E por Kafka
        db.update_station_status(ws_id, "WATERING")
        db.update_station_telemetry(ws_id, flow=0.0, accumulated_vol=0.0, operator_id=operator_id)
        
        self.send_kafka_command(ws_id, "START_WATERING", duration=duration, operator_id=operator_id)
        
        approved_extra = dict(extra_base)
        approved_extra.update({"duration": duration, "ws_id": ws_id})
        self.notify_operator(operator_id, ws_id, "APPROVED", f"Paso 3/3: Riego autorizado. Electroválvula abierta en {ws_id}.", approved_extra)
        self.add_event(ws_id, operator_id, "WATERING", f"Riego iniciado en {ws_id} ({duration}s)")

    def process_ws_telemetry(self, telem: dict):
        ws_id = telem.get("ws_id")
        event_type = telem.get("event")  # 'UPDATE', 'FINISHED', 'ABORTED'
        flow = float(telem.get("flow", 0.0))
        vol = float(telem.get("accumulated_vol", 0.0))
        operator_id = telem.get("operator_id", "CENTRAL")
        duration = int(telem.get("duration", 0))

        if event_type == "UPDATE":
            db.update_station_telemetry(ws_id, flow, vol, operator_id)

        elif event_type in ("FINISHED", "ABORTED"):
            status_text = "COMPLETED" if event_type == "FINISHED" else "ABORTED"
            logger.info(f"🏁 Riego finalizado en {ws_id}. Vol total: {vol:.2f} L, Duración: {duration}s. Estado: {status_text}")
            
            # Registrar en logs de BD
            start_time = telem.get("start_time", datetime.datetime.now().isoformat())
            end_time = datetime.datetime.now().isoformat()
            db.log_watering_event(ws_id, operator_id, start_time, end_time, duration, vol, status_text)

            # Restaurar estado de la estación a AVAILABLE si no está en fuga o fuera de servicio
            curr = db.get_station(ws_id)
            if curr and curr["status"] == "WATERING":
                db.update_station_status(ws_id, "AVAILABLE")

            # Notificar resumen final al operario
            self.notify_operator(operator_id, ws_id, "FINISHED", f"Riego concluido en {ws_id}.", {
                "total_volume_liters": round(vol, 2),
                "duration_sec": duration,
                "status": status_text
            })
            self.add_event(ws_id, operator_id, status_text, f"Fin de riego en {ws_id}: {vol:.1f} L consumidos en {duration}s")

    # =========================================================================
    # 3. ÓRDENES ARBITRARIAS DE CENTRAL (Punto 11 del PDF)
    # =========================================================================
    def central_start_watering(self, ws_id: str, duration: int = 15):
        """
        Iniciar un riego desde CENTRAL de forma arbitraria.
        """
        req = {
            "operator_id": "CENTRAL",
            "ws_id": ws_id,
            "duration": duration
        }
        self.process_operator_request(req)

    def central_block_ws(self, ws_id: str):
        """
        Bloquear la WS: finaliza cualquier riego y pone estado OUT_OF_SERVICE (Naranja).
        """
        logger.info(f"🚫 CENTRAL: Bloqueando estación {ws_id}")
        self.send_kafka_command(ws_id, "BLOCK", reason="CENTRAL_ORDER")
        db.update_station_status(ws_id, "OUT_OF_SERVICE")
        self.add_event(ws_id, "CENTRAL", "BLOCK", f"Estación {ws_id} bloqueada (FUERA DE SERVICIO)")

    def central_activate_ws(self, ws_id: str):
        """
        Activar la WS: vuelve a estado ACTIVADA/DISPONIBLE (Verde).
        """
        logger.info(f"🟢 CENTRAL: Activando estación {ws_id}")
        self.send_kafka_command(ws_id, "ACTIVATE", reason="CENTRAL_ORDER")
        db.update_station_status(ws_id, "AVAILABLE")
        self.add_event(ws_id, "CENTRAL", "AUTH", f"Estación {ws_id} activada por Central (DISPONIBLE)")

# Instancia global del sistema central para endpoints Flask
central_instance: CentralSystem = None

# =============================================================================
# ENDPOINTS REST PARA EL PANEL WEB
# =============================================================================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    if not central_instance:
        return jsonify({"error": "Central no lista"}), 500
    stations = db.get_all_stations()
    with central_instance.events_lock:
        logs = list(central_instance.recent_events)
    return jsonify({
        "stations": stations,
        "logs": logs,
        "timestamp": datetime.datetime.now().isoformat()
    })

@app.route('/api/action', methods=['POST'])
def api_action():
    if not central_instance:
        return jsonify({"success": False, "error": "Central no lista"}), 500
    data = request.json or {}
    action = data.get("action")
    ws_id = data.get("ws_id")
    duration = int(data.get("duration", 15))

    if not ws_id:
        return jsonify({"success": False, "error": "Falta ws_id"}), 400

    if action == "START":
        central_instance.central_start_watering(ws_id, duration)
    elif action == "BLOCK":
        central_instance.central_block_ws(ws_id)
    elif action == "ACTIVATE":
        central_instance.central_activate_ws(ws_id)
    else:
        return jsonify({"success": False, "error": f"Acción desconocida: {action}"}), 400

    return jsonify({"success": True})

def main():
    global central_instance

    if len(sys.argv) < 3:
        print("Uso obligatorio: python WM_Central.py <socket_port> <kafka_broker> [--web-port <port>]")
        print("Ejemplo: python WM_Central.py 9000 caboose.proxy.rlwy.net:37367 --web-port 3000")
        print("Ejemplo con puerto 9092: python WM_Central.py 9000 9092 --web-port 3000")
        sys.exit(1)

    socket_port = int(sys.argv[1])
    kafka_broker_raw = sys.argv[2]
    kafka_broker = kconfig.normalize_bootstrap_servers(kafka_broker_raw)[0]
    
    # Puerto web configurable o variable de entorno PORT (utilizada por Railway)
    web_port = int(os.environ.get("PORT", 3000))
    if "--web-port" in sys.argv:
        idx = sys.argv.index("--web-port")
        if idx + 1 < len(sys.argv):
            web_port = int(sys.argv[idx + 1])

    print("=" * 65)
    print("   🌿 WATER MANAGEMENT NETWORK - CENTRAL (SD 26/27) 🌿")
    print(f"   * Puerto Sockets (para WS_M) : {socket_port}")
    print(f"   * Broker Kafka              : {kafka_broker}")
    print(f"   * Servidor Web (Dashboard)  : http://localhost:{web_port}")
    print("=" * 65)

    central_instance = CentralSystem(socket_port, kafka_broker, web_port)

    # 1. Hilo Servidor de Sockets
    t_socket = threading.Thread(target=central_instance.start_socket_server, daemon=True)
    t_socket.start()

    # 2. Hilo Kafka
    central_instance.start_kafka()

    # 3. Servidor Web en hilo principal
    app.run(host='0.0.0.0', port=web_port, debug=False, use_reloader=False)

if __name__ == '__main__':
    main()
