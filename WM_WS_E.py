# -*- coding: utf-8 -*-
"""
WM_WS_E - Motor de la Estación de Riego (Watering Station Engine)
Asignatura: Sistemas Distribuidos (SD 26/27) - Water Management Network

Parámetros requeridos por el PDF:
    python WM_WS_E.py <kafka_broker> <monitor_ip:port> [--ws-id <ws_id>] [--web-port <port>]

Ejemplos:
    python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001
    python WM_WS_E.py 9092 127.0.0.1:9101 --ws-id WS-01
"""

import os
import sys
import time
import socket
import select
import threading
import random
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

import common.protocol as protocol
import common.kafka_config as kconfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [WM_WS_E] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("WM_WS_E")
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# Instancia global para las rutas Flask del motor
engine_instance = None

ws_app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), 'web', 'templates'),
    static_folder=os.path.join(os.path.dirname(__file__), 'web', 'static')
)

@ws_app.route('/')
def ws_index():
    if not engine_instance:
        return "Motor no inicializado", 500
    return render_template('ws_index.html', ws_id=engine_instance.ws_id, web_port=engine_instance.web_port)

@ws_app.route('/api/status')
def ws_api_status():
    if not engine_instance:
        return jsonify({"error": "Motor no inicializado"}), 500
    return jsonify(engine_instance.get_status_dict())

@ws_app.route('/api/action', methods=['POST'])
def ws_api_action():
    if not engine_instance:
        return jsonify({"success": False, "error": "Motor no inicializado"}), 500
    data = request.json or {}
    action = data.get("action")

    if action == "SIMULATE_KO":
        engine_instance.simulated_ko = True
        if engine_instance.is_watering:
            engine_instance.stop_requested = True
        engine_instance.add_event("FALLO", "MODO KO/FUGA ACTIVADO desde panel web de estacion.")
        logger.warning(f"Simulacion de fuga/KO activada en {engine_instance.ws_id} via Web")
        return jsonify({"success": True, "message": "Modo KO activado: el monitor reportara fuga a Central."})

    elif action == "RESOLVE_KO":
        engine_instance.simulated_ko = False
        engine_instance.add_event("SALUD", "Averia resuelta. Motor en estado OK (PONG).")
        logger.info(f"Simulacion de fuga resuelta en {engine_instance.ws_id} via Web")
        return jsonify({"success": True, "message": "Averia resuelta: el motor responde OK."})

    elif action == "STOP_WATERING":
        if engine_instance.is_watering:
            engine_instance.stop_requested = True
            engine_instance.add_event("PARADA", "Parada de emergencia solicitada desde panel web.")
            logger.info(f"Parada manual de riego solicitada en {engine_instance.ws_id}")
            return jsonify({"success": True, "message": "Riego detenido correctamente."})
        else:
            return jsonify({"success": False, "error": "No hay ningun riego activo en este momento."})

    elif action == "START_WATERING":
        if engine_instance.simulated_ko:
            return jsonify({"success": False, "error": "No se puede regar: la estación tiene una avería activa (KO)."})
        if engine_instance.is_watering:
            return jsonify({"success": False, "error": "Ya hay un riego en curso."})
        
        duration = int(data.get("duration", 15))
        operator = f"LOCAL_{engine_instance.ws_id}"
        engine_instance.start_watering(duration, operator)
        return jsonify({"success": True, "message": f"Riego manual iniciado por {duration} segundos."})

    return jsonify({"success": False, "error": f"Acción desconocida: {action}"}), 400


class WateringStationEngine:
    def __init__(self, kafka_broker: str, monitor_addr: tuple, ws_id: str = "WS-01", web_port: int = 3001):
        self.kafka_broker = kafka_broker
        self.monitor_host, self.monitor_port = monitor_addr
        self.ws_id = ws_id
        self.web_port = web_port

        self.running = True
        self.monitor_sock = None
        self.monitor_connected = False
        self.simulated_ko = False  # Alternado con tecla o web para simular fuga al monitor
        
        # Estado de riego
        self.is_watering = False
        self.valve_open = False
        self.stop_requested = False
        self.current_operator = None
        self.start_time = None
        self.duration_total = 0
        self.elapsed_time = 0
        self.current_flow = 0.0
        self.accumulated_volume = 0.0
        self.watering_thread = None

        self.producer = None

        # Historial de eventos locales
        self.local_events = []
        self.events_lock = threading.Lock()
        self.add_event("SISTEMA", f"Estación {self.ws_id} iniciada. Esperando órdenes.")

    def add_event(self, event_type: str, message: str):
        with self.events_lock:
            evt = {
                "time": datetime.datetime.now().strftime("%H:%M:%S"),
                "type": event_type,
                "message": message
            }
            self.local_events.insert(0, evt)
            if len(self.local_events) > 40:
                self.local_events.pop()

    def get_status_dict(self) -> dict:
        status_str = "AVAILABLE"
        if self.simulated_ko:
            status_str = "LEAK"
        elif self.is_watering:
            status_str = "WATERING"

        with self.events_lock:
            events_copy = list(self.local_events)

        return {
            "ws_id": self.ws_id,
            "status": status_str,
            "valve_open": self.valve_open,
            "flow": round(self.current_flow, 2),
            "accumulated_volume": round(self.accumulated_volume, 2),
            "duration": self.duration_total,
            "elapsed": self.elapsed_time,
            "remaining": max(0, self.duration_total - self.elapsed_time) if self.is_watering else 0,
            "current_operator": self.current_operator or "-",
            "simulated_ko": self.simulated_ko,
            "monitor_connected": self.monitor_connected,
            "monitor_addr": f"{self.monitor_host}:{self.monitor_port}",
            "kafka_broker": self.kafka_broker,
            "web_port": self.web_port,
            "events": events_copy,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def connect_monitor(self):
        """
        Conecta por sockets al WM_WS_M y escucha los heartbeats de salud.
        """
        while self.running:
            try:
                logger.info(f"Conectando al Monitor en {self.monitor_host}:{self.monitor_port}...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.monitor_host, self.monitor_port))
                self.monitor_sock = s
                self.monitor_connected = True
                self.add_event("MONITOR", f"Conectado a Monitor WM_WS_M en {self.monitor_host}:{self.monitor_port}")
                logger.info("✅ Conectado al Monitor WM_WS_M con éxito.")

                while self.running:
                    # Esperar PING del monitor
                    msg = protocol.recv_frame(s, send_ack=False, timeout=10.0)
                    if not msg:
                        break
                    
                    if "PING" in msg:
                        # Si está en modo KO simulado, responder KO, de lo contrario OK
                        if self.simulated_ko:
                            protocol.send_frame(s, "KO", wait_ack=False)
                        else:
                            protocol.send_frame(s, "PONG", wait_ack=False)

            except Exception as e:
                self.monitor_connected = False
                logger.warning(f"Conexión con Monitor perdida: {e}. Reconectando en 3s...")
                self.add_event("MONITOR", f"Conexión con Monitor interrumpida ({e})")
                if self.monitor_sock:
                    try:
                        self.monitor_sock.close()
                    except Exception:
                        pass
                    self.monitor_sock = None
                time.sleep(3)

    def start_kafka(self):
        logger.info(f"Conectando a Kafka broker en {self.kafka_broker}...")
        try:
            self.producer = kconfig.create_producer(self.kafka_broker)
            logger.info("✅ Productor Kafka de Engine inicializado.")
        except Exception as e:
            logger.error(f"❌ Error al inicializar productor Kafka en Engine: {e}")

        # Consumidor de comandos dirigidos a esta estación
        t = threading.Thread(target=self.kafka_commands_loop, daemon=True)
        t.start()

    def kafka_commands_loop(self):
        """
        Escucha órdenes desde CENTRAL en wm-commands.
        """
        consumer = None
        while self.running:
            try:
                consumer = kconfig.create_consumer(
                    topic=kconfig.TOPIC_COMMANDS,
                    bootstrap_servers=self.kafka_broker,
                    group_id=f"wm-engine-{self.ws_id}-group"
                )
                break
            except Exception as e:
                logger.warning(f"Error conectando consumidor de comandos Kafka: {e}. Reintentando...")
                time.sleep(3)

        logger.info(f"👂 Escuchando órdenes para {self.ws_id} en topic '{kconfig.TOPIC_COMMANDS}'...")
        while self.running:
            try:
                msg_batch = consumer.poll(timeout_ms=500)
                for tp, messages in msg_batch.items():
                    for record in messages:
                        data = record.value
                        if isinstance(data, dict):
                            target_ws = data.get("ws_id")
                            # Procesar si es para esta estación o para todas
                            if target_ws in (self.ws_id, "ALL"):
                                self.process_command(data)
            except Exception as e:
                logger.error(f"Error procesando comandos Kafka: {e}")
                time.sleep(1)

    def process_command(self, data: dict):
        cmd = data.get("command")
        logger.info(f"📥 Orden recibida desde Central: {cmd}")

        if cmd == "START_WATERING":
            duration = int(data.get("duration", 15))
            operator_id = data.get("operator_id", "CENTRAL")
            self.start_watering(duration, operator_id)

        elif cmd in ("STOP_WATERING", "BLOCK"):
            if self.is_watering:
                logger.info(f"Deteniendo suministro de agua inmediatamente por orden: {cmd}")
                self.stop_requested = True
            if cmd == "BLOCK":
                self.add_event("BLOQUEO", "Estacion BLOQUEADA por orden de Central.")

        elif cmd == "ACTIVATE":
            logger.info("Estacion reactivada por Central.")
            self.add_event("ACTIVAR", "Estacion reactivada por Central.")

    def start_watering(self, duration_sec: int, operator_id: str):
        if self.is_watering:
            logger.warning("Riego ya en curso. Ignorando solicitud duplicada.")
            return

        if self.simulated_ko:
            logger.error("No se puede iniciar el riego: fallo/fuga activa en el motor.")
            self.add_event("AVISO", "Denegado inicio de riego: averia/fuga activa.")
            return

        self.is_watering = True
        self.valve_open = True
        self.stop_requested = False
        self.current_operator = operator_id
        self.start_time = datetime.datetime.now()
        self.duration_total = duration_sec
        self.elapsed_time = 0
        self.accumulated_volume = 0.0

        self.add_event("RIEGO", f"Electrovalvula ABIERTA por {operator_id} ({duration_sec}s)")

        print("\n" + "=" * 65)
        print(f"   [ELECTROVALVULA ABIERTA] Riego iniciado en {self.ws_id}")
        print(f"   * Operario : {operator_id}")
        print(f"   * Duracion : {duration_sec} segundos")
        print("   (Pulsa 'q' + Enter para detener manualmente)")
        print("=" * 65 + "\n")

        self.watering_thread = threading.Thread(target=self._watering_worker, args=(duration_sec,), daemon=True)
        self.watering_thread.start()

    def _watering_worker(self, duration_sec: int):
        elapsed = 0
        while self.is_watering and elapsed < duration_sec:
            if self.stop_requested or self.simulated_ko:
                break

            time.sleep(1.0)
            elapsed += 1
            self.elapsed_time = elapsed

            # Caudalimetro simulado (ej: 12.0 L/min)
            current_flow = random.uniform(11.5, 12.8)
            self.current_flow = current_flow

            # Litros en 1 segundo = caudal (L/min) / 60
            liters_this_sec = current_flow / 60.0
            self.accumulated_volume += liters_this_sec

            # Requisito 7: enviar telemetria a Central y FO cada segundo por Kafka
            telemetry = {
                "ws_id": self.ws_id,
                "operator_id": self.current_operator,
                "event": "UPDATE",
                "flow": round(current_flow, 2),
                "accumulated_vol": round(self.accumulated_volume, 2),
                "duration": elapsed,
                "remaining": max(0, duration_sec - elapsed),
                "timestamp": datetime.datetime.now().isoformat()
            }
            if self.producer:
                try:
                    self.producer.send(kconfig.TOPIC_TELEMETRY, telemetry)
                    self.producer.flush()
                except Exception as e:
                    logger.error(f"Error enviando telemetria Kafka: {e}")

            print(f"   [RIEGO {self.ws_id}] {elapsed}/{duration_sec}s | Caudal: {current_flow:.1f} L/min | Total: {self.accumulated_volume:.2f} L", end="\r")

        # Fin del riego
        self.valve_open = False
        self.is_watering = False
        self.current_flow = 0.0
        event_status = "FINISHED" if not (self.stop_requested or self.simulated_ko) else "ABORTED"

        self.add_event("RIEGO", f"Electrovalvula CERRADA ({event_status}) - Total: {self.accumulated_volume:.2f} L en {elapsed}s")

        print("\n\n" + "=" * 65)
        print(f"   [ELECTROVALVULA CERRADA] Fin del suministro en {self.ws_id}")
        print(f"   * Motivo          : {event_status}")
        print(f"   * Volumen Total   : {self.accumulated_volume:.2f} Litros")
        print(f"   * Tiempo Riego    : {elapsed} segundos")
        print("=" * 65 + "\n")

        # Notificar fin a Central y Operario
        final_payload = {
            "ws_id": self.ws_id,
            "operator_id": self.current_operator,
            "event": event_status,
            "flow": 0.0,
            "accumulated_vol": round(self.accumulated_volume, 2),
            "duration": elapsed,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "timestamp": datetime.datetime.now().isoformat()
        }
        if self.producer:
            try:
                self.producer.send(kconfig.TOPIC_TELEMETRY, final_payload)
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error enviando mensaje final de riego: {e}")

    def start_web_server(self):
        """
        Arranca la interfaz web dedicada para esta estacion en su puerto asignado.
        """
        logger.info(f"Servidor web local de {self.ws_id} arrancando en http://localhost:{self.web_port}")
        try:
            ws_app.run(host='0.0.0.0', port=self.web_port, debug=False, use_reloader=False)
        except Exception as e:
            logger.error(f"Error al iniciar servidor web local para {self.ws_id}: {e}")

    def console_interactive_loop(self):
        """
        Requisito pag. 11:
        Para simular dichas incidencias, la aplicacion WM_WS_E debera permitir que,
        en tiempo de ejecucion, se pulse una tecla para reportar un KO al monitor.
        """
        print("\n--- CONTROLES EN CONSOLA WM_WS_E ---")
        print(f"  [k] + Enter -> Activar simulacion de KO / FUGA (reporta fallo al monitor)")
        print(f"  [r] + Enter -> Reanudar funcionamiento normal OK (reparacion resuelta)")
        print(f"  [q] + Enter -> Finalizar riego en curso manualmente")
        print(f"  [s] + Enter -> Ver estado actual")
        print(f"  Interfaz Web Local activa en: http://localhost:{self.web_port}")
        print("------------------------------------\n")

        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                cmd = line.strip().lower()
                if cmd == 'k':
                    self.simulated_ko = True
                    self.add_event("FALLO", "MODO KO/FUGA activado desde la consola.")
                    print("\n>> MODO KO / FUGA ACTIVADO: El motor reportara KO al monitor en la siguiente comprobacion <<\n")
                    if self.is_watering:
                        self.stop_requested = True
                elif cmd == 'r':
                    self.simulated_ko = False
                    self.add_event("SALUD", "Salud restaurada a OK desde la consola.")
                    print("\n>> MODO NORMAL RESTABLECIDO: El motor reportara PONG/OK al monitor <<\n")
                elif cmd == 'q':
                    if self.is_watering:
                        print("\n>> Parada manual solicitada desde la consola de la estacion <<\n")
                        self.add_event("PARADA", "Parada manual solicitada desde la consola.")
                        self.stop_requested = True
                    else:
                        print("No hay riego en curso para detener.")
                elif cmd == 's':
                    status_str = "REGANDO" if self.is_watering else "EN REPOSO"
                    health_str = "AVERIADO (KO)" if self.simulated_ko else "SALUDABLE (OK)"
                    print(f"\n[ESTADO {self.ws_id}]: Estado={status_str} | Salud={health_str} | Valvula={'ABIERTA' if self.valve_open else 'CERRADA'} | Web=http://localhost:{self.web_port}\n")
            except Exception as e:
                break

    def run(self):
        # 1. Hilo para conectar con el Monitor por sockets
        t_mon = threading.Thread(target=self.connect_monitor, daemon=True)
        t_mon.start()

        # 2. Conectar a Kafka
        self.start_kafka()

        # 3. Arrancar servidor web dedicado para esta estación
        t_web = threading.Thread(target=self.start_web_server, daemon=True)
        t_web.start()

        # 4. Bucle interactivo de consola
        self.console_interactive_loop()


def main():
    global engine_instance

    if len(sys.argv) < 3:
        print("Uso obligatorio: python WM_WS_E.py <kafka_broker> <monitor_ip:port> [--ws-id <ws_id>] [--web-port <port>]")
        print("Ejemplo: python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001")
        print("Ejemplo con puerto 9092: python WM_WS_E.py 9092 127.0.0.1:9101 --ws-id WS-01")
        sys.exit(1)

    kafka_broker_raw = sys.argv[1]
    # Normalizar broker (ej: si pasan '9092' -> '127.0.0.1:9092')
    kafka_broker = kconfig.normalize_bootstrap_servers(kafka_broker_raw)[0]

    monitor_arg = sys.argv[2]
    
    ws_id = "WS-01"
    if "--ws-id" in sys.argv:
        idx = sys.argv.index("--ws-id")
        if idx + 1 < len(sys.argv):
            ws_id = sys.argv[idx + 1]

    # Determinar puerto web individual para esta estación
    web_port = None
    if "--web-port" in sys.argv:
        idx = sys.argv.index("--web-port")
        if idx + 1 < len(sys.argv):
            web_port = int(sys.argv[idx + 1])
    
    if not web_port:
        digits = "".join([c for c in ws_id if c.isdigit()])
        if digits:
            web_port = 3000 + int(digits)
        else:
            web_port = 3001

    if ":" not in monitor_arg:
        print("Error: monitor_ip:port debe tener formato host:puerto (ej: 127.0.0.1:9101)")
        sys.exit(1)

    m_host, m_port = monitor_arg.split(":")
    m_port = int(m_port)

    print("=" * 65)
    print(f"   WATERING STATION ENGINE - {ws_id} (SD 26/27)")
    print(f"   * Broker Kafka            : {kafka_broker}")
    print(f"   * Conexion a Monitor      : {m_host}:{m_port}")
    print(f"   * ID de Estacion          : {ws_id}")
    print(f"   * Panel Web Dedicado (WS) : http://localhost:{web_port}")
    print("=" * 65)

    engine_instance = WateringStationEngine(kafka_broker, (m_host, m_port), ws_id, web_port)
    try:
        engine_instance.run()
    except KeyboardInterrupt:
        print("\nEngine detenido.")

if __name__ == '__main__':
    main()
