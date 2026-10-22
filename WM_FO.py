# -*- coding: utf-8 -*-
"""
WM_FO - Aplicación del Operario de Campo (Field Operator)
Asignatura: Sistemas Distribuidos (SD 26/27) - Water Management Network

Parámetros requeridos por el PDF:
    python WM_FO.py <kafka_broker> <operator_id> [--file <ruta_fichero>]

Ejemplos:
    Modo Interactivo : python WM_FO.py caboose.proxy.rlwy.net:37367 127.0.0.1:9001 FO-01
    Modo Fichero     : python WM_FO.py caboose.proxy.rlwy.net:37367 127.0.0.1:9001 FO-01 --file servicios.txt
    Con puerto 9092  : python WM_FO.py 9092 127.0.0.1:9001 FO-01
"""

import sys
import os
import time
import uuid
import socket
import threading
import logging
from typing import Optional, Tuple

# Configuración de codificación UTF-8 segura en Windows
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

import common.kafka_config as kconfig
import common.protocol as protocol

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [WM_FO] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("WM_FO")

class FieldOperatorApp:
    def __init__(self, kafka_broker: str, operator_id: str, batch_file: Optional[str] = None):
        self.kafka_broker = kafka_broker
        self.operator_id = operator_id
        self.batch_file = batch_file
        
        self.running = True
        self.producer = None

        # Socket de autenticación con Central
        
        # Sincronización del servicio activo
        self.current_service_event = threading.Event()
        self.consumer_ready = threading.Event()
        self.auth_event = threading.Event()
        self.auth_request_id = None
        self.auth_result = None
        self.auth_message = ""
        self.current_ws_id = None
        self.current_service_result = None
        self.current_request_id = None

    # =========================================================================
    # AUTENTICACIÓN POR SOCKET TCP CON CENTRAL
    # =========================================================================
    def authenticate_kafka(self) -> bool:
        """
        Autentica al operario contra Central a través de Kafka:
        envía {"type": "AUTH"} a wm-requests y espera AUTH_OK/AUTH_FAIL en wm-responses.
        """
        for attempt in range(1, 6):
            self.auth_request_id = uuid.uuid4().hex[:8]
            self.auth_event.clear()
            self.auth_result = None
            try:
                self.producer.send(kconfig.TOPIC_REQUESTS, {
                    "type": "AUTH",
                    "request_id": self.auth_request_id,
                    "operator_id": self.operator_id,
                    "timestamp": time.time(),
                })
                self.producer.flush()
            except Exception as e:
                logger.warning(f"Error enviando AUTH a Kafka ({attempt}/5): {e}")
                time.sleep(2)
                continue

            if self.auth_event.wait(timeout=10.0):
                if self.auth_result == "AUTH_OK":
                    logger.info(f"✅ Autenticación exitosa en Central como operario {self.operator_id}")
                    return True
                logger.error(f"❌ Autenticación denegada por Central: {self.auth_message}")
                return False
            logger.warning(f"Central no respondió a la autenticación ({attempt}/5). Reintentando...")

        logger.error("No se pudo autenticar con Central tras varios intentos. Abortando.")
        return False

    def logout_kafka(self):
        """Avisa a Central del cierre de sesión."""
        try:
            if self.producer:
                self.producer.send(kconfig.TOPIC_REQUESTS, {"type": "LOGOUT", "operator_id": self.operator_id})
                self.producer.flush()
                logger.info("Sesión cerrada correctamente.")
        except Exception:
            pass

    def start_kafka(self):
        logger.info(f"Conectando al broker Kafka en {self.kafka_broker}...")
        self.producer = kconfig.create_producer(self.kafka_broker)
        
        # Hilos consumidores: respuestas de Central y telemetría de WS
        t_resp = threading.Thread(target=self.responses_consumer_loop, daemon=True)
        t_resp.start()

        t_telem = threading.Thread(target=self.telemetry_consumer_loop, daemon=True)
        t_telem.start()

    def responses_consumer_loop(self):
        """
        Escucha respuestas y notificaciones de CENTRAL (wm-responses).
        Se usa un group_id FIJO basado en operator_id (sin UUID aleatorio) y
        auto_offset_reset="earliest" para que, si el FO cae y se recupera,
        pueda leer mensajes FINISHED/REJECTED que Central ya envió durante la caída.
        """
        consumer = None
        # group_id fijo y persistente: garantiza resiliencia ante caídas del FO
        fixed_group_id = f"wm-fo-responses-{self.operator_id}"
        while self.running:
            try:
                consumer = kconfig.create_consumer(
                    topic=kconfig.TOPIC_RESPONSES,
                    bootstrap_servers=self.kafka_broker,
                    group_id=fixed_group_id,
                    auto_offset_reset="earliest"  # Recupera mensajes pendientes si el FO reinicia
                )
                break
            except Exception as e:
                logger.warning(f"Error conectando a Kafka responses: {e}. Reintentando...")
                time.sleep(2)

        # Esperar hasta que el broker asigne particiones al grupo
        while self.running and not consumer.assignment():
            consumer.poll(timeout_ms=200)

        self.consumer_ready.set()
        logger.info(f"Consumidor de respuestas listo (group={fixed_group_id}) y particiones asignadas para {self.operator_id}")

        while self.running:
            try:
                msg_batch = consumer.poll(timeout_ms=500)
                for tp, messages in msg_batch.items():
                    for record in messages:
                        data = record.value
                        # Filtrar solo mensajes dirigidos a este operario
                        if data.get("operator_id") == self.operator_id:
                            self.handle_central_response(data)
            except Exception as e:
                logger.error(f"Error en recepción de respuestas: {e}")
                time.sleep(1)

    def telemetry_consumer_loop(self):
        """
        Requisito pág. 8: Los operarios sólo recibirán los mensajes de la WS
        específica que les esté prestando servicio.
        """
        consumer = None
        while self.running:
            try:
                consumer = kconfig.create_consumer(
                    topic=kconfig.TOPIC_TELEMETRY,
                    bootstrap_servers=self.kafka_broker,
                    group_id=f"wm-fo-telem-{self.operator_id}-{uuid.uuid4().hex[:6]}"
                )
                break
            except Exception as e:
                time.sleep(2)

        while self.running:
            try:
                msg_batch = consumer.poll(timeout_ms=500)
                for tp, messages in msg_batch.items():
                    for record in messages:
                        data = record.value
                        op_id = data.get("operator_id")
                        ws_id = data.get("ws_id")
                        event = data.get("event")
                        
                        # Mostrar solo si este operario está regando esa estación
                        if op_id == self.operator_id and ws_id == self.current_ws_id:
                            if event == "UPDATE":
                                flow = data.get("flow", 0.0)
                                vol = data.get("accumulated_vol", 0.0)
                                rem = data.get("remaining", 0)
                                print(f"   💧 [TELEMETRÍA EN VIVO {ws_id}] Caudal: {flow:.1f} L/min | Volumen: {vol:.2f} L | Restante: {rem}s", end="\r")
            except Exception:
                time.sleep(1)

    def handle_central_response(self, data: dict):
        status = data.get("status")
        msg = data.get("message", "")

        # Respuesta de autenticación (se identifica por request_id, ignora las antiguas)
        if status in ("AUTH_OK", "AUTH_FAIL"):
            if (data.get("data") or {}).get("request_id") == self.auth_request_id:
                self.auth_result = status
                self.auth_message = msg
                self.auth_event.set()
            return

        ws_id = data.get("ws_id", "")
        extra = data.get("data", {})
        req_id = extra.get("request_id")

        # Descartar respuestas de solicitudes pasadas
        if self.current_request_id and req_id and req_id != self.current_request_id:
            return

        if status == "PROGRESS":
            print(f"\n   ℹ️ [CENTRAL] {msg}")

        elif status == "APPROVED":
            print(f"\n   ✅ [AUTORIZADO] {msg}")
            print(f"   🚀 Riego en marcha para la estación {ws_id}...")

        elif status == "REJECTED":
            print(f"\n   ❌ [DENEGADO POR CENTRAL] {msg}")
            self.current_service_result = "REJECTED"
            self.current_service_event.set()

        elif status == "FINISHED":
            vol = extra.get("total_volume_liters", 0.0)
            dur = extra.get("duration_sec", 0)
            print("\n\n" + "=" * 65)
            print(f"   🎉 [RESUMEN FINAL DE RIEGO - {ws_id}] 🎉")
            print(f"   * Mensaje Central   : {msg}")
            print(f"   * Volumen Consumido : {vol:.2f} Litros")
            print(f"   * Tiempo Empleado   : {dur} segundos")
            print("=" * 65 + "\n")
            self.current_service_result = "FINISHED"
            self.current_service_event.set()

    def request_watering(self, ws_id: str, duration_sec: int) -> str:
        """
        Envía solicitud de suministro a CENTRAL y espera a su conclusión.
        """
        req_id = uuid.uuid4().hex[:8]
        self.current_request_id = req_id
        self.current_ws_id = ws_id
        self.current_service_result = None
        self.current_service_event.clear()

        req_payload = {
            "request_id": req_id,
            "operator_id": self.operator_id,
            "ws_id": ws_id,
            "duration": duration_sec,
            "timestamp": time.time()
        }

        print(f"\n📤 [SOLICITUD] Enviando petición a CENTRAL para estación '{ws_id}' ({duration_sec}s)...")
        try:
            self.producer.send(kconfig.TOPIC_REQUESTS, req_payload)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error enviando petición a Kafka: {e}")
            return "ERROR"

        # Esperar hasta que concluya el servicio (aprobación -> telemetría -> fin o rechazo)
        # Timeout amplio (duración + 30s)
        self.current_service_event.wait(timeout=duration_sec + 35)
        
        result = self.current_service_result or "TIMEOUT"
        self.current_ws_id = None
        return result

    def run_batch_file(self):
        """
        Requisito pág. 11:
        'Esta aplicación, además de poder solicitar un suministro puntualmente en un WS,
        podrá leer un fichero todos los servicios que va a solicitar. Enviará el primero a WM_Central
        y, tras su conclusión (sea en éxito o fracaso), esperará 4 segundos y pasará a solicitar el siguiente servicio.'
        """
        if not os.path.exists(self.batch_file):
            print(f"❌ Error: El archivo de peticiones '{self.batch_file}' no existe.")
            return

        print(f"\n📁 Procesando archivo de peticiones automáticas: {self.batch_file}\n")
        with open(self.batch_file, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]

        total = len(lines)
        print(f"Se han encontrado {total} servicios a solicitar.\n")

        for idx, line in enumerate(lines, 1):
            parts = line.split()
            if len(parts) < 2:
                print(f"Línea inválida ignorada: '{line}' (formato esperado: <ws_id> <duracion_seg>)")
                continue

            ws_id = parts[0]
            try:
                duration = int(parts[1])
            except ValueError:
                print(f"Duración inválida en línea: '{line}'")
                continue

            print(f"\n--- [SERVICIO {idx}/{total}] Estación: {ws_id} | Duración: {duration}s ---")
            res = self.request_watering(ws_id, duration)
            print(f"Resultado del servicio: {res}")

            if idx < total:
                print("⏳ Esperando 4 segundos obligatorios antes del siguiente servicio...")
                time.sleep(4.0)

        print("\n🏁 Todos los servicios del archivo han sido completados.")

    def run_interactive(self):
        """
        Menú de consola para que el operario interactúe libremente.
        """
        while self.running:
            print("\n" + "=" * 50)
            print(f"   OPERARIO DE CAMPO: {self.operator_id}")
            print("=" * 50)
            print("  1. Solicitar activación de riego en una estación")
            print("  2. Ver ayuda / instrucciones")
            print("  3. Salir")
            print("=" * 50)
            choice = input("Selecciona una opción (1-3): ").strip()

            if choice == '1':
                ws_id = input("Introduce ID de la estación (ej: WS-01, WS-02): ").strip().upper()
                if not ws_id:
                    continue
                try:
                    dur = int(input("Introduce duración en segundos (ej: 10): ").strip())
                except ValueError:
                    print("Duración no válida.")
                    continue

                self.request_watering(ws_id, dur)

            elif choice == '2':
                print("\n[AYUDA]:")
                print(" - Puedes solicitar riego a cualquier estación registrada en la Central.")
                print(" - La Central comprobará si la estación está disponible antes de autorizar.")
                print(" - Durante el riego verás el caudal y litros suministrados en vivo.")
            elif choice == '3':
                print("Saliendo de la aplicación del operario...")
                break
            else:
                print("Opción no reconocida.")

    def run(self):
        # 1. Conectar a Kafka
        self.start_kafka()
        self.consumer_ready.wait(timeout=10.0)
        time.sleep(1.0)  # Margen de asignación de particiones

        # 2. Autenticación con Central por Kafka (obligatoria antes de operar)
        print(f"\n🔐 Autenticando a {self.operator_id} con Central vía Kafka...")
        if not self.authenticate_kafka():
            print(f"\n❌ Autenticación fallida. El operario {self.operator_id} no puede operar.")
            return
        print(f"✅ Operario {self.operator_id} autenticado.")

        try:
            if self.batch_file:
                self.run_batch_file()
            else:
                self.run_interactive()
        finally:
            self.logout_kafka()

def main():
    # Argumentos posicionales (ignora --file y su valor)
    args = []
    skip = False
    for a in sys.argv[1:]:
        if skip:
            skip = False
        elif a == "--file":
            skip = True
        else:
            args.append(a)

    # Compatibilidad: formato antiguo <broker> <host:puerto> <operator_id> (el host:puerto se ignora)
    if len(args) == 3 and ":" in args[1]:
        args = [args[0], args[2]]

    if len(args) != 2:
        print("Uso: python WM_FO.py <kafka_broker> <operator_id> [--file <archivo_servicios>]")
        print("Ejemplo: python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01")
        print("Ejemplo batch: python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01 --file servicios.txt")
        sys.exit(1)

    kafka_broker = kconfig.normalize_bootstrap_servers(args[0])[0]
    operator_id = args[1]

    batch_file = None
    if "--file" in sys.argv:
        idx = sys.argv.index("--file")
        if idx + 1 < len(sys.argv):
            batch_file = sys.argv[idx + 1]

    print("=" * 65)
    print(f"   🚜 FIELD OPERATOR APP - {operator_id} (SD 26/27) 🚜")
    print(f"   * Broker Kafka          : {kafka_broker}")
    print("   * Autenticación         : vía Kafka")
    print(f"   * ID Operador           : {operator_id}")
    if batch_file:
        print(f"   * Modo Archivo          : {batch_file}")
    else:
        print("   * Modo                  : Interactivo")
    print("=" * 65)

    app = FieldOperatorApp(kafka_broker, operator_id, batch_file)
    try:
        app.run()
    except KeyboardInterrupt:
        print("\nOperario cerrado.")
        app.logout_kafka()

if __name__ == '__main__':
    main()
