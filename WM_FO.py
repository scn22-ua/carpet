# -*- coding: utf-8 -*-
"""
WM_FO - Aplicación del Operario de Campo (Field Operator)
Asignatura: Sistemas Distribuidos (SD 26/27) - Water Management Network

Parámetros requeridos por el PDF:
    python WM_FO.py <kafka_broker> <operator_id> [--file <ruta_fichero>]

Ejemplos:
    Modo Interactivo : python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01
    Modo Fichero     : python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01 --file servicios.txt
    Con puerto 9092  : python WM_FO.py 9092 FO-01
"""

import sys
import os
import time
import uuid
import threading
import logging
from typing import Optional

# Configuración de codificación UTF-8 segura en Windows
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

import common.kafka_config as kconfig

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
        
        # Sincronización del servicio activo
        self.current_service_event = threading.Event()
        self.consumer_ready = threading.Event()
        self.current_ws_id = None
        self.current_service_result = None

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
        """
        consumer = None
        while self.running:
            try:
                consumer = kconfig.create_consumer(
                    topic=kconfig.TOPIC_RESPONSES,
                    bootstrap_servers=self.kafka_broker,
                    group_id=f"wm-fo-{self.operator_id}-{uuid.uuid4().hex[:6]}",
                    auto_offset_reset="latest"
                )
                break
            except Exception as e:
                logger.warning(f"Error conectando a Kafka responses: {e}. Reintentando...")
                time.sleep(2)

        # Esperar hasta que el broker asigne particiones al grupo
        while self.running and not consumer.assignment():
            consumer.poll(timeout_ms=200)

        self.consumer_ready.set()
        logger.info(f"Consumidor de respuestas listo y particiones asignadas para {self.operator_id}")

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
        self.start_kafka()
        self.consumer_ready.wait(timeout=10.0)
        time.sleep(1.0)  # Margen de asignación de particiones

        if self.batch_file:
            self.run_batch_file()
        else:
            self.run_interactive()

def main():
    if len(sys.argv) < 3:
        print("Uso obligatorio: python WM_FO.py <kafka_broker> <operator_id> [--file <archivo_servicios>]")
        print("Ejemplo: python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01")
        print("Ejemplo batch: python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01 --file servicios.txt")
        print("Ejemplo con puerto 9092: python WM_FO.py 9092 FO-01")
        sys.exit(1)

    kafka_broker_raw = sys.argv[1]
    kafka_broker = kconfig.normalize_bootstrap_servers(kafka_broker_raw)[0]
    operator_id = sys.argv[2]
    
    batch_file = None
    if "--file" in sys.argv:
        idx = sys.argv.index("--file")
        if idx + 1 < len(sys.argv):
            batch_file = sys.argv[idx + 1]

    print("=" * 65)
    print(f"   🚜 FIELD OPERATOR APP - {operator_id} (SD 26/27) 🚜")
    print(f"   * Broker Kafka  : {kafka_broker}")
    print(f"   * ID Operador   : {operator_id}")
    if batch_file:
        print(f"   * Modo Archivo  : {batch_file}")
    else:
        print("   * Modo          : Interactivo")
    print("=" * 65)

    app = FieldOperatorApp(kafka_broker, operator_id, batch_file)
    try:
        app.run()
    except KeyboardInterrupt:
        print("\nOperario cerrado.")

if __name__ == '__main__':
    main()
