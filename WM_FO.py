# -*- coding: utf-8 -*-
"""
WM_FO - Aplicación del Operario de Campo (Field Operator)
Asignatura: Sistemas Distribuidos (SD 26/27) - Water Management Network

Parámetros requeridos por el PDF:
    python WM_FO.py <kafka_broker> <central_auth_ip:port> <operator_id> [--file <ruta_fichero>] [--web-port <port>]

Ejemplos:
    Modo Interactivo : python WM_FO.py caboose.proxy.rlwy.net:37367 127.0.0.1:9001 FO-01
    Modo Fichero     : python WM_FO.py caboose.proxy.rlwy.net:37367 127.0.0.1:9001 FO-01 --file servicios.txt
    Con web          : python WM_FO.py caboose.proxy.rlwy.net:37367 127.0.0.1:9001 FO-01 --web-port 4001
"""

import sys
import os
import time
import uuid
import socket
import threading
import datetime
import logging
from typing import Optional, Tuple
from flask import Flask, render_template_string, jsonify, request as flask_request

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
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# =============================================================================
#  INTERFAZ WEB FLASK
# =============================================================================
fo_app = Flask(__name__)
fo_instance_global = None

FO_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Panel Operario - {{ fo_id }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #ffffff; color: #000000; }
        h1 { font-size: 18px; margin: 0 0 2px 0; }
        .cabecera { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 1px solid #000000; padding-bottom: 6px; margin-bottom: 12px; }
        .subtitulo { font-size: 12px; color: #555555; margin-top: 2px; }
        a.enlace-central { font-size: 12px; color: #003366; text-decoration: none; border: 1px solid #bbbbbb; padding: 3px 8px; background: #f0f0f0; }
        a.enlace-central:hover { background: #e0e0e0; }
        .info-red { background-color: #f0f0f0; border: 1px solid #bbbbbb; padding: 6px 10px; font-size: 12px; margin-bottom: 14px; }
        .seccion-titulo { font-size: 14px; font-weight: bold; margin: 14px 0 6px 0; border-bottom: 1px solid #cccccc; padding-bottom: 2px; }
        .grid-dos-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
        @media (max-width: 720px) { .grid-dos-col { grid-template-columns: 1fr; } }
        .bloque { border: 1px solid #cccccc; padding: 12px; background: #fafafa; }
        .bloque-titulo { font-size: 12px; font-weight: bold; text-transform: uppercase; color: #444444; margin-bottom: 10px; border-bottom: 1px solid #dddddd; padding-bottom: 3px; }
        .caja-estado { padding: 10px; text-align: center; font-weight: bold; font-size: 14px; margin-bottom: 12px; border: 1px solid #bbbbbb; background: #eeeeee; color: #333333; }
        .estado-idle { background-color: #e2e3e5; color: #383d41; border-color: #aaaaaa; }
        .estado-waiting { background-color: #fff3cd; color: #856404; border-color: #aaaaaa; }
        .estado-approved { background-color: #cce5ff; color: #004085; border-color: #aaaaaa; }
        .estado-finished { background-color: #d4edda; color: #155724; border-color: #aaaaaa; }
        .estado-rejected { background-color: #f8d7da; color: #721c24; border-color: #aaaaaa; }
        .fila-metrica { display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px solid #eeeeee; font-size: 13px; }
        .fila-metrica:last-child { border-bottom: none; }
        .fila-metrica .etiqueta { color: #555555; }
        .fila-metrica .valor { font-weight: bold; }
        .form-campo { margin-bottom: 10px; }
        .form-campo label { display: block; font-size: 12px; margin-bottom: 3px; color: #333333; }
        .form-campo input { width: 95%; padding: 6px; border: 1px solid #bbbbbb; font-size: 13px; }
        button { background-color: #e8e8e8; border: 1px solid #999999; color: #000000; padding: 6px 12px; font-size: 13px; cursor: pointer; }
        button:hover { background-color: #d0d0d0; }
        .btn-iniciar { background-color: #336699; border-color: #224488; color: #ffffff; width: 100%; text-align: center; }
        .btn-iniciar:hover { background-color: #224488; }
        button:disabled { background-color: #cccccc; border-color: #aaaaaa; color: #666666; cursor: not-allowed; }
        .caja-logs { border: 1px solid #bbbbbb; background-color: #fafafa; height: 160px; overflow-y: scroll; font-family: "Courier New", monospace; font-size: 12px; padding: 6px 8px; color: #333333; }
        .log-item { padding: 2px 0; border-bottom: 1px solid #eeeeee; }
        .log-time { color: #777777; margin-right: 6px; }
        .log-type { font-weight: bold; margin-right: 6px; }
        .type-ok { color: #155724; }
        .type-solicitud { color: #004085; }
        .type-aprobado { color: #004085; }
        .type-finalizado { color: #155724; }
        .type-rechazado { color: #721c24; }
        .type-info { color: #555555; }
        #mensajeAviso { display: none; padding: 6px 10px; margin-bottom: 12px; font-size: 13px; background-color: #d4edda; border: 1px solid #aaaaaa; color: #155724; }
    </style>
</head>
<body>
    <div class="cabecera">
        <div>
            <h1>Operario de Campo: {{ fo_id }}</h1>
            <div class="subtitulo">Panel local de solicitudes y recepcion de telemetria</div>
        </div>
        <a href="http://localhost:3000" target="_blank" class="enlace-central">Panel Central (Puerto 3000)</a>
    </div>

    <div id="mensajeAviso">Accion ejecutada correctamente.</div>

    <div class="info-red">
        ID Operario: <strong>{{ fo_id }}</strong> &nbsp;|&nbsp;
        Broker Kafka: <strong id="txtKafka">{{ kafka_broker }}</strong> &nbsp;|&nbsp;
        Puerto Web: <strong>{{ web_port }}</strong>
    </div>

    <div class="grid-dos-col">
        <div class="bloque">
            <div class="bloque-titulo">Estado y Mediciones en Vivo</div>
            <div id="cajaEstado" class="caja-estado estado-idle">EN ESPERA (IDLE)</div>
            <div class="fila-metrica">
                <span class="etiqueta">Estado actual:</span>
                <span class="valor" id="txtEstado">IDLE</span>
            </div>
            <div class="fila-metrica">
                <span class="etiqueta">Estacion en servicio:</span>
                <span class="valor" id="txtWs">-</span>
            </div>
            <div class="fila-metrica">
                <span class="etiqueta">Caudal instantaneo:</span>
                <span class="valor"><span id="txtCaudal">0.0</span> L/min</span>
            </div>
            <div class="fila-metrica">
                <span class="etiqueta">Volumen acumulado:</span>
                <span class="valor"><span id="txtVolumen">0.00</span> L</span>
            </div>
            <div class="fila-metrica">
                <span class="etiqueta">Tiempo restante:</span>
                <span class="valor"><span id="txtRestante">0</span> s</span>
            </div>
        </div>

        <div class="bloque">
            <div class="bloque-titulo">Solicitar Suministro de Riego</div>
            <div class="form-campo">
                <label>Identificador de Estacion:</label>
                <input type="text" id="inputWsId" placeholder="Ej: WS-01" maxlength="10">
            </div>
            <div class="form-campo">
                <label>Duracion en segundos:</label>
                <input type="number" id="inputDuracion" value="15" min="3" max="300">
            </div>
            <div style="margin-top:14px;">
                <button id="btnEnviar" class="btn-iniciar" onclick="enviarSolicitud()">Enviar Solicitud a Central</button>
            </div>
        </div>
    </div>

    <div class="seccion-titulo">Registro de Eventos del Operario</div>
    <div class="caja-logs" id="cajaLogs">
        <div class="log-item">Sin eventos registrados.</div>
    </div>

    <script>
        const TYPE_CLASS = {
            'OK': 'type-ok',
            'SOLICITUD': 'type-solicitud',
            'APROBADO': 'type-aprobado',
            'FINALIZADO': 'type-finalizado',
            'RECHAZADO': 'type-rechazado',
            'INFO': 'type-info'
        };

        function mostrarAviso(texto, esError) {
            const el = document.getElementById('mensajeAviso');
            el.textContent = texto;
            el.style.backgroundColor = esError ? '#f8d7da' : '#d4edda';
            el.style.color = esError ? '#721c24' : '#155724';
            el.style.display = 'block';
            setTimeout(() => { el.style.display = 'none'; }, 4000);
        }

        async function actualizarDatos() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();

                const caja = document.getElementById('cajaEstado');
                const txtEstado = document.getElementById('txtEstado');
                const txtWs = document.getElementById('txtWs');
                const txtCaudal = document.getElementById('txtCaudal');
                const txtVolumen = document.getElementById('txtVolumen');
                const txtRestante = document.getElementById('txtRestante');

                const status = data.status || 'IDLE';
                txtEstado.textContent = status;
                txtWs.textContent = data.current_ws || '-';

                caja.className = 'caja-estado';
                if (status === 'IDLE') {
                    caja.classList.add('estado-idle');
                    caja.textContent = 'EN ESPERA (IDLE)';
                } else if (status === 'WAITING') {
                    caja.classList.add('estado-waiting');
                    caja.textContent = 'SOLICITUD EN TRAMITE';
                } else if (status === 'APPROVED') {
                    caja.classList.add('estado-approved');
                    caja.textContent = 'RIEGO AUTORIZADO Y EN CURSO';
                } else if (status === 'FINISHED') {
                    caja.classList.add('estado-finished');
                    caja.textContent = 'SERVICIO CONCLUIDO CON EXITO';
                } else if (status === 'REJECTED') {
                    caja.classList.add('estado-rejected');
                    caja.textContent = 'SOLICITUD RECHAZADA';
                } else {
                    caja.classList.add('estado-idle');
                    caja.textContent = status;
                }

                if (data.telemetry) {
                    txtCaudal.textContent = Number(data.telemetry.flow || 0).toFixed(1);
                    txtVolumen.textContent = Number(data.telemetry.vol || 0).toFixed(2);
                    txtRestante.textContent = data.telemetry.remaining || 0;
                } else {
                    txtCaudal.textContent = '0.0';
                    txtVolumen.textContent = '0.00';
                    txtRestante.textContent = '0';
                }

                const cajaLogs = document.getElementById('cajaLogs');
                if (data.events && data.events.length > 0) {
                    cajaLogs.innerHTML = data.events.map(e => {
                        const clase = TYPE_CLASS[e.type] || 'type-info';
                        return `<div class="log-item"><span class="log-time">[${e.time}]</span><span class="log-type ${clase}">[${e.type}]</span><span>${e.message}</span></div>`;
                    }).join('');
                } else {
                    cajaLogs.innerHTML = '<div class="log-item">Sin eventos registrados.</div>';
                }

                const busy = (status === 'WAITING' || status === 'APPROVED');
                document.getElementById('btnEnviar').disabled = busy;
            } catch (err) {
                console.error(err);
            }
        }

        async function enviarSolicitud() {
            const wsId = document.getElementById('inputWsId').value.trim().toUpperCase();
            const duracion = parseInt(document.getElementById('inputDuracion').value) || 15;

            if (!wsId) {
                mostrarAviso('Por favor, indique el identificador de la estacion.', true);
                return;
            }

            document.getElementById('btnEnviar').disabled = true;

            try {
                const res = await fetch('/api/request', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ws_id: wsId, duration: duracion })
                });
                const resp = await res.json();
                if (resp.success) {
                    mostrarAviso(resp.message || 'Solicitud enviada a Central.', false);
                } else {
                    mostrarAviso(resp.error || 'Error al enviar la solicitud.', true);
                }
            } catch (e) {
                mostrarAviso('Error de comunicacion con el servidor local.', true);
            } finally {
                actualizarDatos();
            }
        }

        setInterval(actualizarDatos, 1200);
        actualizarDatos();
    </script>
</body>
</html>
"""

@fo_app.route('/')
def fo_web_index():
    if not fo_instance_global:
        return "FO no inicializado", 500
    return render_template_string(
        FO_HTML,
        fo_id=fo_instance_global.operator_id,
        kafka_broker=fo_instance_global.kafka_broker,
        web_port=fo_instance_global.web_port
    )

@fo_app.route('/api/status')
def fo_web_status():
    if not fo_instance_global:
        return jsonify({"error": "No inicializado"}), 500
    return jsonify(fo_instance_global.get_status_dict())

@fo_app.route('/api/request', methods=['POST'])
def fo_web_request():
    if not fo_instance_global:
        return jsonify({"success": False, "error": "No inicializado"}), 500
    if fo_instance_global.current_service_result is None and fo_instance_global.current_ws_id:
        return jsonify({"success": False, "error": "Ya hay una solicitud en curso"}), 400
    data    = flask_request.json or {}
    ws_id   = data.get("ws_id", "").strip().upper()
    duration = int(data.get("duration", 15))
    if not ws_id:
        return jsonify({"success": False, "error": "Falta ws_id"}), 400
    threading.Thread(
        target=fo_instance_global.request_watering,
        args=(ws_id, duration),
        daemon=True
    ).start()
    return jsonify({"success": True, "message": f"Solicitud enviada: {ws_id} ({duration}s)"})


# =============================================================================
#  CLASE PRINCIPAL
# =============================================================================
class FieldOperatorApp:
    def __init__(self, kafka_broker: str, central_auth_addr: Tuple[str, int],
                 operator_id: str, batch_file: Optional[str] = None, web_port: int = 4001):
        self.kafka_broker = kafka_broker
        self.central_auth_host, self.central_auth_port = central_auth_addr
        self.operator_id = operator_id
        self.batch_file = batch_file
        self.web_port = web_port

        self.running = True
        self.producer = None

        # Socket de autenticación con Central
        self.auth_sock = None
        self.auth_lock = threading.Lock()

        # Sincronización del servicio activo
        self.current_service_event = threading.Event()
        self.consumer_ready = threading.Event()
        self.current_ws_id = None
        self.current_service_result = None
        self.current_request_id = None
        self.current_status = "IDLE"

        # Última telemetría recibida
        self.last_telemetry = None
        self.telem_lock = threading.Lock()

        # Historial de eventos para la web
        self.events = []
        self.events_lock = threading.Lock()
        self.add_event("INFO", f"Operario {self.operator_id} iniciado.")

    # -------------------------------------------------------------------------
    #  EVENTOS (para la interfaz web)
    # -------------------------------------------------------------------------
    def add_event(self, ev_type: str, message: str):
        with self.events_lock:
            self.events.insert(0, {
                "time": datetime.datetime.now().strftime("%H:%M:%S"),
                "type": ev_type,
                "message": message
            })
            if len(self.events) > 30:
                self.events.pop()

    def get_status_dict(self) -> dict:
        with self.telem_lock:
            telem = dict(self.last_telemetry) if self.last_telemetry else None
        with self.events_lock:
            evs = list(self.events)
        return {
            "operator_id": self.operator_id,
            "status": self.current_status,
            "current_ws": self.current_ws_id,
            "telemetry": telem,
            "events": evs,
        }

    # -------------------------------------------------------------------------
    #  SERVIDOR WEB
    # -------------------------------------------------------------------------
    def start_web_server(self):
        logger.info(f"Panel web del operario {self.operator_id} en http://0.0.0.0:{self.web_port}")
        try:
            fo_app.run(host='0.0.0.0', port=self.web_port, debug=False, use_reloader=False)
        except Exception as e:
            logger.error(f"Error en servidor web FO: {e}")

    # =========================================================================
    # AUTENTICACIÓN POR SOCKET TCP CON CENTRAL
    # =========================================================================
    def authenticate_socket(self) -> bool:
        """
        Conecta al servidor de autenticación de Central por socket TCP y
        envía la trama AUTH#fo_id usando el protocolo STX/ETX/LRC.
        Devuelve True si la autenticación fue exitosa, False en caso contrario.

        Si la variable de entorno SKIP_FO_AUTH=1 está definida, se omite
        la autenticación por socket (útil en Docker cuando el puerto TCP
        de auth no está expuesto públicamente).
        """
        if os.environ.get("SKIP_FO_AUTH", "0") == "1":
            logger.info("⚠️  SKIP_FO_AUTH=1: autenticación por socket omitida. Operando solo vía Kafka.")
            self.add_event("INFO", "Auth por socket omitida (SKIP_FO_AUTH=1).")
            return True

        retries = 0
        max_retries = 10
        while retries < max_retries:
            try:
                logger.info(f"Conectando al servidor de autenticación de Central en "
                            f"{self.central_auth_host}:{self.central_auth_port}...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.central_auth_host, self.central_auth_port))

                # Enviar trama de autenticación
                auth_payload = f"AUTH{protocol.DELIMITER}{self.operator_id}"
                protocol.send_frame(s, auth_payload, wait_ack=True)

                # Esperar respuesta de Central
                resp = protocol.recv_frame(s, send_ack=True, timeout=10.0)
                if resp.startswith("AUTH_OK"):
                    logger.info(f"✅ Autenticación exitosa en Central como operario {self.operator_id}")
                    self.auth_sock = s
                    self.add_event("OK", f"Autenticado en Central como {self.operator_id}.")
                    return True
                elif resp.startswith("AUTH_FAIL"):
                    reason = resp.split(protocol.DELIMITER, 1)[-1] if protocol.DELIMITER in resp else resp
                    logger.error(f"❌ Autenticación denegada por Central: {reason}")
                    self.add_event("RECHAZADO", f"Auth denegada: {reason}")
                    s.close()
                    return False
                else:
                    logger.error(f"Respuesta inesperada de Central: {resp}")
                    s.close()
                    return False

            except Exception as e:
                retries += 1
                logger.warning(f"No se pudo conectar a Central para autenticar "
                               f"({retries}/{max_retries}): {e}. Reintentando en 3s...")
                time.sleep(3)

        logger.error("No se pudo autenticar con Central tras varios intentos. Abortando.")
        return False

    def logout_socket(self):
        """Envia LOGOUT a Central y cierra el socket de autenticación."""
        with self.auth_lock:
            if self.auth_sock:
                try:
                    protocol.send_frame(self.auth_sock, "LOGOUT", wait_ack=False)
                    protocol.send_eot(self.auth_sock)
                    self.auth_sock.close()
                except Exception:
                    pass
                finally:
                    self.auth_sock = None
                    logger.info("Sesión de socket cerrada correctamente.")


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
                                vol  = data.get("accumulated_vol", 0.0)
                                rem  = data.get("remaining", 0)
                                with self.telem_lock:
                                    self.last_telemetry = {
                                        "flow": round(flow, 1),
                                        "vol":  round(vol, 2),
                                        "remaining": rem
                                    }
                                print(f"   💧 [TELEMETRÍA EN VIVO {ws_id}] Caudal: {flow:.1f} L/min | Volumen: {vol:.2f} L | Restante: {rem}s", end="\r")
            except Exception:
                time.sleep(1)

    def handle_central_response(self, data: dict):
        status = data.get("status")
        msg    = data.get("message", "")
        ws_id  = data.get("ws_id", "")
        extra  = data.get("data", {})
        req_id = extra.get("request_id")

        # Descartar respuestas de solicitudes pasadas
        if self.current_request_id and req_id and req_id != self.current_request_id:
            return

        if status == "PROGRESS":
            print(f"\n   ℹ️ [CENTRAL] {msg}")
            self.add_event("INFO", msg)

        elif status == "APPROVED":
            print(f"\n   ✅ [AUTORIZADO] {msg}")
            print(f"   🚀 Riego en marcha para la estación {ws_id}...")
            self.current_status = "APPROVED"
            self.add_event("APROBADO", f"{ws_id}: {msg}")

        elif status == "REJECTED":
            print(f"\n   ❌ [DENEGADO POR CENTRAL] {msg}")
            self.current_status = "REJECTED"
            self.add_event("RECHAZADO", f"{ws_id}: {msg}")
            with self.telem_lock:
                self.last_telemetry = None
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
            self.current_status = "FINISHED"
            self.add_event("FINALIZADO", f"{ws_id}: {vol:.2f} L en {dur}s")
            with self.telem_lock:
                self.last_telemetry = None
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
        self.current_status = "WAITING"
        self.current_service_event.clear()
        with self.telem_lock:
            self.last_telemetry = None

        req_payload = {
            "request_id": req_id,
            "operator_id": self.operator_id,
            "ws_id": ws_id,
            "duration": duration_sec,
            "timestamp": time.time()
        }

        print(f"\n📤 [SOLICITUD] Enviando petición a CENTRAL para estación '{ws_id}' ({duration_sec}s)...")
        self.add_event("SOLICITUD", f"Petición a {ws_id} ({duration_sec}s)")
        try:
            self.producer.send(kconfig.TOPIC_REQUESTS, req_payload)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error enviando petición a Kafka: {e}")
            self.current_status = "IDLE"
            return "ERROR"

        # Esperar hasta que concluya el servicio (aprobación -> telemetría -> fin o rechazo)
        self.current_service_event.wait(timeout=duration_sec + 35)

        result = self.current_service_result or "TIMEOUT"
        self.current_ws_id = None
        if result in ("FINISHED", "REJECTED", "TIMEOUT"):
            self.current_status = result if result != "TIMEOUT" else "IDLE"
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
                print(f" - Panel web: http://localhost:{self.web_port}")
            elif choice == '3':
                print("Saliendo de la aplicación del operario...")
                break
            else:
                print("Opción no reconocida.")

    def run(self):
        global fo_instance_global
        fo_instance_global = self

        # 0. Arrancar servidor web en hilo daemon
        t_web = threading.Thread(target=self.start_web_server, daemon=True)
        t_web.start()

        # 1. Autenticación por socket TCP con Central (obligatoria antes de operar)
        print(f"\n🔐 Autenticando con Central en {self.central_auth_host}:{self.central_auth_port}...")
        if not self.authenticate_socket():
            print(f"\n❌ Autenticación fallida. El operario {self.operator_id} no puede operar.")
            return

        print(f"✅ Operario {self.operator_id} autenticado. Conectando a Kafka...")

        # 2. Conectar a Kafka
        self.start_kafka()
        self.consumer_ready.wait(timeout=10.0)
        time.sleep(1.0)  # Margen de asignación de particiones

        try:
            if self.batch_file:
                self.run_batch_file()
            else:
                self.run_interactive()
        finally:
            # Cerrar sesión de socket al terminar
            self.logout_socket()

def main():
    if len(sys.argv) < 4:
        print("Uso obligatorio: python WM_FO.py <kafka_broker> <central_auth_ip:port> <operator_id> [--file <archivo>] [--web-port <port>]")
        print("Ejemplo: python WM_FO.py caboose.proxy.rlwy.net:37367 127.0.0.1:9001 FO-01 --web-port 4001")
        sys.exit(1)

    kafka_broker_raw = sys.argv[1]
    kafka_broker = kconfig.normalize_bootstrap_servers(kafka_broker_raw)[0]
    central_auth_arg = sys.argv[2]
    operator_id = sys.argv[3]

    if ":" not in central_auth_arg:
        print("Error: central_auth_ip:port debe tener formato host:puerto (ej: 127.0.0.1:9001)")
        sys.exit(1)

    auth_host, auth_port_str = central_auth_arg.split(":")
    auth_port = int(auth_port_str)

    batch_file = None
    if "--file" in sys.argv:
        idx = sys.argv.index("--file")
        if idx + 1 < len(sys.argv):
            batch_file = sys.argv[idx + 1]

    # Puerto web: --web-port o derivado del ID del operario (FO-01 → 4001)
    web_port = None
    if "--web-port" in sys.argv:
        idx = sys.argv.index("--web-port")
        if idx + 1 < len(sys.argv):
            web_port = int(sys.argv[idx + 1])
    if not web_port:
        digits = "".join([c for c in operator_id if c.isdigit()])
        web_port = 4000 + int(digits) if digits else 4001

    print("=" * 65)
    print(f"   🚜 FIELD OPERATOR APP - {operator_id} (SD 26/27) 🚜")
    print(f"   * Broker Kafka          : {kafka_broker}")
    print(f"   * Auth. Central (socket): {auth_host}:{auth_port}")
    print(f"   * ID Operador           : {operator_id}")
    print(f"   * Panel Web             : http://localhost:{web_port}")
    if batch_file:
        print(f"   * Modo Archivo          : {batch_file}")
    else:
        print("   * Modo                  : Interactivo")
    print("=" * 65)

    app = FieldOperatorApp(kafka_broker, (auth_host, auth_port), operator_id, batch_file, web_port)
    try:
        app.run()
    except KeyboardInterrupt:
        print("\nOperario cerrado.")
        app.logout_socket()

if __name__ == '__main__':
    main()
