# -*- coding: utf-8 -*-
"""
Configuración y Helpers para Kafka - Práctica WaterManagement
Define topics estándar y funciones para publicación y consumo simplificados con JSON.
Soporta el proxy TCP de Railway (caboose.proxy.rlwy.net:37367) y escucha local/interna en puerto 9092.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, Callable, Optional, Union, List
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Configuración segura de UTF-8 en consola Windows
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

logger = logging.getLogger("KafkaConfig")

# Servidor Kafka por defecto (TCP Proxy de Railway) y puerto de escucha estándar
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "caboose.proxy.rlwy.net:37367")
LOCAL_KAFKA_PORT = 9092

# Topics del sistema
TOPIC_REQUESTS = "wm-requests"      # FO -> Central (solicitud de riego)
TOPIC_RESPONSES = "wm-responses"    # Central -> FO (notificaciones y resúmenes)
TOPIC_COMMANDS = "wm-commands"      # Central -> WS_E (órdenes de apertura/cierre válvula, bloqueo, etc.)
TOPIC_TELEMETRY = "wm-telemetry"    # WS_E -> Central & FO (caudal y volumen segundo a segundo)

ALL_TOPICS = [TOPIC_REQUESTS, TOPIC_RESPONSES, TOPIC_COMMANDS, TOPIC_TELEMETRY]

def normalize_bootstrap_servers(servers: Optional[Union[str, List[str]]] = None) -> List[str]:
    """
    Normaliza el parámetro de bootstrap servers.
    Acepta:
      - 'caboose.proxy.rlwy.net:37367'
      - '9092' o ':9092' -> 'localhost:9092' (para escucha local / interna)
      - 'caboose.proxy.rlwy.net:37367,localhost:9092' (múltiples brokers)
      - Lista de strings
      - Si está vacío o None, usa DEFAULT_BOOTSTRAP (caboose.proxy.rlwy.net:37367)
    """
    if not servers:
        return [DEFAULT_BOOTSTRAP]
    
    if isinstance(servers, str):
        parts = [p.strip() for p in servers.split(",") if p.strip()]
    elif isinstance(servers, (list, tuple)):
        parts = [str(p).strip() for p in servers if str(p).strip()]
    else:
        parts = [str(servers).strip()]

    normalized = []
    for p in parts:
        if p.isdigit():
            # Si solo se pasa '9092', asumimos localhost:9092
            normalized.append(f"127.0.0.1:{p}")
        elif p.startswith(":"):
            normalized.append(f"127.0.0.1{p}")
        elif p.lower() in ("default", "proxy", "railway"):
            normalized.append(DEFAULT_BOOTSTRAP)
        elif p.lower() in ("local", "localhost"):
            normalized.append(f"127.0.0.1:{LOCAL_KAFKA_PORT}")
        else:
            normalized.append(p)

    return normalized or [DEFAULT_BOOTSTRAP]

def safe_json_deserializer(m: bytes) -> Any:
    """
    Deserializa de forma segura JSON recibido de Kafka sin lanzar excepciones
    fatales si se recibe texto plano o bytes no JSON.
    """
    if m is None:
        return None
    try:
        decoded = m.decode('utf-8', errors='replace')
        return json.loads(decoded)
    except Exception:
        try:
            return m.decode('utf-8', errors='replace')
        except Exception:
            return m

def create_producer(bootstrap_servers: Optional[Union[str, List[str]]] = None) -> KafkaProducer:
    """
    Crea un productor de Kafka optimizado para baja latencia en tiempo real.
    """
    servers = normalize_bootstrap_servers(bootstrap_servers)
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8') if not isinstance(v, (bytes, bytearray)) else v,
        request_timeout_ms=30000,
        max_block_ms=30000,
        linger_ms=5,
        retries=5,
        acks=1,
    )

def create_consumer(
    topic: str,
    bootstrap_servers: Optional[Union[str, List[str]]] = None,
    group_id: Optional[str] = None,
    auto_offset_reset: str = "latest"
) -> KafkaConsumer:
    """
    Crea un consumidor Kafka suscrito a un topic con deserialización JSON automática y segura.
    """
    servers = normalize_bootstrap_servers(bootstrap_servers)
    consumer_kwargs = {
        "bootstrap_servers": servers,
        "value_deserializer": safe_json_deserializer,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": True,
        "consumer_timeout_ms": 1000,  # Permite que poll/iteraciones no bloqueen para siempre
    }
    if group_id:
        consumer_kwargs["group_id"] = group_id

    return KafkaConsumer(topic, **consumer_kwargs)
