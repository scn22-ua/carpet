# -*- coding: utf-8 -*-
import sys
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError
import common.kafka_config as kconfig

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "caboose.proxy.rlwy.net:37367"
if BOOTSTRAP.isdigit():
    BOOTSTRAP = f"127.0.0.1:{BOOTSTRAP}"

TOPIC = "topic-prueba"

def fase(n, txt):
    print(f"\n===== FASE {n}: {txt} =====")

# ---------- FASE 1: DNS + TCP ----------
fase(1, f"Resolución DNS y conexión TCP al broker ({BOOTSTRAP})")
if ":" in BOOTSTRAP:
    host, port = BOOTSTRAP.split(":")
else:
    host, port = BOOTSTRAP, "9092"

try:
    ip = socket.gethostbyname(host)
    print(f"✅ DNS OK: {host} -> {ip}")
except socket.gaierror as e:
    print(f"❌ DNS falla: {e}")
    sys.exit(1)

try:
    s = socket.create_connection((host, int(port)), timeout=8)
    s.close()
    print(f"✅ TCP OK: {host}:{port} acepta conexión")
except Exception as e:
    print(f"❌ TCP falla: {e}")
    sys.exit(1)

# ---------- FASE 2: Handshake Kafka ----------
fase(2, "Handshake Kafka y obtención de metadatos")
try:
    producer = kconfig.create_producer(BOOTSTRAP)
    print("✅ Productor creado (¡el broker ha respondido!)")
except KafkaError as e:
    print(f"❌ KafkaError: {type(e).__name__}: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error inesperado: {type(e).__name__}: {e}")
    sys.exit(1)

# ---------- FASE 3: Metadatos del topic ----------
fase(3, f"Metadatos del topic '{TOPIC}'")
try:
    partes = producer.partitions_for(TOPIC)
    if partes:
        print(f"✅ Topic existe y tiene particiones: {partes}")
    else:
        print(f"⚠️  Topic '{TOPIC}' no existe (o no hay metadatos todavía).")
except Exception as e:
    print(f"❌ Error consultando particiones: {type(e).__name__}: {e}")

# ---------- FASE 4: Publicar ----------
fase(4, "Enviar mensaje de prueba en formato JSON")
try:
    fut = producer.send(TOPIC, {"ping": "caboose_test", "timestamp": "now"})
    md = fut.get(timeout=30)
    print(f"✅ Enviado: topic={md.topic} partition={md.partition} offset={md.offset}")
except Exception as e:
    print(f"❌ Fallo al enviar: {type(e).__name__}: {e}")

producer.close()
print("\n=== FIN: PRUEBA KAFKA EXITOSA ===")