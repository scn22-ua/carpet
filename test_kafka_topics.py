# -*- coding: utf-8 -*-
"""
Test de publicación y consumo en topics Kafka del proyecto
"""
import sys
import time
import common.kafka_config as kconfig

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "caboose.proxy.rlwy.net:37367"

def test_kafka():
    print("Probando Kafka con el broker de Railway:", BOOTSTRAP)
    producer = kconfig.create_producer(BOOTSTRAP)
    
    test_msg = {"test": "hello_water_management", "time": time.time()}
    future = producer.send(kconfig.TOPIC_REQUESTS, test_msg)
    record_metadata = future.get(timeout=15)
    print(f"✅ Mensaje enviado a topic '{record_metadata.topic}', partición {record_metadata.partition}, offset {record_metadata.offset}")
    producer.close()
    print("Prueba de Kafka exitosa.")

if __name__ == '__main__':
    test_kafka()
