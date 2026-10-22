FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Exponer el puerto web de Railway y el puerto de sockets
EXPOSE 3000
EXPOSE 9000

# Broker Kafka y puerto de sockets configurables por variables de entorno
ENV KAFKA_BOOTSTRAP_SERVERS=caboose.proxy.rlwy.net:37367
ENV SOCKET_PORT=9000

CMD ["sh", "-c", "python -u WM_Central.py ${SOCKET_PORT} ${KAFKA_BOOTSTRAP_SERVERS}"]
