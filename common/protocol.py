# -*- coding: utf-8 -*-
"""
Módulo de Protocolo de Sockets - Práctica WaterManagement
Implementa la especificación recomendada de la asignatura:
<STX><DATA><ETX><LRC> con control por tramas y caracteres ASCII estándar:
- <ENQ> (0x05): Solicitud de inicio de diálogo
- <ACK> (0x06): Confirmación positiva
- <NAK> (0x15): Confirmación negativa
- <STX> (0x02): Inicio de texto
- <ETX> (0x03): Fin de texto
- <LRC>: Checksum XOR byte a byte del mensaje (entre STX y ETX exclusivo)
- <EOT> (0x04): Fin de transmisión
"""

import socket
import logging

STX = b'\x02'
ETX = b'\x03'
EOT = b'\x04'
ENQ = b'\x05'
ACK = b'\x06'
NAK = b'\x15'

DELIMITER = '#'

logger = logging.getLogger("Protocol")

def calculate_lrc(data_bytes: bytes) -> int:
    """
    Calcula el Longitudinal Redundancy Check (LRC):
    XOR acumulado byte a byte de los datos del mensaje.
    """
    lrc = 0
    for b in data_bytes:
        lrc ^= b
    return lrc

def pack_message(content: str) -> bytes:
    """
    Empaqueta una cadena de texto en formato <STX><DATA><ETX><LRC>.
    """
    data_bytes = content.encode('utf-8')
    lrc_val = calculate_lrc(data_bytes)
    return STX + data_bytes + ETX + bytes([lrc_val])

def recv_exact(sock: socket.socket, num_bytes: int, timeout: float = 10.0) -> bytes:
    """
    Lee exactamente num_bytes del socket o lanza excepción si se cierra.
    """
    sock.settimeout(timeout)
    chunks = []
    received = 0
    while received < num_bytes:
        chunk = sock.recv(num_bytes - received)
        if not chunk:
            raise ConnectionResetError("Conexión cerrada por el extremo remoto")
        chunks.append(chunk)
        received += len(chunk)
    return b''.join(chunks)

def send_frame(sock: socket.socket, message: str, wait_ack: bool = True, timeout: float = 10.0) -> bool:
    """
    Envía un mensaje con formato <STX><DATA><ETX><LRC> y opcionalmente espera <ACK>.
    """
    sock.settimeout(timeout)
    frame = pack_message(message)
    sock.sendall(frame)
    if wait_ack:
        resp = recv_exact(sock, 1, timeout=timeout)
        if resp == ACK:
            return True
        elif resp == NAK:
            logger.warning("Recibido NAK del receptor")
            return False
        else:
            logger.warning(f"Respuesta inesperada al enviar frame: {resp}")
            return False
    return True

def recv_frame(sock: socket.socket, send_ack: bool = True, timeout: float = 10.0) -> str:
    """
    Lee un mensaje <STX><DATA><ETX><LRC> del socket, verifica el LRC y opcionalmente envía <ACK>/<NAK>.
    Devuelve la cadena desempaquetada.
    """
    sock.settimeout(timeout)
    # 1. Buscar STX (puede haber bytes de control intermedios como ENQ o EOT)
    while True:
        b = recv_exact(sock, 1, timeout=timeout)
        if b == STX:
            break
        elif b == EOT:
            return ""  # Fin de transmisión
        elif b == ENQ:
            sock.sendall(ACK)
            continue
        # Ignorar caracteres no esperados hasta encontrar STX

    # 2. Leer hasta ETX
    data_bytes = bytearray()
    while True:
        b = recv_exact(sock, 1, timeout=timeout)
        if b == ETX:
            break
        data_bytes.append(b[0])

    # 3. Leer el byte LRC
    lrc_byte = recv_exact(sock, 1, timeout=timeout)
    calculated = calculate_lrc(bytes(data_bytes))

    if lrc_byte[0] != calculated:
        if send_ack:
            sock.sendall(NAK)
        raise ValueError(f"Error de integridad LRC: calculado {calculated}, recibido {lrc_byte[0]}")

    if send_ack:
        sock.sendall(ACK)

    return bytes(data_bytes).decode('utf-8', errors='replace')

def send_enq_handshake(sock: socket.socket, timeout: float = 10.0) -> bool:
    """
    Inicia diálogo enviando <ENQ> y esperando <ACK>.
    """
    try:
        sock.settimeout(timeout)
        sock.sendall(ENQ)
        resp = recv_exact(sock, 1, timeout=timeout)
        return resp == ACK
    except Exception as e:
        logger.error(f"Error en handshake ENQ: {e}")
        return False

def send_eot(sock: socket.socket):
    """
    Envía <EOT> para indicar fin de sesión de sockets.
    """
    try:
        sock.sendall(EOT)
    except Exception:
        pass
