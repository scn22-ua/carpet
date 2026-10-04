# -*- coding: utf-8 -*-
"""
Módulo de Base de Datos SQLite - Práctica WaterManagement
Gestiona las estaciones de riego (WS), operarios (FO) e historial de riegos.
"""

import sqlite3
import datetime
import os
import threading
from typing import List, Dict, Any, Optional

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "water_management.db")
_lock = threading.Lock()

def get_connection(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path: str = DB_FILE):
    """
    Inicializa las tablas SQLite y carga datos iniciales de parques y operarios.
    """
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()

        # Tabla de Estaciones de Riego (WS)
        # Estados válidos:
        # - AVAILABLE (Disponible - Verde)
        # - WATERING (Regando - Verde Parpadeante)
        # - LEAK (Fuga - Rojo)
        # - OUT_OF_SERVICE (Fuera de Servicio - Naranja)
        # - OFFLINE (Desconectada - Gris)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS watering_stations (
                id TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OFFLINE',
                flow REAL NOT NULL DEFAULT 0.0,
                accumulated_vol REAL NOT NULL DEFAULT 0.0,
                current_operator TEXT,
                last_seen TEXT
            )
        ''')

        # Tabla de Operarios de Campo (FO)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operators (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            )
        ''')

        # Tabla de Historial y Logs de Riegos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS watering_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ws_id TEXT NOT NULL,
                operator_id TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_sec INTEGER DEFAULT 0,
                volume_liters REAL DEFAULT 0.0,
                status TEXT NOT NULL
            )
        ''')

        # Cargar datos iniciales de ejemplo si no existen
        cursor.execute("SELECT COUNT(*) FROM watering_stations")
        if cursor.fetchone()[0] == 0:
            initial_stations = [
                ("WS-01", "Parque de Canalejas", "OFFLINE"),
                ("WS-02", "Jardines del Palmeral", "OFFLINE"),
                ("WS-03", "Parque La Marjal", "OFFLINE"),
                ("WS-04", "Parque Central", "OFFLINE"),
                ("WS-05", "Explanada de España", "OFFLINE"),
            ]
            cursor.executemany(
                "INSERT INTO watering_stations (id, location, status) VALUES (?, ?, ?)",
                initial_stations
            )

        cursor.execute("SELECT COUNT(*) FROM operators")
        if cursor.fetchone()[0] == 0:
            initial_operators = [
                ("FO-01", "Carlos Gómez - Jardinería Zona Centro"),
                ("FO-02", "Ana Martínez - Mantenimiento Parques Sur"),
                ("FO-03", "Luis Navarro - Supervisor Playas y Jardines"),
            ]
            cursor.executemany(
                "INSERT INTO operators (id, name) VALUES (?, ?)",
                initial_operators
            )

        conn.commit()
        conn.close()

def reset_stations_to_offline(db_path: str = DB_FILE):
    """
    Requisito Punto 1: Al arrancar CENTRAL, todas las estaciones registradas
    se muestran como DESCONECTADAS (OFFLINE) hasta que se conecten físicamente.
    """
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE watering_stations 
            SET status = 'OFFLINE', flow = 0.0, accumulated_vol = 0.0, current_operator = NULL
        ''')
        conn.commit()
        conn.close()

def get_all_stations(db_path: str = DB_FILE) -> List[Dict[str, Any]]:
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watering_stations ORDER BY id")
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows

def get_station(ws_id: str, db_path: str = DB_FILE) -> Optional[Dict[str, Any]]:
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watering_stations WHERE id = ?", (ws_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

def register_station_if_new(ws_id: str, location: str, db_path: str = DB_FILE):
    """
    Registra una estación si es nueva o actualiza su ubicación.
    """
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO watering_stations (id, location, status) 
            VALUES (?, ?, 'AVAILABLE')
            ON CONFLICT(id) DO UPDATE SET 
                location = excluded.location,
                status = 'AVAILABLE',
                last_seen = datetime('now')
        ''', (ws_id, location))
        conn.commit()
        conn.close()

def update_station_status(ws_id: str, status: str, db_path: str = DB_FILE):
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        if status in ('AVAILABLE', 'OFFLINE', 'LEAK', 'OUT_OF_SERVICE'):
            cursor.execute('''
                UPDATE watering_stations 
                SET status = ?, flow = 0.0, current_operator = NULL, last_seen = ?
                WHERE id = ?
            ''', (status, now, ws_id))
        else:
            cursor.execute('''
                UPDATE watering_stations 
                SET status = ?, last_seen = ?
                WHERE id = ?
            ''', (status, now, ws_id))
        conn.commit()
        conn.close()

def update_station_telemetry(ws_id: str, flow: float, accumulated_vol: float, operator_id: Optional[str] = None, db_path: str = DB_FILE):
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        now = datetime.datetime.now().isoformat()
        if operator_id:
            cursor.execute('''
                UPDATE watering_stations 
                SET flow = ?, accumulated_vol = ?, current_operator = ?, last_seen = ?
                WHERE id = ?
            ''', (flow, accumulated_vol, operator_id, now, ws_id))
        else:
            cursor.execute('''
                UPDATE watering_stations 
                SET flow = ?, accumulated_vol = ?, last_seen = ?
                WHERE id = ?
            ''', (flow, accumulated_vol, now, ws_id))
        conn.commit()
        conn.close()

def get_operator(operator_id: str, db_path: str = DB_FILE) -> Optional[Dict[str, Any]]:
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM operators WHERE id = ?", (operator_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

def log_watering_event(ws_id: str, operator_id: str, start_time: str, end_time: str, duration_sec: int, volume_liters: float, status: str, db_path: str = DB_FILE):
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO watering_logs (ws_id, operator_id, start_time, end_time, duration_sec, volume_liters, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (ws_id, operator_id, start_time, end_time, duration_sec, volume_liters, status))
        conn.commit()
        conn.close()

def get_recent_logs(limit: int = 15, db_path: str = DB_FILE) -> List[Dict[str, Any]]:
    with _lock:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watering_logs ORDER BY id DESC LIMIT ?", (limit,))
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
