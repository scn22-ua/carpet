# 🌿 Water Management Network - Práctica de Sistemas Distribuidos (SD 26/27)

Sistema distribuido para la simulación, monitorización y control en tiempo real del riego urbano en parques y jardines.

Desarrollado según la especificación oficial de la asignatura **Sistemas Distribuidos** (Curso 26/27), combinando **comunicación mediante Sockets TCP de bajo nivel**, **Streaming de eventos y colas con Apache Kafka**, **persistencia en SQLite** y un **cuadro de mandos web en tiempo real**.

---

## 🏛️ 1. Arquitectura y Conceptos Clave

El sistema se compone de tres entidades distribuidas:

```
                      +------------------------------------------+
                      |         CLOUD / HOST CENTRAL             |
                      |  +------------------------------------+  |
                      |  |             WM_Central             |  |
                      |  |   (Lógica, Sockets, Panel Web)     |  |
                      |  +-----------------+------------------+  |
                      |         |          |                     |
                      |    [SQLite]   [Sockets TCP]              |
                      +---------|----------|---------------------+
                                |          | (AUTH, LEAK)
                                |          v
+-------------------------+     |     +-------------------------+
|     PC / DISPOSITIVO    |     |     |    PC ESTACIONES WS     |
|   FIELD OPERATOR (FO)   |     |     |  +-------------------+  |
|  +-------------------+  |     |     |  |      WM_WS_M      |  |
|  |       WM_FO       |  |     |     |  |     (Monitor)     |  |
|  +---------+---------+  |     |     |  +---------+---------+  |
+------------|------------+     |     +------------| (Health    +
             |                  |                  |  Check 1s) 
             | (Peticiones      |                  v            
             |  y Telemetría)   |     +-------------------------+
             |                  |     |         WM_WS_E         |
             |                  |     |        (Engine)         |
             |                  |     +------------+------------+
             |                  |                  | (Telemetría
             v                  v                  v  y Comandos)
       +-------------------------------------------------+
       |              APACHE KAFKA BROKER                |
       |  - wm-requests   : Peticiones de operarios      |
       |  - wm-responses  : Aprobaciones y resúmenes     |
       |  - wm-commands   : Órdenes a electroválvulas    |
       |  - wm-telemetry  : Caudal y litros cada segundo |
       +-------------------------------------------------+
```

### ¿Por qué dos paradigmas de comunicación?
1. **Sockets TCP (`<STX><DATA><ETX><LRC>`)**:
   - Usados para la supervisión directa y de baja latencia entre el **Monitor (`WM_WS_M`)** y la **Central (`WM_Central`)**, y entre el **Monitor** y el **Motor (`WM_WS_E`)**.
   - Garantizan acoplamiento estrecho, control de tramas y detección inmediata de caídas de red o roturas de canal.
   - Incluyen un cálculo de integridad **LRC** (Longitudinal Redundancy Check: XOR acumulativo byte a byte) y caracteres de control ASCII (`<ENQ>`, `<ACK>`, `<NAK>`, `<STX>`, `<ETX>`, `<EOT>`).
2. **Apache Kafka (Streaming de Eventos y Mensajería Asíncrona)**:
   - Usado para la coordinación de riegos, el envío continuo de telemetría (caudal y volumen) y las peticiones de los operarios de campo.
   - Permite alta concurrencia, resiliencia y desacoplamiento total: la Central y múltiples operarios pueden recibir eventos en tiempo real sin bloquearse mutuamente.

---

## 🚦 2. Estados de las Estaciones de Riego (WS)

En el panel de monitorización de `WM_Central` y en la base de datos, cada estación refleja uno de los siguientes 5 estados:

| Estado | Color Fondo | Significado |
| :--- | :--- | :--- |
| **DISPONIBLE** | 🟢 **Verde** | Estación conectada, autenticada y lista para recibir solicitudes de riego. |
| **REGANDO** | ❇️ **Verde Parpadeante** | Electroválvula abierta suministrando agua. Muestra en tiempo real caudal (L/min), volumen (L) y operario. |
| **FUGA** | 🔴 **Rojo** | Sensor de avería activado. Riego detenido inmediatamente por seguridad. |
| **FUERA DE SERVICIO** | 🟠 **Naranja** | Bloqueada deliberadamente por orden de `WM_Central`. No acepta riegos. |
| **DESCONECTADA** | ⚪ **Gris** | Estación dada de alta en BD pero sin conexión de socket activa con Central. |

---

## 💻 3. Cómo Ejecutarlo Esta Noche (Paso a Paso)

Puedes probar todo el sistema en tu propio ordenador abriendo varias terminales, o distribuirlo entre múltiples PCs según el PDF.

### Requisitos Previos
Instala las dependencias en tu entorno de Python (si aún no las tienes):
```bash
pip install -r requirements.txt
```

### 3.1. En un único PC (Prueba rápida con scripts `.bat`)
Se han creado scripts preparados para Windows con codificación UTF-8:

1. **Terminal 1 - Arrancar Central:**
   Doble clic en `run_central.bat` o ejecuta:
   ```bash
   python WM_Central.py 9000 caboose.proxy.rlwy.net:37367 --web-port 3000
   ```
   *Nota:* También puedes pasar el puerto de escucha local `9092` si ejecutas Kafka en local: `python WM_Central.py 9000 9092 --web-port 3000`.
   *Abre en tu navegador:* `http://localhost:3000` para ver el panel de control central en tiempo real. Todas las estaciones aparecerán en color **Gris (Desconectada)** como exige el requisito 1 del PDF.

2. **Terminal 2 - Arrancar Estación de Riego WS-01:**
   Doble clic en `run_ws1.bat` (o ejecuta manualmente):
   - Monitor:
     ```bash
     python WM_WS_M.py 9101 127.0.0.1:9000 WS-01
     ```
   - Engine (con panel web dedicado en puerto 3001):
     ```bash
     python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001
     ```
   *En el panel central verás cómo `WS-01` pasa inmediatamente a **Verde (Disponible)**.*
   *Panel dedicado exclusivo para WS-01:* Abre `http://localhost:3001` para ver el panel de actuador en tiempo real de únicamente `WS-01`, con controles locales, simulación de fuga/OK, electroválvula animada y telemetría propia.

3. **Terminal 3 - Arrancar Operario de Campo FO-01 (Modo Interactivo):**
   Doble clic en `run_fo1.bat` o ejecuta:
   ```bash
   python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01
   ```
   - Elige la opción `1` para solicitar riego.
   - Introduce `WS-01` y duración `10` segundos.
   - Observarás cómo:
     - La Central comprueba disponibilidad y notifica al operario paso a paso.
     - La electroválvula de `WS-01` se abre (se ve el flujo de agua animado en su panel dedicado `http://localhost:3001`).
     - La tarjeta en el panel central parpadea en azul/verde y muestra el caudal y volumen en vivo.
     - En la consola del operario se muestra la telemetría segundo a segundo.
     - Al terminar, Central envía el resumen final con los litros totales consumidos.

4. **Terminal 4 - Probar Simulación de Fuga (Tecla `k` o botón Web):**
   - En la consola de `WM_WS_E`, pulsa `k` y pulsa Enter (o pulsa el botón **"🚨 Simular Fuga (KO)"** en el panel web de la estación `http://localhost:3001`).
   - El motor responderá `KO` al monitor en el siguiente segundo.
   - El monitor detectará la avería y enviará una trama `FAULT#WS-01#LEAK` a Central.
   - En los paneles web la estación cambiará a **Rojo (Fuga)** y detendrá cualquier riego.
   - Para resolver la avería, pulsa `r` + Enter en `WM_WS_E` (o el botón **"🟢 Reparar / Salud OK"** en la web). Volverá a **Verde**.

5. **Terminal 5 - Modo Fichero por Lotes (Batch):**
   Doble clic en `run_fo_batch.bat` o ejecuta:
   ```bash
   python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01 --file servicios.txt
   ```
   - Ejecutará secuencialmente las solicitudes indicadas en `servicios.txt` (ej: WS-01 y WS-02).
   - **Cumple el requisito del PDF: espera obligatoria de 4 segundos entre servicios.**

---

## 🌐 4. Despliegue en Varios Ordenadores (Laboratorio o Red Local)

Para la evaluación oficial de la práctica (Figura 4 del PDF, pág. 13):

### Escenario Físico:
- **Cloud / Servidor (o PC Servidor)**: `WM_Central`, base de datos SQLite y Apache Kafka.
- **PC Laboratorio 1**: Módulos de Operarios (`WM_FO`).
- **PC Laboratorio 2**: Módulos de Estaciones de Riego (`WM_WS_M` y `WM_WS_E`).

### Pasos de Configuración en Red:
1. **Averiguar la IP del PC Servidor donde corre `WM_Central`**:
   - En Windows: `ipconfig` (ej: `192.168.1.50`).
   - Asegúrate de que el puerto `9000` (sockets) y `3000` (web) estén abiertos en el firewall del PC de la Central.
2. **En el PC de las Estaciones de Riego (PC Laboratorio 2)**:
   - Arranca el Monitor indicando la IP de la Central:
     ```bash
     python WM_WS_M.py 9101 192.168.1.50:9000 WS-01
     ```
   - Arranca el Engine (comunicándose localmente con el monitor en su puerto 9101 y al Kafka en la nube):
     ```bash
     python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001
     ```
3. **En el PC de los Operarios (PC Laboratorio 1)**:
   - Arranca el operario apuntando al broker de Kafka:
     ```bash
     python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01
     ```
4. **Abrir el panel web desde cualquier PC o móvil de la red**:
   - Visita `http://192.168.1.50:3000` para supervisar todo el sistema.

---

## ☁️ 5. Despliegue en Railway (Cloud)

El repositorio incluye el archivo `Dockerfile` configurado:
1. Sube este repositorio a tu cuenta de GitHub (`git add .`, `git commit -m "WaterManagement v1"`, `git push origin main`).
2. En tu proyecto de Railway:
   - Crea un nuevo servicio seleccionando tu repositorio de GitHub.
   - Railway detectará automáticamente el `Dockerfile` y compilará la imagen.
   - En **Settings > Networking**, genera un dominio público HTTP (Railway asignará automáticamente la variable `PORT`, que `WM_Central` toma por defecto).
   - Para el puerto de sockets `9000`, puedes habilitar un **TCP Proxy** en Railway para conectar las estaciones remotas directamente a tu Central en la nube.

---

## 📋 6. Estructura del Código Fuente

- `WM_Central.py`: Servidor central, base de datos SQLite, sockets de estaciones, consumidor/productor Kafka y servidor web de control.
- `WM_WS_M.py`: Monitor de estación de riego. Autentica ante Central y vigila la salud del motor mediante sockets.
- `WM_WS_E.py`: Motor de estación de riego. Simula electroválvula y caudalímetro, escucha órdenes Kafka y permite simular averías por teclado.
- `WM_FO.py`: Aplicación del operario de campo en modo interactivo o por archivo de lotes con espera de 4s.
- `common/protocol.py`: Protocolo de sockets estándar `<STX><DATA><ETX><LRC>`.
- `common/kafka_config.py`: Definición de topics y helpers de serialización JSON para Kafka.
- `database/db.py`: Capa de persistencia SQLite para estaciones, operarios y registro histórico de riegos.
- `web/templates/index.html`: Cuadro de mandos visual interactivo en tiempo real.
