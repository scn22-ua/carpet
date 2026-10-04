# Water Management Network

Práctica de Sistemas Distribuidos (26/27).

Es un sistema para controlar y simular el riego de diferentes estaciones.

## ¿Qué tiene?

El proyecto está dividido en varias partes:

* `WM_Central.py` → servidor central y panel web.
* `WM_WS_M.py` → monitor de las estaciones.
* `WM_WS_E.py` → motor de las estaciones.
* `WM_FO.py` → programa del operario.
* `common/` → protocolos y configuración de Kafka.
* `database/` → base de datos SQLite.
* `web/` → página web.

Para comunicarse se usan **Sockets TCP** y **Apache Kafka**. Los datos se guardan en **SQLite**.

## Instalar

Necesitas Python y las dependencias del proyecto:

```bash
pip install -r requirements.txt
```

## Ejecutar

Para probarlo todo en el mismo PC, abre varias terminales.

### 1. Central

```bash
python WM_Central.py 9000 caboose.proxy.rlwy.net:37367 --web-port 3000
```

Panel web:

```text
http://localhost:3000
```

### 2. Estación WS-01

Monitor:

```bash
python WM_WS_M.py 9101 127.0.0.1:9000 WS-01
```

Motor:

```bash
python WM_WS_E.py caboose.proxy.rlwy.net:37367 127.0.0.1:9101 --ws-id WS-01 --web-port 3001
```

Panel de la estación:

```text
http://localhost:3001
```

### 3. Operario

```bash
python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01
```

Desde aquí se puede solicitar un riego.

También se pueden ejecutar servicios desde un archivo:

```bash
python WM_FO.py caboose.proxy.rlwy.net:37367 FO-01 --file servicios.txt
```

## Estados de una estación

Una estación puede estar en estos estados:

* **DISPONIBLE** → está conectada y preparada.
* **REGANDO** → está realizando un riego.
* **FUGA** → se ha detectado una fuga.
* **FUERA DE SERVICIO** → está bloqueada.
* **DESCONECTADA** → no tiene conexión con la Central.

## Probar una fuga

Desde `WM_WS_E.py`:

```text
k
```

Esto simula una fuga.

Para volver al estado normal:

```text
r
```

También se puede hacer desde el panel web de la estación.

## Ejecutarlo en varios PCs

La Central debe estar en un ordenador y las estaciones y operarios pueden estar en otros.

Por ejemplo, si la Central tiene la IP `192.168.1.50`:

```bash
python WM_WS_M.py 9101 192.168.1.50:9000 WS-01
```

El resto de programas se configuran usando la IP de la Central y el broker de Kafka.

## Railway

El proyecto incluye un `Dockerfile`, por lo que se puede desplegar en Railway.

En Railway hay que configurar el servicio y el acceso a los puertos necesarios.

## Estructura

```text
WM_Central.py
WM_WS_M.py
WM_WS_E.py
WM_FO.py

common/
    protocol.py
    kafka_config.py

database/
    db.py

web/
    templates/
        index.html
```
