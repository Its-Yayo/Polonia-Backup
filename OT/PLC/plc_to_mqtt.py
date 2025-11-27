import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta

import snap7
from snap7.type import Area
from snap7.util import get_bool
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WriteOptions

# ------------------- ZONA HORARIA LOCAL -------------------
MX_TZ = timezone(timedelta(hours=-6))
# ------------------- CONFIGURACIÓN DESDE ENV -------------------

PLC_IP = os.getenv("PLC_IP", "192.168.100.10")  # 192.168.0.33 para local
PLC_RACK = int(os.getenv("PLC_RACK", "0"))
PLC_SLOT = int(os.getenv("PLC_SLOT", "1"))

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "plc/values")

READ_INTERVAL = float(os.getenv("READ_INTERVAL", "1"))

# InfluxDB
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb2:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "OT-TOKEN-HERE")
INFLUX_ORG = os.getenv("INFLUX_ORG", "OT")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Historian")

# ------------------- TAGS -------------------

TAGS = [
    {"name": "boton123",  "address": "%I0.2"},
    {"name": "y1",      "address": "%I0.1"},
    {"name": "y2",      "address": "%I0.0"},

    {"name": "sale1",   "address": "%Q0.0"},
    {"name": "entra1",  "address": "%Q0.1"},

    {"name": "Tag_e",   "address": "%I0.3"},
    {"name": "boton_e", "address": "%Q12.5"},

    {"name": "iy3",     "address": "%I20.3"},
    {"name": "iy4",     "address": "%I20.4"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ------------------- FUNCIONES DE AYUDA -------------------

def parse_address(address: str):
    """
    Convierte algo como '%I0.2' o '%Q12.5' en:
    area (Area.PE/Area.PA), byte, bit
    """
    addr = address.strip()
    if not addr.startswith("%"):
        raise ValueError(f"Dirección inválida: {address}")

    area_char = addr[1].upper()  # I / Q / E / A
    rest = addr[2:]              # '0.2', '12.5', etc.

    byte_str, bit_str = rest.split(".")
    byte = int(byte_str)
    bit = int(bit_str)

    if area_char in ("I", "E"):
        area = Area.PE      # Entradas
    elif area_char in ("Q", "A"):
        area = Area.PA      # Salidas
    else:
        raise ValueError(f"Área no soportada en dirección: {address}")

    return area, byte, bit

# ------------------- PLC -------------------

def connect_plc():
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    if not client.get_connected():
        raise RuntimeError("No se pudo conectar al PLC")
    logging.info("Conectado al PLC %s (rack=%s, slot=%s)", PLC_IP, PLC_RACK, PLC_SLOT)
    return client

def read_bool_tag(plc_client, tag):
    area, byte, bit = parse_address(tag["address"])
    data = plc_client.read_area(area, 0, byte, 1)  # leemos 1 byte
    value = bool(get_bool(data, 0, bit))
    return value

def read_all_tags(plc_client):
    values = {}
    for tag in TAGS:
        try:
            values[tag["name"]] = read_bool_tag(plc_client, tag)
        except Exception as e:
            logging.error(
                "Error leyendo tag %s (%s): %s",
                tag["name"], tag["address"], e
            )
    return values

# ------------------- MQTT -------------------

def connect_mqtt():
    client = mqtt.Client(client_id="plc_publisher")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()  # hilo para manejar MQTT
    logging.info("Conectado a MQTT %s:%s", MQTT_BROKER, MQTT_PORT)
    return client

def publish_values(mqtt_client, values, now_local):
    payload = {
        "timestamp": now_local.isoformat(),  # fecha/hora local
        "values": values,
    }
    mqtt_client.publish(MQTT_TOPIC, json.dumps(payload), qos=0)
    logging.info("Publicado en %s: %s", MQTT_TOPIC, payload)

# ------------------- INFLUXDB -------------------

def connect_influx():
    client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
    )
    write_api = client.write_api(write_options=WriteOptions(batch_size=1))
    logging.info("Conectado a InfluxDB en %s", INFLUX_URL)
    return client, write_api

def write_values_influx(write_api, values, now_local):
    """
    Escribe todos los tags como puntos en InfluxDB.
    measurement: plc_tags
    tag: tag=<nombre del tag>
    field: value=<0/1>
    time: now_local (Influx lo normaliza a UTC)
    """
    points = []

    for name, val in values.items():
        p = (
            Point("plc_tags")
            .tag("tag", name)
            .field("value", int(bool(val)))  # 0 o 1
            .time(now_local)
        )
        points.append(p)

    if points:
        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=points,
        )
        logging.info("Escritos %d puntos en InfluxDB", len(points))

# ------------------- MAIN LOOP -------------------

def main():
    influx_client = None
    write_api = None

    while True:
        try:
            plc_client = connect_plc()
            mqtt_client = connect_mqtt()
            influx_client, write_api = connect_influx()

            while True:
                # hora local para este ciclo (misma para MQTT e Influx)
                now_local = datetime.now(MX_TZ)

                values = read_all_tags(plc_client)
                publish_values(mqtt_client, values, now_local)
                write_values_influx(write_api, values, now_local)

                time.sleep(READ_INTERVAL)

        except Exception as e:
            logging.error("Error en el ciclo principal: %s", e)
            logging.info("Reintentando en 5 segundos...")
            time.sleep(5)
        finally:
            if influx_client is not None:
                influx_client.close()
                influx_client = None
                write_api = None

if __name__ == "__main__":
    main()
