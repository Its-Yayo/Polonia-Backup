import os
import time
import json
import logging
import random
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt

MX_TZ = timezone(timedelta(hours=-6))

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "plc/values")
READ_INTERVAL = float(os.getenv("READ_INTERVAL", "1"))

TAGS = [
    {"name": "boton123", "address": "%I0.2"},
    {"name": "y1",       "address": "%I0.1"},
    {"name": "y2",       "address": "%I0.0"},
    {"name": "sale1",    "address": "%Q0.0"},
    {"name": "entra1",   "address": "%Q0.1"},
    {"name": "Tag_e",    "address": "%I0.3"},
    {"name": "boton_e",  "address": "%Q12.5"},
    {"name": "iy3",      "address": "%I20.3"},
    {"name": "iy4",      "address": "%I20.4"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def connect_mqtt():
    client = mqtt.Client(client_id="plc_simulator")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    logging.info("Simulador conectado a MQTT %s:%s", MQTT_BROKER, MQTT_PORT)
    return client

def generate_fake_values():
    values = {}
    for tag in TAGS:
        values[tag["name"]] = random.choice([0, 1])
    return values

def publish_values(mqtt_client, values, now_local):
    payload = {
        "timestamp": now_local.isoformat(),
        "values": values,
    }
    mqtt_client.publish(MQTT_TOPIC, json.dumps(payload), qos=0)
    logging.info("SIMULADOR publicó en %s: %s", MQTT_TOPIC, payload)

def main():
    mqtt_client = connect_mqtt()

    while True:
        now_local = datetime.now(MX_TZ)
        values = generate_fake_values()
        publish_values(mqtt_client, values, now_local)
        time.sleep(READ_INTERVAL)

if __name__ == "__main__":
    main()
