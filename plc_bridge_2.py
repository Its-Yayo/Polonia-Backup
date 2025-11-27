#!/usr/bin/env python3
import paho.mqtt.client as mqtt
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json, uuid, time

# ============= CONFIGURACIÓN LOCAL (OT -> IT) =============
LOCAL_MQTT_HOST = "192.168.20.3"     # Broker OT 
LOCAL_MQTT_PORT = 1883
LOCAL_TOPIC = "plc/values"          

AWS_ENDPOINT = "a38uh0g3ieldg9-ats.iot.us-east-1.amazonaws.com"
AWS_TOPIC = "plc/PLC1500/values"

CA = "/home/yayo/mqtt-iotcore/certificados-plc/AmazonRootCA1.pem"
KEY = "/home/yayo/mqtt-iotcore/certificados-plc/plc-private.pem.key"
CERT = "/home/yayo/mqtt-iotcore/certificados-plc/plc-certificate.pem.crt"


aws = AWSIoTMQTTClient("PLC1500Bridge")
aws.configureEndpoint(AWS_ENDPOINT, 8883)
aws.configureCredentials(CA, KEY, CERT)
aws.configureAutoReconnectBackoffTime(1, 32, 20)
aws.configureConnectDisconnectTimeout(10)
aws.configureMQTTOperationTimeout(5)

print("🔌 Conectando a AWS IoT Core...")
aws.connect()
print("✅ Conectado a AWS!\n")


def on_local_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        data["id"] = str(uuid.uuid4())
        data["timestamp_bridge"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")

        aws.publish(AWS_TOPIC, json.dumps(data), 1)
        print("📤 Enviado a AWS:", data)

    except Exception as e:
        print("❌ Error procesando mensaje:", e)


local = mqtt.Client()
local.on_message = on_local_message

print(f"📡 Conectando al broker OT {LOCAL_MQTT_HOST}:{LOCAL_MQTT_PORT} ...")
local.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT)
local.subscribe(LOCAL_TOPIC)
print(f"👂 Escuchando tópico local: {LOCAL_TOPIC}")

local.loop_forever()
