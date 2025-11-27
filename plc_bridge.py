#!/usr/bin/env python3
import paho.mqtt.client as mqtt
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json, uuid, time

# ============= CONFIGURACIÓN LOCAL (OT -> IT) =============
LOCAL_MQTT_HOST = "192.168.20.3"
LOCAL_MQTT_PORT = 1883
LOCAL_TOPIC = "plc/values"

AWS_ENDPOINT = "a38uh0g3ieldg9-ats.iot.us-east-1.amazonaws.com"
AWS_TOPIC = "plc/PLC1500/values"

CA = "/home/yayo/mqtt-iotcore/certificados-plc/AmazonRootCA1.pem"
KEY = "/home/yayo/mqtt-iotcore/certificados-plc/plc-private.pem.key"
CERT = "/home/yayo/mqtt-iotcore/certificados-plc/plc-certificate.pem.crt"


# ============= CONFIGURACIÓN AWS =============
aws = AWSIoTMQTTClient("PLC1500Bridge")
aws.configureEndpoint(AWS_ENDPOINT, 8883)
aws.configureCredentials(CA, KEY, CERT)
aws.configureAutoReconnectBackoffTime(1, 32, 20)
aws.configureConnectDisconnectTimeout(10)
aws.configureMQTTOperationTimeout(5)

print("🔌 Conectando a AWS IoT Core...")
aws.connect()
print("✅ Conectado a AWS!\n")


# ============= CALLBACK: MENSAJES =============
def on_local_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode())

        # Debug: ver exactamente lo que viene de OT
        print("📥 Payload recibido desde OT:", raw)

        # Soportar ambos formatos: plano o con "values"
        if isinstance(raw, dict) and "values" in raw and isinstance(raw["values"], dict):
            data = raw["values"]      # Formato antiguo
        else:
            data = raw               # Formato nuevo plano

        # Debug: ver estructura ya seleccionada
        print("📦 Datos mapeados:", data)

        # Crear mensaje final
        mensaje = {
            "id": str(uuid.uuid4()),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "timestamp_bridge": time.strftime("%Y-%m-%dT%H:%M:%S%z"),

            "inicio_plc1500": data.get("boton123"),
            "sensor_capacitivo_1500": data.get("y1"),
            "sensor_carrera_1500": data.get("y2"),

            "ev_extender_piston": data.get("sale1"),
            "ev_retraer_piston": data.get("entra1"),

            "inicio_plc1200": data.get("Tag_e"),
            "senal_a_plc1200": data.get("boton_e"),
            "sensor_capacitivo_1200": data.get("iy3"),
            "sensor_carrera_1200": data.get("iy4")
        }

        # Debug final: ver lo que se enviará a AWS
        print("📤 Enviando a AWS:", mensaje)

        aws.publish(AWS_TOPIC, json.dumps(mensaje), 1)

    except Exception as e:
        print("❌ Error procesando mensaje:", e)


# ============= CALLBACK: AL CONECTAR =============
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("🔄 Conectado/Re-conectado a OT. Resuscribiendo...")
        client.subscribe(LOCAL_TOPIC)
        print(f"📡 Suscrito a {LOCAL_TOPIC}")
    else:
        print(f"⚠ Error al conectar OT: rc={rc}")


# ============= CALLBACK: AL DESCONECTAR =============
def on_disconnect(client, userdata, rc):
    print("❌ Desconectado del broker OT, intentando re-conexión...")
    while True:
        try:
            client.reconnect()
            print("🔄 Reconexión exitosa a broker OT")
            break
        except Exception:
            print("⏳ OT caído, reintento en 3s...")
            time.sleep(3)


# ============= CONFIGURE EL CLIENTE MQTT LOCAL =============
local = mqtt.Client()
local.on_message = on_local_message
local.on_connect = on_connect
local.on_disconnect = on_disconnect

print(f"📡 Intentando conexión al broker OT {LOCAL_MQTT_HOST}:{LOCAL_MQTT_PORT} ...")

connected = False
while not connected:
    try:
        local.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT)
        connected = True
    except Exception:
        print("⏳ Broker OT no accesible, reintentando en 3s...")
        time.sleep(3)

local.loop_start()

# Mantenemos vivo el servicio
while True:
    time.sleep(1)
