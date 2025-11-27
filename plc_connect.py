import json
import time
import sys
import uuid
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

print("🚀 Iniciando conexión MQTT...")

# Configurar MQTT
mqtt_client = AWSIoTMQTTClient("PLC1500")
mqtt_client.configureEndpoint("a38uh0g3ieldg9-ats.iot.us-east-1.amazonaws.com", 8883)
mqtt_client.configureCredentials(
    "certificados-plc/AmazonRootCA1.pem",
    "certificados-plc/plc-private.pem.key", 
    "certificados-plc/plc-certificate.pem.crt"
)

mqtt_client.configureAutoReconnectBackoffTime(1, 32, 20)
mqtt_client.configureConnectDisconnectTimeout(10)
mqtt_client.configureMQTTOperationTimeout(5)

try:
    print("🔌 Conectando a AWS IoT...")
    
    if not mqtt_client.connect():
        print("❌ No se pudo conectar - timeout")
        sys.exit(1)
    
    print("✅ Conectado exitosamente!")
    
    mensaje = {
        "id": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "values": {
            "boton1": True, "y1": False, "y2": True,
            "sale1": False, "entra1": False, "Tag_e": False,
            "boton_e": False, "iy3": True, "iy4": False
        }
    }
    
    print("📤 Enviando mensaje...")
    print(f"📝 Contenido: {json.dumps(mensaje, indent=2)}")
    
    mqtt_client.publish("plc/PLC1500/values", json.dumps(mensaje), 1)
    print("✅ Mensaje publicado!")
    
    time.sleep(1)
    
except Exception as e:
    print(f"❌ Error: {e}")
    
finally:
    print("🔌 Desconectando...")
    mqtt_client.disconnect()
    print("🏁 Script completado")

