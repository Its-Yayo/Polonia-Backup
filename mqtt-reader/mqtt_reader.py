import paho.mqtt.client as mqtt
import json
import time
import logging

# Configuración logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración MQTT
MQTT_BROKER = "IP_O_HOST_DEL_SERVIDOR"  # Cambiar por tu servidor
MQTT_PORT = 1883
MQTT_TOPICS = [
    "sensors/temperature",
    "sensors/humidity",
    "sensors/#"  # Para escuchar todos los sensores
]

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("✅ Conectado al broker MQTT")
        # Suscribirse a todos los topics
        for topic in MQTT_TOPICS:
            client.subscribe(topic)
            logger.info(f"👂 Suscrito a: {topic}")
    else:
        logger.error(f"❌ Error de conexión: Código {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        logger.info(f"📨 Mensaje recibido - Topic: {msg.topic}, Payload: {payload}")
        
        # Aquí procesas los datos como necesites
        process_data(msg.topic, payload)
        
    except Exception as e:
        logger.error(f"💥 Error procesando mensaje: {e}")

def process_data(topic, payload):
    """Procesa los datos recibidos"""
    try:
        # Intentar parsear como JSON
        if payload.startswith('{') or payload.startswith('['):
            data = json.loads(payload)
            logger.info(f"📊 JSON parseado: {data}")
        else:
            # Es texto plano
            logger.info(f"📝 Texto plano: {payload}")
            
        # Aquí puedes:
        # - Guardar en archivo local
        # - Enviar a otra API
        # - Procesar los datos
        # - Preparar para enviar a AWS después
        
    except json.JSONDecodeError:
        logger.warning(f"⚠️  Payload no es JSON válido: {payload}")
    except Exception as e:
        logger.error(f"💥 Error en process_data: {e}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    # Configurar si necesitas autenticación
    # client.username_pw_set("usuario", "contraseña")
    
    try:
        logger.info(f"🚀 Conectando a {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
        
    except KeyboardInterrupt:
        logger.info("🛑 Deteniendo cliente MQTT...")
        client.disconnect()
    except Exception as e:
        logger.error(f"💥 Error de conexión: {e}")

if __name__ == "__main__":
    main()
