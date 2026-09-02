import os
import logging


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


mqtt_broker_configs = {
    "HOST": os.getenv("MQTT_HOST", "localhost"),
    "PORT": int(os.getenv("MQTT_PORT", "1883")),
    "CLIENT_ID": os.getenv("MQTT_CLIENT_ID", "mqtt_client"),
    "KEEPALIVE": int(os.getenv("MQTT_KEEPALIVE", "60")),
    "USERNAME": os.getenv("MQTT_USERNAME"),
    "PASSWORD": os.getenv("MQTT_PASSWORD"),
    "TLS_ENABLED": _env_bool("MQTT_TLS_ENABLED"),
    "CA_CERT": os.getenv("MQTT_CA_CERT"),
    "TOPIC": [
        ("application/devices/#", 1),   # (tópico, QoS)
        ("application/devices/#", 2),
    ],    
    "DB_PATH": "mqtt_data.db"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)