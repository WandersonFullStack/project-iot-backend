import logging

mqtt_broker_configs = {
    "HOST": "localhost",
    "PORT": 1883,
    "CLIENT_ID": "client_project",
    "KEEPALIVE": 60,
    "TOPIC": [
        ("home/sensors/#", 1),  # (tópico, QoS)
        ("home/alerts", 2),
    ],
    "DB_PATH": "mqtt_data.db"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)