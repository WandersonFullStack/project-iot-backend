from __future__ import annotations
import json

from app.config.broker_configs import log
from app.controllers.database_controller import DatabaseController
from app.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller
from backend.app.auth.device_auth import check_api_key

class ProtocolBridge:
    """
    Ponto único de integração entre protocolos industriais e o núcleodo gateway.
    """
    def __init__(self, db: DatabaseController, mqtt: CallbacksMQTTContrller):
        self.db = db
        self.mqtt = mqtt

    def authenticate(self, device_id: str, api_key: str) -> dict:
        """
        Valida as credenciais do dispositivo.

        Retorna o registro do dispositivo (dict) em caso de sucesso,
        ou None se o device_id não existir, estiver inativo ou a
        api_key não corresponder ao hash armazenado.
        """
        row = self.db.search_device(device_id)
        if not row or not row["active"]:
            log.warning("Auth failed -> uncknown or inactive device_id: %s", device_id)
            return None
        if not check_api_key(api_key, row["api_key_hash"]):
            log.warning("Auth failed -> api_key invalid for: %s", device_id)
            return None
        log.info("Auth OK -> device: %s (%s)", device_id, row["name"])
        return dict(row)
    
    def to_forward(
            self,
            device_id: str,
            topic: str,
            payload: str | dict,
            qos: int = 1,
            content_type: str = "application/json"
    ) -> int:
        """
        Recebe dado de qualquer adaptador, publica no MQTT e persiste no banco.
        Retorna o ID do registro inserido no banco.
        """
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str)

        # Publica no broker MQTT
        self.mqtt._publish(
            topic=topic,
            payload=payload,
            qos=qos,
            content_type=content_type,
            user_properties=[("device_id", device_id), ("origin", "industrial_protocol")]
        )

        # Persiste no banco vinculando ao dispositivo
        row_id = self.db.insert_messages(
            device_id=device_id,
            topic=topic,
            payload=payload,
            qos=qos,
            retain=False,
            content_type=content_type
        )

        # Mantém o status de presença atualizado
        self.db.update_status(device_id, "online")
        return row_id
