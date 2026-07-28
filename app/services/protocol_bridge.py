from __future__ import annotations
import json

from sqlalchemy.ext.asyncio import AsyncSession

from app.config.broker_configs import log
from app.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller
from app.auth.device_auth import check_api_key

from app.controllers import (
    device_controller as dc,
    message_controller as mc
)

class ProtocolBridge:
    """
    Ponto único de integração entre protocolos industriais (TCP, Modbus)
    e o núcleo do gateway (MQTT + banco).
    """
    def __init__(self, mqtt: CallbacksMQTTContrller):
        self.mqtt = mqtt

    async def authenticate(self, db: AsyncSession, device_id: str, api_key: str) -> dict:
        """
        Valida as credenciais do dispositivo.

        Retorna o registro do dispositivo (dict) em caso de sucesso,
        ou None se o device_id não existir, estiver inativo ou a
        api_key não corresponder ao hash armazenado.
        """
        device = await dc.search_device(db, device_id)
        if not device or not device.active:
            log.warning("Auth failed -> uncknown or inactive device_id: %s", device_id)
            return None
        if not check_api_key(api_key, device.api_key_hash):
            log.warning("Auth failed -> api_key invalid for: %s", device_id)
            return None
        log.info("Auth OK -> device: %s (%s)", device_id, device.name)
        return {
            "device": device.device_id,
            "name": device.name,
            "active": device.active,
            "status": device.status,
        }
    
    async def to_forward(
            self,
            db: AsyncSession,
            device_id: str,
            topic: str,
            payload: str | dict | int | float | bool | bytes | bytearray,
            qos: int = 1,
            content_type: str = "application/json"
    ) -> int:
        """
        Recebe dado de qualquer adaptador, publica no MQTT e persiste no banco.
        Retorna o ID do registro inserido no banco.
        """
        if isinstance(payload, dict):
            payload = json.dumps(payload, default=str, ensure_ascii=False)
        elif isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", errors="replace")
        elif not isinstance(payload, str):
            payload = str(payload)

        # Publica no broker MQTT
        rc, mid = await self.mqtt.publish(
            topic=topic,
            payload=payload,
            qos=qos,
            content_type=content_type,
            user_properties=[
                ("device_id", device_id), 
                ("origin", "industrial_protocol")
            ]
        )

        # Persiste no banco vinculando ao dispositivo
        message = await mc.insert_message(
            db,
            device_id=device_id,
            topic=topic,
            payload=payload,
            qos=qos,
            retain=False,
            content_type=content_type
        )
        
        # Mantém o status de presença atualizado
        await dc.update_status(db, device_id, "online")

        await db.commit()

        self.mqtt.broadcast_message(
            device_id=device_id,
            topic=topic,
            payload=payload,
            qos=qos,
        )

        log.info(
            "Forwarded: device=%s topic=%s mid=%d msg_id=%d",
            device_id,
            topic,
            mid,
            message.id,
        )
        return message.id
