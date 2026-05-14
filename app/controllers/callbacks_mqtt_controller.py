from __future__ import annotations
import asyncio
import logging
from typing import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

from .database_controller import DatabaseController
from app.config.broker_configs import mqtt_broker_configs as config, log

class CallbacksMQTTContrller:
    """
    Encapsula o cliente pahi e todos os callbacks do MQTT v5.
    Recebe uma instância de DatabaseController para persistência.
    """

    def __init__(self, db: DatabaseController):
        self.db = db
        self._connected = False
        self._loop: asyncio.AbstractEventLoop | None = None

        # Conjunto de filas - uma por WebSocket conectado.
        self._ws_queues: set[asyncio.Queue] = set()

        # Criação do cliente
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=config["CLIENT_ID"],
            protocol=mqtt.MQTTv5,
        )

        # Registrar callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self. _on_subscribe
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish

    # -> Registro de WebSocket queues

    def register_ws_queue(self, q: asyncio.Queue):
        """Chamado quando um cliente WebSocket conecta."""
        self._ws_queues.add(q)

    def remove_ws_queue(self, q: asyncio.Queue):
        """Chamado quando um cliente WebSocket desconecta."""
        self._ws_queues.discard(q)

    def _broadcast_for_ws(self, data: dict):
        """Envia `data` para todas as filas de WebSocket registradas."""

        if not self._loop:
            return
        for q in list(self._ws_queues):
            try:
                self._loop.call_soon_threadsafe(q.put_nowait, data)
            except asyncio.QueueFull:
                log.warning("WebSocket queue cheia - mensagem descartada")

    # -> Callbacks MQTT

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        """
        Camado após o broker processar o pacote CONNECT e responder com CONNACK.
        """

        if reason_code.value == 0:
            self._connected = True
            log.info(
                "MQTT Conectado | session_present=%s | props=%s",
                connect_flags.session_present,
                properties,
            )

            # Montar propriedade de assinatura
            self._subscribe_topics()
        else:
            log.error("MQTT Falha na conexão: %s (%d)", reason_code.getName(), reason_code.value)

    def _subscribe_topics(self):
        """
        Constrói as propriedades de SUBISCRIBE do MQTT e assina os tópicos.
        """
        props = Properties(PacketTypes.SUBSCRIBE)
        props.SubscriptionIdentifier = 1
        for topic, qos in config["TOPIC"]:
            result, mid = self.client.subscribe(topic, qos=qos, properties=props)
            log.info("SUBSCRIBE enviado | tópico='%s' qos=%d mid=%d", topic, qos, mid)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """
        Chamado quando a conexão com o broker é encerrada.
        """
        self._connected = False
        origin = "broker" if disconnect_flags.is_disconnect_packet_from_server else "client"
        log.warning("MQTT Desconectado | origem=%s motivo='%s'", origin, 
                    reason_code.getName() if reason_code else "-")

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        """
        Chamado quando broker responde com SUBACK.
        """

        for i, rc in enumerate(reason_codes):
            if rc.value <= 2:
                log.info("SUBACK |mid=%d topic[%d] QoS=%d", mid, i, rc.value)
            else:
                log.error("SUBACK refused | mid=%d topic[%d] %s", mid, i, rc.getName())

    def _on_message(self, client, userdata, message):
        """ 
        Identificação de dispositivo via UserProperty.
        """

        topic = message.topic
        payload = message.payload.decode("utf-8", errors="replace")
        props = message.properties

        content_type = getattr(props, "ContentType", None)
        user_props_raw = getattr(props, "UserProperty", []) or []
        user_props_str = str(user_props_raw) if user_props_raw else None

        # Extrair device_id das UserProperties
        device_id = None
        for key, value in user_props_raw:
            if key == "device_id":
                device_id = value
                break

        # Atualizar status do dispositivo se identificado
        if device_id:
            device = self.db.search_device(device_id)
            if device and device["active"]:
                self.db.update_status(device_id, "online")
                log.info("MSG of device '%s' | topic='%s'", device_id,topic)
            else:
                log.warning("MSG with unknown/inactive device_id: '%s'", device_id)
                device_id = None    # não vincula ao banco se não existe

        # Persistir no banco
        row_id = self.db.insert_messages(
            device_id=device_id,
            topic=topic,
            payload=payload,
            qos=message.qos,
            retain=message.retain,
            content_type=content_type,
            user_props=user_props_str,
        )

        # Broadcast para WebSockets
        self._broadcast_for_ws({
            "id": row_id,
            "device_id": device_id,
            "topic": topic,
            "payload": payload,
            "qos": message.qos,
            "retain": message.retain,
        })

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """
        Chamado quando o broker confirma a entrega de uma mensagem publicada.

        O momento da chamada varia conforme o QoS:
            QoS 0 -> imediatamente após o envio (sem comfirmação real do broker)
            QoS 1 -> ao receber PUBACK do broker
            QoS 2 -> ao receber PUBCOMP
        """

        name = reason_code.getName() if reason_code else "-"
        if not reason_code or reason_code.value in (0x00, 0x10):
            log.info("PUBACK | mid=%d reason='%s'", mid, name)
            self.db.confirm_publication(mid)
        else:
            log.error("Puvlicação falhou | mid=%d reason='%s'", mid, name)


    # PUBLICAÇÃO COM PROPRIEDADES
    def _publish(
            self,
            topic: str,
            payload: str,
            qos: int = 1,
            retain: bool = False,
            content_type: str = "application/json",
            expiry_interval: int | None = None,
            user_properties: list[tuple[str, str]] | None =None,
    ) -> tuple[int, int]:
        """
        Publica uma mensagem com propriedades MQTT.
        """

        props = Properties(PacketTypes.PUBLISH)
        props.ContentType = content_type
        props.PayloadFormatIndicator = 1 #UTF-8

        if user_properties:
            props.UserProperty = user_properties
        if expiry_interval:
            props.MessageExpiryInterval = expiry_interval

        info = self.client.publish(topic, payload, qos=qos, retain=retain, properties=props)
        # info.rc: código de retorno imediato (não é a confirmação do broker)
        # info.mid: message-id para rastrear no on_plublish
        self.db.register_publication(topic, payload, qos, info.mid)
        log.info("PUBLISH enfileirado | tópico='%s' mid=%d", topic, info.mid)

        return info.rc, info.mid
    
    # CONEXÃO COM PROPRIEDADES
    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Recebe o loop asyncio do FastAPI para uso no bridge de threads.
        Deve ser chamado dentro do lifespan, após o loop estar ativo.
        """
        self._loop = loop

        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = 300
        props.ReceiveMaximum = 20
        props.RequestProblemInformation = 1

        self.client.connect(
                config["HOST"], 
                config["PORT"], 
                keepalive=config["KEEPALIVE"],
                properties=props
            )
        self.client.loop_start() # thread daemon gerenciada pelo Paho
        log.info("Controller MQTT started.")
        
    def stop(self):
        """Encerra a conexão de forma limpa, publicando o DISCONNECT com props."""

        props = Properties(PacketTypes.DISCONNECT)
        props.SessionExpiryInterval = 0 # broker limpa a sessão
        self.client.disconnect(properties=props)
        self.client.loop_stop()
        self._connected = False
        log.info("Controller MQTT closed.")

    @property
    def connected(self) -> bool:
        return self._connected