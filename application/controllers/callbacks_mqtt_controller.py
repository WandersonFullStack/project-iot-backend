import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

from .database_controller import DatabaseController
from application.config.broker_configs import mqtt_broker_configs as config, log

class CallbacksMQTTContrller:
    """
    Encapsula o cliente pahi e todos os callbacks do MQTT v5.
    Recebe uma instância de DatabaseController para persistência.
    """

    def __init__(self, db: DatabaseController):
        self.db = db

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
        self.client.on_log = self._on_log

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        """
        Camado após o broker processar o pacote CONNECT e responder com CONNACK.
        """

        if reason_code.value == 0:
            log.info(
                "Conectado | session_present=%s | props=%s",
                connect_flags.session_present,
                properties,
            )

            # Montar propriedade de assinatura
            self._subscribe_topics()
        else:
            log.error("Falha na conexão: %s (%d)", reason_code.getName(), reason_code.value)

    def _subscribe_topics(self):
        """
        Constrói as propriedades de SUBISCRIBE do MQTT e assina os tópicos.
        """
        props_sensors = Properties(PacketTypes.SUBSCRIBE)
        props_sensors.SubscriptionIdentifier = 1
        props_sensors.UserProperty = [("app", "monitor_mqtt")]

        props_alert = Properties(PacketTypes.SUBSCRIBE)
        props_alert.SubscriptionIdentfier = 2

        for (topic, qos), props in zip(config["TOPIC"], [props_sensors, props_alert]):
            result, mid = self.client.subscribe(topic, qos=qos, properties=props)
            log.info("SUBSCRIBE enviado | tópico='%s' qos=%d mid=%d", topic, qos, mid)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """
        Chamado quando a conexão com o broker é encerrada.
        """

        reason = reason_code.getName() if reason_code else "desconhecido"
        origin = "broker" if disconnect_flags.is_disconnect_packet_from_server else "client"
        log.warning("Desconectado | origem=%s motivo='%s'", origin, reason)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        """
        Chamado quando broker responde com SUBACK.
        """

        for i, rc in enumerate(reason_codes):
            if rc.value <= 2:
                log.info("SUBACK |mid=%d tópico[%d] QoS_concedido=%", mid, i, rc.value)
            else:
                log.error("SUBACK recusado | mid=%d tópico[%d] erro='%s'", mid, i, rc.getName())

    def _on_message(self, client, userdata, message):
        """
        Chamado para cada mensagem recebida nos tópicos asinados.
        """

        topic = message.topic
        payload = message.payload.decode("utf-8", errors="replace")
        qos = message.qos
        retain = message.retain
        props = message.properties

        # Extrair propriedades
        content_type = getattr(props, "ContentType", None)
        user_props = str(getattr(props, "UserProperty", [])) or None
        sub_ids = getattr(props, "SubscriptionIdentifier", [])
        resp_topic = getattr(props, "ResponseTopic", None)
        corr_data = getattr(props, "CorrelationData", None)

        log.info(
            "MSG | tópico='%s' qos=%d retain=%s sub_id=%s content_type=%s",
            topic, qos, retain, sub_ids, content_type,
        )

        # Persistir no banco
        row_id = self.db.inserir_mensagens(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            content_type=content_type,
            user_props=user_props,
        )
        log.info("Mensagem salva no banco | id=%d", row_id)

        # Roteamento por tópico
        if "temperatura" in topic:
            self._tratar_temperatura(payload)
        elif "umidade" in topic:
            self._tratar_umidade(payload)
        elif "alertas" in topic:
            self._tratar_alerta(payload)

        # Padrão Request/Reply do MQTT
        if resp_topic and corr_data:
            response = f'{{"status": "ok", "echo": {payload}"}}'
            self._publicar(resp_topic, response, qos=1, correlation_data=corr_data)

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
            self.db.confirmar_publicacao(mid)
        else:
            log.error("Puvlicação falhou | mid=%d reason='%s'", mid, name)

    def _on_log(self, client, userdata, level, buf):
        """
        Callback de diagnóstico do Paho. Utilizar só durante
        o desenvolvimento para depurar proplemas de protocolo.
        """

        if level == mqtt.MQTT_LOG_ERR:
            log.debug("[PAHO ERR] %s", buf)

    # PUBLICAÇÃO COM PROPRIEDADES
    def _publicar(
            self,
            topic: str,
            payload: str,
            qos: int = 1,
            content_type: str = "application/json",
            correlation_data: bytes | None = None,
            user_properties: list[tuple[str, str]] | None =None,
            expiry_interval: int | None = None,
    ):
        """
        Publica uma mensagem com propriedades MQTT.
        """

        props = Properties(PacketTypes.PUBLISH)
        props.ContentType = content_type
        props.PayloadFormatIndicator = 1 #UTF-8

        if correlation_data:
            props.CorrelationData = correlation_data
        if user_properties:
            props.UserProperty = user_properties
        if expiry_interval:
            props.MessageExpiryInterval = expiry_interval

        info = self.client.publish(topic, payload, qos=qos, properties=props)
        # info.rc: código de retorno imediato (não é a confirmação do broker)
        # info.mid: message-id para rastrear no on_plublish
        self.db.registrar_publicacao(topic, payload, qos, info.mid)
        log.info("PUBLISH enfileirado | tópico='%s' mid=%d", topic, info.mid)

        return info
    
    # HANDLERS DE NEGÓCIO
    def _tratar_temperatura(self, value_str: str):
        try:
            value = float(value_str)
            log.info(" -> Temperatura: %.1fC", value)
            if value > 35.0:
                self._publicar(
                    "home/alerts",
                    f'{{"tipo": "temperatura_alta", "valor": {value}}}',
                    qos=2,
                    expiry_interval=60,
                    user_properties=[("severidade", "alta")],
                )
        except ValueError:
            log.warning("Payload de temperatura invalido: %s", value_str)

    def _tratar_umidade(self, value_str: str):
        try:
            log.info(" -> Umidade: %.1f%%", float(value_str))
        except ValueError:
            log.warning("Payload de umidade invalido: %s", value_str)

    def _tratar_alerta(self, payload: str):
        log.warning(" [ALERTA] %s", payload)

    # CONEXÃO COM PROPRIEDADES
    def conectar(self):
        """
        Conecta ao broker com propriedaes MQTT no pacote CONNECT.
        """

        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = 300
        props.ReceiveMaximum = 20
        props.RequestProblemInformation = 1
        props.UserProperty = [("app", "monitor_mqtt"), ("versao", "1.0")]

        self.client.connect(
                config["HOST"], 
                config["PORT"], 
                keepalive=config["KEEPALIVE"],
                properties=props
            )
        
    def iniciar(self):
        """Conecta e inicia o loop de rede em background."""
        
        self.conectar()
        self.client.loop_start() #thread daemon - não bloqueia
        log.info("Loop MQTT iniciado em background.")

    def parar(self):
        """Encerra a conexão de forma limpa, publicando o DISCONNECT com props."""

        props = Properties(PacketTypes.DISCONNECT)
        props.SessionExpiryInterval = 0 # broker limpa a sessão
        self.client.disconnect(properties=props)
        self.client.loop_stop()
        log.info("Client disconnected.")