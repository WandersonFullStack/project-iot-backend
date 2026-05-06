import time

from application.controllers.database_controller import DatabaseController
from application.config.broker_configs import mqtt_broker_configs as config, log
from application.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller

def start():
    db = DatabaseController(config["DB_PATH"])
    ctr1 = CallbacksMQTTContrller(db)

    # Publicar uma mensagem de status com propriedades
    time.sleep(0.1) # aguarda conexão estabelecer
    ctr1._publicar(
        "home/status",
        '{"status": "online", "protocolo": "MQTTv5"}',
        qos=1,
        content_type="application/json",
        user_properties=[("client", config["CLIENT_ID"])],
        expiry_interval=3600,
    )

    try:
        while True:
            time.sleep(5)
            msgs = db.ultimas_mensagens(3)
            if msgs:
                log.info("=== Últimas mensagens no banco ===")
                for m in msgs:
                    log.info(" [%s] %s -> %s", m["received_in"][:19], m["topic"], m["payload"])
    except KeyboardInterrupt:
        log.info("Encerrando...")
    finally:
        ctr1.parar()