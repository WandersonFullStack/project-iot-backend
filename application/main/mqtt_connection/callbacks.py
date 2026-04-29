from application.config.broker_configs import mqtt_broker_configs

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f'Erro ao me conectar! código={reason_code}')
    else:
        print(f'Cliente conectado com sucesso: {client}')
        client.subscribe(mqtt_broker_configs["TOPIC"])

def on_subscribe(client, userdata, mid, granted_qos, properties):
    print(f'Client subscribed at {mqtt_broker_configs["TOPIC"]}')
    print(f'QoS: {granted_qos}')

    if properties is not None:
        print(f'Properties: {properties}')

def on_message(client, userdata, message):
    print('Menssagem recebida!')
    print(client)
    print(message.payload)