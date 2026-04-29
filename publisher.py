import paho.mqtt.client as mqtt

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'test_bub')
mqtt_client.connect(host='localhost', port=1883)
mqtt_client.publish(topic='/messages', payload='{ "minha": "menssagem" }')

print('Enviado!')