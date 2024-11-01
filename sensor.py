import random
import json
import time
from confluent_kafka import Producer, Consumer, KafkaException

producer_conf = {'bootstrap.servers': 'lab9.alumchat.lol:9092'}
producer = Producer(producer_conf)

topic = "21008"

def generateData():
    temperature = round(random.uniform(0, 110), 2)
    humidity = random.randint(0, 100)
    wind_direction = random.choice(["E", "NE", "N", "NO", "O", "SO", "S", "SE"])

    return { "temperatura": temperature, "humedad": humidity, "direccion_viento": wind_direction }

def sendData():
    while True:
        data = json.dumps(generateData())
        producer.produce(topic, value=data)
        producer.flush()
        print(data)

        time.sleep(random.randint(15, 30))

try:
    sendData()
except KeyboardInterrupt:
    print("Envío de datos del sensor interrumpido.")
