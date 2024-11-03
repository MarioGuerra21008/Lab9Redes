import random
import json
import time
from confluent_kafka import Producer, Consumer, KafkaException
import matplotlib.pyplot as plt

server = "lab9.alumchat.lol:9092"
topic = "21008"

# productor
producer_conf = {'bootstrap.servers': server}
producer = Producer(producer_conf)


# Configuración del consumidor
consumer_conf = {'bootstrap.servers': server,
    'group.id': 'foo2',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([topic])

all_temp = []
all_hume = []
all_wind = []

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
        
def plotData():
    plt.ion()
    fig, ax = plt.subplots(3, 1, figsize=(10, 8))

    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
            break
        if msg.error():
            print(f"Error al recibir mensaje: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        print(f"Datos recibidos: {data}")
        
        all_temp.append(data["temperatura"])
        all_hume.append(data["humedad"])
        all_wind.append(data["direccion_viento"])

        # Actualizar gráficos
        ax[0].clear()
        ax[0].plot(all_temp, label='Temperatura (°C)')
        ax[0].set_title("Temperatura")
        ax[0].legend(loc='upper left')

        ax[1].clear()
        ax[1].plot(all_hume, label='Humedad (%)', color='orange')
        ax[1].set_title("Humedad Relativa")
        ax[1].legend(loc='upper left')

        ax[2].clear()
        ax[2].hist(all_wind, bins=8, edgecolor='black')
        ax[2].set_title("Dirección del Viento")
        ax[2].set_xticks(range(len(["E", "NE", "N", "NO", "O", "SO", "S", "SE"])))
        ax[2].set_xticklabels(["E", "NE", "N", "NO", "O", "SO", "S", "SE"])

        plt.pause(1)

try:
    sendData()
except KeyboardInterrupt:
    print("Envío de datos del sensor interrumpido.")
finally:
    producer.flush()

try: 
    plotData()
except KeyboardInterrupt:
    print("Recepción de datos del sensor interrumpida.")
finally:
    consumer.close()
