import random
import time
from confluent_kafka import Producer, Consumer, KafkaException
import matplotlib.pyplot as plt

server = "lab9.alumchat.lol:9092"
topic = "21008"

# productor
producer_conf = {'bootstrap.servers': server}
producer = Producer(producer_conf)


# consumidor
consumer_conf = {'bootstrap.servers': server,
    'group.id': 'foo2',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([topic])

all_temp = []
all_hume = []
all_wind = []

def encode_data(temperature, humidity, wind_direction):
    # Temperatura - 14 bits
    temperature = int(temperature * 100)
    # humedad - 7 bits
    humidity = humidity
    # Direccion del viento - 3 bits
    wind_dict = {"E": 0, "NE": 1, "N": 2, "NO": 3, "O": 4, "SO": 5, "S": 6, "SE": 7}
    wind_direction = wind_dict[wind_direction]

    combined_bits = (temperature << 10) | (humidity << 3) | wind_direction
    return combined_bits.to_bytes(3, byteorder='big')

def decode_data(encoded_data):
    combined_bits = int.from_bytes(encoded_data, byteorder='big')
    
    # Temperatura - 14 bits
    temperature = (combined_bits >> 10) & 0x3FFF
    temperature = temperature / 100.0
    
    # Humedad - 7 bits
    humidity = (combined_bits >> 3) & 0x7F
    
    # Direccion del viento - 3 bits
    wind_direction = combined_bits & 0x7
    wind_dict = {0: "E", 1: "NE", 2: "N", 3: "NO", 4: "O", 5: "SO", 6: "S", 7: "SE"}
    wind_direction = wind_dict[wind_direction]

    return {"temperatura": temperature, "humedad": humidity, "direccion_viento": wind_direction}


def sendData():
    while True:
        temperature = round(random.uniform(0, 110), 2)
        humidity = random.randint(0, 100)
        wind_direction = random.choice(["E", "NE", "N", "NO", "O", "SO", "S", "SE"])
        
        encoded_data = encode_data(temperature, humidity, wind_direction)
        
        producer.produce(topic, value=encoded_data)
        producer.flush()
        print(f"Datos enviados (codificados): {encoded_data}")
        time.sleep(random.randint(15, 30))
        
def plotData():
    plt.ion()
    fig, ax = plt.subplots(3, 1, figsize=(10, 8))

    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
            continue
        if msg.error():
            print(f"Error al recibir mensaje: {msg.error()}")
            continue

        # Decodificar mensaje recibido
        data = decode_data(msg.value())
        print(f"Datos recibidos (decodificados): {data}")

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
