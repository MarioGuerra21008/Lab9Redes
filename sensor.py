import random
import json

def generateData():
    temperature = round(random.uniform(0, 110), 2)
    humidity = random.randint(0, 100)
    wind_direction = random.choice(["E", "NE", "N", "NO", "O", "SO", "S", "SE"])

    data = {
        "temperatura": temperature,
        "humedad": humidity,
        "direccion_viento": wind_direction
    }

    return json.dumps(data)

print(generateData())