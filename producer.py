import time
import logging
import math
import random
import json
from datetime import datetime
from kafka import KafkaProducer

def get_sensor_measurement(t, device_id="machine-01", frequency=0.05, noise_std=2, outlier_prob=0.05):
    """Simulates a temperature measurement for an IoT sensor at time t"""
    # create a noisy sin function
    base_temp = 22 + 15 * math.sin(2 * math.pi * frequency * t)
    noise = random.gauss(0, noise_std)
    temp = base_temp + noise
    # Add sometimes some random outlier
    if random.random() < outlier_prob:
        temp += random.choice([20, 50])

    # Create a message as a dictionary (will be later translated into a JSON)
    message = {
        "timestamp": datetime.now().isoformat(),
        "device_id": device_id,
        "temperature": round(temp, 2)
    }
    return message

def main():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )
    
    t = 0
    try:
        while True:
            measurement = get_sensor_measurement(t)
            logging.debug(f"Got measurement: {measurement}")
            
            # Send message to Kafka
            producer.send(
                topic='sensor',
                key=measurement["device_id"],
                value=measurement
            )
            
            logging.info("Produced. Sleeping...")
            time.sleep(1)
            t = t + 1
            
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()