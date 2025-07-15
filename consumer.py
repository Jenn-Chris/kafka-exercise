import json
import logging
from kafka import KafkaConsumer

def temp_transform(msg):
    celsius = msg["temperature"]
    fahrenheit = (celsius * 9 / 5) + 32
    kelvin = celsius + 273.15
    new_msg = {
        "celsius": celsius,
        "fahrenheit": round(fahrenheit, 2),
        "kelvin": round(kelvin, 2),
        "device_id": msg["device_id"],
        "timestamp": msg["timestamp"],
    }
    return new_msg

def alert_check(msg):
    kelvin = msg["kelvin"]
    if kelvin > 303:
        logging.error(" Temperature too high!")
        return True
    else:
        return False

def main():
    logging.info("Starting consumer...")
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        'sensor',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    try:
        for message in consumer:
            # Get the message data
            sensor_data = message.value
            logging.debug(f"Received: {sensor_data}")
            
            # Transform the data
            transformed_data = temp_transform(sensor_data)
            
            # Check for alerts
            if alert_check(transformed_data):
                print(f"ALERT: {transformed_data}")
            else:
                print(f"Normal: {transformed_data}")
                
    except KeyboardInterrupt:
        logging.info("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()