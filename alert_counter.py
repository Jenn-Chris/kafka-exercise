import json
import logging
from collections import deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import time

def main():
    logging.info("Starting alert counter")
    
    # Create consumer for sensor data
    consumer = KafkaConsumer(
        'sensor',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert_counter',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Create producer for alert counts
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Buffer to store alerts in last 5 seconds
    alert_buffer = deque()
    
    try:
        for message in consumer:
            sensor_data = message.value
            current_time = datetime.now()
            
            # Transform temperature to Kelvin
            celsius = sensor_data["temperature"]
            kelvin = celsius + 273.15
            
            # Check if it's an alert 
            if kelvin > 303:
                alert_buffer.append(current_time)
                logging.info(f"Alert detected- Temperature: {celsius}°C ({kelvin}K)")
            
            # Remove alerts older than 5 seconds
            cutoff_time = current_time - timedelta(seconds=5)
            while alert_buffer and alert_buffer[0] < cutoff_time:
                alert_buffer.popleft()
            
            # Count alerts in last 5 sec
            alert_count = len(alert_buffer)
            
            # Create message with alert count
            alert_count_msg = {
                "timestamp": current_time.isoformat(),
                "alert_count_5sec": alert_count,
                "window_size": "5_seconds"
            }
            
            # Send to alert_count topic
            producer.send('alert_count', value=alert_count_msg)
            logging.debug(f"Alert count in last 5 seconds: {alert_count}")
            
    except KeyboardInterrupt:
        logging.info("Stopping alert counter")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()