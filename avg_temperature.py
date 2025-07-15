import json
import logging
from collections import deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import statistics

def main():
    consumer = KafkaConsumer('sensor', bootstrap_servers=['localhost:9092'], auto_offset_reset='latest', group_id='avg_temp', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    temp_buffer = deque()
    
    for message in consumer:
        sensor_data = message.value
        current_time = datetime.now()
        temperature = sensor_data["temperature"]
        temp_buffer.append((current_time, temperature))
        
        cutoff_time = current_time - timedelta(seconds=10)
        while temp_buffer and temp_buffer[0][0] < cutoff_time:
            temp_buffer.popleft()
        
        if temp_buffer:
            temperatures = [temp for _, temp in temp_buffer]
            avg_temp = statistics.mean(temperatures)
            avg_temp_msg = {"timestamp": current_time.isoformat(), "avg_temperature_10sec": round(avg_temp, 2)}
            producer.send('avg_temperature', value=avg_temp_msg)
            print(f"Average temperature (10s): {avg_temp:.2f}°C")

if __name__ == "__main__":
    main()