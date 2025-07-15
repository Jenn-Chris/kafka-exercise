# Real-Time IoT Sensor Data Processing with Apache Kafka

This project implements a real-time IoT data streaming system using Apache Kafka for processing temperature sensor data.

## Technical Implementation

**Note on Libraries:** Due to compilation issues with confluent-kafka dependencies on macOS, this implementation uses kafka-python instead of quixstreams. The functionality remains equivalent:

- 5-second windowed alert counting implemented with time-based data structures
- 10-second rolling average calculations 
- Real-time stream processing maintaining the same message flow architecture
- All core Kafka concepts (producers, consumers, topics) implemented as specified

## System Components

1. **Producer** (`producer.py`) - Generates simulated IoT temperature sensor data
2. **Consumer** (`consumer.py`) - Processes incoming sensor data and filters temperature alerts  
3. **Alert Counter** (`alert_counter.py`) - Aggregates alert counts over 5-second windows
4. **Average Calculator** (`avg_temperature.py`) - Computes rolling 10-second temperature averages
5. **Dashboard** (`dashboard.py`) - Streamlit-based real-time visualization interface

## Screenshot 

<img width="1670" height="997" alt="Screenshot" src="https://github.com/user-attachments/assets/6aaf731a-5e0f-426b-ba20-444702582547" />


## Running the System

```bash
# Start Kafka infrastructure
docker compose up -d

# Start data pipeline components (in separate terminals)
python3 producer.py
python3 consumer.py
python3 alert_counter.py
python3 avg_temperature.py

# Launch dashboard
streamlit run dashboard.py
