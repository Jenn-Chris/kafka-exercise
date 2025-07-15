import streamlit as st
import json
import time
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd

st.title(" Real-Time IoT Dashboard")

# Initialize data buffers for the last 100 readings
if 'temperature_buffer' not in st.session_state:
    st.session_state.temperature_buffer = deque(maxlen=100)
    st.session_state.timestamp_buffer = deque(maxlen=100)
    st.session_state.previous_temp = 0

# Create placeholders for real-time updates
temp_metric = st.empty()
chart_placeholder = st.empty()
alert_placeholder = st.empty()

# Kafka consumer setup
@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'sensor',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='dashboard',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000  # Timeout after 1 second if no messages
    )

consumer = get_kafka_consumer()

# Main dashboard loop
while True:
    try:
        # Poll for new messages
        message_pack = consumer.poll(timeout_ms=1000)
        
        if message_pack:
            for topic_partition, messages in message_pack.items():
                for message in messages:
                    sensor_data = message.value
                    
                    # Extract data
                    temperature = sensor_data.get('temperature', 0)
                    device_id = sensor_data.get('device_id', 'unknown')
                    timestamp_str = sensor_data.get('timestamp', '')
                    
                    # Parse timestamp for display
                    try:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        time_display = timestamp.strftime("%H:%M:%S")
                    except:
                        time_display = datetime.now().strftime("%H:%M:%S")
                    
                    # Calculate temperature difference
                    temp_diff = temperature - st.session_state.previous_temp
                    st.session_state.previous_temp = temperature
                    
                    # Update metric
                    temp_metric.metric(
                        label=f" {device_id}",
                        value=f"{temperature:.1f}°C",
                        delta=f"{temp_diff:.1f}°C"
                    )
                    
                    # Add to buffers
                    st.session_state.temperature_buffer.append(temperature)
                    st.session_state.timestamp_buffer.append(time_display)
                    
                    # Update chart if we have data
                    if len(st.session_state.temperature_buffer) > 1:
                        chart_data = pd.DataFrame({
                            'Time': list(st.session_state.timestamp_buffer),
                            'Temperature (°C)': list(st.session_state.temperature_buffer)
                        })
                        
                        chart_placeholder.line_chart(
                            chart_data.set_index('Time'),
                            use_container_width=True,
                            height=400
                        )
                    
                    # Check for alerts (temperature > 30°C)
                    if temperature > 30:
                        alert_placeholder.error(
                            f" HIGH TEMPERATURE ALERT: {temperature:.1f}°C at {time_display}"
                        )
                    else:
                        alert_placeholder.success(f"✅ Normal temperature: {temperature:.1f}°C")
        
        # Small delay to prevent excessive CPU usage
        time.sleep(0.1)
        
    except Exception as e:
        st.error(f"Error: {e}")
        time.sleep(1)