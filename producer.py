from kafka import KafkaProducer
import json
import time

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic to which data will be produced
topic = 'assignment'

# Path to the JSON file
json_file = 'outputt.json'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to read data from JSON file and produce it to Kafka
def produce_data():
    message_count = 0
    with open(json_file) as f:
        for line in f:
            if message_count >= 10:
                break
            try:
                data = json.loads(line.strip())
                # Produce data to Kafka topic
                producer.send(topic, value=data)
                print("Produced: ", data)
                message_count += 1
            except Exception as e:
                print("Error producing message:", e)

# Start producing data
produce_data()

# Close Kafka producer
producer.close()
