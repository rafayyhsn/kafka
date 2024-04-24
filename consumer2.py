from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic from which data will be consumed
topic = 'assignment'

# MongoDB connection setup
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['c3']  # The database where you want to store the data
pcy_collection = mongo_db['PCY']  # Collection for PCY results

# Support threshold and hash buckets
support_threshold = 2  # Define your support threshold
hash_buckets = 1000  # Define the number of hash buckets

# Create Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to hash itemsets
def hash_function(itemset):
    # A simple hash function for a pair of items
    return hash(str(itemset)) % hash_buckets

# Pass 1: Count item frequencies and build hash buckets
item_counts = defaultdict(int)
hash_table = defaultdict(int)

for message in consumer:
    data = message.value  # Expected to be a dictionary containing "title" and "also_buy"
    
    # Extract "title" and "also_buy" from the message
    title = data.get("title", "")
    also_buy = data.get("also_buy", [])
    
    # Count individual item frequencies
    item_counts[title] += 1
    
    # Count hashed itemset frequencies
    for item in also_buy:
        hash_value = hash_function((title, item))
        hash_table[hash_value] += 1

# Find frequent individual items
frequent_items = {item for item, count in item_counts.items() if count >= support_threshold}

# Find frequent hash buckets
frequent_buckets = {key for key, count in hash_table.items() if count >= support_threshold}

# Pass 2: Find frequent itemsets using frequent items and frequent buckets
for message in consumer:
    data = message.value
    
    # Extract "title" and "also_buy" from the message
    title = data.get("title", "")
    also_buy = data.get("also_buy", [])
    
    # Filter data to only include frequent items
    if title in frequent_items:
        filtered_also_buy = [item for item in also_buy if item in frequent_items]
        
        # Generate all pairs and check if they hash to a frequent bucket
        for item in filtered_also_buy:
            hash_value = hash_function((title, item))
            if hash_value in frequent_buckets:
                frequent_itemset_doc = {"title": title, "also_buy": item}
                pcy_collection.insert_one(frequent_itemset_doc)
                print("Frequent Itemset Found:", (title, item))

# Close MongoDB connection
mongo_client.close()

consumer.close()  # Close the consumer
