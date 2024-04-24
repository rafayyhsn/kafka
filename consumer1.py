import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import defaultdict
from itertools import combinations

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic from which data will be consumed
topic = 'assignment'

# MongoDB connection setup
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['c3']  # The database where you want to store the data
apriori_collection = mongo_db['Apriori']  # Collection for Apriori results

# Create Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to calculate support for itemsets
def calculate_support(dataset, itemset):
    count = 0
    for transaction in dataset:
        if set(itemset).issubset(transaction):
            count += 1
    return count / len(dataset)

# Function to generate candidate itemsets
def generate_candidates(frequent_itemsets, k):
    candidates = set()
    frequent_items = [set(itemset) for itemset in frequent_itemsets]
    
    # Generate combinations of size k
    for itemset1 in frequent_items:
        for itemset2 in frequent_items:
            union = itemset1.union(itemset2)
            if len(union) == k:
                candidates.add(frozenset(union))
    
    return candidates

# Function to implement the Apriori algorithm
def apriori(dataset, min_support):
    k = 1
    frequent_itemsets = []
    candidate_itemsets = {frozenset([item]) for transaction in dataset for item in transaction}
    
    while candidate_itemsets:
        valid_itemsets = []
        
        # Calculate support for each candidate itemset
        for itemset in candidate_itemsets:
            support = calculate_support(dataset, itemset)
            if support >= min_support:
                valid_itemsets.append((itemset, support))
                # Insert frequent itemset into MongoDB
                apriori_collection.insert_one({"itemset": list(itemset), "support": support})
        
        if not valid_itemsets:
            break
        
        # Store valid itemsets and generate new candidates for the next iteration
        frequent_itemsets.extend(valid_itemsets)
        candidate_itemsets = generate_candidates([itemset for itemset, _ in valid_itemsets], k + 1)
        
        k += 1
    
    return frequent_itemsets

# Stream data from Kafka and apply Apriori algorithm
min_support = 0.3
dataset = []

for message in consumer:
    # Extract "title" and "buy" from the message
    transaction = [(item['title'], item['also_buy']) for item in message.value]
    
    # Add transaction to the dataset
    dataset.append(transaction)
    
    # Run the Apriori algorithm and get frequent itemsets
    frequent_itemsets = apriori(dataset, min_support)
    
    # Print out frequent itemsets
    for itemset, support in frequent_itemsets:
        print(f"Frequent Itemset: {itemset}, Support: {support:.2f}")

# Close Kafka consumer
consumer.close()
