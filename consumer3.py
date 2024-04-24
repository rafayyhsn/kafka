import re
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from textblob import TextBlob  # For sentiment analysis
from collections import defaultdict

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic from which data will be consumed
topic = 'assignment'

# MongoDB connection setup
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['c3']  # The database where you want to store the data
analysis_collection = mongo_db['Analysis']  # Collection for all analysis results

# Create Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to preprocess messages
def preprocess_message(message):
    # Remove the "$" sign from the 'price' field if present
    if 'price' in message:
        message['price'] = message['price'].replace('$', '')
    # Lowercase the title and remove non-alphabetic characters
    if 'title' in message:
        message['title'] = re.sub(r'[^a-zA-Z\s]', '', message['title']).lower()
    return message

# Sentiment Analysis
def analyze_sentiment(title):
    blob = TextBlob(title)
    sentiment_score = blob.sentiment.polarity
    return sentiment_score

# Brand Analysis
def analyze_brand(data):
    brand_counts = defaultdict(int)
    for item in data:
        brand = item.get('brand', 'Unknown')
        brand_counts[brand] += 1
    return brand_counts

# Price Range Analysis
def analyze_price_range(data):
    prices = [float(item.get('price', 0)) for item in data]
    min_price = min(prices)
    max_price = max(prices)
    return min_price, max_price

# Stream data from Kafka and perform analysis
dataset = []

for message in consumer:
    # Preprocess message
    message = preprocess_message(message.value)
    
    # Add message to dataset
    dataset.append(message)
    
    # Sentiment Analysis
    sentiment_score = analyze_sentiment(message.get('title', ''))
    
    # Brand Analysis
    brand_counts = analyze_brand(dataset)
    
    # Price Range Analysis
    min_price, max_price = analyze_price_range(dataset)
    
    # Store analysis results in MongoDB
    analysis_result = {
        "title": message.get('title', ''),
        "sentiment_score": sentiment_score,
        "brands": brand_counts,
        "min_price": min_price,
        "max_price": max_price
    }
    analysis_collection.insert_one(analysis_result)
    
    # Print analysis results (for demonstration)
    print("Analysis Results:")
    print("Title:", message.get('title', ''))
    print("Sentiment Score:", sentiment_score)
    print("Brand Analysis Results:")
    for brand, count in brand_counts.items():
        print(f"Brand: {brand}, Count: {count}")
    print("Price Range Analysis Results:")
    print(f"Min Price: {min_price}, Max Price: {max_price}")

# Close MongoDB connection
mongo_client.close()

consumer.close()  # Close the consumer
