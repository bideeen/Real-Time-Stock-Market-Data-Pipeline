import finnhub
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
import time
from datetime import datetime

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key="d141cl1r01qs7glkgtt0d141cl1r01qs7glkgttg")

#kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'finnhub_producer'
}

producer = Producer(kafka_config)

# Schema Registry configuration
schema_registry_conf = {
    'url': 'http://localhost:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define the Avro schema
schema_str = """
{
    "type": "record",
    "name": "StockPrice",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "volume", "type": "int"},
        {"name": "timestamp", "type": "string"}
    ]
}
"""

# Create Avro serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

def delivery_report(err, msg):
    """Callback to handle delivery report."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        
def fetch_and_produce():
    symbols = ['AAPL', 'GOOGL', 'AMZN']  # List of stock symbols to fetch
    topic = 'stock_prices'  # Kafka topic to produce messages to 
    while True:
        for symbol in symbols:
            try:
                # Fetch real-time stock price data
                res = finnhub_client.quote(symbol)
                if res['c'] is not None:  # Check if current price is available
                    stock_data = {
                        'symbol': symbol,
                        'price': res['c'],
                        'volume': res['v'],
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # Serialize the data to Avro format
                    avro_data = avro_serializer(stock_data)
                    
                    # Produce the message to Kafka topic
                    producer.produce(topic=topic, value=avro_data, callback=delivery_report)
                    producer.flush()  # Ensure message is sent immediately
                    
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
        
        time.sleep(60)  # Wait before fetching again
        
if __name__ == "__main__":
    fetch_and_produce()
# This script fetches real-time stock prices from Finnhub and produces them to a Kafka topic in Avro format.