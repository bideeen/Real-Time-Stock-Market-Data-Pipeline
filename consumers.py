from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import snowflake.connector
import pandas as pd
import os


# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'stock_consumer',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(kafka_config)

# Schema Registry setup
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

# Snowflake connection
snowflake_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

# Moving average calculation
def calculate_moving_average(data, window=5):
    df = pd.DataFrame([data])
    df['moving_average'] = df['price'].rolling(window=window, min_periods=1).mean()
    return df.iloc[-1]

def consume_and_process():
    topic = 'stock_prices'
    consumer.subscribe([topic])
    stock_data = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        data = avro_deserializer(msg.value(), None)
        print(f"Consumed: {data}")
        stock_data.append(data)
        if len(stock_data) >= 5:  # Process in batches of 5
            processed = calculate_moving_average(data)
            cursor = snowflake_conn.cursor()
            cursor.execute(
                """
                INSERT INTO stock_prices (symbol, price, volume, timestamp, moving_average)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    processed['symbol'],
                    processed['price'],
                    processed['volume'],
                    processed['timestamp'],
                    processed['moving_average']
                )
            )
            snowflake_conn.commit()
            cursor.close()
            stock_data.pop(0)  # Maintain sliding window

if __name__ == "__main__":
    consume_and_process()