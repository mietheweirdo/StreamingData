from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time
import random
from datetime import datetime

# Config Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sales_transactions'

# Create Kafka Topic
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
try:
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])
    print(f"Topic {TOPIC_NAME} created successfully!")
except Exception as e:
    print(f"Topic creation failed: {e}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate random transaction data
def generate_random_transaction():
    return {
        "transaction_id": random.randint(1000, 9999),  # Random transaction ID
        "product_id": random.randint(1, 10),  # 10 different products
        "quantity": random.randint(1, 5),  # Quantity from 1 to 5
        "price": round(random.uniform(5, 50), 2),  # Price from 5.00 to 50.00
        "timestamp": datetime.now().isoformat(timespec='milliseconds'),  # Current timestamp in milliseconds
        "customer_id": random.randint(1, 100),  # 100 customers
        "store_id": random.randint(1, 100)  # 100 stores
    }

# Send random transactions to Kafka
print("Sending random transactions to Kafka...")
try:
    while True:  # Continuously send data
        transaction = generate_random_transaction()
        producer.send(TOPIC_NAME, transaction)
        print(f"Sent: {transaction}")
        time.sleep(1)  # Send every second
except KeyboardInterrupt:
    print("\nStopped sending data.")
finally:
    producer.flush()
    producer.close()