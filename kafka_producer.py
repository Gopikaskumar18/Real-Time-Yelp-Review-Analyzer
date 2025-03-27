import json
import time
from kafka import KafkaProducer

# Load sample reviews from file
def load_reviews(file_path):
    with open(file_path, 'r') as f:
        for line in f:
            yield json.loads(line)

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "yelp-reviews"

if __name__ == "__main__":
    print("Starting to stream reviews to Kafka...")

    for review in load_reviews('sample_reviews.jsonl'):
        print(f"Sending: {review['text']}")
        producer.send(topic, review)
        time.sleep(1)  # Simulate real-time stream

    producer.flush()
    print("Finished streaming.")
