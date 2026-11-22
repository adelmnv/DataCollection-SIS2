import json
import os
import sys
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KAFKA_SERVER, KAFKA_TOPIC, RAW_CSV_FILE
from stats import save_stat


def to_json_bytes(record):
    return json.dumps(record).encode("utf-8")

def run_producer():
    if not os.path.exists(RAW_CSV_FILE):
        print("Error: raw_data.csv not found, run api_client.py first")
        sys.exit(1)

    df = pd.read_csv(RAW_CSV_FILE)
    df = df.astype(object).where(df.notnull(), None)
    records = df.to_dict("records")

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=to_json_bytes,
            acks="all",
            retries=3,
        )
    except Exception as error:
        print("Error: cannot connect to Kafka at", KAFKA_SERVER)
        print("Check that the container is running: docker ps")
        print("Details:", error)
        sys.exit(1)

    published = 0
    failed = 0

    for record in records:
        try:
            future = producer.send(KAFKA_TOPIC, value=record)
            future.get(timeout=10)
            published += 1
        except KafkaError as error:
            failed += 1
            print("Failed to send record", record["id"], error)

    producer.flush()
    producer.close()

    print("Messages published:", published)
    print("Messages failed:", failed)

    save_stat("kafka_published", published)
    save_stat("kafka_failed", failed)

    if published == 0:
        sys.exit(1)


if __name__ == "__main__":
    run_producer()