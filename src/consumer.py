import json
import sys
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config import (KAFKA_SERVER, KAFKA_TOPIC, KAFKA_GROUP, CONSUMER_TIMEOUT_MS, CONSUMED_FILE)
from stats import save_stat


def run_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            group_id=KAFKA_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
        )
    except Exception as error:
        print("Error: cannot connect to Kafka at", KAFKA_SERVER)
        print("Details:", error)
        sys.exit(1)

    records = []
    consumed = 0
    bad_messages = 0

    print("Consumer started, waiting for messages...")

    try:
        for message in consumer:
            try:
                text = message.value.decode("utf-8")
                record = json.loads(text)
                if "id" not in record:
                    raise ValueError("message has no id")
                records.append(record)
                consumed += 1
                if consumed % 100 == 0:
                    print("Consumed so far:", consumed)
            except ValueError as error:
                bad_messages += 1
                print("Bad message skipped:", error)
        consumer.commit()
    except KafkaError as error:
        print("Kafka error while reading:", error)
    finally:
        consumer.close()

    print("Messages consumed:", consumed)
    print("Bad messages:", bad_messages)

    if consumed == 0:
        print("Error: no messages received")
        sys.exit(1)

    df = pd.DataFrame(records)
    df.to_csv(CONSUMED_FILE, index=False)
    print("Saved:", CONSUMED_FILE)

    save_stat("kafka_consumed", consumed)
    save_stat("kafka_bad_messages", bad_messages)


if __name__ == "__main__":
    run_consumer()