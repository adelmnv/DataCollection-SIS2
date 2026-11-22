import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "src"))

from config import DB_FILE
from stats import load_stats, reset_stats

# order matters: every script reads the file made by the previous one
steps = [
    ("api_client.py", "getting earthquakes from USGS"),
    ("producer.py", "sending records to kafka"),
    ("consumer.py", "reading records from kafka"),
    ("validator.py", "pandera checks"),
    ("cleaner.py", "cleaning + transformations"),
    ("loader.py", "saving to sqlite (2 runs)"),
    ("analysis.py", "EDA and plots"),
]

# delete old db so every run starts from zero
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print("old output.db deleted")
reset_stats()

for i, (script, description) in enumerate(steps, start=1):
    print(f"\n>> [{i}/{len(steps)}] {script} - {description}")
    result = subprocess.run([sys.executable, os.path.join(BASE_DIR, "src", script)])
    if result.returncode != 0:
        print(f"!! {script} failed, pipeline stopped")
        sys.exit(1)

stats = load_stats()
print("\n----- summary -----")
print("api records:        ", stats.get("api_records"))
print("sent to kafka:      ", stats.get("kafka_published"))
print("read from kafka:    ", stats.get("kafka_consumed"))
print("valid / invalid:    ", stats.get("records_valid"), "/", stats.get("records_invalid"))
print("test records caught:", stats.get("test_records_detected"), "of", stats.get("test_records_total"))
print("duplicates removed: ", stats.get("duplicates_removed"))
print("after cleaning:     ", stats.get("records_after_cleaning"))
print("in sqlite:          ", stats.get("records_stored_db"))
print("added on 2nd load:  ", stats.get("second_load_inserted"))

if stats.get("api_records") == stats.get("kafka_published") == stats.get("kafka_consumed"):
    print("\nall records went through kafka")
else:
    print("\nsome records were lost between api and kafka")

if stats.get("records_after_cleaning") == stats.get("records_stored_db"):
    print("db has the same number of rows as clean_data.csv")
else:
    print("db row count is different from clean_data.csv")