import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "src"))

from config import DB_FILE
from stats import load_stats, reset_stats


def run_step(title, script_name):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    script_path = os.path.join(BASE_DIR, "src", script_name)
    result = subprocess.run([sys.executable, script_path])
    if result.returncode != 0:
        print("Step failed:", title)
        sys.exit(1)


# Clean start: the old database is removed so the numbers are reproducible
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print("Old database removed for a clean run")
reset_stats()

run_step("STEP 1: Collect data from USGS API", "api_client.py")
run_step("STEP 2: Kafka Producer", "producer.py")
run_step("STEP 3: Kafka Consumer", "consumer.py")
run_step("STEP 4: Pandera validation", "validator.py")
run_step("STEP 5: Cleaning and transformation", "cleaner.py")
run_step("STEP 6: Load into SQLite (twice, idempotency test)", "loader.py")
run_step("STEP 7: EDA and visualization", "analysis.py")

# Final verification
stats = load_stats()
print("\n" + "=" * 60)
print("PIPELINE SUMMARY")
print("=" * 60)
print("Collected from API:            ", stats.get("api_records"))
print("Published to Kafka:            ", stats.get("kafka_published"))
print("Consumed from Kafka:           ", stats.get("kafka_consumed"))
print("Valid records:                 ", stats.get("records_valid"))
print("Invalid records:               ", stats.get("records_invalid"))
print("Test records detected:         ", stats.get("test_records_detected"), "of", stats.get("test_records_total"))
print("Duplicates removed in cleaning:", stats.get("duplicates_removed"))
print("Records after cleaning:        ", stats.get("records_after_cleaning"))
print("Records stored in SQLite:      ", stats.get("records_stored_db"))
print("Rows added by the second load: ", stats.get("second_load_inserted"))

if stats.get("api_records") == stats.get("kafka_published") == stats.get("kafka_consumed"):
    print("\nAPI, Kafka published and Kafka consumed counts match")
else:
    print("\nKafka counts differ, explain it in the report")

if stats.get("records_after_cleaning") == stats.get("records_stored_db"):
    print("Cleaned records equal stored records")
else:
    print("Cleaned and stored counts differ, explain it in the report")