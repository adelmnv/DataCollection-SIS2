import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
FIGURES_DIR = os.path.join(BASE_DIR, "figures")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(FIGURES_DIR, exist_ok=True)

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
API_PARAMS = {
    "format": "geojson",
    "starttime": "2026-10-01",
    "endtime": "2026-11-01",
    "minmagnitude": 4.5,
    "orderby": "time",
    "limit": 500,
}

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = "earthquakes"
KAFKA_GROUP = "earthquake_group"
CONSUMER_TIMEOUT_MS = 15000

RAW_JSON_FILE = os.path.join(DATA_DIR, "raw_response.json")
RAW_CSV_FILE = os.path.join(DATA_DIR, "raw_data.csv")
CONSUMED_FILE = os.path.join(DATA_DIR, "consumed_data.csv")
VALID_FILE = os.path.join(DATA_DIR, "valid_data.csv")
INVALID_FILE = os.path.join(DATA_DIR, "invalid_data.csv")
ERRORS_FILE = os.path.join(DATA_DIR, "validation_errors.csv")
CLEAN_FILE = os.path.join(DATA_DIR, "clean_data.csv")
DB_FILE = os.path.join(DATA_DIR, "output.db")
STATS_FILE = os.path.join(DATA_DIR, "pipeline_stats.json")