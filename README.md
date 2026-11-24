# USGS Earthquake Data Pipeline: Kafka + Pandera + SQLite3

**Course:** Data Collection & Preparation, SIS2: Data Streaming and Validation Pipeline

## 1. Project overview

The pipeline collects real earthquake data (magnitude ≥ 4.5, October 2026) from the
**USGS Earthquake Catalog API**, streams it through a real **Apache Kafka** broker, validates it
with **Pandera**, cleans and transforms it with **pandas/NumPy**, stores it in **SQLite3**, and
produces exploratory analysis and figures.

```
USGS API ─► api_client.py ─► data/raw_response.json + data/raw_data.csv
                                         │
                                         ▼
                 producer.py ──JSON──► Kafka topic "earthquakes" ──JSON──► consumer.py
                                                                             │
                                                                             ▼
                                                                data/consumed_data.csv
                                                                             │
        validator.py (Pandera) ─► data/valid_data.csv / data/invalid_data.csv
                                                                             │
        cleaner.py (pandas, NumPy) ─► data/clean_data.csv                    │
                                                                             ▼
        loader.py (SQLite3, loaded twice) ─► data/output.db ─► analysis.py ─► figures/*.png
```

| # | Script | Input | Output |
|---|---|---|---|
| 1 | `src/api_client.py` | USGS API | `data/raw_response.json`, `data/raw_data.csv` |
| 2 | `src/producer.py` | `data/raw_data.csv` | Kafka topic `earthquakes` |
| 3 | `src/consumer.py` | Kafka topic `earthquakes` | `data/consumed_data.csv` |
| 4 | `src/validator.py` | `data/consumed_data.csv` | `data/valid_data.csv`, `data/invalid_data.csv` |
| 5 | `src/cleaner.py` | `data/valid_data.csv` | `data/clean_data.csv` |
| 6 | `src/loader.py` | `data/clean_data.csv` | `data/output.db` |
| 7 | `src/analysis.py` | `data/output.db` | `figures/fig1…fig5*.png` |

Helper modules: `src/config.py` (all paths and settings) and `src/stats.py` (each stage writes
its record counts to `data/pipeline_stats.json`, and `main.py` prints them in the final summary).

## 2. Pipeline stages

### 2.1 API data collection (`src/api_client.py`)
Sends a GET request to the USGS API (parameters in `src/config.py`), handles connection errors,
unsuccessful HTTP status codes, invalid JSON and empty results, saves the original response to
`data/raw_response.json`, flattens the GeoJSON features into 18 columns (`id`, `time`, `mag`,
`magType`, `place`, `depth_km`, `latitude`, `longitude`, `sig`, `nst`, `gap`, `dmin`, `rms`,
`status`, `type`, `net`, `alert`, `tsunami`), converts `time` from epoch milliseconds to a
datetime and saves `data/raw_data.csv`.

### 2.2 Kafka producer (`src/producer.py`)
Reads `raw_data.csv`, replaces `NaN` with `None`, serializes every record to JSON (UTF-8 bytes)
and sends it to the topic `earthquakes` with `acks="all"`, waiting for the broker's confirmation
of each message. Prints the numbers of published and failed messages.

### 2.3 Kafka consumer (`src/consumer.py`)
Subscribes to `earthquakes` in the group `earthquake_group` (`auto_offset_reset="earliest"`),
deserializes each message from JSON, skips and counts bad messages, stops after 15 seconds
without new messages, commits the offsets and saves `data/consumed_data.csv`.

### 2.4 Pandera validation (`src/validator.py`)
Validates the consumed data against a `DataFrameSchema` with `lazy=True`, so all errors are
collected at once. Rules:

| Column(s) | Rule |
|---|---|
| `id` | string, not null, **unique** |
| `time` | datetime, not null, between 2000-01-01 and now |
| `mag` | float, not null, range −2…10 |
| `depth_km` | float, not null, range −10…800 km |
| `latitude` / `longitude` | float, not null, range −90…90 / −180…180 |
| `sig`, `nst`, `dmin`, `rms` | ≥ 0 |
| `gap` | range 0…360 |
| `magType` | one of the allowed magnitude types |
| `status` | `automatic` or `reviewed` |
| `type` | one of the allowed event types |
| `alert` | nullable, `green` / `yellow` / `orange` / `red` |
| `tsunami` | 0 or 1 |

Invalid rows are moved to `data/invalid_data.csv`, and their errors are saved to
`data/validation_errors.csv` (only created when errors exist). Valid rows go to
`data/valid_data.csv`. Afterwards, 3 deliberately invalid test records
(`TEST_1_bad_magnitude`, `TEST_2_bad_latitude`, `TEST_3_bad_category`) are validated as a
separate DataFrame to show that the schema catches errors. They are never mixed into the real data.

### 2.5 Cleaning and transformation (`src/cleaner.py`)
- Missing values: numeric quality fields are filled with the median, `alert` with `"none"`, and `place` / `magType` with `"unknown"`.
- Duplicates removed by `id`.
- Type conversion (`time` → datetime, `mag` / `depth_km` → float, `sig` / `tsunami` → int).
- Text normalization (strip, lower case), `magType` → `mag_type`, `type` → `event_type`, and a new column `region` extracted from `place`.
- NumPy: `np.select` for `depth_category` (shallow / intermediate / deep) and `mag_class`, `energy_log10_joules = 1.5·mag + 4.8`, magnitude z-score with `np.mean` / `np.std`.
- Filtering with multiple conditions (strong + shallow + reviewed; tsunami flag or orange/red alert).
- GroupBy aggregation by magnitude type and by region.
- Reshaping: `pivot_table` (depth category × magnitude class) and `melt`.
- Result: `data/clean_data.csv` (23 columns).

### 2.6 SQLite3 loading (`src/loader.py`)
Creates the table `earthquakes` with `id TEXT PRIMARY KEY`, inserts the rows with
`INSERT OR IGNORE`, loads the same data **twice** and compares row counts and checksums
(idempotency test), then runs 5 SQL verification queries.

### 2.7 EDA and visualization (`src/analysis.py`)
Reads the data from `output.db` and prints the dimensions, data types, descriptive statistics,
categorical counts, the correlation matrix, means by depth category, the top regions, daily
counts and the strongest event. It saves 5 figures to `figures/` (see section 10).

### 2.8 Orchestration (`main.py`)
Runs stages 2.1–2.7 in order as separate processes, stops at the first failure, and prints a
summary of the record counts with automatic consistency checks.

## 3. Software requirements

- **Python 3.11+** (the pinned pandas 3.x and NumPy 2.4 do not support older versions)
- **Docker Desktop** (or Docker Engine + Docker Compose) to run the Kafka broker
- Internet access to reach the USGS API
- Python packages: `requirements.txt` (main ones: `requests`, `kafka-python`, `pandas`, `numpy`,
  `pandera`, `matplotlib`, `seaborn`)

## 4. Installation

```bash
git clone https://github.com/adelmnv/DataCollection-SIS2.git
cd DataCollection-SIS2

python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

## 5. Kafka broker setup and startup

The broker runs from `docker-compose.yml` using the official `apache/kafka:4.4.0` image.
With its default configuration it starts a single-node broker in **KRaft mode** (no ZooKeeper)
that listens on `localhost:9092`.

```bash
docker compose up -d     # start the broker
docker ps                # the "kafka" container must have status "Up"
```

Wait about 10 seconds after the start before running the producer.

## 6. Kafka topic configuration

Topic: **`earthquakes`** (set in `src/config.py`, `KAFKA_TOPIC`).

The broker allows automatic topic creation (default `auto.create.topics.enable=true`), so the
topic is created by the producer on the first send with the broker defaults:
**1 partition, replication factor 1**. To create it explicitly beforehand (optional, same result):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic earthquakes --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Check the topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic earthquakes --bootstrap-server localhost:9092
```

## 7. API configuration

The USGS API is public and needs **no API key**, so the repository contains no credentials or
private configuration. The request parameters are in `src/config.py`:

```python
API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
API_PARAMS = {
    "format": "geojson",
    "starttime": "2026-10-01",
    "endtime": "2026-11-01",
    "minmagnitude": 4.5,
    "orderby": "time",
    "limit": 500,
}
```

The Kafka address defaults to `localhost:9092` and can be overridden with an environment variable:

```bash
export KAFKA_SERVER=localhost:9092      # Windows (PowerShell): $env:KAFKA_SERVER="localhost:9092"
```

## 8. Running the pipeline

All commands are run from the project root with the virtual environment activated and Kafka running.

### Option A: whole pipeline with one command

```bash
python main.py
```

`main.py` deletes the old `data/output.db`, resets `data/pipeline_stats.json`, runs the seven
scripts in order, stops if any of them fails, and prints the summary of record counts.
To save the console output: `python main.py | tee run_log.txt`.

### Option B: step by step (execution order)

```bash
python src/api_client.py     # 1. collect 500 records from USGS
python src/producer.py       # 2. publish 500 JSON messages to topic "earthquakes"
python src/consumer.py       # 3. consume messages from the topic
python src/validator.py      # 4. Pandera validation + test with 3 invalid records
python src/cleaner.py        # 5. cleaning and transformations
python src/loader.py         # 6. load into SQLite3 twice + verification queries
python src/analysis.py       # 7. EDA and figures
```

The producer and the consumer are separate scripts. The consumer can be started either after the
producer (it reads the topic from the beginning) or before it, in a second terminal.

**Consumer termination condition:** the consumer stops when no new message has arrived for
15 seconds (`CONSUMER_TIMEOUT_MS = 15000` in `src/config.py`). It then commits the offsets,
closes the connection and saves `data/consumed_data.csv`.

**Note on re-running the consumer:** the consumer group `earthquake_group` commits its offsets.
Running `consumer.py` a second time without new messages therefore receives 0 messages and stops
with `Error: no messages received`. To repeat stages 2–3, run the producer again first, or run
the whole pipeline with `python main.py`.

## 9. Database generation and verification

`src/loader.py` creates `data/output.db` with the table `earthquakes` (`id TEXT PRIMARY KEY`),
inserts the cleaned records with `INSERT OR IGNORE`, then **loads the same data a second time**
to show idempotency (it compares row counts and a checksum of `mag`, `depth_km` and `sig`).
After that it runs 5 SQL verification queries and prints their results.

Manual check (requires the `sqlite3` command-line tool or DB Browser for SQLite):

```bash
sqlite3 data/output.db "SELECT COUNT(*), COUNT(DISTINCT id) FROM earthquakes;"
```

Expected: `500|500`.

## 10. Analysis results and figures

`python src/analysis.py` reads the table from `data/output.db`, prints dimensions, data types,
descriptive statistics, categorical counts, a correlation matrix and grouped statistics, and saves:

| File | Content |
|---|---|
| `figures/fig1_magnitude_histogram.png` | Distribution of earthquake magnitudes |
| `figures/fig2_epicenters_map.png` | Epicenters by longitude/latitude, coloured by depth category |
| `figures/fig3_daily_count.png` | Number of earthquakes per day |
| `figures/fig4_magnitude_by_depth.png` | Magnitude by depth category (box plot) |
| `figures/fig5_depth_magnitude_heatmap.png` | Earthquakes by depth category and magnitude class |

## 11. Expected outputs

| Stage | Count |
|---|---|
| Records collected from the API | 500 |
| Messages published to Kafka | 500 (failed: 0) |
| Messages consumed from Kafka | 500 (bad messages: 0) |
| Valid / invalid records | 500 / 0 |
| Deliberately invalid test records detected | 3 of 3 |
| Duplicates removed | 0 |
| Records after cleaning | 500 |
| Rows in SQLite3 after the 1st load | 500 |
| Rows added by the 2nd load | 0 (total still 500) |

Files created in `data/`: `raw_response.json`, `raw_data.csv`, `consumed_data.csv`,
`valid_data.csv`, `invalid_data.csv`, `clean_data.csv`, `output.db`, `pipeline_stats.json`.

The reported results were produced with the parameters above. USGS keeps revising its catalog
(magnitudes and statuses are updated), so a later run may return slightly different values.
The original API response used for the report is kept in `data/raw_response.json`.

## 12. How to verify that the run succeeded

The final summary of `python main.py` should show:

```
api records:         500
sent to kafka:       500
read from kafka:     500
valid / invalid:     500 / 0
test records caught: 3 of 3
duplicates removed:  0
after cleaning:      500
in sqlite:           500
added on 2nd load:   0

all records went through kafka
db has the same number of rows as clean_data.csv
```

Additional checks:

1. The loader prints `Inserted: 0 | Skipped: 500 | Rows in table: 500` for the second load,
   the two checksums are equal, and it prints `No duplicated records detected during second execution`.
2. The messages are really stored in the broker:
   ```bash
   docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh \
     --bootstrap-server localhost:9092 --topic earthquakes
   ```
   After one run the result is `earthquakes:0:500` (it grows by 500 after each new producer run).
3. Five PNG files are present in `figures/`.

## 13. Stopping Kafka

```bash
docker compose down     # stops and removes the container together with the topic data
```

## 14. Repository structure

```
DataCollection-SIS2/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── main.py
├── run_log.txt              console output of a full run
├── src/
│   ├── config.py
│   ├── stats.py
│   ├── api_client.py
│   ├── producer.py
│   ├── consumer.py
│   ├── validator.py
│   ├── cleaner.py
│   ├── loader.py
│   └── analysis.py
├── data/                    raw, intermediate and clean data, output.db, pipeline_stats.json
├── figures/                 fig1–fig5
├── docs/
│   └── data_dictionary.md
└── report/
    └── project_report.pdf
```

## 15. Data source

USGS Earthquake Hazards Program, FDSN Event Web Service:
https://earthquake.usgs.gov/fdsnws/event/1/
Description of the database columns: `docs/data_dictionary.md`.
