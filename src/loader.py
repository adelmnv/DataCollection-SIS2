import sqlite3
import sys
import pandas as pd
from config import CLEAN_FILE, DB_FILE
from stats import save_stat

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS earthquakes (
    id                  TEXT PRIMARY KEY,
    time                TEXT NOT NULL,
    mag                 REAL NOT NULL,
    mag_type            TEXT,
    place               TEXT,
    region              TEXT,
    depth_km            REAL NOT NULL,
    latitude            REAL NOT NULL,
    longitude           REAL NOT NULL,
    sig                 INTEGER,
    nst                 REAL,
    gap                 REAL,
    dmin                REAL,
    rms                 REAL,
    status              TEXT,
    event_type          TEXT,
    net                 TEXT,
    alert               TEXT,
    tsunami             INTEGER,
    depth_category      TEXT,
    mag_class           TEXT,
    energy_log10_joules REAL,
    mag_zscore          REAL
)
"""

INSERT_SQL = """
INSERT OR IGNORE INTO earthquakes (
    id, time, mag, mag_type, place, region, depth_km, latitude, longitude,
    sig, nst, gap, dmin, rms, status, event_type, net, alert, tsunami,
    depth_category, mag_class, energy_log10_joules, mag_zscore
) VALUES (
    :id, :time, :mag, :mag_type, :place, :region, :depth_km, :latitude, :longitude,
    :sig, :nst, :gap, :dmin, :rms, :status, :event_type, :net, :alert, :tsunami,
    :depth_category, :mag_class, :energy_log10_joules, :mag_zscore
)
"""


def load_data(df):
    df = df.astype(object).where(df.notnull(), None)
    records = df.to_dict("records")

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    cursor.execute(CREATE_TABLE_SQL)

    inserted = 0
    skipped = 0
    for record in records:
        cursor.execute(INSERT_SQL, record)
        if cursor.rowcount == 1:
            inserted += 1
        else:
            skipped += 1
    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM earthquakes")
    total = cursor.fetchone()[0]
    connection.close()
    return inserted, skipped, total


def get_checksum():
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()
    cursor.execute("SELECT ROUND(SUM(mag), 4), ROUND(SUM(depth_km), 4), ROUND(SUM(sig), 4) FROM earthquakes")
    result = cursor.fetchone()
    connection.close()
    return result


def run_query(title, sql):
    connection = sqlite3.connect(DB_FILE)
    print("\n" + title)
    print(sql.strip())
    print(pd.read_sql_query(sql, connection))
    connection.close()


def run_loader():
    df = pd.read_csv(CLEAN_FILE)
    processed = len(df)
    print("Records to load:", processed)

    try:
        print("\n--- Load run 1 ---")
        inserted1, skipped1, total1 = load_data(df)
        checksum1 = get_checksum()
        print("Inserted:", inserted1, "| Skipped:", skipped1, "| Rows in table:", total1)

        print("\n--- Load run 2 (same dataset) ---")
        inserted2, skipped2, total2 = load_data(df)
        checksum2 = get_checksum()
        print("Inserted:", inserted2, "| Skipped:", skipped2, "| Rows in table:", total2)
    except sqlite3.Error as error:
        print("Database error:", error)
        sys.exit(1)

    print("\nChecksum after run 1:", checksum1)
    print("Checksum after run 2:", checksum2)
    if total1 == total2 and checksum1 == checksum2 and inserted2 == 0:
        print("IDEMPOTENCY OK: second run added no rows and changed no values")
    else:
        print("IDEMPOTENCY FAILED")

    print("\nProcessed records:", processed, "| Stored records:", total2)

    run_query("Query 1: number of rows", "SELECT COUNT(*) AS total_rows FROM earthquakes")
    run_query("Query 2: unique ids", "SELECT COUNT(DISTINCT id) AS unique_ids FROM earthquakes")
    run_query("Query 3: top 5 strongest earthquakes",
              "SELECT id, time, mag, place FROM earthquakes ORDER BY mag DESC LIMIT 5")
    run_query("Query 4: count and average magnitude by magnitude type",
              "SELECT mag_type, COUNT(*) AS events, ROUND(AVG(mag), 2) AS avg_mag "
              "FROM earthquakes GROUP BY mag_type ORDER BY events DESC")
    run_query("Query 5: events by depth category",
              "SELECT depth_category, COUNT(*) AS events, ROUND(AVG(depth_km), 1) AS avg_depth "
              "FROM earthquakes GROUP BY depth_category")

    save_stat("records_stored_db", total2)
    save_stat("second_load_inserted", inserted2)


if __name__ == "__main__":
    run_loader()