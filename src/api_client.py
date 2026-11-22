import json
import sys
import requests
import pandas as pd
from config import API_URL, API_PARAMS, RAW_JSON_FILE, RAW_CSV_FILE
from stats import save_stat


def fetch_earthquakes():
    try:
        response = requests.get(API_URL, params=API_PARAMS, timeout=30)
    except requests.exceptions.Timeout:
        print("Error: request timed out")
        return None
    except requests.exceptions.ConnectionError:
        print("Error: no connection to the API")
        return None
    except requests.exceptions.RequestException as error:
        print("Request error:", error)
        return None

    if response.status_code != 200:
        print("Error: API returned status code", response.status_code)
        print(response.text[:300])
        return None

    try:
        data = response.json()
    except ValueError:
        print("Error: response is not valid JSON")
        return None

    return data


def run_api_client():
    print("Requesting data from USGS API...")
    data = fetch_earthquakes()
    if data is None:
        sys.exit(1)

    with open(RAW_JSON_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file)

    features = data.get("features", [])
    if len(features) == 0:
        print("Error: API returned no records")
        sys.exit(1)

    rows = []
    for feature in features:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        row = {
            "id": feature["id"],
            "time": props["time"],
            "mag": props["mag"],
            "magType": props["magType"],
            "place": props["place"],
            "depth_km": coords[2],
            "latitude": coords[1],
            "longitude": coords[0],
            "sig": props["sig"],
            "nst": props["nst"],
            "gap": props["gap"],
            "dmin": props["dmin"],
            "rms": props["rms"],
            "status": props["status"],
            "type": props["type"],
            "net": props["net"],
            "alert": props["alert"],
            "tsunami": props["tsunami"],
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], unit="ms")

    df.to_csv(RAW_CSV_FILE, index=False)
    print("Collected records:", len(df))
    print("Saved:", RAW_CSV_FILE)

    if len(df) < 200:
        print("Warning: fewer than 200 records, change dates or minmagnitude in config.py")

    save_stat("api_records", len(df))


if __name__ == "__main__":
    run_api_client()