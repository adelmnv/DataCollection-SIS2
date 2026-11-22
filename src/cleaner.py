import numpy as np
import pandas as pd
from config import (VALID_FILE, CLEAN_FILE, STRONG_FILE, GROUPBY_TYPE_FILE,
                    GROUPBY_REGION_FILE, PIVOT_FILE)
from stats import save_stat


def run_cleaner():
    df = pd.read_csv(VALID_FILE)
    print("Records after validation:", len(df))

    print("\nMissing values before cleaning:")
    missing = df.isnull().sum()
    print(missing[missing > 0])

    df["nst"] = df["nst"].fillna(df["nst"].median())
    df["gap"] = df["gap"].fillna(df["gap"].median())
    df["dmin"] = df["dmin"].fillna(df["dmin"].median())
    df["rms"] = df["rms"].fillna(df["rms"].median())
    df["alert"] = df["alert"].fillna("none")
    df["place"] = df["place"].fillna("unknown")
    df["magType"] = df["magType"].fillna("unknown")
    print("Missing values after cleaning:", int(df.isnull().sum().sum()))

    before = len(df)
    df = df.drop_duplicates(subset="id")
    duplicates_removed = before - len(df)
    print("Duplicates removed:", duplicates_removed)

    df["time"] = pd.to_datetime(df["time"])
    df["mag"] = df["mag"].astype(float)
    df["depth_km"] = df["depth_km"].astype(float)
    df["sig"] = df["sig"].astype(int)
    df["tsunami"] = df["tsunami"].astype(int)

    df["place"] = df["place"].str.strip()
    df["mag_type"] = df["magType"].str.strip().str.lower()
    df["status"] = df["status"].str.strip().str.lower()
    df["event_type"] = df["type"].str.strip().str.lower()
    df["net"] = df["net"].str.strip().str.lower()
    df["alert"] = df["alert"].str.strip().str.lower()
    df["region"] = df["place"].str.split(",").str[-1].str.strip().str.title()

    depth_values = df["depth_km"].to_numpy()
    mag_values = df["mag"].to_numpy()

    df["depth_category"] = np.select(
        [depth_values < 70, depth_values < 300],
        ["shallow", "intermediate"],
        default="deep",
    )
    df["mag_class"] = np.select(
        [mag_values < 5, mag_values < 6, mag_values < 7],
        ["M<5", "M5-5.9", "M6-6.9"],
        default="M7+",
    )
    df["energy_log10_joules"] = np.round(1.5 * mag_values + 4.8, 2)
    mean_mag = np.mean(mag_values)
    std_mag = np.std(mag_values)
    df["mag_zscore"] = np.round((mag_values - mean_mag) / std_mag, 3)
    print("\nMean magnitude (NumPy):", round(mean_mag, 3))
    print("Std of magnitude (NumPy):", round(std_mag, 3))

    strong_shallow = df[(df["mag"] >= 5.5) & (df["depth_category"] == "shallow") & (df["status"] == "reviewed")]
    dangerous = df[(df["tsunami"] == 1) | (df["alert"].isin(["orange", "red"]))]
    print("\nStrong, shallow and reviewed earthquakes:", len(strong_shallow))
    print("Earthquakes with tsunami flag or orange/red alert:", len(dangerous))
    strong_shallow.to_csv(STRONG_FILE, index=False)

    by_type = df.groupby("mag_type").agg(
        events=("id", "count"),
        mean_mag=("mag", "mean"),
        max_mag=("mag", "max"),
        mean_depth=("depth_km", "mean"),
    ).round(2).reset_index()
    by_region = df.groupby("region").agg(
        events=("id", "count"),
        mean_mag=("mag", "mean"),
        max_mag=("mag", "max"),
    ).round(2).sort_values("events", ascending=False).head(10).reset_index()
    print("\nGroupBy magnitude type:")
    print(by_type)
    print("\nGroupBy region (top 10):")
    print(by_region)
    by_type.to_csv(GROUPBY_TYPE_FILE, index=False)
    by_region.to_csv(GROUPBY_REGION_FILE, index=False)

    pivot = pd.pivot_table(df, index="depth_category", columns="mag_class",
                           values="id", aggfunc="count", fill_value=0)
    print("\nPivot table (depth category x magnitude class):")
    print(pivot)
    pivot.to_csv(PIVOT_FILE)
    melted = pivot.reset_index().melt(id_vars="depth_category", var_name="mag_class", value_name="events")
    print("\nMelt result (first rows):")
    print(melted.head())

    final_columns = ["id", "time", "mag", "mag_type", "place", "region", "depth_km",
                     "latitude", "longitude", "sig", "nst", "gap", "dmin", "rms",
                     "status", "event_type", "net", "alert", "tsunami",
                     "depth_category", "mag_class", "energy_log10_joules", "mag_zscore"]
    df = df[final_columns]
    df.to_csv(CLEAN_FILE, index=False)
    print("\nRecords after cleaning:", len(df))
    print("Saved:", CLEAN_FILE)

    save_stat("duplicates_removed", duplicates_removed)
    save_stat("records_after_cleaning", len(df))


if __name__ == "__main__":
    run_cleaner()