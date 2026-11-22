import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import os
from config import DB_FILE, FIGURES_DIR

DEPTH_ORDER = ["shallow", "intermediate", "deep"]
MAG_ORDER = ["M<5", "M5-5.9", "M6-6.9", "M7+"]


def run_analysis():
    connection = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM earthquakes", connection)
    connection.close()

    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date

    print("Shape (rows, columns):", df.shape)
    print("\nData types:")
    print(df.dtypes)

    numeric_columns = ["mag", "depth_km", "sig", "nst", "gap", "dmin", "rms", "energy_log10_joules"]
    print("\nDescriptive statistics:")
    print(df[numeric_columns].describe().round(2))

    print("\nCategorical statistics:")
    print(df["mag_type"].value_counts())
    print(df["status"].value_counts())
    print(df["depth_category"].value_counts())
    print(df["mag_class"].value_counts())
    print(df["alert"].value_counts())

    print("\nCorrelation matrix:")
    print(df[["mag", "depth_km", "sig", "nst", "gap", "rms"]].corr().round(2))

    print("\nMean values by depth category:")
    print(df.groupby("depth_category")[["mag", "sig"]].mean().round(2))

    print("\nTop 10 regions by number of earthquakes:")
    print(df["region"].value_counts().head(10))

    daily = df.groupby("date").size()
    print("\nDay with most earthquakes:", daily.idxmax(), "with", daily.max(), "events")
    print("Average earthquakes per day:", round(daily.mean(), 1))

    strongest = df.sort_values("mag", ascending=False).iloc[0]
    print("\nStrongest earthquake: M", strongest["mag"], "|", strongest["place"], "|", strongest["time"])

    plt.figure(figsize=(8, 5))
    sns.histplot(df["mag"], bins=20, kde=True)
    plt.title("Distribution of Earthquake Magnitudes")
    plt.xlabel("Magnitude")
    plt.ylabel("Number of earthquakes")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "fig1_magnitude_histogram.png"), dpi=150)
    plt.close()

    plt.figure(figsize=(11, 6))
    sns.scatterplot(data=df, x="longitude", y="latitude", hue="depth_category",
                    hue_order=DEPTH_ORDER, size="mag", sizes=(10, 150), alpha=0.7)
    plt.title("Earthquake Epicenters by Depth Category")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "fig2_epicenters_map.png"), dpi=150)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(daily.index, daily.values, marker="o", label="Earthquakes per day")
    plt.title("Number of Earthquakes per Day")
    plt.xlabel("Date")
    plt.ylabel("Number of earthquakes")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "fig3_daily_count.png"), dpi=150)
    plt.close()

    plt.figure(figsize=(8, 5))
    sns.boxplot(data=df, x="depth_category", y="mag", order=DEPTH_ORDER)
    plt.title("Magnitude by Depth Category")
    plt.xlabel("Depth category")
    plt.ylabel("Magnitude")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "fig4_magnitude_by_depth.png"), dpi=150)
    plt.close()

    pivot = pd.pivot_table(df, index="depth_category", columns="mag_class",
                           values="id", aggfunc="count", fill_value=0)
    pivot = pivot.reindex(index=DEPTH_ORDER, columns=MAG_ORDER, fill_value=0)
    plt.figure(figsize=(8, 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="YlOrRd")
    plt.title("Earthquakes by Depth Category and Magnitude Class")
    plt.xlabel("Magnitude class")
    plt.ylabel("Depth category")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "fig5_depth_magnitude_heatmap.png"), dpi=150)
    plt.close()

    print("\nFigures saved to:", FIGURES_DIR)


if __name__ == "__main__":
    run_analysis()