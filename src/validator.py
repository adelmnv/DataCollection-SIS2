import sys
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors
from config import (CONSUMED_FILE, VALID_FILE, INVALID_FILE,
                    ERRORS_FILE)
from stats import save_stat

MAG_TYPES = ["mb", "mb_lg", "md", "mh", "ml", "mlg", "ms", "ms_20", "mw", "mwb",
             "mwc", "mwr", "mww", "mwp", "mi", "mfa", "me", "mun"]
EVENT_TYPES = ["earthquake", "quarry blast", "explosion", "ice quake", "other event"]
ALERT_TYPES = ["green", "yellow", "orange", "red"]

schema = pa.DataFrameSchema(
    {
        "id": pa.Column(str, nullable=False, unique=True),
        "time": pa.Column(pa.DateTime, nullable=False, coerce=True, checks=pa.Check.in_range(pd.Timestamp("2000-01-01"), pd.Timestamp.now())),
        "mag": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-2, 10)),
        "magType": pa.Column(str, nullable=True, checks=pa.Check.isin(MAG_TYPES)),
        "place": pa.Column(str, nullable=True),
        "depth_km": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-10, 800)),
        "latitude": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-90, 90)),
        "longitude": pa.Column(float, nullable=False, coerce=True, checks=pa.Check.in_range(-180, 180)),
        "sig": pa.Column(int, nullable=False, coerce=True, checks=pa.Check.ge(0)),
        "nst": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.ge(0)),
        "gap": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.in_range(0, 360)),
        "dmin": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.ge(0)),
        "rms": pa.Column(float, nullable=True, coerce=True, checks=pa.Check.ge(0)),
        "status": pa.Column(str, nullable=False, checks=pa.Check.isin(["automatic", "reviewed"])),
        "type": pa.Column(str, nullable=False, checks=pa.Check.isin(EVENT_TYPES)),
        "net": pa.Column(str, nullable=False),
        "alert": pa.Column(nullable=True, checks=pa.Check.isin(ALERT_TYPES)),
        "tsunami": pa.Column(int, nullable=False, coerce=True, checks=pa.Check.isin([0, 1])),
    }
)


def validate_dataframe(df):
    try:
        schema.validate(df, lazy=True)
        errors = pd.DataFrame()
        bad_index = []
    except SchemaErrors as error:
        errors = error.failure_cases
        bad_index = errors["index"].dropna().astype(int).unique()
    return errors, bad_index


def run_validation_test():
    base = {
        "id": "TEST_ok", "time": "2026-10-15 10:00:00", "mag": 5.0, "magType": "mww",
        "place": "Test place", "depth_km": 10.0, "latitude": 40.0, "longitude": 70.0,
        "sig": 400, "nst": 20.0, "gap": 50.0, "dmin": 1.0, "rms": 0.5,
        "status": "reviewed", "type": "earthquake", "net": "us", "alert": None, "tsunami": 0,
    }
    test1 = base.copy()
    test1["id"] = "TEST_1_bad_magnitude"
    test1["mag"] = 15.0

    test2 = base.copy()
    test2["id"] = "TEST_2_bad_latitude"
    test2["latitude"] = 120.0

    test3 = base.copy()
    test3["id"] = "TEST_3_bad_category"
    test3["magType"] = "wrongtype"
    test3["status"] = "unknown"

    test_df = pd.DataFrame([test1, test2, test3])
    errors, bad_index = validate_dataframe(test_df)

    print("\n--- Validation test with 3 invalid test records ---")
    if len(errors) > 0:
        errors["test_record_id"] = test_df.loc[errors["index"].astype(int), "id"].values
        print(errors[["test_record_id", "column", "check", "failure_case"]])
    print("Test records checked:", len(test_df))
    print("Test records detected as invalid:", len(bad_index))

    save_stat("test_records_total", len(test_df))
    save_stat("test_records_detected", len(bad_index))


def run_validator():
    df = pd.read_csv(CONSUMED_FILE)
    total = len(df)
    print("Records received from Kafka:", total)

    errors, bad_index = validate_dataframe(df)

    invalid_df = df.loc[bad_index]
    valid_df = df.drop(index=bad_index)

    valid_df.to_csv(VALID_FILE, index=False)
    invalid_df.to_csv(INVALID_FILE, index=False)

    print("Valid records:", len(valid_df))
    print("Invalid records:", len(invalid_df))

    if len(errors) > 0:
        errors.to_csv(ERRORS_FILE, index=False)
        print("\nValidation errors by column and rule:")
        print(errors.groupby(["column", "check"]).size())
        print("\nFirst errors:")
        print(errors[["index", "column", "check", "failure_case"]].head(10))
    else:
        print("No validation errors in the real data")

    save_stat("records_valid", len(valid_df))
    save_stat("records_invalid", len(invalid_df))

    run_validation_test()

    if len(valid_df) == 0:
        print("Error: no valid records")
        sys.exit(1)


if __name__ == "__main__":
    run_validator()