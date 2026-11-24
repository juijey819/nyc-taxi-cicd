
import pytest

from src.ingest import (
    validate_trip_rows,
    transform_trips
)


def sample_trip():

    return {
        "vendorid": "2",
        "tpep_pickup_datetime": "2023-01-01T10:00:00",
        "tpep_dropoff_datetime": "2023-01-01T10:30:00",
        "passenger_count": "2",
        "trip_distance": "3.5",
        "pulocationid": "132",
        "dolocationid": "237",
        "fare_amount": "15.5"
    }


def test_valid_trip_data():

    rows = [sample_trip()]

    result = validate_trip_rows(rows)

    assert result == 1


def test_missing_required_column():

    rows = [sample_trip()]

    del rows[0]["fare_amount"]

    with pytest.raises(ValueError):

        validate_trip_rows(rows)


def test_empty_trip_data():

    assert validate_trip_rows([]) == 0


def test_trip_duration_calculation():

    rows = [sample_trip()]

    df = transform_trips(
        rows,
        "2026-11-25T00:00:00Z"
    )

    assert df.loc[0, "trip_duration_minutes"] == 30.0
