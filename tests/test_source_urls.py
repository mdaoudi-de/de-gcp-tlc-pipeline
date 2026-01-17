from datetime import date

import pytest

from de_pipeline.ingestion.source import TLCSources, month_start_n_months_back


def test_trip_url_format():
    src = TLCSources(
        base_url="https://example.com",
        trips_path_template="trip-data/green_tripdata_{yyyy}-{mm}.parquet",
        zones_path="misc/taxi_zone_lookup.csv",
    )
    assert (
        src.trip_url(2024, 1)
        == "https://example.com/trip-data/green_tripdata_2024-01.parquet"
    )


def test_months_back():
    months = month_start_n_months_back(date(2024, 1, 15), 3)
    assert months == [(2023, 12), (2023, 11), (2023, 10)]


def test_months_back_invalid():
    with pytest.raises(ValueError):
        month_start_n_months_back(date(2024, 1, 1), 0)
