"""Tests pour les modules de transformation et validation."""
from __future__ import annotations

from datetime import date

from de_pipeline.ingestion.source import TLCSources, month_start_n_months_back


def test_sources_trip_url():
    """Test construction URL trips."""
    src = TLCSources(
        base_url="https://example.com",
        trips_path_template="trip-data/green_tripdata_{yyyy}-{mm}.parquet",
        zones_path="misc/taxi_zone_lookup.csv",
    )
    url = src.trip_url(2024, 1)
    assert url == "https://example.com/trip-data/green_tripdata_2024-01.parquet"


def test_sources_zones_url():
    """Test construction URL zones."""
    src = TLCSources(
        base_url="https://example.com",
        trips_path_template="trip-data/green_tripdata_{yyyy}-{mm}.parquet",
        zones_path="misc/taxi_zone_lookup.csv",
    )
    url = src.zones_url()
    assert url == "https://example.com/misc/taxi_zone_lookup.csv"


def test_month_back_calculation():
    """Test logique months_back."""
    months = month_start_n_months_back(date(2024, 6, 15), 3)
    # Doit retourner mai, avril, mars (pas juin car mois courant exclu)
    assert months == [(2024, 5), (2024, 4), (2024, 3)]


def test_month_back_year_boundary():
    """Test month_back at année boundary."""
    months = month_start_n_months_back(date(2024, 2, 1), 3)
    # Janvier, décembre 2023, novembre 2023
    assert months == [(2024, 1), (2023, 12), (2023, 11)]


def test_month_back_invalid():
    """Test month_back avec paramètre invalide."""
    try:
        month_start_n_months_back(date(2024, 1, 1), 0)
        assert False, "Should have raised ValueError"
    except ValueError:
        assert True
