"""Transformations : créer fact et dim tables dans BigQuery."""
from __future__ import annotations

import os

from google.cloud import bigquery

from de_pipeline.common.logging import get_logger

logger = get_logger(__name__)


def create_dim_datetime(client: bigquery.Client, project_id: str, dataset_id: str) -> None:
    """Créer table de dimension date/heure."""
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_datetime` AS
    SELECT
        TIMESTAMP(DATE(TIMESTAMP_ADD(TIMESTAMP('2020-01-01'), INTERVAL pos MINUTE))) as datetime_key,
        DATE(TIMESTAMP_ADD(TIMESTAMP('2020-01-01'), INTERVAL pos MINUTE)) as date,
        EXTRACT(HOUR FROM TIMESTAMP_ADD(TIMESTAMP('2020-01-01'), INTERVAL pos MINUTE)) as hour,
        EXTRACT(MINUTE FROM TIMESTAMP_ADD(TIMESTAMP('2020-01-01'), INTERVAL pos MINUTE)) as minute,
        FORMAT_TIMESTAMP('%A', TIMESTAMP_ADD(TIMESTAMP('2020-01-01'), INTERVAL pos MINUTE)) as day_name,
    FROM UNNEST(GENERATE_ARRAY(0, 525600)) AS pos
    """
    client.query(query).result()
    logger.info("✅ Table dim_datetime créée")


def create_fact_green_tripdata(
    client: bigquery.Client,
    project_id: str,
    dataset_raw: str,
    dataset_curated: str,
) -> None:
    """Créer fact table trips avec nettoyage."""
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_curated}.fact_green_tripdata` AS
    SELECT
        GENERATE_UUID() as trip_id,
        VendorID,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        store_and_fwd_flag,
        RatecodeID,
        PULocationID,
        DOLocationID,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        CURRENT_TIMESTAMP() as load_timestamp,
    FROM `{project_id}.{dataset_raw}.green_tripdata_raw`
    WHERE
        lpep_pickup_datetime IS NOT NULL
        AND lpep_dropoff_datetime IS NOT NULL
        AND trip_distance > 0
        AND fare_amount > 0
    """
    client.query(query).result()
    logger.info("✅ Table fact_green_tripdata créée")


def create_dim_location(
    client: bigquery.Client,
    project_id: str,
    dataset_raw: str,
    dataset_curated: str,
) -> None:
    """Créer dimension location."""
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_curated}.dim_location` AS
    SELECT
        LocationID,
        Borough,
        Zone,
        service_zone,
    FROM `{project_id}.{dataset_raw}.taxi_zone_lookup`
    """
    client.query(query).result()
    logger.info("✅ Table dim_location créée")


def main() -> None:
    """Exécuter toutes les transformations."""
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW")
    dataset_curated = os.getenv("BQ_DATASET_CURATED")

    client = bigquery.Client(project=project_id)

    # Créer dataset curated s'il n'existe pas
    try:
        client.get_dataset(f"{project_id}.{dataset_curated}")
        logger.info(f"✅ Dataset {dataset_curated} existe")
    except Exception:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_curated}")
        dataset.location = "EU"
        client.create_dataset(dataset)
        logger.info(f"✅ Dataset {dataset_curated} créé")

    logger.info("🔄 Création des tables transformées...")
    create_dim_datetime(client, project_id, dataset_curated)
    create_fact_green_tripdata(client, project_id, dataset_raw, dataset_curated)
    create_dim_location(client, project_id, dataset_raw, dataset_curated)

    logger.info("✅ Transformations complétées!")


if __name__ == "__main__":
    main()
