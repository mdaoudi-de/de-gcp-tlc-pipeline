"""Charger données Parquet depuis GCS vers BigQuery."""
from __future__ import annotations

import os

from google.cloud import bigquery

from de_pipeline.common.logging import get_logger

logger = get_logger(__name__)


def create_dataset_if_not_exists(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
) -> None:
    """Créer un dataset s'il n'existe pas."""
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"✅ Dataset {dataset_id} existe déjà")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset)
        logger.info(f"✅ Dataset {dataset_id} créé")


def load_parquet_to_bq(
    gcs_path: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> None:
    """Charger un fichier Parquet de GCS vers BigQuery."""
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    load_job = client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)
    load_job.result()

    logger.info(f"✅ Loaded {gcs_path} -> {table_ref}")


def load_csv_to_bq(
    gcs_path: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> None:
    """Charger un fichier CSV de GCS vers BigQuery."""
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Remplacer zones
    )

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    load_job = client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)
    load_job.result()

    logger.info(f"✅ Loaded {gcs_path} -> {table_ref}")


if __name__ == "__main__":
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW")
    bucket_name = os.getenv("GCS_RAW_BUCKET")

    client = bigquery.Client(project=project_id)
    create_dataset_if_not_exists(client, project_id, dataset_raw)

    # Charger les données
    logger.info("Loading Parquet files to BigQuery...")
    load_parquet_to_bq(
        f"gs://{bucket_name}/raw/green_taxi/ingestion_date=*/green_tripdata_*.parquet",
        project_id,
        dataset_raw,
        "green_tripdata_raw",
    )

    logger.info("Loading CSV (zones) to BigQuery...")
    load_csv_to_bq(
        f"gs://{bucket_name}/raw/green_taxi/ingestion_date=*/taxi_zone_lookup.csv",
        project_id,
        dataset_raw,
        "taxi_zone_lookup",
    )

    logger.info("✅ All data loaded to BigQuery!")
