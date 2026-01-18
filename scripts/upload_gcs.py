"""Upload données locales vers Google Cloud Storage (GCS)."""
from __future__ import annotations

import os
from pathlib import Path

from google.cloud import storage

from de_pipeline.common.logging import get_logger

logger = get_logger(__name__)


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    gcs_prefix: str,
) -> None:
    """Upload un fichier ou dossier vers GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    if local_path.is_file():
        # Upload un seul fichier
        blob_path = f"{gcs_prefix}/{local_path.name}"
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(local_path))
        logger.info(f"✅ Uploaded: {local_path.name} -> gs://{bucket_name}/{blob_path}")

    elif local_path.is_dir():
        # Upload dossier récursivement
        for file in local_path.rglob("*"):
            if file.is_file():
                relative_path = file.relative_to(local_path.parent)
                blob_path = f"{gcs_prefix}/{relative_path}".replace("\\", "/")
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(file))
                logger.info(f"✅ Uploaded: {file.name} -> gs://{bucket_name}/{blob_path}")


if __name__ == "__main__":
    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = os.getenv("GCS_RAW_BUCKET")
    local_raw = Path("data/raw/raw/green_taxi")

    logger.info(f"Starting upload to gs://{bucket_name}")
    upload_to_gcs(local_raw, bucket_name, "raw/green_taxi")
    logger.info("Upload completed!")
