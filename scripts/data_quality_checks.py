"""Data Quality checks sur les données chargées."""
from __future__ import annotations

import os

from google.cloud import bigquery

from de_pipeline.common.logging import get_logger

logger = get_logger(__name__)


def run_dq_checks(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> dict[str, bool]:
    """Exécuter des checks de qualité de données."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    results = {}

    # Check 1: Table existe et a des lignes
    count_query = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
    result = client.query(count_query).result().to_pandas()
    count = result.iloc[0]["cnt"]
    results["has_data"] = count > 0
    logger.info(f"  ✅ Lignes: {count}")

    # Check 2: Pas de NULL dans colonnes clés
    if "green_tripdata" in table_id:
        null_check = f"""
        SELECT
            COUNTIF(lpep_pickup_datetime IS NULL) as null_pickup,
            COUNTIF(lpep_dropoff_datetime IS NULL) as null_dropoff,
            COUNTIF(fare_amount IS NULL) as null_fare,
        FROM `{table_ref}`
        """
        result = client.query(null_check).result().to_pandas()
        nulls = result.iloc[0].sum()
        results["no_null_keys"] = nulls == 0
        if nulls > 0:
            logger.warning(f"  ⚠️  NULLs détectés dans colonnes clés: {nulls}")
        else:
            logger.info("  ✅ Pas de NULLs dans colonnes clés")

    # Check 3: Pas de doublons
    dup_check = f"""
    SELECT COUNT(*) - COUNT(DISTINCT *) as duplicates
    FROM `{table_ref}`
    """
    result = client.query(dup_check).result().to_pandas()
    dups = result.iloc[0]["duplicates"]
    results["no_duplicates"] = dups == 0
    logger.info(f"  ✅ Doublons: {dups}")

    return results


def main() -> None:
    """Exécuter tous les checks DQ."""
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW")
    dataset_curated = os.getenv("BQ_DATASET_CURATED")

    client = bigquery.Client(project=project_id)

    logger.info("🔍 Exécution des Data Quality checks...")

    tables_to_check = [
        (dataset_raw, "green_tripdata_raw"),
        (dataset_raw, "taxi_zone_lookup"),
        (dataset_curated, "fact_green_tripdata"),
        (dataset_curated, "dim_location"),
    ]

    all_pass = True
    for dataset, table in tables_to_check:
        logger.info(f"\n📊 Checks sur {dataset}.{table}:")
        try:
            results = run_dq_checks(client, project_id, dataset, table)
            if all(results.values()):
                logger.info("  ✅ Tous les checks passés")
            else:
                logger.warning(f"  ⚠️  Certains checks échoués: {results}")
                all_pass = False
        except Exception as e:
            logger.error(f"  ❌ Erreur: {e}")
            all_pass = False

    if all_pass:
        logger.info("\n✅ TOUS LES CHECKS DE QUALITE PASSES!")
    else:
        logger.warning("\n⚠️  CERTAINS CHECKS DE QUALITE ECHOUES!")


if __name__ == "__main__":
    main()
