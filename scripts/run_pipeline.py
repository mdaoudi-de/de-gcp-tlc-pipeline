"""Orchestrateur principal de la pipeline TLC end-to-end."""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime

from de_pipeline.common.logging import get_logger

logger = get_logger(__name__)


def run_command(cmd: list[str], description: str) -> bool:
    """Exécuter une commande et retourner True si succès."""
    logger.info(f"▶️  {description}...")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,
            text=True,
        )
        logger.info(f"✅ {description} réussi")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} échoué: {e}")
        return False


def main() -> None:
    """Pipeline complète : Téléchargement -> Upload GCS -> BigQuery."""
    start_time = datetime.now(UTC)
    logger.info("=" * 80)
    logger.info("🚀 DEMARRAGE PIPELINE TLC END-TO-END")
    logger.info(f"📅 {start_time.isoformat()}")
    logger.info("=" * 80)

    # Vérifier GCP config
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        logger.error("❌ GCP_PROJECT_ID non configuré dans .env")
        sys.exit(1)
    logger.info(f"✅ Project GCP: {project_id}")

    steps = [
        (
            ["python", "scripts/download_raw_local.py"],
            "1️⃣  Téléchargement données locales",
        ),
        (
            ["python", "scripts/upload_gcs.py"],
            "2️⃣  Upload vers Google Cloud Storage",
        ),
        (
            ["python", "scripts/load_to_bigquery.py"],
            "3️⃣  Chargement dans BigQuery",
        ),
    ]

    failed_steps = []
    for cmd, description in steps:
        if not run_command(cmd, description):
            failed_steps.append(description)
            if description.startswith("1️⃣"):  # Continuer même si upload fail
                continue
            break

    # Résumé
    logger.info("=" * 80)
    if not failed_steps:
        logger.info("✅ PIPELINE COMPLETEE AVEC SUCCES!")
        duration = (datetime.now(UTC) - start_time).total_seconds()
        logger.info(f"⏱️  Durée: {duration:.2f}s")
    else:
        logger.warning(f"⚠️  PIPELINE AVEC ERREURS: {len(failed_steps)} étape(s) échouée(s)")
        for step in failed_steps:
            logger.warning(f"   - {step}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
